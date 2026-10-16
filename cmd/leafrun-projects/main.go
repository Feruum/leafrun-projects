package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/leafrun/leafrun-platform/authn"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	auditv1 "github.com/leafrun/leafrun-contracts/audit/v1"
	proof "github.com/leafrun/leafrun-contracts/ownerproof/v1"
	"github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/events"
	"github.com/leafrun/leafrun-projects/internal/config"
	"github.com/leafrun/leafrun-projects/internal/eventrelay"
	"github.com/leafrun/leafrun-projects/internal/service"
	"github.com/leafrun/leafrun-projects/internal/store"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	if err := run(); err != nil {
		// A driver or remote response error can include a connection string or
		// other configured secret, so log only its type at the process boundary.
		slog.Error("Projects process stopped", "error_type", fmt.Sprintf("%T", err))
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open Projects database: %w", err)
	}
	defer pool.Close()
	if err = pool.Ping(ctx); err != nil {
		return fmt.Errorf("Projects database is unavailable: %w", err)
	}
	state, err := store.New(pool, store.Config{PinHolderID: service.ProjectsHolderID, PinHolderDomain: service.ProjectsHolderDomain})
	if err != nil {
		return err
	}
	if cfg.Role == config.RoleMigrate {
		return state.Migrate(ctx)
	}
	if err = state.Ready(ctx); err != nil {
		return err
	}
	if cfg.Role == config.RoleEventRelay {
		return serveEventRelay(ctx, cfg, pool)
	}
	runtime, err := newRuntime(state, cfg)
	if err != nil {
		return err
	}
	switch cfg.Role {
	case config.RoleAPI:
		return serve(ctx, cfg.HTTPAddr, runtime.Handler())
	case config.RoleReconcile:
		return serveReconciler(ctx, cfg, runtime)
	default:
		return errors.New("unsupported Projects process role")
	}
}

func serveEventRelay(ctx context.Context, cfg config.Config, pool *pgxpool.Pool) error {
	connection, err := nats.Connect(cfg.NATSAddress,
		nats.UserInfo(cfg.NATSUser, cfg.NATSPassword),
		nats.CustomInboxPrefix("_INBOX.projects"),
		nats.Name("projects"),
		nats.Timeout(5*time.Second),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		return errors.New("connect Projects event relay to NATS")
	}
	defer connection.Close()

	jetStream, err := jetstream.New(connection)
	if err != nil {
		return errors.New("configure Projects JetStream publisher")
	}
	publisher, err := events.NewOwnerAuditPublisher(events.JetStreamPublisher{Client: jetStream}, auditv1.ProducerProjects, []string{"leafrun.projects.release.candidate.v1", "leafrun.projects.release.ready.v1", "leafrun.projects.release.published.v1", "leafrun.projects.release.withdrawn.v1", "leafrun.projects.release.deleted.v1", "leafrun.projects.release.failed.v1"})
	if err != nil {
		return err
	}
	return eventrelay.Run(ctx, pool, publisher)
}

func newRuntime(state *store.Store, cfg config.Config) (*service.Runtime, error) {
	transport := &http.Client{Timeout: 30 * time.Second}
	storageClient, err := storagev1.NewClient(cfg.StorageURL, cfg.StorageToken, transport)
	if err != nil {
		return nil, fmt.Errorf("configure Projects Storage client: %w", err)
	}
	access, err := service.NewOwnerAccess(cfg.OwnerURL, cfg.OwnerToken, transport)
	if err != nil {
		return nil, fmt.Errorf("configure Projects Access owner: %w", err)
	}
	var authenticator authn.Authenticator
	var guard *authn.BrowserGuard
	if cfg.AuthMode == "access" {
		public, createErr := authn.NewClientWithCookieNames(cfg.OwnerURL, cfg.OwnerToken, authn.ConfiguredCookieNames(cfg.CookieName, cfg.GuestCookieName), transport)
		if createErr != nil {
			return nil, createErr
		}
		access.UseCurrent(public)
		authenticator = public
		guard, err = authn.NewBrowserGuard(cfg.BrowserOrigins, cfg.LoopbackHTTP)
		if err != nil {
			return nil, err
		}
	}
	var importVerifier proof.SnapshotVerifier
	if cfg.ImportVerifierToken != "" {
		client, err := proof.NewClient(cfg.StorageURL, cfg.ImportStorageToken, transport)
		if err != nil {
			return nil, err
		}
		importVerifier = client
	}
	return service.NewRuntime(state, storageClient, access, service.Config{
		ImportVerifierToken: cfg.ImportVerifierToken, ImportStorageVerifier: importVerifier,
		AuthMode: cfg.AuthMode, DevFixtures: cfg.DevFixtures, Authenticator: authenticator, CookieName: cfg.CookieName, GuestCookieName: cfg.GuestCookieName, BrowserGuard: guard, InternalTokens: []string{cfg.OwnerToken, cfg.StorageToken, cfg.ImportStorageToken},
		Credentials: cfg.Credentials, OwnerCallbackToken: cfg.OwnerCallbackToken, GenerationToken: cfg.GenerationToken,
		TempDir: cfg.TempDir, StorageHolderID: service.ProjectsHolderID,
		StorageHolderDomain: service.ProjectsHolderDomain,
	})
}

func serve(ctx context.Context, address string, handler http.Handler) error {
	server := &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	return serveUntilStopped(ctx, server)
}

func serveReconciler(ctx context.Context, cfg config.Config, runtime *service.Runtime) error {
	interval, err := time.ParseDuration(cfg.ReconcileIntervalText)
	if err != nil || interval < time.Second || interval > time.Minute {
		return errors.New("PROJECTS_RECONCILE_INTERVAL must be between 1 second and 1 minute")
	}
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: runtime.HealthHandler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()
	reconciler, err := service.NewReconciler(runtime)
	if err != nil {
		_ = server.Close()
		return err
	}
	workerErr := make(chan error, 1)
	go func() { workerErr <- reconciler.Run(ctx, interval) }()
	select {
	case err = <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case err = <-workerErr:
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
}

func serveUntilStopped(ctx context.Context, server *http.Server) error {
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			return err
		}
		return nil
	}
}

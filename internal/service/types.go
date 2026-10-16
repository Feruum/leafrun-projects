package service

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	proof "github.com/leafrun/leafrun-contracts/ownerproof/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
	"github.com/leafrun/leafrun-projects/internal/store"
)

const (
	ProjectsHolderID     = "leafrun-projects"
	ProjectsHolderDomain = "service"
	ReleasePinPolicy     = "release"
)

type Storage interface {
	GetSnapshot(context.Context, string) (storagev1.Snapshot, error)
	GetResource(context.Context, string) (storagev1.Resource, error)
	ResourceContent(context.Context, string) (*http.Response, error)
	AcquirePin(context.Context, storagev1.AcquirePinRequest) (storagev1.Pin, error)
	GetPin(context.Context, string) (storagev1.Pin, error)
	ActivatePin(context.Context, string) (storagev1.Pin, error)
	ReleasePinBound(context.Context, string, storagev1.PinReleaseRequest) (storagev1.PinReleaseReceipt, error)
}

type Config struct {
	ImportVerifierToken   string
	ImportStorageVerifier proof.SnapshotVerifier
	AuthMode              string
	DevFixtures           bool
	Authenticator         authn.Authenticator
	GuestCookieName       string
	CookieName            string
	BrowserGuard          *authn.BrowserGuard
	InternalTokens        []string
	Credentials           map[string]projectsv1.Principal
	OwnerCallbackToken    string
	GenerationToken       string
	TempDir               string
	StorageHolderID       string
	StorageHolderDomain   string
}

type Runtime struct {
	Store            *store.Store
	Storage          Storage
	Access           AccessChecker
	Config           Config
	credentialValues []credential
}

type credential struct {
	token     string
	principal projectsv1.Principal
}

func NewRuntime(state *store.Store, storage Storage, access AccessChecker, config Config) (*Runtime, error) {
	if err := authn.ConfiguredCookieNames(config.CookieName, config.GuestCookieName).Validate(); err != nil {
		return nil, err
	}
	if config.AuthMode != "access" && config.AuthMode != "dev" {
		return nil, errors.New("explicit Projects authentication mode required")
	}
	if config.AuthMode == "dev" && (!config.DevFixtures || config.Authenticator != nil || len(config.Credentials) == 0) {
		return nil, errors.New("explicit Projects dev fixture gate required")
	}
	if config.AuthMode == "access" && (config.DevFixtures || config.Credentials != nil || config.Authenticator == nil || config.BrowserGuard == nil) {
		return nil, errors.New("complete Projects Access mode configuration required")
	}
	if state == nil || storage == nil || access == nil {
		return nil, errors.New("Projects store, Storage and Access owner are required")
	}
	if !validServiceToken(config.OwnerCallbackToken) {
		return nil, errors.New("Projects owner callback token is required")
	}
	if config.GenerationToken != "" && !validServiceToken(config.GenerationToken) {
		return nil, errors.New("Projects Generation owner credential is invalid")
	}
	if config.GenerationToken != "" && config.GenerationToken == config.OwnerCallbackToken {
		return nil, errors.New("Generation and Storage owner callback credentials must be distinct")
	}
	if config.AuthMode == "dev" && (len(config.Credentials) == 0 || len(config.Credentials) > 1000) {
		return nil, errors.New("at least one fixed Projects user credential is required")
	}
	if config.StorageHolderID == "" {
		config.StorageHolderID = ProjectsHolderID
	}
	if config.StorageHolderDomain == "" {
		config.StorageHolderDomain = ProjectsHolderDomain
	}
	if config.StorageHolderID != ProjectsHolderID || config.StorageHolderDomain != ProjectsHolderDomain {
		return nil, errors.New("Projects Storage holder identity is fixed")
	}
	values := make([]credential, 0, len(config.Credentials))
	if config.ImportVerifierToken != "" {
		if !validServiceToken(config.ImportVerifierToken) || config.ImportStorageVerifier == nil || config.ImportVerifierToken == config.GenerationToken || config.ImportVerifierToken == config.OwnerCallbackToken {
			return nil, errors.New("invalid dedicated Projects evidence configuration")
		}
		for _, secret := range config.InternalTokens {
			if secret == config.ImportVerifierToken {
				return nil, errors.New("distinct Projects evidence credential required")
			}
		}
		config.InternalTokens = append(config.InternalTokens, config.ImportVerifierToken)
	}
	for token, principal := range config.Credentials {
		if token == config.GenerationToken || token == config.OwnerCallbackToken || token == config.ImportVerifierToken {
			return nil, errors.New("public user credentials must be distinct from service credentials")
		}
		if len(token) < 16 || len(token) > 4096 || strings.TrimSpace(token) != token || strings.ContainsAny(token, "\x00\t\r\n ") || !projectsv1.ValidIdentifier(principal.Subject) || !projectsv1.ValidIdentifier(principal.Domain) {
			return nil, errors.New("Projects user credential map is invalid")
		}
		values = append(values, credential{token: token, principal: principal})
	}
	runtime := &Runtime{Store: state, Storage: storage, Access: access, Config: config, credentialValues: values}
	if config.AuthMode == "access" {
		if _, ok := access.(canonicalScopeChecker); !ok {
			return nil, errors.New("canonical Access scope required")
		}
		state.SetAuthorityGate(runtime.durableAuthorityGate)
		state.SetCurrentMutationGate(runtime.currentMutationGate)
		state.SetAuditScopeGate(runtime.auditScopeGate)
	}
	return runtime, nil
}

func validServiceToken(value string) bool {
	return len(value) >= 16 && len(value) <= 4096 && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\t\r\n ")
}

func OpenStore(ctx context.Context, databaseURL string) (*pgxpool.Pool, *store.Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, nil, err
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, nil, err
	}
	state, err := store.New(pool, store.Config{PinHolderID: ProjectsHolderID, PinHolderDomain: ProjectsHolderDomain})
	if err != nil {
		pool.Close()
		return nil, nil, err
	}
	return pool, state, nil
}

func (r *Runtime) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return r.Store.Ready(ctx)
}

// Package config owns the fail-closed process configuration for Projects.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"

	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
)

const maxCredentialsJSON = 64 << 10

type Role string

const (
	RoleAPI        Role = "api"
	RoleReconcile  Role = "reconcile"
	RoleMigrate    Role = "migrate"
	RoleEventRelay Role = "event-relay"
)

type Config struct {
	ImportVerifierToken, ImportStorageToken string
	AuthMode                                string
	DevFixtures                             bool
	GuestCookieName                         string
	CookieName                              string
	BrowserOrigins                          []string
	LoopbackHTTP                            bool
	Role                                    Role
	HTTPAddr                                string
	DatabaseURL                             string
	StorageURL                              string
	StorageToken                            string
	OwnerURL                                string
	OwnerToken                              string
	OwnerCallbackToken                      string
	GenerationToken                         string
	NATSAddress                             string
	NATSUser                                string
	NATSPassword                            string
	Credentials                             map[string]storagev1.Principal
	ReconcileIntervalText                   string
	TempDir                                 string
}

func Load(getenv func(string) string) (Config, error) {
	if getenv == nil {
		return Config{}, errors.New("environment reader is required")
	}
	var c Config
	var err error
	role := getenv("PROJECTS_ROLE")
	if role == "" {
		role = string(RoleAPI)
	}
	c.Role = Role(role)
	if c.Role != RoleAPI && c.Role != RoleReconcile && c.Role != RoleMigrate && c.Role != RoleEventRelay {
		return Config{}, errors.New("PROJECTS_ROLE must be api, reconcile, migrate, or event-relay")
	}
	if c.Role == RoleEventRelay {
		c.DatabaseURL, err = required(getenv, "PROJECTS_DATABASE_URL")
		if err != nil {
			return Config{}, err
		}
		c.NATSAddress, err = required(getenv, "PROJECTS_NATS_ADDRESS")
		if err != nil {
			return Config{}, err
		}
		if !validNATSAddress(c.NATSAddress) {
			return Config{}, errors.New("PROJECTS_NATS_ADDRESS must be a valid nats:// or tls:// host:port URL")
		}
		c.NATSUser, err = required(getenv, "PROJECTS_NATS_USER")
		if err != nil {
			return Config{}, err
		}
		if len(c.NATSUser) > 256 || strings.ContainsAny(c.NATSUser, " \t\r\n\x00") {
			return Config{}, errors.New("PROJECTS_NATS_USER is invalid")
		}
		c.NATSPassword, err = required(getenv, "PROJECTS_NATS_PASSWORD")
		if err != nil {
			return Config{}, err
		}
		if !validBearerSecret(c.NATSPassword) {
			return Config{}, errors.New("PROJECTS_NATS_PASSWORD must be a bounded opaque credential")
		}
		return c, nil
	}
	for name, destination := range map[string]*string{
		"PROJECTS_DATABASE_URL":         &c.DatabaseURL,
		"PROJECTS_STORAGE_URL":          &c.StorageURL,
		"PROJECTS_STORAGE_TOKEN":        &c.StorageToken,
		"PROJECTS_OWNER_URL":            &c.OwnerURL,
		"PROJECTS_OWNER_TOKEN":          &c.OwnerToken,
		"PROJECTS_OWNER_CALLBACK_TOKEN": &c.OwnerCallbackToken,
		"PROJECTS_GENERATION_TOKEN":     &c.GenerationToken,
	} {
		*destination, err = required(getenv, name)
		if err != nil {
			return Config{}, err
		}
	}
	for name, secret := range map[string]string{
		"PROJECTS_STORAGE_TOKEN":        c.StorageToken,
		"PROJECTS_OWNER_TOKEN":          c.OwnerToken,
		"PROJECTS_OWNER_CALLBACK_TOKEN": c.OwnerCallbackToken,
		"PROJECTS_GENERATION_TOKEN":     c.GenerationToken,
	} {
		if !validBearerSecret(secret) {
			return Config{}, fmt.Errorf("%s must be a bounded opaque bearer credential", name)
		}
	}
	c.HTTPAddr = getenv("PROJECTS_HTTP_ADDR")
	if c.HTTPAddr == "" {
		c.HTTPAddr = "127.0.0.1:8084"
	}
	if len(c.HTTPAddr) > 256 || strings.ContainsAny(c.HTTPAddr, "\r\n") {
		return Config{}, errors.New("PROJECTS_HTTP_ADDR is invalid")
	}
	_, portText, splitErr := net.SplitHostPort(c.HTTPAddr)
	if splitErr != nil {
		return Config{}, errors.New("PROJECTS_HTTP_ADDR must be a host:port address")
	}
	port, portErr := strconv.Atoi(portText)
	if portErr != nil || port < 1 || port > 65535 {
		return Config{}, errors.New("PROJECTS_HTTP_ADDR port must be between 1 and 65535")
	}
	c.ReconcileIntervalText = getenv("PROJECTS_RECONCILE_INTERVAL")
	if c.ReconcileIntervalText == "" {
		c.ReconcileIntervalText = "2s"
	}
	if len(c.ReconcileIntervalText) > 32 || strings.ContainsAny(c.ReconcileIntervalText, " \t\r\n") {
		return Config{}, errors.New("PROJECTS_RECONCILE_INTERVAL is invalid")
	}
	c.TempDir = getenv("PROJECTS_TEMP_DIR")
	if strings.ContainsAny(c.TempDir, "\x00\r\n") {
		return Config{}, errors.New("PROJECTS_TEMP_DIR is invalid")
	}
	c.AuthMode = getenv("PROJECTS_AUTH_MODE")
	c.ImportVerifierToken = getenv("PROJECTS_IMPORT_VERIFIER_TOKEN")
	c.ImportStorageToken = getenv("PROJECTS_IMPORT_STORAGE_TOKEN")
	if c.ImportVerifierToken != "" || c.ImportStorageToken != "" {
		if !validBearerSecret(c.ImportVerifierToken) || !validBearerSecret(c.ImportStorageToken) || c.ImportVerifierToken == c.ImportStorageToken {
			return Config{}, errors.New("dedicated Projects evidence credentials required")
		}
		for _, secret := range []string{c.StorageToken, c.OwnerToken, c.OwnerCallbackToken, c.GenerationToken} {
			if secret == c.ImportVerifierToken || secret == c.ImportStorageToken {
				return Config{}, errors.New("distinct Projects evidence credentials required")
			}
		}
	}
	c.DevFixtures = getenv("PROJECTS_DEV_FIXTURES") == "true"
	c.CookieName = getenv("PROJECTS_SESSION_COOKIE")
	c.GuestCookieName = getenv("LEAFRUN_GUEST_COOKIE_NAME")
	c.LoopbackHTTP = getenv("PROJECTS_LOOPBACK_HTTP") == "true"
	if err := authn.ConfiguredCookieNames(c.CookieName, c.GuestCookieName).Validate(); err != nil {
		return Config{}, err
	}
	switch c.AuthMode {
	case "dev":
		if !c.DevFixtures {
			return Config{}, errors.New("explicit Projects dev fixture gate required")
		}
		credentialJSON, credentialErr := required(getenv, "PROJECTS_CREDENTIALS_JSON")
		if credentialErr != nil {
			return Config{}, credentialErr
		}
		c.Credentials, err = ParseCredentials(credentialJSON)
		if err != nil {
			return Config{}, errors.New("PROJECTS_CREDENTIALS_JSON is invalid")
		}
	case "access":
		if c.DevFixtures || getenv("PROJECTS_CREDENTIALS_JSON") != "" {
			return Config{}, errors.New("Projects fixture fallback forbidden in Access mode")
		}
		if err = jsonvalue.Unmarshal([]byte(getenv("PROJECTS_BROWSER_ORIGINS_JSON")), &c.BrowserOrigins, 32<<10); err != nil {
			return Config{}, errors.New("explicit Projects browser origins required")
		}
		if _, err = authn.NewBrowserGuard(c.BrowserOrigins, c.LoopbackHTTP); err != nil {
			return Config{}, err
		}
	default:
		return Config{}, errors.New("explicit Projects authentication mode required")
	}
	if c.GenerationToken == c.OwnerCallbackToken {
		return Config{}, errors.New("Generation and Storage owner callback credentials must be distinct")
	}
	for token := range c.Credentials {
		if token == c.GenerationToken || token == c.OwnerCallbackToken || token == c.StorageToken || token == c.OwnerToken || token == c.ImportVerifierToken || token == c.ImportStorageToken {
			return Config{}, errors.New("public user credentials must be distinct from service credentials")
		}
	}
	return c, nil
}

func validNATSAddress(address string) bool {
	if len(address) > 512 || strings.TrimSpace(address) != address || strings.ContainsAny(address, "\x00\t\r\n") {
		return false
	}
	parsed, err := url.Parse(address)
	if err != nil || (parsed.Scheme != "nats" && parsed.Scheme != "tls") || parsed.User != nil ||
		parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" {
		return false
	}
	host, portText, err := net.SplitHostPort(parsed.Host)
	if err != nil || host == "" {
		return false
	}
	port, err := strconv.Atoi(portText)
	return err == nil && port >= 1 && port <= 65535
}

func validBearerSecret(value string) bool {
	return len(value) >= 16 && len(value) <= 4096 && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\t\r\n ")
}

func required(getenv func(string) string, name string) (string, error) {
	value := getenv(name)
	if value == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n") {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}

// ParseCredentials accepts a strict object mapping opaque bearer tokens to
// server-asserted principals. No identity headers or public JSON are trusted.
func ParseCredentials(raw string) (map[string]storagev1.Principal, error) {
	if len(raw) == 0 || len(raw) > maxCredentialsJSON || !utf8.ValidString(raw) {
		return nil, errors.New("credential document size or encoding is invalid")
	}
	normalized, err := jsonvalue.NormalizeObject([]byte(raw))
	if err != nil {
		return nil, errors.New("credential document must be a JSON object without duplicate keys")
	}
	var credentials map[string]storagev1.Principal
	decoder := json.NewDecoder(strings.NewReader(string(normalized)))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&credentials); err != nil {
		return nil, err
	}
	if credentials == nil || len(credentials) == 0 || len(credentials) > 1000 {
		return nil, errors.New("credential count is invalid")
	}
	for token, principal := range credentials {
		if len(token) < 16 || len(token) > 4096 || strings.TrimSpace(token) != token || strings.ContainsAny(token, "\x00\t\r\n ") {
			return nil, errors.New("credential token is invalid")
		}
		if !validPrincipalPart(principal.Subject) || !validPrincipalPart(principal.Domain) {
			return nil, errors.New("credential principal is invalid")
		}
	}
	return credentials, nil
}

func validPrincipalPart(value string) bool {
	return len(value) > 0 && len(value) <= 128 && utf8.ValidString(value) && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}

// MarshalCredentials is a convenience for tests and explicit dev fixtures.
func MarshalCredentials(credentials map[string]storagev1.Principal) (string, error) {
	data, err := json.Marshal(credentials)
	return string(data), err
}

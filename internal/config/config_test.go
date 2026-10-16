package config

import (
	"strings"
	"testing"

	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

func TestLoadRequiresSeparateServiceCredentialsAndStrictUserTokens(t *testing.T) {
	values := map[string]string{
		"LEAFRUN_GUEST_COOKIE_NAME": "custom_guest", "PROJECTS_SESSION_COOKIE": "custom_human", "PROJECTS_AUTH_MODE": "dev", "PROJECTS_DEV_FIXTURES": "true", "PROJECTS_DATABASE_URL": "postgres://projects@localhost/projects",
		"PROJECTS_STORAGE_URL":          "http://storage:8081",
		"PROJECTS_STORAGE_TOKEN":        strings.Repeat("s", 32),
		"PROJECTS_OWNER_URL":            "http://owner:8082",
		"PROJECTS_OWNER_TOKEN":          strings.Repeat("o", 32),
		"PROJECTS_OWNER_CALLBACK_TOKEN": strings.Repeat("c", 32),
		"PROJECTS_GENERATION_TOKEN":     strings.Repeat("g", 32),
		"PROJECTS_CREDENTIALS_JSON":     `{"` + strings.Repeat("u", 32) + `":{"subject":"demo","domain":"core"}}`,
	}
	config, err := Load(func(key string) string { return values[key] })
	if err != nil {
		t.Fatal(err)
	}
	if config.GuestCookieName != "custom_guest" || config.CookieName != "custom_human" {
		t.Fatal("configured cookie names lost")
	}
	if config.Role != RoleAPI || config.HTTPAddr != "127.0.0.1:8084" || len(config.Credentials) != 1 || config.Credentials[strings.Repeat("u", 32)] != (storagev1.Principal{Subject: "demo", Domain: "core"}) {
		t.Fatalf("unexpected default Projects config: %#v", config)
	}
	values["PROJECTS_CREDENTIALS_JSON"] = `{"` + strings.Repeat("u", 32) + `":{"subject":"demo","domain":"core","isAdmin":true}}`
	if _, err = Load(func(key string) string { return values[key] }); err == nil {
		t.Fatal("unknown credential principal field was accepted")
	}
}

func TestLoadRejectsMalformedBearerSecretsAndHTTPPorts(t *testing.T) {
	base := map[string]string{
		"PROJECTS_AUTH_MODE": "dev", "PROJECTS_DEV_FIXTURES": "true", "PROJECTS_DATABASE_URL": "postgres://projects@localhost/projects",
		"PROJECTS_STORAGE_URL":          "http://storage:8081",
		"PROJECTS_STORAGE_TOKEN":        strings.Repeat("s", 32),
		"PROJECTS_OWNER_URL":            "http://owner:8082",
		"PROJECTS_OWNER_TOKEN":          strings.Repeat("o", 32),
		"PROJECTS_OWNER_CALLBACK_TOKEN": strings.Repeat("c", 32),
		"PROJECTS_GENERATION_TOKEN":     strings.Repeat("g", 32),
		"PROJECTS_CREDENTIALS_JSON":     `{"` + strings.Repeat("u", 32) + `":{"subject":"demo","domain":"core"}}`,
	}
	cases := map[string]map[string]string{
		"colliding cookies":       {"PROJECTS_SESSION_COOKIE": "custom_guest", "LEAFRUN_GUEST_COOKIE_NAME": "custom_guest"},
		"invalid guest":           {"LEAFRUN_GUEST_COOKIE_NAME": "bad name"},
		"short storage secret":    {"PROJECTS_STORAGE_TOKEN": "short"},
		"whitespace owner secret": {"PROJECTS_OWNER_TOKEN": "owner token with spaces"},
		"invalid callback secret": {"PROJECTS_OWNER_CALLBACK_TOKEN": "short"},
		"invalid port":            {"PROJECTS_HTTP_ADDR": ":70000"},
		"zero port":               {"PROJECTS_HTTP_ADDR": ":0"},
	}
	for name, changes := range cases {
		t.Run(name, func(t *testing.T) {
			values := make(map[string]string, len(base)+len(changes))
			for key, value := range base {
				values[key] = value
			}
			for key, value := range changes {
				values[key] = value
			}
			if _, err := Load(func(key string) string { return values[key] }); err == nil {
				t.Fatal("malformed configuration was accepted")
			}
		})
	}
}

func TestParseCredentialsRejectsDuplicateAndMalformedPrincipals(t *testing.T) {
	token := strings.Repeat("t", 32)
	for _, input := range []string{
		`{"` + token + `":{"subject":"a","domain":"core"},"` + token + `":{"subject":"b","domain":"core"}}`,
		`{"` + token + `":{"subject":"a","domain":"core","extra":true}}`,
		`{"` + token + `":{"subject":"","domain":"core"}}`,
		`[]`,
	} {
		if _, err := ParseCredentials(input); err == nil {
			t.Fatalf("invalid credentials accepted: %s", input)
		}
	}
}

func TestLoadRejectsPublicCredentialMatchingGenerationServiceToken(t *testing.T) {
	serviceToken := strings.Repeat("g", 32)
	values := map[string]string{
		"PROJECTS_AUTH_MODE": "dev", "PROJECTS_DEV_FIXTURES": "true", "PROJECTS_DATABASE_URL": "postgres://projects@localhost/projects", "PROJECTS_STORAGE_URL": "http://storage:8081",
		"PROJECTS_STORAGE_TOKEN": strings.Repeat("s", 32), "PROJECTS_OWNER_URL": "http://owner:8082", "PROJECTS_OWNER_TOKEN": strings.Repeat("o", 32),
		"PROJECTS_OWNER_CALLBACK_TOKEN": strings.Repeat("c", 32), "PROJECTS_GENERATION_TOKEN": serviceToken,
		"PROJECTS_CREDENTIALS_JSON": `{"` + serviceToken + `":{"subject":"ordinary-user","domain":"user"}}`,
	}
	if _, err := Load(func(key string) string { return values[key] }); err == nil {
		t.Fatal("a public user credential could impersonate the trusted Generation service")
	}
}

func TestLoadEventRelayRequiresOnlyProjectsDatabaseAndNATS(t *testing.T) {
	values := map[string]string{
		"PROJECTS_ROLE":      "event-relay",
		"PROJECTS_AUTH_MODE": "dev", "PROJECTS_DEV_FIXTURES": "true", "PROJECTS_DATABASE_URL": "postgres://projects@localhost/projects",
		"PROJECTS_NATS_ADDRESS":  "nats://nats:4222",
		"PROJECTS_NATS_USER":     "projects",
		"PROJECTS_NATS_PASSWORD": strings.Repeat("n", 32),
	}
	config, err := Load(func(key string) string { return values[key] })
	if err != nil {
		t.Fatal(err)
	}
	if config.Role != RoleEventRelay || config.DatabaseURL != values["PROJECTS_DATABASE_URL"] ||
		config.NATSAddress != values["PROJECTS_NATS_ADDRESS"] || config.NATSUser != values["PROJECTS_NATS_USER"] ||
		config.NATSPassword != values["PROJECTS_NATS_PASSWORD"] {
		t.Fatalf("unexpected event relay config: %#v", config)
	}
}

func TestLoadEventRelayRequiresValidNATSConfiguration(t *testing.T) {
	base := map[string]string{
		"PROJECTS_ROLE":      "event-relay",
		"PROJECTS_AUTH_MODE": "dev", "PROJECTS_DEV_FIXTURES": "true", "PROJECTS_DATABASE_URL": "postgres://projects@localhost/projects",
		"PROJECTS_NATS_ADDRESS":  "nats://nats:4222",
		"PROJECTS_NATS_USER":     "projects",
		"PROJECTS_NATS_PASSWORD": strings.Repeat("n", 32),
	}
	cases := map[string]map[string]string{
		"missing database":       {"PROJECTS_AUTH_MODE": "dev", "PROJECTS_DEV_FIXTURES": "true", "PROJECTS_DATABASE_URL": ""},
		"missing NATS address":   {"PROJECTS_NATS_ADDRESS": ""},
		"missing NATS user":      {"PROJECTS_NATS_USER": ""},
		"missing NATS password":  {"PROJECTS_NATS_PASSWORD": ""},
		"malformed NATS address": {"PROJECTS_NATS_ADDRESS": "http://nats:4222"},
		"short NATS password":    {"PROJECTS_NATS_PASSWORD": "short"},
	}
	for name, changes := range cases {
		t.Run(name, func(t *testing.T) {
			values := make(map[string]string, len(base)+len(changes))
			for key, value := range base {
				values[key] = value
			}
			for key, value := range changes {
				values[key] = value
			}
			if _, err := Load(func(key string) string { return values[key] }); err == nil {
				t.Fatal("invalid event relay configuration was accepted")
			}
		})
	}
}

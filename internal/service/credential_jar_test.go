package service

import (
	"context"
	storage "github.com/leafrun/leafrun-contracts/storage/v1"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestOwnerAccessDoesNotSendAmbientHumanCookies(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Cookie") != "" {
			t.Error("ambient human cookie reached fixed owner dependency")
		}
		if r.Header.Get("Authorization") != "Bearer fixed-service-test-token" {
			t.Error("fixed bearer missing")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"allowed":false}`))
	}))
	defer server.Close()
	jar, _ := cookiejar.New(nil)
	u, _ := url.Parse(server.URL)
	jar.SetCookies(u, []*http.Cookie{{Name: "ory_kratos_session", Value: "synthetic-human", Path: "/"}})
	transport := &http.Client{Jar: jar}
	client, err := NewOwnerAccess(server.URL, "fixed-service-test-token", transport)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Check(context.Background(), storage.AccessCheck{}); err != nil {
		t.Fatal(err)
	}
	if transport.Jar != jar {
		t.Fatal("caller transport mutated")
	}
}

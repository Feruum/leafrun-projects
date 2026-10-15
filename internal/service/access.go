package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

type AccessChecker interface {
	Check(context.Context, storagev1.AccessCheck) (bool, error)
}

type OwnerAccess struct {
	baseURL string
	token   string
	client  *http.Client
}

func NewOwnerAccess(baseURL, token string, client *http.Client) (*OwnerAccess, error) {
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || !validServiceToken(token) {
		return nil, errors.New("Projects owner URL and fixed service token are required")
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	copy := *client
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &OwnerAccess{baseURL: strings.TrimRight(baseURL, "/"), token: token, client: &copy}, nil
}

func (a *OwnerAccess) Check(ctx context.Context, input storagev1.AccessCheck) (bool, error) {
	if a == nil || a.client == nil || a.token == "" {
		return false, errors.New("Projects owner access client is unavailable")
	}
	body, err := json.Marshal(input)
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/internal/v1/checks/access", bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	request.Header.Set("Authorization", "Bearer "+a.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := a.client.Do(request)
	if err != nil {
		return false, errors.New("Projects Access owner is unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return false, fmt.Errorf("Projects Access owner returned HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if err != nil || len(data) > 64<<10 {
		return false, errors.New("Projects Access owner returned an invalid response")
	}
	normalized, err := jsonvalue.NormalizeLimit(data, 64<<10)
	if err != nil {
		return false, errors.New("Projects Access owner returned invalid JSON")
	}
	var decision storagev1.AccessDecision
	decoder := json.NewDecoder(bytes.NewReader(normalized))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&decision); err != nil {
		return false, errors.New("Projects Access owner returned an invalid decision")
	}
	return decision.Allowed, nil
}

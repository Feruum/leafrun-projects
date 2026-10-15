package service

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
)

func TestWriteReleaseFormPreservesRawSchemaStrings(t *testing.T) {
	raw := json.RawMessage(`{"description":"<>&` + "\u2028\u2029" + ` literal \\u2028 \\u2029 quote \"","default":1.2300e+04}`)
	response := httptest.NewRecorder()
	writeReleaseForm(response, projectsv1.ReleaseForm{Schema: raw, SchemaResources: map[string]json.RawMessage{"schema.json": raw}})
	if response.Code != 200 || !bytes.Contains(response.Body.Bytes(), raw) {
		t.Fatalf("bounded raw schema was changed: %d %s", response.Code, response.Body.String())
	}
}

func TestWriteReleaseFormRejectsInvalidRawJSONBeforeWriting(t *testing.T) {
	response := httptest.NewRecorder()
	writeReleaseForm(response, projectsv1.ReleaseForm{Schema: json.RawMessage(`{"description":`), SchemaResources: map[string]json.RawMessage{}})
	if response.Code != 413 || !strings.Contains(response.Body.String(), `"code":"response_too_large"`) {
		t.Fatalf("invalid raw schema partially published: %d %s", response.Code, response.Body.String())
	}
}

func TestWriteJSONKeepsResponseByteLimit(t *testing.T) {
	response := httptest.NewRecorder()
	writeJSON(response, 200, map[string]string{"value": strings.Repeat("x", int(projectsv1.ResponseLimit))})
	if response.Code != 413 || !strings.Contains(response.Body.String(), `"code":"response_too_large"`) {
		t.Fatalf("oversize response escaped cap: %d", response.Code)
	}
}

package service

import (
	"errors"
	"net/http"

	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	"github.com/leafrun/leafrun-projects/internal/store"
)

type Fault struct {
	Status  int
	Code    string
	Message string
	Fields  []projectsv1.FieldError
}

func (f *Fault) Error() string {
	if f == nil {
		return "Projects request failed"
	}
	return f.Code
}

func fault(status int, code, message string) *Fault {
	return &Fault{Status: status, Code: code, Message: message}
}

func faultFields(status int, code, message string, fields []projectsv1.FieldError) *Fault {
	return &Fault{Status: status, Code: code, Message: message, Fields: fields}
}

func storeFault(err error) error {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return fault(http.StatusNotFound, "not_found", "Projects record was not found")
	case errors.Is(err, store.ErrConflict):
		return fault(http.StatusConflict, "conflict", "The requested immutable Projects binding conflicts with existing data")
	case errors.Is(err, store.ErrMetadataTooBig):
		return fault(http.StatusRequestEntityTooLarge, "metadata_too_large", "Projects metadata exceeds 12 MiB")
	case errors.Is(err, store.ErrInvalidState):
		return fault(http.StatusConflict, "invalid_state", "The Projects record cannot make this state transition")
	case errors.Is(err, store.ErrLeaseHeld), errors.Is(err, store.ErrLeaseLost):
		return fault(http.StatusServiceUnavailable, "reconciler_unavailable", "Projects pin reconciliation is temporarily unavailable")
	default:
		return fault(http.StatusServiceUnavailable, "projects_unavailable", "Projects could not complete the request")
	}
}

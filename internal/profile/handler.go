package profile

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

const (
	codeInvalidRequest     = "INVALID_REQUEST"
	codeNotFound           = "NOT_FOUND"
	codeEmailAlreadyExists = "EMAIL_ALREADY_EXISTS"
	codeInternal           = "INTERNAL"
)

type httpMetricsSink interface {
	RecordHTTPRequest(method, endpoint string, status int, duration time.Duration)
}

type Handler struct {
	service *Service
	metrics httpMetricsSink
}

func NewHandler(service *Service, m httpMetricsSink) *Handler {
	return &Handler{service: service, metrics: m}
}

func NewRouter(h *Handler) *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc("/api/v1/investors/{id}", h.UpdateProfile).Methods(http.MethodPatch)
	r.HandleFunc("/health", h.HealthCheck).Methods(http.MethodGet)
	return r
}

func (h *Handler) UpdateProfile(w http.ResponseWriter, r *http.Request) {
	const endpoint = "/api/v1/investors/{id}"
	start := time.Now()
	status := http.StatusOK
	defer func() {
		if h.metrics != nil {
			h.metrics.RecordHTTPRequest(r.Method, endpoint, status, time.Since(start))
		}
	}()

	id := mux.Vars(r)["id"]
	if id == "" {
		status = http.StatusBadRequest
		h.respondError(w, status, codeInvalidRequest, "investor id is required")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		status = http.StatusBadRequest
		h.respondError(w, status, codeInvalidRequest, "failed to read request body")
		return
	}

	var patch UpdateProfilePatch
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&patch); err != nil {
		status = http.StatusBadRequest
		h.respondError(w, status, codeInvalidRequest, "invalid JSON: "+err.Error())
		return
	}

	profile, err := h.service.UpdateProfile(r.Context(), id, patch)
	if err != nil {
		switch {
		case IsValidationError(err):
			status = http.StatusBadRequest
			h.respondError(w, status, codeInvalidRequest, err.Error())
		case errors.Is(err, ErrNotFound):
			status = http.StatusNotFound
			h.respondError(w, status, codeNotFound, "investor "+id+" not found")
		case errors.Is(err, ErrEmailExists):
			status = http.StatusConflict
			h.respondError(w, status, codeEmailAlreadyExists, "email already exists")
		default:
			status = http.StatusInternalServerError
			log.Printf("UpdateProfile failed for %s: %v", id, err)
			h.respondError(w, status, codeInternal, "internal server error")
		}
		return
	}

	status = http.StatusOK
	h.respondJSON(w, status, profile)
}

func (h *Handler) HealthCheck(w http.ResponseWriter, _ *http.Request) {
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) respondJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func (h *Handler) respondError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	resp := map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("encode error response: %v", err)
	}
}


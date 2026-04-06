package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/vladimirzankov/investor-cache/internal/cache"
	"github.com/vladimirzankov/investor-cache/internal/domain"
	"github.com/vladimirzankov/investor-cache/internal/metrics"
)

const routeInvestorByID = "/api/v1/investors/{id}"

type ProfileHandler struct {
	cacheManager *cache.CacheManager
	repo         domain.ProfileRepository
	metrics      *metrics.Collector
}

func NewProfileHandler(cm *cache.CacheManager, repo domain.ProfileRepository, m *metrics.Collector) *ProfileHandler {
	return &ProfileHandler{
		cacheManager: cm,
		repo:         repo,
		metrics:      m,
	}
}

func (h *ProfileHandler) GetProfile(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	cacheResult := cache.CacheResultUnknown
	statusLabel := "ok"
	defer func() {
		h.metrics.RecordMiddlewareRequest(routeInvestorByID, string(cacheResult), statusLabel, time.Since(start))
	}()

	vars := mux.Vars(r)
	id := vars["id"]

	if r.Header.Get("Cache-Control") == "no-cache" {
		cacheResult = cache.CacheResultBypass
		profile, err := h.repo.GetByID(r.Context(), id)
		if err != nil {
			statusLabel = "error"
			h.respondError(w, err)
			return
		}
		h.metrics.RecordCacheBypass()
		h.respondJSON(w, http.StatusOK, profile)
		return
	}

	profile, result, err := h.cacheManager.GetProfile(r.Context(), id)
	cacheResult = result
	if err != nil {
		statusLabel = "error"
		h.respondError(w, err)
		return
	}
	h.respondJSON(w, http.StatusOK, profile)
}

func (h *ProfileHandler) HealthCheck(w http.ResponseWriter, _ *http.Request) {
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *ProfileHandler) respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("failed to encode response: %v", err)
	}
}

func (h *ProfileHandler) respondError(w http.ResponseWriter, err error) {
	log.Printf("request error: %v", err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func NewRouter(h *ProfileHandler) *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc("/api/v1/investors/{id}", h.GetProfile).Methods("GET")
	r.HandleFunc("/health", h.HealthCheck).Methods("GET")
	return r
}

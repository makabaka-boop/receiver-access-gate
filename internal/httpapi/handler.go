// Package httpapi exposes the access gate over HTTP.
package httpapi

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"

	"accessgate/internal/store"
)

// NewHandler builds the application router.
func NewHandler(s *store.Store) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.Ping(r.Context()); err != nil {
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "database unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /v1/receivers/{receiver}/grants", func(w http.ResponseWriter, r *http.Request) {
		receiver := r.PathValue("receiver")
		if !validReceiver(receiver) {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", "receiver must be a non-empty identifier")
			return
		}

		var req struct {
			Mode string `json:"mode"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<10))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil || strings.TrimSpace(req.Mode) == "" {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", `body must be JSON like {"mode":"SHARED|EXCLUSIVE"}`)
			return
		}
		req.Mode = strings.ToUpper(strings.TrimSpace(req.Mode))

		grant, token, err := s.CreateGrant(r.Context(), receiver, req.Mode)
		switch {
		case errors.Is(err, store.ErrInvalidMode):
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", "mode must be SHARED or EXCLUSIVE")
			return
		case errors.Is(err, store.ErrBusy):
			writeError(w, http.StatusConflict, "BUSY", "receiver is busy")
			return
		case err != nil:
			log.Printf("create grant: %v", err)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "internal error")
			return
		}

		// The owner token appears in this response and nowhere else.
		writeJSON(w, http.StatusCreated, map[string]any{
			"grant_id":    grant.ID,
			"receiver":    grant.Receiver,
			"mode":        grant.Mode,
			"status":      grant.Status,
			"owner_token": token,
			"created_at":  grant.CreatedAt,
		})
	})

	mux.HandleFunc("POST /v1/receivers/{receiver}/grants/{grant}/release", func(w http.ResponseWriter, r *http.Request) {
		receiver := r.PathValue("receiver")
		grantID := r.PathValue("grant")
		if !validReceiver(receiver) || !validID(grantID) {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid receiver or grant id")
			return
		}

		var req struct {
			Token string `json:"owner_token"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<10))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil || req.Token == "" {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", `body must be JSON like {"owner_token":"..."}`)
			return
		}

		grant, err := s.Release(r.Context(), receiver, grantID, req.Token)
		switch {
		case errors.Is(err, store.ErrForbidden):
			writeError(w, http.StatusForbidden, "FORBIDDEN", "invalid owner token")
			return
		case errors.Is(err, store.ErrNotFound):
			writeError(w, http.StatusNotFound, "NOT_FOUND", "grant not found")
			return
		case err != nil:
			log.Printf("release grant: %v", err)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "internal error")
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"grant_id":   grant.ID,
			"receiver":   grant.Receiver,
			"mode":       grant.Mode,
			"status":     grant.Status,
			"created_at": grant.CreatedAt,
		})
	})

	mux.HandleFunc("GET /v1/receivers/{receiver}/grants", func(w http.ResponseWriter, r *http.Request) {
		receiver := r.PathValue("receiver")
		if !validReceiver(receiver) {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", "receiver must be a non-empty identifier")
			return
		}

		grants, err := s.ListByReceiver(r.Context(), receiver)
		if err != nil {
			log.Printf("list grants: %v", err)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "internal error")
			return
		}
		if grants == nil {
			grants = []store.GrantInfo{}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"receiver": receiver,
			"grants":   grants,
		})
	})

	return requestLog(mux)
}

func validReceiver(s string) bool {
	if s == "" || len(s) > 200 {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func validID(s string) bool {
	if !strings.HasPrefix(s, "g_") || len(s) != 2+32 {
		return false
	}
	for _, c := range s[2:] {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{"error": code, "message": msg})
}

func requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		log.Printf("%s %s -> %d", r.Method, r.URL.Path, sw.status)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	runtime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

type capacityRuntime interface {
	PrepareAdmission(context.Context, runtime.AdmissionRequest) (string, error)
	CancelAdmission(context.Context, string) error
	ExecuteAdmitted(context.Context, []string, []byte, string) (runtime.Result, error)
	Capacity(context.Context) (runtime.CapacityStatus, error)
	SetCapacityLimits(context.Context, runtime.CapacityLimits) error
}

func capacityRefusal(w http.ResponseWriter, err error) bool {
	if !errors.Is(err, runtime.ErrCapacityUnavailable) {
		return false
	}
	w.Header().Set("Retry-After", "1")
	writeJSON(w, http.StatusConflict, map[string]any{
		"error": "Native capacity is unavailable; retain the cold workload and retry",
		"code":  "CapacityUnavailable", "operationStarted": false,
	})
	return true
}

func (h *hostHandler) capacity(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r) {
		return
	}
	manager, ok := h.runtime.(capacityRuntime)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "Native capacity manager is unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.config.timeout)
	defer cancel()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	switch r.URL.Path {
	case "/v1/capacity/prepare":
		var request runtime.AdmissionRequest
		if decoder.Decode(&request) != nil {
			writeError(w, http.StatusBadRequest, "Invalid native admission request")
			return
		}
		token, err := manager.PrepareAdmission(ctx, request)
		if capacityRefusal(w, err) {
			return
		}
		if err != nil {
			writeError(w, http.StatusBadRequest, "Cannot prepare native admission")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"token": token})
	case "/v1/capacity/cancel":
		var request struct {
			Token string `json:"token"`
		}
		if decoder.Decode(&request) != nil || request.Token == "" {
			writeError(w, http.StatusBadRequest, "Invalid native admission token")
			return
		}
		if manager.CancelAdmission(ctx, request.Token) != nil {
			writeError(w, http.StatusConflict, "Cannot cancel a dispatched native reservation")
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"cancelled": true})
	case "/v1/capacity/limits":
		var limits runtime.CapacityLimits
		if decoder.Decode(&limits) != nil {
			writeError(w, http.StatusBadRequest, "Invalid native capacity limits")
			return
		}
		if manager.SetCapacityLimits(ctx, limits) != nil {
			writeError(w, http.StatusConflict, "Cannot update native capacity limits")
			return
		}
		writeJSON(w, http.StatusOK, limits)
	default:
		status, err := manager.Capacity(ctx)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "Native capacity inventory is unavailable")
			return
		}
		writeJSON(w, http.StatusOK, status)
	}
}

package main

import (
	"context"
	"net/http"

	runtime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

type storageRuntime interface {
	Storage(context.Context) (runtime.StorageReport, error)
	PruneStorage(context.Context, runtime.StoragePruneRequest) (runtime.StoragePruneReport, error)
}

func (h *hostHandler) storage(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r) {
		return
	}
	manager, ok := h.runtime.(storageRuntime)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "Native storage maintenance is unavailable")
		return
	}
	if r.Method == http.MethodGet {
		ctx, cancel := context.WithTimeout(r.Context(), h.config.timeout)
		defer cancel()
		report, err := manager.Storage(ctx)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "Native storage inventory is unavailable")
			return
		}
		writeJSON(w, http.StatusOK, report)
		return
	}
	request, err := runtime.DecodeStoragePruneRequest(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "Invalid native storage cleanup request")
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), h.config.timeout)
	defer cancel()
	report, err := manager.PruneStorage(ctx, request)
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "Native storage cleanup did not complete; inspect inventory before retrying", "report": report})
		return
	}
	writeJSON(w, http.StatusOK, report)
}

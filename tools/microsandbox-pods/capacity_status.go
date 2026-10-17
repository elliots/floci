package main

import (
	"context"
	"net/http"
	"time"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

type capacityReporter interface {
	Capacity(context.Context) (msbruntime.CapacityStatus, error)
}

// Inventory is an explicit diagnostic request, never part of the frequent
// readiness check. Its budget is shared with every client of the runtime home.
func (p *provider) capacityStatus(w http.ResponseWriter, r *http.Request) {
	reporter, ok := p.runner.runtime.(capacityReporter)
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "native capacity inventory is unavailable"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	status, err := reporter.Capacity(ctx)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "native capacity inventory could not be verified"})
		return
	}
	writeJSON(w, http.StatusOK, status)
}

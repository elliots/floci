package main

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"time"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

type storageReporter interface {
	Storage(context.Context) (msbruntime.StorageReport, error)
	PruneStorage(context.Context, msbruntime.StoragePruneRequest) (msbruntime.StoragePruneReport, error)
}

type workloadStorageIdentity struct {
	Namespace string   `json:"namespace"`
	Name      string   `json:"name"`
	UID       string   `json:"uid"`
	VM        string   `json:"vm"`
	Snapshots []string `json:"snapshots"`
}

func (p *provider) storageStatus(w http.ResponseWriter, r *http.Request) {
	reporter, ok := p.runner.runtime.(storageReporter)
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "native storage maintenance is unavailable"})
		return
	}
	if r.Method == http.MethodGet {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		report, err := reporter.Storage(ctx)
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "native storage inventory is unavailable"})
			return
		}
		// VM names and immutable snapshot IDs join SDK item names. Mapping can
		// change during a lifecycle operation; this is an observation, not a
		// retention plan or exclusive allocation measurement.
		p.mu.Lock()
		entries := make([]*podEntry, 0, len(p.pods))
		for _, entry := range p.pods {
			entries = append(entries, entry)
		}
		p.mu.Unlock()
		workloads := make([]workloadStorageIdentity, 0, len(entries))
		busy := 0
		for _, entry := range entries {
			if !entry.op.TryLock() {
				busy++
				continue
			}
			entry.mu.Lock()
			identity := workloadStorageIdentity{Namespace: entry.pod.Namespace, Name: entry.pod.Name, UID: string(entry.pod.UID), VM: entry.vm, Snapshots: []string{}}
			for _, id := range entry.snapshotIDs {
				identity.Snapshots = append(identity.Snapshots, id)
			}
			entry.mu.Unlock()
			entry.op.Unlock()
			slices.Sort(identity.Snapshots)
			workloads = append(workloads, identity)
		}
		slices.SortFunc(workloads, func(a, b workloadStorageIdentity) int {
			return strings.Compare(a.Namespace+"/"+a.Name, b.Namespace+"/"+b.Name)
		})
		writeJSON(w, http.StatusOK, struct {
			msbruntime.StorageReport
			Workloads     []workloadStorageIdentity `json:"workloads"`
			BusyWorkloads int                       `json:"busyWorkloads"`
		}{report, workloads, busy})
		return
	}
	request, err := msbruntime.DecodeStoragePruneRequest(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid storage cleanup request"})
		return
	}
	// Never evict the base needed by currently declared but never-started Pods.
	request.RetainImages = append(request.RetainImages, normalizeImage(guestImage), guestImage)
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 2*time.Minute)
	defer cancel()
	report, err := reporter.PruneStorage(ctx, request)
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "storage cleanup did not complete; inspect inventory before retrying", "report": report})
		return
	}
	writeJSON(w, http.StatusOK, report)
}

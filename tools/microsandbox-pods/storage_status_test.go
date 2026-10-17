package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

type storageProbe struct {
	nativeRuntime
	request msbruntime.StoragePruneRequest
}

func (p *storageProbe) Storage(context.Context) (msbruntime.StorageReport, error) {
	return msbruntime.StorageReport{AvailableBytes: 1234}, nil
}
func (p *storageProbe) PruneStorage(_ context.Context, request msbruntime.StoragePruneRequest) (msbruntime.StoragePruneReport, error) {
	p.request = request
	return msbruntime.StoragePruneReport{DryRun: request.DryRun}, nil
}

func TestPodStoragePruningRetainsColdRuntimeBase(t *testing.T) {
	probe := &storageProbe{}
	p := &provider{runner: commandRunner{runtime: probe}}
	response := httptest.NewRecorder()
	p.storageStatus(response, httptest.NewRequest(http.MethodPost, "/v1/storage/prune", strings.NewReader(`{"images":true}`)))
	if response.Code != http.StatusOK || !probe.request.DryRun || !slices.Contains(probe.request.RetainImages, normalizeImage(guestImage)) {
		t.Fatalf("cold runtime image is not retained: %d %+v", response.Code, probe.request)
	}
}

func TestPodStorageInventoryJoinsImmutableCheckpointIdentities(t *testing.T) {
	probe := &storageProbe{}
	e := &podEntry{pod: validPod(), vm: "owned", state: "SLEEPING", snapshotIDs: map[string]string{"owned:sleep-1": "immutable-checkpoint"}}
	p := &provider{runner: commandRunner{runtime: probe}, pods: map[string]*podEntry{"default/app": e}}
	response := httptest.NewRecorder()
	p.storageStatus(response, httptest.NewRequest(http.MethodGet, "/v1/storage", nil))
	for _, text := range []string{`"availableBytes":1234`, `"namespace":"default"`, `"vm":"owned"`, `"immutable-checkpoint"`, `"busyWorkloads":0`} {
		if !strings.Contains(response.Body.String(), text) {
			t.Fatalf("missing %s: %s", text, response.Body)
		}
	}
	e.op.Lock()
	defer e.op.Unlock()
	response = httptest.NewRecorder()
	p.storageStatus(response, httptest.NewRequest(http.MethodGet, "/v1/storage", nil))
	if !strings.Contains(response.Body.String(), `"busyWorkloads":1`) {
		t.Fatal("busy workload omitted without reporting it")
	}
}

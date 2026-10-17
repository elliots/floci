package main

import (
	"context"
	"net/http"
	"strings"
	"testing"

	runtime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

type storageProbe struct {
	executor
	request runtime.StoragePruneRequest
	calls   int
}

func (s *storageProbe) Storage(context.Context) (runtime.StorageReport, error) {
	s.calls++
	return runtime.StorageReport{AvailableBytes: 1234}, nil
}
func (s *storageProbe) PruneStorage(_ context.Context, request runtime.StoragePruneRequest) (runtime.StoragePruneReport, error) {
	s.calls++
	s.request = request
	return runtime.StoragePruneReport{DryRun: request.DryRun}, nil
}

func TestStorageManagementRequiresAuthenticationAndExplicitApplication(t *testing.T) {
	probe := &storageProbe{executor: unexpectedExecutor(t)}
	handler := newHandler(testConfig(), probe)
	for _, path := range []string{"/v1/storage", "/v1/storage/prune"} {
		method := http.MethodGet
		if strings.HasSuffix(path, "prune") {
			method = http.MethodPost
		}
		response := request(handler, method, path, `{}`, "")
		if response.Code != http.StatusUnauthorized || probe.calls != 0 {
			t.Fatal("storage endpoint accepted unauthenticated request")
		}
	}
	response := request(handler, http.MethodPost, "/v1/storage/prune", `{}`, "test-token")
	if response.Code != http.StatusOK || !probe.request.DryRun || probe.request.OlderThanSeconds != 86400 {
		t.Fatalf("unsafe cleanup defaults: %d %s", response.Code, response.Body)
	}
	response = request(handler, http.MethodPost, "/v1/storage/prune", `{"dryRun":false,"olderThanSeconds":3600,"images":true,"retainImages":["app:v1"]}`, "test-token")
	if response.Code != http.StatusOK || probe.request.DryRun || !probe.request.Images || probe.request.RetainImages[0] != "app:v1" {
		t.Fatalf("explicit cleanup lost: %d %s", response.Code, response.Body)
	}
	before := probe.calls
	response = request(handler, http.MethodPost, "/v1/storage/prune", `{"force":true}`, "test-token")
	if response.Code != http.StatusBadRequest || probe.calls != before {
		t.Fatal("unknown cleanup parameter accepted")
	}
	response = request(handler, http.MethodGet, "/v1/storage", ``, "test-token")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"availableBytes":1234`) {
		t.Fatalf("inventory: %d %s", response.Code, response.Body)
	}
}

package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	runtime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

func TestCapacityRefusalHasExplicitNonMutationContract(t *testing.T) {
	handler := newHandler(testConfig(), executorFunc(func(context.Context, []string, []byte) (runtime.Result, error) {
		return runtime.Result{}, errors.Join(runtime.ErrCapacityUnavailable, errors.New("private host details"))
	}))
	response := request(handler, http.MethodPost, "/v1/command", `{"arguments":["start","guest"]}`, "test-token")
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), `"operationStarted":false`) ||
		!strings.Contains(response.Body.String(), `"code":"CapacityUnavailable"`) || strings.Contains(response.Body.String(), "private host") {
		t.Fatalf("capacity refusal: %d %s", response.Code, response.Body)
	}
	if response.Header().Get("Retry-After") != "1" {
		t.Fatal("capacity refusal missing retry guidance")
	}
}

func TestCapacityEndpointsRequireAuthentication(t *testing.T) {
	for _, path := range []string{"/v1/capacity", "/v1/capacity/prepare", "/v1/capacity/cancel", "/v1/capacity/limits"} {
		method := http.MethodPost
		if path == "/v1/capacity" {
			method = http.MethodGet
		}
		response := request(newHandler(testConfig(), unexpectedExecutor(t)), method, path, `{}`, "wrong")
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("capacity endpoint %s accepted unauthenticated access: %d", path, response.Code)
		}
	}
}

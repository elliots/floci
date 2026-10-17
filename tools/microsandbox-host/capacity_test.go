package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
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

type captureCapacityRuntime struct {
	executor
	prepare  func(runtime.AdmissionRequest) (string, error)
	admitted func([]string, string) (runtime.Result, error)
	limits   runtime.CapacityLimits
}

func (c *captureCapacityRuntime) PrepareAdmission(_ context.Context, request runtime.AdmissionRequest) (string, error) {
	return c.prepare(request)
}
func (c *captureCapacityRuntime) ExecuteAdmitted(_ context.Context, args []string, _ []byte, token string) (runtime.Result, error) {
	return c.admitted(args, token)
}
func (c *captureCapacityRuntime) CancelAdmission(context.Context, string) error { return nil }
func (c *captureCapacityRuntime) Capacity(context.Context) (runtime.CapacityStatus, error) {
	return runtime.CapacityStatus{Limits: c.limits, Disk: &runtime.DiskCapacityStatus{ReservedBytes: 1024}}, nil
}
func (c *captureCapacityRuntime) SetCapacityLimits(_ context.Context, limits runtime.CapacityLimits) error {
	c.limits = limits
	return nil
}

func TestCaptureCapacityWireContractAndDiskLimits(t *testing.T) {
	args := []string{"snap", "create", "sleep", "--sandbox", "guest", "--full", "--integrity"}
	c := &captureCapacityRuntime{executor: unexpectedExecutor(t)}
	c.prepare = func(request runtime.AdmissionRequest) (string, error) {
		if !slices.Equal(request.Arguments, args) || request.SourceName != "" {
			t.Fatalf("capture preparation changed: %+v", request)
		}
		return "capture-permit", nil
	}
	c.admitted = func(actual []string, token string) (runtime.Result, error) {
		if !slices.Equal(actual, args) || token != "capture-permit" {
			t.Fatalf("capture dispatch changed: %v %q", actual, token)
		}
		return runtime.Result{Stdout: "saved"}, nil
	}
	handler := newHandler(testConfig(), c)
	body, _ := json.Marshal(runtime.AdmissionRequest{Arguments: args})
	response := request(handler, http.MethodPost, "/v1/capacity/prepare", string(body), "test-token")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"token":"capture-permit"`) {
		t.Fatalf("capture preparation: %d %s", response.Code, response.Body)
	}
	body, _ = json.Marshal(map[string]any{"arguments": args, "admission": "capture-permit"})
	response = request(handler, http.MethodPost, "/v1/command", string(body), "test-token")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"stdout":"saved"`) {
		t.Fatalf("capture dispatch: %d %s", response.Code, response.Body)
	}
	response = request(handler, http.MethodPost, "/v1/capacity/limits", `{"diskMiB":8192,"minFreeDiskMiB":2048}`, "test-token")
	if response.Code != http.StatusOK || c.limits.DiskMiB != 8192 || c.limits.MinFreeDiskMiB != 2048 {
		t.Fatalf("disk limits: %d %s", response.Code, response.Body)
	}
	response = request(handler, http.MethodGet, "/v1/capacity", "", "test-token")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"reservedBytes":1024`) {
		t.Fatalf("disk capacity status: %d %s", response.Code, response.Body)
	}
	c.prepare = func(runtime.AdmissionRequest) (string, error) { return "", runtime.ErrCapacityUnavailable }
	response = request(handler, http.MethodPost, "/v1/capacity/prepare", `{"arguments":["snap","create","sleep","--sandbox","guest"]}`, "test-token")
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), `"operationStarted":false`) {
		t.Fatalf("capture refusal: %d %s", response.Code, response.Body)
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

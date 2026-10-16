package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"

	microsandboxruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

func TestGuestTaskPlanDoesNotExecuteAndKeepsEnvironmentInStdin(t *testing.T) {
	w := request(newHandler(testConfig(), unexpectedExecutor(t)), http.MethodPost, "/v1/guest-task/plan",
		`{"action":"start","id":"worker","command":["ctr","run","image","worker"],"environment":{"PASSWORD":"secret'value"}}`, "test-token")
	var plan struct {
		Arguments []string
		Stdin     string
	}
	if err := json.Unmarshal(w.Body.Bytes(), &plan); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK || strings.Contains(strings.Join(plan.Arguments, " "), "secret") || !strings.Contains(plan.Stdin, "secret") {
		t.Fatalf("invalid private-input plan: status=%d", w.Code)
	}
}

func TestGuestTaskRoutesValidateBeforeExecution(t *testing.T) {
	for _, path := range []string{"/v1/guest-task", "/v1/guest-task/plan"} {
		handler := newHandler(testConfig(), unexpectedExecutor(t))
		if w := request(handler, http.MethodPost, path, `{}`, "wrong"); w.Code != http.StatusUnauthorized {
			t.Fatal(w.Code)
		}
		for _, body := range []string{`{"action":"start","id":"../escape","command":["sh"]}`, `{"action":"status","id":"worker","unknown":true}`, `{"action":"status","id":"worker"}{}`} {
			if w := request(handler, http.MethodPost, path, body, "test-token"); w.Code != http.StatusBadRequest {
				t.Fatalf("%s: %d", path, w.Code)
			}
		}
	}
}

func TestGuestTaskExecUsesTheNamedVMAndPreservesUnknownOutcome(t *testing.T) {
	calls := 0
	handler := newHandler(testConfig(), executorFunc(func(_ context.Context, args []string, input []byte) (microsandboxruntime.Result, error) {
		calls++
		if !slices.Equal(args[:4], []string{"exec", "--stream", "owned-vm", "--"}) || len(input) != 0 {
			t.Fatal("wrong guest transport")
		}
		return microsandboxruntime.Result{}, errors.New("private-token")
	}))
	w := request(handler, http.MethodPost, "/v1/guest-task", `{"vm":"owned-vm","action":"status","id":"worker"}`, "test-token")
	if w.Code != http.StatusInternalServerError || calls != 1 || strings.Contains(w.Body.String(), "private-token") {
		t.Fatal(w.Code, calls)
	}
}

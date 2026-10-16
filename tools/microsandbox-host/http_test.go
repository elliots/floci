package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	microsandboxruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

type executorFunc func(context.Context, []string, []byte) (microsandboxruntime.Result, error)

func (f executorFunc) Execute(ctx context.Context, args []string, input []byte) (microsandboxruntime.Result, error) {
	return f(ctx, args, input)
}

func testConfig() hostConfig {
	return hostConfig{home: "/dedicated/home", token: "test-token", version: "dev", timeout: time.Second}
}

func request(handler http.Handler, method, path, body, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

func unexpectedExecutor(t *testing.T) executor {
	t.Helper()
	return executorFunc(func(context.Context, []string, []byte) (microsandboxruntime.Result, error) {
		t.Error("request must not execute a runtime operation")
		return microsandboxruntime.Result{}, errors.New("unexpected execution")
	})
}

func TestHealthContractWithoutRuntime(t *testing.T) {
	w := request(newHandler(testConfig(), nil), http.MethodGet, "/health", "", "")
	var health map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &health); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK || health["status"] != "ok" || health["name"] != "microsandbox" || health["contract"] != "1" || health["version"] != "dev" {
		t.Fatalf("health contract: %d %s", w.Code, w.Body)
	}
}

func TestFunctionalEndpointsRequireConfigurationAndAuthorization(t *testing.T) {
	for _, path := range []string{"/v1/command", "/v1/port"} {
		t.Run(path, func(t *testing.T) {
			for _, missing := range []string{"home", "token", "runtime"} {
				config := testConfig()
				runtime := unexpectedExecutor(t)
				switch missing {
				case "home":
					config.home = ""
				case "token":
					config.token = ""
				case "runtime":
					runtime = nil
				}
				w := request(newHandler(config, runtime), http.MethodPost, path, `{}`, "test-token")
				if w.Code != http.StatusServiceUnavailable {
					t.Fatalf("missing %s: %d %s", missing, w.Code, w.Body)
				}
			}
			w := request(newHandler(testConfig(), unexpectedExecutor(t)), http.MethodPost, path, "invalid json", "wrong-token")
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("unauthorized: %d %s", w.Code, w.Body)
			}
		})
	}
}

func TestCommandPreservesLiteralArgumentsInputAndGuestExit(t *testing.T) {
	args := []string{"exec", "guest", "sh", "-c", `printf '%s' '$HOME; $(id)'`}
	input := "first line\nsecond line\n"
	body, err := json.Marshal(map[string]any{"arguments": args, "input": input})
	if err != nil {
		t.Fatal(err)
	}
	want := microsandboxruntime.Result{ExitCode: 17, Stdout: "guest output\n", Stderr: "guest error\n"}
	runtime := executorFunc(func(ctx context.Context, got []string, stdin []byte) (microsandboxruntime.Result, error) {
		if !slices.Equal(got, args) || string(stdin) != input {
			t.Errorf("command changed: %q, input %q", got, stdin)
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("operation needs a bounded deadline")
		}
		return want, nil
	})
	w := request(newHandler(testConfig(), runtime), http.MethodPost, "/v1/command", string(body), "test-token")
	var got microsandboxruntime.Result
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK || got != want {
		t.Fatalf("guest result: %d %+v", w.Code, got)
	}
}

func TestMalformedCommandsAreRejected(t *testing.T) {
	for _, body := range []string{`{`, `null`, `{}`, `{"arguments":[]}`, `{"arguments":"exec"}`, `{"arguments":[null]}`, `{"arguments":["exec","bad\u0000name"]}`} {
		t.Run(body, func(t *testing.T) {
			w := request(newHandler(testConfig(), unexpectedExecutor(t)), http.MethodPost, "/v1/command", body, "test-token")
			if w.Code != http.StatusBadRequest {
				t.Fatalf("malformed request: %d %s", w.Code, w.Body)
			}
		})
	}
}

func TestErrorsDoNotLeakArgumentsOrInventKnownExit(t *testing.T) {
	for _, test := range []struct {
		name   string
		err    error
		status int
	}{
		{"invalid", fmt.Errorf("secret: %w", microsandboxruntime.ErrInvalidArgument), http.StatusBadRequest},
		{"runtime", errors.New("secret guest credentials in SDK error"), http.StatusInternalServerError},
		{"timeout", context.DeadlineExceeded, http.StatusInternalServerError},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime := executorFunc(func(context.Context, []string, []byte) (microsandboxruntime.Result, error) {
				return microsandboxruntime.Result{ExitCode: 1, Stderr: "secret"}, test.err
			})
			w := request(newHandler(testConfig(), runtime), http.MethodPost, "/v1/command", `{"arguments":["exec","secret"]}`, "test-token")
			if w.Code != test.status || strings.Contains(w.Body.String(), "secret") || strings.Contains(w.Body.String(), "exitCode") {
				t.Fatalf("unsafe error result: %d %s", w.Code, w.Body)
			}
		})
	}
}

func TestAcceptedMutationOutlivesDisconnectedClient(t *testing.T) {
	started := make(chan struct{})
	finish := make(chan struct{})
	completed := make(chan error, 1)
	runtime := executorFunc(func(ctx context.Context, _ []string, _ []byte) (microsandboxruntime.Result, error) {
		close(started)
		<-finish
		completed <- ctx.Err()
		return microsandboxruntime.Result{}, nil
	})
	config := testConfig()
	config.timeout = time.Minute
	server := httptest.NewServer(newHandler(config, runtime))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/command", strings.NewReader(`{"arguments":["start","guest"]}`))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer test-token")
	clientDone := make(chan error, 1)
	go func() {
		response, err := server.Client().Do(r)
		if response != nil {
			response.Body.Close()
		}
		clientDone <- err
	}()
	<-started
	cancel()
	if err := <-clientDone; !errors.Is(err, context.Canceled) {
		t.Errorf("client did not cancel: %v", err)
	}
	close(finish)
	if err := <-completed; err != nil {
		t.Fatalf("accepted mutation was canceled with the HTTP connection: %v", err)
	}
}

func TestOperationTimeoutIsUnknownOutcome(t *testing.T) {
	config := testConfig()
	config.timeout = time.Millisecond
	runtime := executorFunc(func(ctx context.Context, _ []string, _ []byte) (microsandboxruntime.Result, error) {
		<-ctx.Done()
		return microsandboxruntime.Result{}, ctx.Err()
	})
	w := request(newHandler(config, runtime), http.MethodPost, "/v1/command", `{"arguments":["start","guest"]}`, "test-token")
	if w.Code != http.StatusInternalServerError || strings.Contains(w.Body.String(), "exitCode") {
		t.Fatalf("timeout result: %d %s", w.Code, w.Body)
	}
}

func TestCandidatePortAndJSONRoutingErrors(t *testing.T) {
	handler := newHandler(testConfig(), unexpectedExecutor(t))
	w := request(handler, http.MethodPost, "/v1/port", "", "test-token")
	var port struct {
		Port int `json:"port"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &port); err != nil || w.Code != http.StatusOK || port.Port < 1 || port.Port > 65535 {
		t.Fatalf("candidate port: %d %s (%v)", w.Code, w.Body, err)
	}
	for _, path := range []string{"/v1/command", "/unknown"} {
		w := request(handler, http.MethodGet, path, "", "test-token")
		if path == "/v1/command" && (w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != "POST") {
			t.Fatalf("method routing: %d %s", w.Code, w.Header())
		}
		var body map[string]string
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body["error"] == "" || w.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("JSON routing error: %d %s (%v)", w.Code, w.Body, err)
		}
	}
}

type repeatedByteReader struct{}

func (repeatedByteReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

func TestTransportLimits(t *testing.T) {
	handler := newHandler(testConfig(), unexpectedExecutor(t))
	r := httptest.NewRequest(http.MethodPost, "/v1/command", io.LimitReader(repeatedByteReader{}, maxTransportBytes+1))
	r.Header.Set("Authorization", "Bearer test-token")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize request: %d %s", w.Code, w.Body)
	}
	runtime := executorFunc(func(context.Context, []string, []byte) (microsandboxruntime.Result, error) {
		return microsandboxruntime.Result{Stdout: strings.Repeat("x", maxTransportBytes+1)}, nil
	})
	w = request(newHandler(testConfig(), runtime), http.MethodPost, "/v1/command", `{"arguments":["exec","guest"]}`, "test-token")
	if w.Code != http.StatusInternalServerError || strings.Contains(w.Body.String(), "exitCode") {
		t.Fatalf("oversize result: %d %s", w.Code, w.Body)
	}
}

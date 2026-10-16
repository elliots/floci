package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	microsandboxruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

const maxTransportBytes = 64 * 1024 * 1024

type executor interface {
	Execute(context.Context, []string, []byte) (microsandboxruntime.Result, error)
}

type hostConfig struct {
	home, token, version string
	timeout              time.Duration
}

func (c hostConfig) configured() bool {
	return strings.TrimSpace(c.home) != "" && strings.TrimSpace(c.token) != ""
}

type hostHandler struct {
	config  hostConfig
	runtime executor
}

func newHandler(config hostConfig, runtime executor) http.Handler {
	h := &hostHandler{config: config, runtime: runtime}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("POST /v1/command", h.command)
	mux.HandleFunc("POST /v1/port", h.port)
	mux.HandleFunc("POST /v1/guest-task/plan", h.guestTaskPlan)
	mux.HandleFunc("POST /v1/guest-task", h.guestTask)
	mux.HandleFunc("GET /v1/exec", h.execStream)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, pattern := mux.Handler(r)
		if pattern == "" {
			if r.URL.Path == "/health" || r.URL.Path == "/v1/command" || r.URL.Path == "/v1/port" || r.URL.Path == "/v1/guest-task/plan" || r.URL.Path == "/v1/guest-task" || r.URL.Path == "/v1/exec" {
				allow := "POST"
				if r.URL.Path == "/health" || r.URL.Path == "/v1/exec" {
					allow = "GET, HEAD"
				}
				w.Header().Set("Allow", allow)
				writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
			} else {
				writeError(w, http.StatusNotFound, "Not found")
			}
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (h *hostHandler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok", "name": "microsandbox", "version": h.config.version, "contract": "1",
		"networkPolicy": h.networkPolicy(),
	})
}

func (h *hostHandler) authorize(w http.ResponseWriter, r *http.Request) bool {
	if !h.config.configured() || h.runtime == nil {
		writeError(w, http.StatusServiceUnavailable, "Configure MICROSANDBOX_HOME and MICROSANDBOX_TOKEN")
		return false
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+h.config.token)) != 1 {
		writeError(w, http.StatusUnauthorized, "Unauthorized")
		return false
	}
	if expected := r.Header.Get("X-Floci-Network-Policy"); expected != "" && expected != h.networkPolicy() {
		writeError(w, http.StatusConflict, "Runtime network boundary does not match the requested environment")
		return false
	}
	return true
}

func (h *hostHandler) networkPolicy() string {
	if runtime, ok := h.runtime.(interface{ NetworkPolicyFingerprint() string }); ok {
		return runtime.NetworkPolicyFingerprint()
	}
	return ""
}

func (h *hostHandler) port(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r) {
		return
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Cannot allocate host port")
		return
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		writeError(w, http.StatusInternalServerError, "Cannot release candidate host port")
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"port": port})
}

func (h *hostHandler) command(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r) {
		return
	}
	var request struct {
		Arguments []*string `json:"arguments"`
		Input     string    `json:"input"`
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxTransportBytes))
	if err != nil {
		if _, tooLarge := errors.AsType[*http.MaxBytesError](err); tooLarge {
			writeError(w, http.StatusRequestEntityTooLarge, "Request exceeds 64 MiB")
		} else {
			writeError(w, http.StatusBadRequest, "Cannot read runtime request")
		}
		return
	}
	if json.Unmarshal(body, &request) != nil || len(request.Arguments) == 0 {
		writeError(w, http.StatusBadRequest, "A command argument array is required")
		return
	}
	args := make([]string, len(request.Arguments))
	for i, value := range request.Arguments {
		if value == nil || strings.ContainsRune(*value, '\x00') {
			writeError(w, http.StatusBadRequest, "Command arguments must be strings without NUL")
			return
		}
		args[i] = *value
	}
	// A lost HTTP connection does not cancel an accepted runtime mutation.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), h.config.timeout)
	defer cancel()
	result, err := h.runtime.Execute(ctx, args, []byte(request.Input))
	if errors.Is(err, microsandboxruntime.ErrInvalidArgument) {
		writeError(w, http.StatusBadRequest, "Unsupported runtime command or options")
		return
	}
	if err != nil {
		// Runtime errors and command arguments can contain application credentials.
		slog.Warn("runtime operation failed; completion may be unknown")
		writeError(w, http.StatusInternalServerError, "Runtime operation failed; inspect its state before retrying")
		return
	}
	if len(result.Stdout) > maxTransportBytes || len(result.Stderr) > maxTransportBytes {
		writeError(w, http.StatusInternalServerError, "Runtime output exceeded 64 MiB; completion may be unknown")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Debug("HTTP response disconnected")
	}
}

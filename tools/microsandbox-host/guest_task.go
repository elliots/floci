package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/floci-io/floci/tools/microsandbox-runtime/guesttask"
)

type guestTaskRequest struct {
	guesttask.Request
	VM string `json:"vm,omitempty"`
}

func (h *hostHandler) readGuestTask(w http.ResponseWriter, r *http.Request) (guestTaskRequest, guesttask.Command, bool) {
	var request guestTaskRequest
	if !h.authorize(w, r) {
		return request, guesttask.Command{}, false
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxTransportBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid guest task request")
		return request, guesttask.Command{}, false
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		writeError(w, http.StatusBadRequest, "A single guest task request is required")
		return request, guesttask.Command{}, false
	}
	plan, err := guesttask.Plan(request.Request)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Invalid guest task action or options")
		return request, guesttask.Command{}, false
	}
	return request, plan, true
}

// Planning has no guest side effects. Java callers execute the returned plan
// through their existing operation journal to retain uncertain-outcome fencing.
func (h *hostHandler) guestTaskPlan(w http.ResponseWriter, r *http.Request) {
	request, plan, ok := h.readGuestTask(w, r)
	if !ok {
		return
	}
	if request.VM != "" {
		writeError(w, http.StatusBadRequest, "Planning does not accept a VM")
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Arguments []string `json:"arguments"`
		Stdin     string   `json:"stdin"`
	}{plan.Args, string(plan.Stdin)})
}

func (h *hostHandler) guestTask(w http.ResponseWriter, r *http.Request) {
	request, plan, ok := h.readGuestTask(w, r)
	if !ok {
		return
	}
	if request.VM == "" || strings.HasPrefix(request.VM, "-") || strings.ContainsRune(request.VM, 0) {
		writeError(w, http.StatusBadRequest, "A VM name is required")
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), h.config.timeout)
	defer cancel()
	args := append([]string{"exec", "--stream", request.VM, "--"}, plan.Args...)
	result, err := h.runtime.Execute(ctx, args, plan.Stdin)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Guest task operation failed; inspect its state before retrying")
		return
	}
	if len(result.Stdout) > maxTransportBytes || len(result.Stderr) > maxTransportBytes {
		writeError(w, http.StatusInternalServerError, "Guest task output exceeded 64 MiB")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

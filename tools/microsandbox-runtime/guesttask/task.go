// Package guesttask builds the container supervisor commands shared by native
// Kubernetes pods and ECS tasks. The caller owns image preparation, containerd
// arguments, readiness and restart policy. One supervisor runs inside each VM.
package guesttask

import (
	"errors"
	"fmt"
	"maps"
	"path"
	"slices"
	"strconv"
	"strings"
)

const Root = "/tmp/floci-tasks"
const Containerd = "ctr --address /run/containerd/containerd.sock"
const BootstrapPath = Root + "/supervisor.sh"

var ErrInvalidArgument = errors.New("invalid guest task argument")

type Request struct {
	Action           string            `json:"action"`
	ID               string            `json:"id"`
	Command          []string          `json:"command,omitempty"`
	Environment      map[string]string `json:"environment,omitempty"`
	WorkingDirectory string            `json:"workingDirectory,omitempty"`
	Signal           string            `json:"signal,omitempty"`
	Tail             int               `json:"tail,omitzero"`
}

// Command contains guest exec arguments and private stdin. Launch arguments and
// environment values occur only in Stdin, never in host process arguments.
type Command struct {
	Args  []string
	Stdin []byte
}

type Status struct {
	State      string `json:"state"`
	PID        int    `json:"pid,omitzero"`
	ExitCode   *int   `json:"exitCode,omitempty"`
	StartedAt  string `json:"startedAt,omitempty"`
	FinishedAt string `json:"finishedAt,omitempty"`
}

func Directory(id string) (string, error) {
	if len(id) < 1 || len(id) > 128 || id == "." || id == ".." {
		return "", fmt.Errorf("%w: task ID must be 1..128 safe characters", ErrInvalidArgument)
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return "", fmt.Errorf("%w: task ID contains an unsafe character", ErrInvalidArgument)
		}
	}
	return path.Join(Root, id), nil
}

func Plan(request Request) (Command, error) {
	if request.Action == "bootstrap" {
		if request.ID != "" || len(request.Command) != 0 || len(request.Environment) != 0 || request.WorkingDirectory != "" || request.Signal != "" || request.Tail != 0 {
			return Command{}, fmt.Errorf("%w: bootstrap does not accept task options", ErrInvalidArgument)
		}
		return Command{Args: []string{"/bin/sh", "-ec", "umask 077; mkdir -p " + Root + "; cat >" + BootstrapPath + ".pending; chmod 0700 " + BootstrapPath + ".pending; mv " + BootstrapPath + ".pending " + BootstrapPath}, Stdin: []byte(BootstrapScript())}, nil
	}
	return planAt(Root, request, "")
}

// StartWithCompletion runs a trusted accounting hook after the child exits and
// before publishing its exit status. It is intentionally not a JSON field.
// The hook must not start another long-lived process. A failed hook is recorded
// separately from the child's exit code, so callers can fail closed.
func StartWithCompletion(request Request, complete string) (Command, error) {
	if request.Action != "start" {
		return Command{}, fmt.Errorf("%w: completion hook requires start", ErrInvalidArgument)
	}
	return planAt(Root, request, complete)
}

func planAt(root string, request Request, complete string) (Command, error) {
	dir, err := Directory(request.ID)
	if err != nil {
		return Command{}, err
	}
	dir = path.Join(root, path.Base(dir))
	if request.Action != "start" && (len(request.Command) != 0 || len(request.Environment) != 0 || request.WorkingDirectory != "") {
		return Command{}, fmt.Errorf("%w: launch fields require start", ErrInvalidArgument)
	}
	if request.Signal != "" && request.Action != "stop" || request.Tail != 0 && request.Action != "logs" {
		return Command{}, fmt.Errorf("%w: option does not apply to action", ErrInvalidArgument)
	}
	var script string
	var input []byte
	switch request.Action {
	case "start":
		if len(request.Command) == 0 || request.Command[0] == "" {
			return Command{}, fmt.Errorf("%w: start requires a command", ErrInvalidArgument)
		}
		for _, value := range append(slices.Clone(request.Command), request.WorkingDirectory) {
			if strings.ContainsRune(value, 0) {
				return Command{}, fmt.Errorf("%w: command contains NUL", ErrInvalidArgument)
			}
		}
		for key, value := range request.Environment {
			if !environmentName(key) || strings.ContainsRune(value, 0) {
				return Command{}, fmt.Errorf("%w: invalid environment entry", ErrInvalidArgument)
			}
		}
		input = []byte(processScript(dir, request, complete))
		script = `set -eu
umask 077
mkdir -p ` + quote(root) + ` ` + quote(dir) + `
lock=` + quote(dir+"/submission") + `
mkdir "$lock" || { echo 'task is already queued or active' >&2; exit 1; }
trap 'rmdir "$lock"' EXIT
test ! -f ` + quote(dir+"/request") + ` && test ! -f ` + quote(dir+"/active") + ` || { echo 'task is already queued or active' >&2; exit 1; }
cat >` + quote(dir+"/request.pending") + `
if test -f ` + quote(dir+"/application.log") + `; then mv ` + quote(dir+"/application.log") + ` ` + quote(dir+"/application.previous.log") + `; fi
if test -f ` + quote(dir+"/status.json") + `; then mv ` + quote(dir+"/status.json") + ` ` + quote(dir+"/previous.json") + `; fi
rm -f ` + quote(dir+"/application.exit") + ` ` + quote(dir+"/application.oom-error") + `
printf '%s\n' '{"state":"queued"}' >` + quote(dir+"/status.pending") + `
mv ` + quote(dir+"/status.pending") + ` ` + quote(dir+"/status.json") + `
mv ` + quote(dir+"/request.pending") + ` ` + quote(dir+"/request") + `
printf '%s\n' '{"state":"queued"}'
`
	case "status":
		script = "if test -f " + quote(dir+"/status.json") + "; then cat " + quote(dir+"/status.json") + "; else printf '%s\\n' '{\"state\":\"not_found\"}'; fi"
	case "logs":
		if request.Tail < 0 {
			return Command{}, fmt.Errorf("%w: tail must not be negative", ErrInvalidArgument)
		}
		script = "cat " + quote(dir+"/application.log")
		if request.Tail > 0 {
			script = "tail -n " + strconv.Itoa(request.Tail) + " " + quote(dir+"/application.log")
		}
	case "stop":
		signal := request.Signal
		if signal == "" {
			signal = "SIGTERM"
		}
		if !slices.Contains([]string{"SIGTERM", "SIGKILL", "SIGINT", "SIGHUP", "SIGQUIT", "SIGUSR1", "SIGUSR2"}, signal) {
			return Command{}, fmt.Errorf("%w: unsupported stop signal", ErrInvalidArgument)
		}
		script = "test -f " + quote(dir+"/application.exit") + " || " + Containerd + " tasks kill --signal " + signal + " " + quote(request.ID) + " || test -f " + quote(dir+"/application.exit")
	default:
		return Command{}, fmt.Errorf("%w: unsupported action", ErrInvalidArgument)
	}
	return Command{Args: []string{"/bin/sh", "-ec", script}, Stdin: input}, nil
}

// BootstrapScript runs as the VM's persistent entrypoint. Child containers are
// ordinary guest processes, never microsandbox managed detached exec jobs.
func BootstrapScript() string {
	return "containerd --log-level=error &\n" + SupervisorScript()
}

func SupervisorScript() string { return supervisorAt(Root) }

func supervisorAt(root string) string {
	return `set -eu
umask 077
mkdir -p ` + quote(root) + `
while true; do
    for task in ` + quote(root) + `/*; do
        test -f "$task/request" || continue
        if mv "$task/request" "$task/active"; then
            (sh "$task/active"; rm -f "$task/active") </dev/null >/dev/null 2>&1 &
        fi
    done
    sleep 0.1
done
`
}

func processScript(dir string, request Request, complete string) string {
	var command strings.Builder
	for _, key := range slices.Sorted(maps.Keys(request.Environment)) {
		command.WriteString("export " + key + "=" + quote(request.Environment[key]) + "\n")
	}
	if request.WorkingDirectory != "" {
		command.WriteString("cd " + quote(request.WorkingDirectory) + " || exit 126\n")
	}
	command.WriteString("exec " + join(request.Command))
	if complete == "" {
		complete = ":"
	}
	return `#!/bin/sh
set +e
umask 077
started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
(
` + command.String() + `
) >` + quote(dir+"/application.log") + ` 2>&1 &
pid=$!
printf '{"state":"running","pid":%s,"startedAt":"%s"}\n' "$pid" "$started" >` + quote(dir+"/status.pending") + ` && mv ` + quote(dir+"/status.pending") + ` ` + quote(dir+"/status.json") + ` || exit 125
wait "$pid"
code=$?
(` + complete + `)
if test $? -ne 0; then
    printf '%s\n' 'container completion accounting failed' >` + quote(dir+"/application.oom-error") + ` || exit 125
fi
finished=$(date -u +%Y-%m-%dT%H:%M:%SZ)
printf '%s\n' "$code" >` + quote(dir+"/application.exit.pending") + ` && mv ` + quote(dir+"/application.exit.pending") + ` ` + quote(dir+"/application.exit") + ` || exit 125
printf '{"state":"exited","pid":%s,"exitCode":%s,"startedAt":"%s","finishedAt":"%s"}\n' "$pid" "$code" "$started" "$finished" >` + quote(dir+"/status.pending") + ` && mv ` + quote(dir+"/status.pending") + ` ` + quote(dir+"/status.json") + ` || exit 125
`
}

func environmentName(value string) bool {
	if value == "" {
		return false
	}
	for i, c := range value {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' || i > 0 && c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

func quote(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'" }
func join(args []string) string {
	parts := make([]string, len(args))
	for i, value := range args {
		parts[i] = quote(value)
	}
	return strings.Join(parts, " ")
}

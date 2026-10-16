// Package microsandboxruntime adapts Floci's existing runtime command contract
// to the pinned microsandbox Go SDK. It is shared by the host bridge and pods.
package microsandboxruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

const MaxOutputBytes = 64 << 20

// ErrInvalidArgument identifies rejected requests before any runtime operation.
var ErrInvalidArgument = errors.New("invalid microsandbox command")

var errOutputLimit = errors.New("microsandbox command output exceeds limit")

type Config struct {
	Executable        string
	Home              string
	NetworkConfigFile string
}

type Result struct {
	ExitCode int    `json:"exitCode"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
}

type backend interface {
	initialize() error
	execute(context.Context, command, io.Reader, io.Writer, io.Writer) (int, error)
}

type Client struct {
	config   Config
	core     backend
	cli      func(context.Context, []string, io.Writer, io.Writer) (int, error)
	boundary *nativeNetworkBoundary
}

// New validates configuration without loading the SDK or running a subprocess.
// The native SDK caches one runtime home and executable per process. Clients in
// the same process must use the same configuration.
func New(config Config) (*Client, error) {
	if strings.ContainsRune(config.Executable, 0) || strings.ContainsRune(config.Home, 0) {
		return nil, invalid("configuration contains a NUL byte")
	}
	if config.Executable == "" {
		config.Executable = "msb"
	}
	if config.Home == "" {
		config.Home = os.Getenv("MSB_HOME")
	}
	if config.Home == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, errors.New("cannot resolve microsandbox home")
		}
		config.Home = filepath.Join(home, ".microsandbox")
	}
	home, err := filepath.Abs(config.Home)
	if err != nil {
		return nil, errors.New("cannot resolve microsandbox home")
	}
	config.Home = filepath.Clean(home)
	if config.NetworkConfigFile == "" {
		config.NetworkConfigFile = os.Getenv("FLOCI_NETWORK_CONFIG_FILE")
	}
	boundary, err := readNetworkBoundary(config.NetworkConfigFile)
	if err != nil {
		return nil, err
	}
	c := &Client{config: config, core: &sdkBackend{config: config}, boundary: boundary}
	c.cli = c.executeCLI
	return c, nil
}

// Execute captures bounded, separate stdout and stderr. An SDK/transport error
// does not imply that a requested mutation failed to happen. Callers must retain
// their operation journal and reconcile it. Guest exit codes are normal results.
func (c *Client) Execute(ctx context.Context, args []string, input []byte) (Result, error) {
	var stdout, stderr bytes.Buffer
	out, errs := &boundedWriter{dst: &stdout}, &boundedWriter{dst: &stderr}
	code, err := c.execute(ctx, args, bytes.NewReader(input), len(input) != 0,
		out, errs)
	if outputErr := errors.Join(out.failure(), errs.failure()); outputErr != nil {
		err = errors.Join(err, safeError("output", outputErr))
	}
	if err != nil {
		return Result{}, err
	}
	return Result{ExitCode: code, Stdout: stdout.String(), Stderr: stderr.String()}, nil
}

// ExecuteStream streams a guest exec without allocating output-sized buffers.
// The reader should be cancellable by its owner when ctx ends (as Kubernetes
// remote-command streams are). Streaming does not impose a total-output limit.
func (c *Client) ExecuteStream(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) (int, error) {
	return c.ExecuteStreamWithOptions(ctx, args, stdin, stdout, stderr, StreamOptions{})
}

// ExecuteStreamWithOptions supports interactive terminals without changing the
// command contract used by buffered lifecycle operations. Cancellation kills
// this attached exec session, not the sandbox or its application processes.
func (c *Client) ExecuteStreamWithOptions(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, options StreamOptions) (int, error) {
	if len(args) == 0 || args[0] != "exec" {
		return 0, invalid("streaming requires exec")
	}
	request, err := parse(args)
	if err != nil {
		return 0, err
	}
	if request.detached {
		return 0, invalid("streaming requires attached exec")
	}
	return c.executeWithOptions(ctx, args, stdin, stdin != nil, stdout, stderr, options)
}

func (c *Client) execute(ctx context.Context, args []string, stdin io.Reader, hasInput bool, stdout, stderr io.Writer) (int, error) {
	return c.executeWithOptions(ctx, args, stdin, hasInput, stdout, stderr, StreamOptions{})
}

func (c *Client) executeWithOptions(ctx context.Context, args []string, stdin io.Reader, hasInput bool, stdout, stderr io.Writer, options StreamOptions) (int, error) {
	req, err := parse(args)
	if err != nil {
		return 0, err
	}
	if err := options.validate(req); err != nil {
		return 0, err
	}
	req.stream = options
	req.boundary = c.boundary
	if c.boundary != nil && (req.op == "run" || req.op == "snap restore" && req.config.Network != nil) {
		req.config.Network, err = c.boundary.restrict(req.config.Network)
		if err != nil {
			return 0, err
		}
		if req.op == "run" {
			if _, exists := req.config.Labels[networkPolicyLabel]; exists {
				return 0, invalid("network policy label is reserved")
			}
			args, err = c.boundary.launchArguments(args, req.config.Network, c.config.Home)
			if err != nil {
				return 0, err
			}
		}
	}
	if hasInput && (req.op != "exec" || req.noStdin || req.detached) {
		return 0, invalid("stdin is only supported for attached exec")
	}
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	if req.fallback {
		if req.op == "run" {
			if err := c.core.initialize(); err != nil {
				return 0, safeError("initialize", err)
			}
			unlock, err := lockMutation(ctx, req)
			if err != nil {
				return 0, safeError(req.op, err)
			}
			defer unlock()
		}
		code, err := c.cli(ctx, args, stdout, stderr)
		if req.op == "run" && err == nil && code != 0 {
			err = errors.New("detached launch failed with unknown runtime outcome")
		}
		return code, safeError(req.op, err)
	}
	if !hasInput {
		stdin = nil
	}
	code, err := c.core.execute(ctx, req, stdin, stdout, stderr)
	return code, safeError(req.op, err)
}

type boundedWriter struct {
	mu      sync.Mutex
	dst     io.Writer
	written int
	err     error
}

func (w *boundedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(p) > MaxOutputBytes-w.written {
		w.err = errOutputLimit
		return 0, errOutputLimit
	}
	n, err := w.dst.Write(p)
	w.written += n
	if err != nil {
		w.err = err
	}
	return n, err
}

func (w *boundedWriter) failure() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func invalid(reason string) error { return fmt.Errorf("%w: %s", ErrInvalidArgument, reason) }

// Native errors can contain complete guest command lines, environment values or
// registry credentials. Preserve errors.Is without putting that text on the wire.
type operationError struct {
	operation string
	cause     error
}

func (e *operationError) Error() string {
	return "microsandbox " + e.operation + " failed; reconcile runtime state before retrying"
}
func (e *operationError) Unwrap() error { return e.cause }
func safeError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return &operationError{operation: operation, cause: err}
}

func (c *Client) executeCLI(ctx context.Context, args []string, stdout, stderr io.Writer) (int, error) {
	cmd := exec.CommandContext(ctx, c.config.Executable, args...)
	cmd.Env = append(os.Environ(), "MSB_HOME="+c.config.Home)
	cmd.Stdout, cmd.Stderr = stdout, stderr
	err := cmd.Run()
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		return exit.ExitCode(), nil
	}
	return 0, err
}

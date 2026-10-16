package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

// Native operations have one production implementation. Test fixtures inject
// their command simulator explicitly rather than falling back to a CLI.
type nativeRuntime interface {
	Execute(context.Context, []string, []byte) (msbruntime.Result, error)
	ExecuteStream(context.Context, []string, io.Reader, io.Writer, io.Writer) (int, error)
	PrepareAdmission(context.Context, msbruntime.AdmissionRequest) (string, error)
	CancelAdmission(context.Context, string) error
	ExecuteAdmitted(context.Context, []string, []byte, string) (msbruntime.Result, error)
}

type interactiveRuntime interface {
	ExecuteStreamWithOptions(context.Context, []string, io.Reader, io.Writer, io.Writer, msbruntime.StreamOptions) (int, error)
}

func newCommandRunner(cfg config) (commandRunner, error) {
	client, err := msbruntime.New(msbruntime.Config{Executable: cfg.msb, Home: cfg.home})
	if err != nil {
		return commandRunner{}, &nativeCommandError{operation: "initialize", exitCode: -1, cause: err}
	}
	return commandRunner{cfg: cfg, runtime: client}, nil
}

func (r commandRunner) nativeOutput(ctx context.Context, args []string, input []byte) ([]byte, error) {
	if r.runtime == nil {
		return nil, errors.New("native SDK runtime is not configured")
	}
	var result msbruntime.Result
	var err error
	if token, _ := ctx.Value(nativeAdmissionKey{}).(string); token != "" {
		result, err = r.runtime.ExecuteAdmitted(ctx, args, input, token)
	} else {
		result, err = r.runtime.Execute(ctx, args, input)
	}
	if err != nil || result.ExitCode != 0 {
		return nil, nativeFailure(args, result.ExitCode, err)
	}
	return []byte(result.Stdout), nil
}

type nativeAdmissionKey struct{}

func (r commandRunner) prepareAdmission(ctx context.Context, args []string, source string) (string, error) {
	return r.runtime.PrepareAdmission(ctx, msbruntime.AdmissionRequest{Arguments: args, SourceName: source})
}

func (r commandRunner) nativeStream(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if r.runtime == nil {
		return errors.New("native SDK runtime is not configured")
	}
	exitCode, err := r.runtime.ExecuteStream(ctx, args, stdin, stdout, stderr)
	if err != nil || exitCode != 0 {
		return nativeFailure(args, exitCode, err)
	}
	return nil
}

func (r commandRunner) nativeTerminal(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, options msbruntime.StreamOptions) error {
	runtime, ok := r.runtime.(interactiveRuntime)
	if !ok {
		return errors.New("native SDK terminal transport is not configured")
	}
	exitCode, err := runtime.ExecuteStreamWithOptions(ctx, args, stdin, stdout, stderr, options)
	if err != nil || exitCode != 0 {
		return nativeFailure(args, exitCode, err)
	}
	return nil
}

func nativeFailure(args []string, exitCode int, err error) error {
	operation := "command"
	if len(args) > 0 {
		operation = args[0]
	}
	if err != nil {
		exitCode = -1
	}
	return &nativeCommandError{operation: operation, exitCode: exitCode, cause: err}
}

// SDK errors can contain guest arguments or output. Keep their causes available
// to errors.Is without exposing either in Kubernetes status or HTTP responses.
type nativeCommandError struct {
	operation string
	exitCode  int
	cause     error
}

func (e *nativeCommandError) Error() string {
	if e.exitCode >= 0 {
		return fmt.Sprintf("native %s failed with exit code %d", e.operation, e.exitCode)
	}
	return fmt.Sprintf("native %s failed", e.operation)
}

func (e *nativeCommandError) Unwrap() error { return e.cause }

// Kubernetes uses these methods to send a command's exit status over the exec
// protocol, separately from transport failures.
func (e *nativeCommandError) String() string  { return e.Error() }
func (e *nativeCommandError) Exited() bool    { return e.exitCode >= 0 }
func (e *nativeCommandError) ExitStatus() int { return e.exitCode }

package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"time"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
)

// Attached streams terminate with their Pod, even when their client stays open.
func podOperationContext(ctx context.Context, e *podEntry) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	if e.lifetime == nil {
		return ctx, cancel
	}
	stop := context.AfterFunc(e.lifetime, cancel)
	return ctx, func() { stop(); cancel() }
}

type containerExec struct {
	task   string
	cancel context.CancelFunc
	done   chan struct{}
}

// Called under e.op before replacing a container or removing its VM. Completion
// is signalled before reacquiring e.op, so cleanup can drain under this lock.
func stopContainerExecs(ctx context.Context, e *podEntry, task string) error {
	var pending []*containerExec
	for operation := range e.execs {
		if task == "" || operation.task == task {
			operation.cancel()
			pending = append(pending, operation)
		}
	}
	for _, operation := range pending {
		select {
		case <-operation.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (p *provider) RunInContainer(ctx context.Context, namespace, name, container string, args []string, attach api.AttachIO) error {
	if len(args) == 0 {
		return errdefs.InvalidInput("exec requires a command")
	}
	e, err := p.entry(namespace, name)
	if err != nil {
		return err
	}
	ctx, cancel := podOperationContext(ctx, e)
	defer cancel()
	e.op.Lock()
	if err := ctx.Err(); err != nil {
		e.op.Unlock()
		return err
	}
	if e.deleted || e.lifetime != nil && e.lifetime.Err() != nil {
		e.op.Unlock()
		return errdefs.InvalidInput("pod is terminating")
	}
	task, err := executingContainerTask(e, container)
	if err != nil {
		e.op.Unlock()
		return err
	}
	vm := e.vm
	operation := &containerExec{task: task, cancel: cancel, done: make(chan struct{})}
	if e.execs == nil {
		e.execs = make(map[*containerExec]struct{})
	}
	e.execs[operation] = struct{}{}
	e.lastActivity = time.Now()
	e.activitySequence++
	refreshView(e)
	e.op.Unlock()
	defer func() {
		close(operation.done)
		e.op.Lock()
		defer e.op.Unlock()
		delete(e.execs, operation)
		e.lastActivity = time.Now()
		e.activitySequence++
		refreshView(e)
	}()
	execID := "exec-" + rand.Text()
	command := []string{"exec", "--stream", vm, "--", "ctr", "--address", "/run/containerd/containerd.sock", "tasks", "exec", "--exec-id", execID}
	if attach.TTY() {
		command = append(command, "--tty")
	}
	command = append(command, task)
	command = append(command, args...)
	if attach.TTY() {
		err = p.runner.nativeTerminal(ctx, command, attach.Stdin(), attach.Stdout(), attach.Stderr(),
			msbruntime.StreamOptions{TTY: true, Resize: terminalResize(ctx, attach.Resize())})
	} else {
		err = p.runner.nativeStream(ctx, command, attach.Stdin(), attach.Stdout(), attach.Stderr())
	}
	failure, knownExit := errors.AsType[*nativeCommandError](err)
	knownExit = knownExit && failure.Exited()
	if ctx.Err() != nil || (err != nil && !knownExit) {
		// Killing the SDK's ctr client does not kill an exec process inside its
		// container. Address only this invocation, never the application task.
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer stop()
		_, cleanupErr := p.runner.guest(cleanup, vm, ctr+" tasks kill --exec-id "+quote(execID)+" --signal SIGKILL "+quote(task))
		if cleanupErr != nil {
			// A completed command already removed its exec process. Transport
			// failures are still reported to the caller with the original error.
			slog.Debug("exec process cleanup did not find a live process", "pod", name, "container", container)
		}
	}
	if ctx.Err() != nil {
		return errors.Join(err, ctx.Err())
	}
	return err
}

const guestConnector = "/tmp/floci-operations/relay"

func (p *provider) installGuestConnector(ctx context.Context, e *podEntry) error {
	if p.runner.cfg.relayBinary == "" {
		return errdefs.InvalidInput("port-forward requires --network-relay-binary")
	}
	file, err := os.Open(p.runner.cfg.relayBinary)
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 64*1024*1024+1))
	if err != nil {
		return err
	}
	if len(data) == 0 || len(data) > 64*1024*1024 {
		return errors.New("network relay binary must contain at most 64 MiB")
	}
	want := fmt.Sprintf("%x", sha256.Sum256(data))
	current, err := p.runner.guest(ctx, e.vm, "if test -x "+guestConnector+"; then sha256sum "+guestConnector+" | cut -d' ' -f1; fi")
	if err != nil || current == want {
		return err
	}
	return p.runner.guestInput(ctx, e.vm, "umask 077; mkdir -p /tmp/floci-operations; cat > "+guestConnector+".pending; chmod 700 "+guestConnector+".pending; mv "+guestConnector+".pending "+guestConnector, data)
}

func (p *provider) PortForward(ctx context.Context, namespace, name string, port int32, stream io.ReadWriteCloser) error {
	defer stream.Close()
	if port < 1 || port > 65535 {
		return errdefs.InvalidInput("port-forward requires a TCP port between 1 and 65535")
	}
	e, err := p.entry(namespace, name)
	if err != nil {
		return err
	}
	ctx, cancel := podOperationContext(ctx, e)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { stream.Close() })
	defer stop()
	e.op.Lock()
	if err := ctx.Err(); err != nil {
		e.op.Unlock()
		return err
	}
	if e.deleted || (e.state != "COLD" && e.state != "SLEEPING" && e.state != "READY" && e.state != "RUNNING" && e.state != "INITIALIZING") {
		e.op.Unlock()
		return errdefs.InvalidInput("pod is not available for port-forward")
	}
	if err := p.resume(ctx, e); err != nil {
		e.op.Unlock()
		return err
	}
	if err := p.installGuestConnector(ctx, e); err != nil {
		e.op.Unlock()
		return err
	}
	e.activeConnections++
	e.activitySequence++
	e.lastActivity = time.Now()
	refreshView(e)
	e.op.Unlock()
	defer func() {
		e.op.Lock()
		defer e.op.Unlock()
		e.activeConnections--
		e.activitySequence++
		e.lastActivity = time.Now()
		refreshView(e)
	}()
	// A Pod shares this VM's network namespace. Loopback also reaches applications
	// that deliberately do not bind their published Pod address.
	command := []string{"exec", "--stream", e.vm, "--", guestConnector, "connect", "--address", "127.0.0.1", "--port", strconv.Itoa(int(port))}
	return p.runner.nativeStream(ctx, command, stream, stream, io.Discard)
}

func terminalResize(ctx context.Context, input <-chan api.TermSize) <-chan msbruntime.TerminalSize {
	if input == nil {
		return nil
	}
	output := make(chan msbruntime.TerminalSize)
	go func() {
		defer close(output)
		for {
			select {
			case <-ctx.Done():
				return
			case size, ok := <-input:
				if !ok {
					return
				}
				if size.Width == 0 || size.Height == 0 {
					continue
				}
				select {
				case <-ctx.Done():
					return
				case output <- msbruntime.TerminalSize{Rows: size.Height, Cols: size.Width}:
				}
			}
		}
	}()
	return output
}

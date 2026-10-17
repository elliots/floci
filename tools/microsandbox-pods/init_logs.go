package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
)

const maxInitLogBytes = 1 << 20

func (p *provider) initLogDirectory(e *podEntry) string {
	return filepath.Join(p.runner.cfg.state, "logs", string(e.pod.UID))
}

func (p *provider) initLogPath(e *podEntry, index int) string {
	return filepath.Join(p.initLogDirectory(e), initTask(index)+".log")
}

// Keep a bounded completed log outside the guest so failed init diagnostics
// remain available after releasing its RAM and across controller recovery.
func (p *provider) captureInitLogs(ctx context.Context, e *podEntry, index int) error {
	if err := p.retainContainerLogs(ctx, e, logContainer{index: index, init: true}); err != nil {
		return err
	}
	data, err := p.initLogOutput(ctx, e, index)
	if err != nil {
		return fmt.Errorf("capture init container logs: %w", err)
	}
	directory := p.initLogDirectory(e)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, ".init-log-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), p.initLogPath(e, index)); err != nil {
		return err
	}
	return errors.Join(syncDirectory(directory), syncDirectory(filepath.Dir(directory)))
}

func (p *provider) initLogOutput(ctx context.Context, e *podEntry, index int) ([]byte, error) {
	root := initRoot(index)
	if restartableInit(e.pod.Spec.InitContainers[index]) {
		root = (managedContainer{index: index, init: true}).root(e)
	}
	script := fmt.Sprintf("tail -c %d %s", maxInitLogBytes, quote(root+"/application.log"))
	// Logs are bytes, unlike command metadata. Preserve leading/trailing
	// whitespace and do not merge guest errors into a successful log response.
	data, err := p.runner.nativeOutput(ctx, []string{"exec", "--no-stdin", e.vm, "--", "sh", "-ec", script}, nil)
	if err != nil {
		return nil, fmt.Errorf("read init container logs: %w", err)
	}
	return data, nil
}

func (p *provider) readInitLogs(ctx context.Context, e *podEntry, index int, opts api.ContainerLogOpts) ([]byte, error) {
	status := e.pod.Status.InitContainerStatuses[index]
	var data []byte
	if (e.state == "INITIALIZING" || restartableInit(e.pod.Spec.InitContainers[index]) && (e.state == "RUNNING" || e.state == "READY" || e.state == "DRAINING")) && status.State.Running != nil {
		var err error
		data, err = p.initLogOutput(ctx, e, index)
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		data, err = os.ReadFile(p.initLogPath(e, index))
		if errors.Is(err, os.ErrNotExist) {
			return nil, errdefs.InvalidInput("init container has no captured execution log")
		}
		if err != nil {
			return nil, err
		}
	}
	if opts.Tail > 0 {
		lines := strings.SplitAfter(string(data), "\n")
		if lines[len(lines)-1] == "" {
			lines = lines[:len(lines)-1]
		}
		if len(lines) > opts.Tail {
			data = []byte(strings.Join(lines[len(lines)-opts.Tail:], ""))
		}
	}
	if opts.LimitBytes > 0 && len(data) > opts.LimitBytes {
		data = data[:opts.LimitBytes]
	}
	return data, nil
}

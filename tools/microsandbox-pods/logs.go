package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/floci-io/floci/tools/microsandbox-runtime/guestlog"
	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	corev1 "k8s.io/api/core/v1"
)

const maxContainerLogBytes = 3 << 20

type logTailZeroKey struct{}

// Virtual-kubelet's options use zero both for omitted tailLines and for an
// explicit zero. Retain that distinction at the existing kubelet HTTP route.
func containerLogOptionsHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/containerLogs/") && r.URL.Query().Get("tailLines") == "0" {
			r = r.WithContext(context.WithValue(r.Context(), logTailZeroKey{}, true))
		}
		next.ServeHTTP(w, r)
	})
}

type containerLogSnapshot struct {
	Version  int               `json:"version"`
	Attempt  int32             `json:"attempt"`
	Complete bool              `json:"complete,omitzero"`
	Records  []guestlog.Record `json:"records,omitempty"`
	Raw      []byte            `json:"raw,omitempty"`
}

type logContainer struct {
	index int
	init  bool
}

func (c logContainer) status(e *podEntry) corev1.ContainerStatus {
	if c.init {
		return e.pod.Status.InitContainerStatuses[c.index]
	}
	return e.pod.Status.ContainerStatuses[c.index]
}
func (c logContainer) task(e *podEntry) string {
	if c.init {
		return initTask(c.index)
	}
	return applicationTask(e.pod, c.index)
}
func (c logContainer) root(e *podEntry) string {
	if c.init && !restartableInit(e.pod.Spec.InitContainers[c.index]) {
		return initRoot(c.index)
	}
	return (managedContainer{index: c.index, init: c.init}).root(e)
}
func (p *provider) logPath(e *podEntry, c logContainer, previous bool) string {
	suffix := ".current.json"
	if previous {
		suffix = ".previous.json"
	}
	return filepath.Join(p.initLogDirectory(e), c.task(e)+suffix)
}

func (p *provider) liveLogs(ctx context.Context, e *podEntry, c logContainer) (containerLogSnapshot, error) {
	status := c.status(e)
	snapshot := containerLogSnapshot{Attempt: status.RestartCount,
		Complete: status.State.Terminated != nil || status.State.Waiting != nil && status.LastTerminationState.Terminated != nil}
	root := c.root(e)
	script := "if test -f " + quote(root+"/application.records") + "; then printf 'RECORDS\\n'; " +
		"if test -f " + quote(root+"/application.records.1") + "; then cat " + quote(root+"/application.records.1") + "; fi; cat " + quote(root+"/application.records") +
		"; elif test -f " + quote(root+"/application.log") + "; then printf 'RAW\\n'; tail -c 1048576 " + quote(root+"/application.log") + "; else printf 'EMPTY\\n'; fi"
	legacy := p.runner.cfg.guestFSBinary == ""
	if legacy {
		script = "if test -f " + quote(root+"/application.log") + "; then tail -c 1048576 " + quote(root+"/application.log") + "; fi"
	}
	check, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	output, err := p.runner.nativeOutput(check, []string{"exec", "--no-stdin", e.vm, "--", "sh", "-ec", script}, nil)
	if err != nil {
		return snapshot, err
	}
	if len(output) > maxContainerLogBytes {
		return snapshot, errors.New("container log capture exceeded its bounded size")
	}
	if legacy {
		snapshot.Raw = output
		return snapshot, nil
	}
	kind, data, found := bytes.Cut(output, []byte{'\n'})
	if !found {
		return snapshot, errors.New("invalid container log response")
	}
	switch string(kind) {
	case "RECORDS":
		snapshot.Version = 1
		for line := range bytes.SplitSeq(data, []byte{'\n'}) {
			if len(line) == 0 {
				continue
			}
			var record guestlog.Record
			if err := json.Unmarshal(line, &record); err != nil {
				// A writer can be between its write syscall and the last byte
				// of its current record. Retry that trailing record next poll.
				if !bytes.HasSuffix(data, []byte{'\n'}) && bytes.HasSuffix(data, line) {
					break
				}
				return snapshot, fmt.Errorf("invalid timestamped container log: %w", err)
			}
			if record.At.IsZero() || record.Offset < 0 {
				return snapshot, errors.New("invalid timestamped container log metadata")
			}
			snapshot.Records = append(snapshot.Records, record)
		}
	case "RAW":
		snapshot.Raw = bytes.Clone(data)
		if len(data) == 0 {
			snapshot.Version = 1
		}
	case "EMPTY":
		snapshot.Version = 1
	default:
		return snapshot, errors.New("unknown container log encoding")
	}
	return snapshot, nil
}

func guestLogsAvailable(e *podEntry) bool {
	switch e.state {
	case "RUNNING", "READY", "INITIALIZING", "DRAINING", "HIBERNATING", "RESTARTING", "APPSTARTING", "INITSTARTING", "TERMINATED":
		return true
	default:
		return false
	}
}

func (p *provider) saveLogs(e *podEntry, c logContainer, snapshot containerLogSnapshot) error {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	if len(data) > maxContainerLogBytes {
		return errors.New("container log retention exceeded its bounded size")
	}
	directory := p.initLogDirectory(e)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, ".container-log-")
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
	if err := os.Rename(file.Name(), p.logPath(e, c, false)); err != nil {
		return err
	}
	return syncDirectory(directory)
}

func (p *provider) retainContainerLogs(ctx context.Context, e *podEntry, c logContainer) error {
	snapshot, err := p.liveLogs(ctx, e, c)
	if err != nil {
		return err
	}
	return p.saveLogs(e, c, snapshot)
}

func (p *provider) captureContainerLogs(ctx context.Context, e *podEntry) error {
	for index := range e.pod.Spec.Containers {
		if err := p.retainContainerLogs(ctx, e, logContainer{index: index}); err != nil {
			return err
		}
	}
	for index := range e.pod.Spec.InitContainers {
		if err := p.retainContainerLogs(ctx, e, logContainer{index: index, init: true}); err != nil {
			return err
		}
	}
	return nil
}

func (p *provider) archiveContainerLogs(ctx context.Context, e *podEntry, c logContainer) error {
	status := c.status(e)
	if status.LastTerminationState.Terminated == nil {
		return nil
	}
	if err := p.retainContainerLogs(ctx, e, c); err != nil {
		return err
	}
	if err := os.Rename(p.logPath(e, c, false), p.logPath(e, c, true)); err != nil {
		return err
	}
	root := c.root(e)
	_, err := p.runner.guest(ctx, e.vm, "rm -f "+quote(root+"/application.records")+" "+quote(root+"/application.records.1")+" "+quote(root+"/application.log.1"))
	return errors.Join(err, syncDirectory(p.initLogDirectory(e)))
}

func (p *provider) retainedLogs(e *podEntry, c logContainer, previous bool) (containerLogSnapshot, error) {
	var snapshot containerLogSnapshot
	data, err := os.ReadFile(p.logPath(e, c, previous))
	if errors.Is(err, os.ErrNotExist) && c.init && !previous {
		data, err = os.ReadFile(p.initLogPath(e, c.index))
		if err == nil {
			return containerLogSnapshot{Raw: data, Complete: true}, nil
		}
	}
	if errors.Is(err, os.ErrNotExist) {
		return snapshot, errdefs.InvalidInput("container has no retained execution log")
	}
	if err != nil {
		return snapshot, err
	}
	if len(data) > maxContainerLogBytes {
		return snapshot, errors.New("retained container log exceeded its bounded size")
	}
	err = json.Unmarshal(data, &snapshot)
	return snapshot, err
}

func (p *provider) readContainerLogs(ctx context.Context, e *podEntry, c logContainer, previous bool) (containerLogSnapshot, error) {
	status := c.status(e)
	if previous && status.LastTerminationState.Terminated == nil {
		return containerLogSnapshot{}, errdefs.InvalidInput("previous terminated container does not exist")
	}
	// During restart backoff the current files still belong to the last exit.
	previousFile := previous && !(status.State.Waiting != nil && status.LastTerminationState.Terminated != nil)
	if !previousFile && guestLogsAvailable(e) && !e.deleted {
		if snapshot, err := p.liveLogs(ctx, e, c); err == nil {
			return snapshot, nil
		} else if e.state != "TERMINATED" {
			return snapshot, err
		}
	}
	return p.retainedLogs(e, c, previousFile)
}

func renderContainerLogs(snapshot containerLogSnapshot, opts api.ContainerLogOpts, after int64, initial bool) ([]byte, int64, error) {
	if snapshot.Version < 0 || snapshot.Version > 1 {
		return nil, after, errors.New("unsupported retained container log version")
	}
	if snapshot.Version == 0 {
		if opts.Timestamps || !opts.SinceTime.IsZero() || opts.SinceSeconds != 0 {
			return nil, after, errdefs.InvalidInput("legacy container log has no capture timestamps; restart the container with the current guestfs helper")
		}
		if opts.Follow && !snapshot.Complete && len(snapshot.Raw) >= 1<<20 {
			return nil, after, errdefs.InvalidInput("legacy container log reached its capture limit; restart the container with the current guestfs helper to follow rotated output")
		}
		data := snapshot.Raw
		end := int64(len(data))
		if after > 0 {
			data = data[min(after, end):]
		}
		if initial && opts.Tail > 0 {
			data = tailLogLines(data, opts.Tail)
		}
		return bytes.Clone(data), end, nil
	}
	var data bytes.Buffer
	end := after
	for _, record := range snapshot.Records {
		next := record.Offset + int64(len(record.Data))
		end = max(end, next)
		if next <= after || !opts.SinceTime.IsZero() && record.At.Before(opts.SinceTime) {
			continue
		}
		content := record.Data
		start := record.LineStart
		if record.Offset < after {
			content = content[after-record.Offset:]
			start = false
		}
		if opts.Timestamps && start {
			data.WriteString(record.At.Format(time.RFC3339Nano))
			data.WriteByte(' ')
		}
		data.Write(content)
	}
	result := data.Bytes()
	if initial && opts.Tail > 0 {
		result = tailLogLines(result, opts.Tail)
	}
	return result, end, nil
}

func tailLogLines(data []byte, count int) []byte {
	end := len(data)
	if end > 0 && data[end-1] == '\n' {
		end--
	}
	for i := end - 1; i >= 0; i-- {
		if data[i] == '\n' {
			count--
			if count == 0 {
				return data[i+1:]
			}
		}
	}
	return data
}

type cancelLogReader struct {
	*io.PipeReader
	cancel context.CancelFunc
}

func (r cancelLogReader) Close() error { r.cancel(); return r.PipeReader.Close() }

func (p *provider) GetContainerLogs(ctx context.Context, namespace, name, container string, opts api.ContainerLogOpts) (io.ReadCloser, error) {
	e, err := p.entry(namespace, name)
	if err != nil {
		return nil, err
	}
	if opts.SinceSeconds < 0 || opts.Tail < 0 || opts.LimitBytes < 0 || opts.SinceSeconds != 0 && !opts.SinceTime.IsZero() {
		return nil, errdefs.InvalidInput("invalid container log options")
	}
	if opts.SinceSeconds > 0 {
		opts.SinceTime = time.Now().Add(-time.Duration(opts.SinceSeconds) * time.Second)
	}
	selected, found := logContainer{}, false
	for index, declared := range e.pod.Spec.Containers {
		if declared.Name == container {
			selected, found = logContainer{index: index}, true
			break
		}
	}
	for index, declared := range e.pod.Spec.InitContainers {
		if declared.Name == container {
			selected, found = logContainer{index: index, init: true}, true
			break
		}
	}
	if !found {
		return nil, errdefs.NotFound("container not found")
	}
	e.op.Lock()
	snapshot, err := p.readContainerLogs(ctx, e, selected, opts.Previous)
	e.op.Unlock()
	if err != nil {
		return nil, err
	}
	data, offset, err := renderContainerLogs(snapshot, opts, 0, true)
	if err != nil {
		return nil, err
	}
	if zero, _ := ctx.Value(logTailZeroKey{}).(bool); zero {
		data = nil
	}
	if opts.LimitBytes > 0 && len(data) > opts.LimitBytes {
		data = data[:opts.LimitBytes]
	}
	if !opts.Follow || opts.Previous || snapshot.Complete || opts.LimitBytes > 0 && len(data) >= opts.LimitBytes {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	follow, cancel := context.WithCancel(ctx)
	reader, writer := io.Pipe()
	stop := context.AfterFunc(follow, func() { _ = writer.CloseWithError(follow.Err()) })
	go func() {
		defer stop()
		defer writer.Close()
		defer cancel()
		remaining := opts.LimitBytes
		write := func(value []byte) error {
			if len(value) == 0 {
				return nil
			}
			if remaining > 0 && len(value) > remaining {
				value = value[:remaining]
			}
			n, err := writer.Write(value)
			if opts.LimitBytes > 0 {
				remaining -= n
			}
			return err
		}
		if err := write(data); err != nil {
			return
		}
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-follow.Done():
				_ = writer.CloseWithError(follow.Err())
				return
			case <-ticker.C:
			}
			e.op.Lock()
			next, err := p.readContainerLogs(follow, e, selected, false)
			deleted := e.deleted
			e.op.Unlock()
			if deleted {
				return
			}
			if err != nil {
				_ = writer.CloseWithError(err)
				return
			}
			if next.Attempt != snapshot.Attempt {
				return
			}
			data, offset, err = renderContainerLogs(next, opts, offset, false)
			if err != nil {
				_ = writer.CloseWithError(err)
				return
			}
			if err := write(data); err != nil {
				return
			}
			if next.Complete || opts.LimitBytes > 0 && remaining == 0 {
				return
			}
		}
	}()
	return cancelLogReader{PipeReader: reader, cancel: cancel}, nil
}

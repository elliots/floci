package main

import (
	"cmp"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
	corev1 "k8s.io/api/core/v1"
)

type lifecycleView struct {
	VCPUs             int    `json:"vcpus,omitzero"`
	MemoryMiB         int    `json:"memoryMiB,omitzero"`
	RootDiskMiB       int    `json:"rootDiskMiB,omitzero"`
	ProxyURL          string `json:"proxyUrl,omitempty"`
	ProxyAvailability bool   `json:"proxyAvailability"`
	ProxyProtocol     string `json:"proxyProtocol,omitempty"`
	IdleOwner         string `json:"idleOwner,omitempty"`
	ActiveConnections int    `json:"activeConnections"`
	ActiveRequests    int    `json:"activeRequests"`
	ActivitySequence  uint64 `json:"activitySequence"`
	UID               string `json:"uid"`
	Namespace         string `json:"namespace"`
	Name              string `json:"name"`
	State             string `json:"state"`
	Generation        uint64 `json:"generation"`
	PodIP             string `json:"podIP"`
	BackendURL        string `json:"backendUrl,omitempty"`
	Snapshot          string `json:"snapshot,omitempty"`
	VM                string `json:"vm"`
	UDPPort           int    `json:"udpPort"`
	HTTPPort          int    `json:"httpPort,omitzero"`
	ApplicationPort   int    `json:"applicationPort,omitzero"`
}

func view(e *podEntry) lifecycleView {
	v := lifecycleView{ProxyAvailability: e.proxyAvailability, IdleOwner: e.idleOwner, ActiveConnections: e.activeConnections, UID: string(e.pod.UID), Namespace: e.pod.Namespace, Name: e.pod.Name, State: e.state,
		Generation: e.generation, PodIP: e.ip, VM: e.vm, UDPPort: e.port, HTTPPort: e.httpPort, ApplicationPort: e.appPort}
	v.VCPUs, v.MemoryMiB = e.allocation.VCPUs, e.allocation.MemoryMiB
	v.RootDiskMiB = e.allocation.RootDiskMiB
	if e.proxyAvailability {
		v.ProxyProtocol = proxyProtocol(e.pod)
	}
	v.ActiveRequests, v.ActivitySequence = e.activeRequests, e.activitySequence
	if e.httpPort != 0 {
		v.BackendURL = fmt.Sprintf("http://127.0.0.1:%d", e.httpPort)
	}
	if len(e.proxyPorts) > 0 {
		v.ProxyURL = fmt.Sprintf("http://%s:%d", cmp.Or(e.proxyHost, "127.0.0.1"), e.proxyPorts[0].ProxyPort)
	}
	if len(e.snapshots) > 0 {
		v.Snapshot = e.snapshots[len(e.snapshots)-1]
	}
	return v
}

func refreshView(e *podEntry) {
	current := view(e)
	e.publishedView.Store(&current)
}

func (p *provider) serveControl(address, tokenFile string) (*http.Server, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return nil, errors.New("control listener must bind an explicit loopback IP")
	}
	bytes, err := os.ReadFile(tokenFile)
	if err != nil {
		return nil, err
	}
	token := strings.TrimSpace(string(bytes))
	if len(token) < 32 {
		return nil, errors.New("control token must have at least 32 bytes")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/capacity", p.capacityStatus)
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, r *http.Request) {
		p.budgetMu.Lock()
		allocated := p.activeMemoryMiB
		p.budgetMu.Unlock()
		writeJSON(w, 200, map[string]any{"status": "ok", "name": "microsandbox-pods", "contract": 1, "clusterName": p.runner.cfg.clusterName,
			"activeMemoryMiB": allocated, "maxActiveMemoryMiB": p.runner.cfg.maxActiveMemoryMiB, "maxPods": p.runner.cfg.maxPods,
			"networkPolicy": p.runner.networkFingerprint()})
	})
	mux.HandleFunc("GET /v1/deployments/{namespace}/{name}", p.deployment)
	mux.HandleFunc("GET /v1/deployments", p.deploymentSnapshot)
	mux.HandleFunc("GET /v1/pods/{uid}", func(w http.ResponseWriter, r *http.Request) {
		e := p.byUID(r.PathValue("uid"))
		if e == nil {
			writeJSON(w, 404, map[string]string{"error": "pod UID not found"})
			return
		}
		e.op.Lock()
		defer e.op.Unlock()
		writeJSON(w, 200, view(e))
	})
	mux.HandleFunc("POST /v1/pods/{uid}/{operation}", func(w http.ResponseWriter, r *http.Request) {
		e := p.byUID(r.PathValue("uid"))
		if e == nil {
			writeJSON(w, 404, map[string]string{"error": "pod UID not found"})
			return
		}
		operation := r.PathValue("operation")
		if operation != "hibernate" && operation != "resume" {
			writeJSON(w, 404, map[string]string{"error": "operation not found"})
			return
		}
		var request struct {
			ExpectedGeneration       *uint64 `json:"expectedGeneration"`
			ExpectedActivitySequence *uint64 `json:"expectedActivitySequence"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil || request.ExpectedGeneration == nil {
			writeJSON(w, 400, map[string]string{"error": "expectedGeneration is required"})
			return
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 4*time.Minute)
		defer cancel()
		e.op.Lock()
		defer e.op.Unlock()
		if e.deleted || e.lifetime.Err() != nil {
			writeJSON(w, 409, map[string]string{"error": "pod deletion is in progress"})
			return
		}
		if *request.ExpectedGeneration != e.generation {
			writeJSON(w, 409, map[string]string{"error": "lifecycle generation changed"})
			return
		}
		if operation == "hibernate" && request.ExpectedActivitySequence != nil && *request.ExpectedActivitySequence != e.activitySequence {
			writeJSON(w, 409, map[string]string{"error": "pod activity changed"})
			return
		}
		var err error
		if operation == "hibernate" {
			err = p.hibernate(ctx, e)
		} else {
			err = p.resume(ctx, e)
		}
		if err != nil {
			writeJSON(w, 409, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, view(e))
	})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value, bearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !bearer || subtle.ConstantTimeCompare([]byte(value), []byte(token)) != 1 {
			writeJSON(w, 401, map[string]string{"error": "unauthorized"})
			return
		}
		mux.ServeHTTP(w, r)
	})
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, "lifecycle listener:", err)
		}
	}()
	return server, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		return
	}
}
func (p *provider) byUID(uid string) *podEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.pods {
		if string(e.pod.UID) == uid {
			return e
		}
	}
	return nil
}

func (p *provider) journal(e *podEntry) error {
	refreshView(e)
	path := filepath.Join(p.runner.cfg.state, string(e.pod.UID)+".json")
	file, err := os.CreateTemp(p.runner.cfg.state, ".journal-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if err := file.Chmod(0600); err != nil {
		file.Close()
		return err
	}
	e.mu.Lock()
	pod := e.pod.DeepCopy()
	e.mu.Unlock()
	allocation, err := validateStoredAllocation(e.allocation)
	if err != nil {
		file.Close()
		return err
	}
	if err := json.NewEncoder(file).Encode(journalRecord{Writeback: e.writeback, PersistentVolumes: e.persistentVolumes, Freeze: e.freeze, Version: podJournalVersion(e.pod), CompletionPhase: e.completionPhase, ShutdownDeadline: e.shutdownDeadline, Applications: e.applicationRecords(), Allocation: allocation, StartupComplete: e.startupComplete, ProbeFailure: e.probeFailure, TerminationReason: e.terminationReason, DeclaredSpec: e.declaredSpec, ProxyPorts: e.proxyPorts, BackendIP: e.backendIP, SnapshotIDs: e.snapshotIDs, RestoredParent: e.restoredParent, View: view(e), Pod: pod, Slot: e.slot, GuestIP: e.guestIP, Started: e.started, Snapshots: e.snapshots}); err != nil {
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
	if err := os.Rename(name, path); err != nil {
		return err
	}
	directory, err := os.Open(p.runner.cfg.state)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (p *provider) unavailable(e *podEntry, reason string) {
	e.actualReady = false
	e.mu.Lock()
	e.pod.Status.Reason = reason
	for i := range e.pod.Status.ContainerStatuses {
		status := &e.pod.Status.ContainerStatuses[i]
		status.Ready = false
		if !concurrentApplications(e.pod) {
			status.Started = new(false)
			status.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}}
		}
	}
	for i := range e.pod.Status.InitContainerStatuses {
		if restartableInit(e.pod.Spec.InitContainers[i]) {
			e.pod.Status.InitContainerStatuses[i].Ready = false
		}
	}
	e.pod.Status.Conditions = podConditionsFor(e.pod, false)
	e.mu.Unlock()
	p.publish(e)
}

func (p *provider) hibernate(ctx context.Context, e *podEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.probeFailure != "" {
		return errors.New("cannot hibernate a container awaiting probe-triggered termination")
	}
	for _, progress := range e.applications {
		if !progress.nextKill.IsZero() {
			return errors.New("cannot hibernate a container awaiting probe-triggered termination")
		}
	}
	if !e.applicationMissingSince.IsZero() {
		return errors.New("cannot hibernate a container with a missing application task")
	}
	if e.activeConnections > 0 {
		return errors.New("proxy connections are active")
	}
	if e.activeRequests > 0 {
		return errors.New("HTTP requests are active")
	}
	if e.state == "COLD" {
		_, exists, err := p.inspectOwned(ctx, e)
		if err != nil {
			return err
		}
		if exists {
			return errors.New("cold pod unexpectedly owns a VM")
		}
		return nil
	}
	if e.state == "SLEEPING" {
		return p.requireVMStatus(ctx, e, "Stopped")
	}
	if e.state != "READY" && e.state != "RUNNING" {
		return fmt.Errorf("cannot hibernate pod in state %s", e.state)
	}
	if err := p.requireVMStatus(ctx, e, "Running"); err != nil {
		return err
	}
	snapshot := fmt.Sprintf("sleep-%d", e.generation+1)
	args := []string{"snap", "create", snapshot, "--sandbox", e.vm, "--full", "--integrity"}
	permit, err := p.runner.prepareAdmission(ctx, args, "")
	if err != nil {
		return err
	}
	captureDispatched := false
	defer func() {
		if !captureDispatched {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			_ = p.runner.runtime.CancelAdmission(cleanup, permit)
		}
	}()
	e.generation++
	e.state = "HIBERNATING"
	e.applicationMissingSince = time.Time{}
	if err := p.journal(e); err != nil {
		return err
	}
	p.unavailable(e, "Hibernating")
	if err := p.freezeApplications(ctx, e); err != nil {
		return p.rollbackFreeze(e, err)
	}
	if err := p.syncPersistentVolumes(ctx, e); err != nil {
		return p.rollbackFreeze(e, err)
	}
	if _, err := p.runner.vm(ctx, "pause", e.vm); err != nil {
		return p.transitionFailed(e, err)
	}
	selector := e.vm + ":" + snapshot
	captureDispatched = true
	if _, err := p.runner.vm(context.WithValue(ctx, nativeAdmissionKey{}, permit), args...); err != nil {
		if errors.Is(err, msbruntime.ErrCapacityUnavailable) {
			captureDispatched = false
		}
		return p.rollbackPause(e, err)
	}
	e.snapshots = append(e.snapshots, selector)
	if err := p.rememberSnapshot(ctx, e, selector); err != nil {
		return p.rollbackPause(e, err)
	}
	if _, err := p.runner.vm(ctx, "snap", "verify", selector); err != nil {
		return p.rollbackPause(e, err)
	}
	e.state = "CHECKPOINTED"
	if err := p.journal(e); err != nil {
		return p.transitionFailed(e, err)
	}
	p.network.closeIngress(e)
	if _, err := p.runner.vm(ctx, "stop", "--force", e.vm); err != nil {
		return p.transitionFailed(e, err)
	}
	if err := p.requireVMStatus(ctx, e, "Stopped"); err != nil {
		return p.transitionFailed(e, err)
	}
	e.state = "SLEEPING"
	p.unavailable(e, "Hibernated")
	if err := p.journal(e); err != nil {
		return err
	}
	p.releaseMemory(e)
	return nil
}

func (p *provider) rollbackPause(e *podEntry, cause error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := p.requireVMStatus(ctx, e, "Paused"); err != nil {
		return p.transitionFailed(e, errors.Join(cause, err))
	}
	if _, err := p.runner.vm(ctx, "resume", e.vm); err != nil {
		return p.transitionFailed(e, errors.Join(cause, err))
	}
	if err := p.requireVMStatus(ctx, e, "Running"); err != nil {
		return p.transitionFailed(e, errors.Join(cause, err))
	}
	return p.rollbackFreeze(e, cause)
}

func (p *provider) transitionFailed(e *podEntry, cause error) error {
	e.state = "FAILED"
	p.unavailable(e, "NativeRuntimeUncertain")
	return errors.Join(cause, p.journal(e))
}

func (p *provider) requireVMStatus(ctx context.Context, e *podEntry, wanted string) error {
	status, exists, err := p.inspectOwned(ctx, e)
	if err != nil {
		return err
	}
	if !exists || status != wanted {
		return fmt.Errorf("owned VM must be %s, observed exists=%t status=%s", wanted, exists, status)
	}
	return nil
}

func (p *provider) resume(ctx context.Context, e *podEntry) error {
	if e.lifetime != nil {
		linked, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(e.lifetime, cancel)
		defer stop()
		defer cancel()
		ctx = linked
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.state == "COLD" {
		if err := p.checkCurrentPod(ctx, e); err != nil {
			return err
		}
		projections, err := p.readVolumeSources(ctx, e)
		if err != nil {
			if errors.Is(err, errPersistentVolumePending) {
				p.persistentVolumeUnavailable(e, err)
			}
			return err
		}
		e.initialVolumes = projections
		defer func() { e.initialVolumes = nil }()
		release, err := p.acquireStart(ctx, e)
		if err != nil {
			return err
		}
		defer release()
		e.generation++
		e.state = "STARTING"
		if err := p.journal(e); err != nil {
			return err
		}
		if err := p.startAdmitted(ctx, e); err != nil {
			if errors.Is(err, msbruntime.ErrCapacityUnavailable) || errors.Is(err, errPersistentVolumePending) {
				p.releaseMemory(e)
				e.state = "COLD"
				if errors.Is(err, errPersistentVolumePending) {
					p.persistentVolumeUnavailable(e, err)
				}
				return errors.Join(err, p.journal(e))
			}
			return p.transitionFailed(e, err)
		}
		wakeMonitor(e)
		return nil
	}
	if e.state == "READY" || e.state == "RUNNING" || e.state == "INITIALIZING" {
		return p.requireVMStatus(ctx, e, "Running")
	}
	if e.state != "SLEEPING" || len(e.snapshots) == 0 {
		return fmt.Errorf("cannot resume pod in state %s", e.state)
	}
	if err := p.checkCurrentPod(ctx, e); err != nil {
		return err
	}
	if err := p.requireVMStatus(ctx, e, "Stopped"); err != nil {
		return err
	}
	if err := requirePersistentWriteback(e); err != nil {
		return err
	}
	if err := requireFrozenSnapshot(e); err != nil {
		return err
	}
	if err := p.preflightWakeTokens(ctx, e); err != nil {
		return err
	}
	if err := p.preparePersistentVolumes(ctx, e); err != nil {
		p.persistentVolumeUnavailable(e, err)
		return err
	}
	selector := e.snapshots[len(e.snapshots)-1]
	if err := p.checkSnapshot(ctx, e, selector); err != nil {
		return err
	}
	if _, err := p.runner.vm(ctx, "snap", "verify", selector); err != nil {
		return err
	}
	args := []string{"snap", "restore", selector, "--name", e.vm, "--port", fmt.Sprintf("127.0.0.1:%d:8472/udp", e.port),
		"--net-default", "deny", "--net-rule", "allow:egress@host,allow:egress@private,allow:egress@public", "--net-rule", "allow:ingress@any"}
	args = append(args, persistentMountArgs(e)...)
	if e.proxyAvailability {
		for _, binding := range e.proxyPorts {
			args = append(args, "--port", fmt.Sprintf("127.0.0.1:%d:%d", binding.BackendPort, binding.ContainerPort))
		}
	} else if e.httpPort != 0 {
		args = append(args, "--port", fmt.Sprintf("127.0.0.1:%d:%d", e.httpPort, e.appPort))
	}
	release, err := p.acquireStart(ctx, e)
	if err != nil {
		return err
	}
	defer release()
	permit, err := p.runner.prepareAdmission(ctx, args, e.vm)
	if err != nil {
		p.releaseMemory(e)
		return err
	}
	sourceRemovalStarted := false
	defer func() {
		if !sourceRemovalStarted {
			_ = p.runner.runtime.CancelAdmission(context.WithoutCancel(ctx), permit)
		}
	}()
	e.generation++
	e.state = "RESUMING"
	if err := p.journal(e); err != nil {
		return err
	}
	sourceRemovalStarted = true
	p.network.closeIngress(e)
	if _, err := p.runner.vm(ctx, "rm", e.vm); err != nil {
		return p.transitionFailed(e, err)
	}
	if _, exists, err := p.inspectOwned(ctx, e); err != nil || exists {
		return p.transitionFailed(e, errors.Join(err, errors.New("source removal fence did not complete")))
	}
	e.created = false
	e.restoredParent = e.snapshotIDs[selector]
	if err := p.journal(e); err != nil {
		return err
	}
	if _, err := p.runner.vm(context.WithValue(ctx, nativeAdmissionKey{}, permit), args...); err != nil {
		return p.transitionFailed(e, err)
	}
	e.created = true
	if err := p.requireVMStatus(ctx, e, "Running"); err != nil {
		return p.transitionFailed(e, err)
	}
	if err := p.network.ensureIngress(e); err != nil {
		return p.transitionFailed(e, err)
	}
	guestIP, err := p.runner.guest(ctx, e.vm, "ip -4 -o addr show dev eth0 | awk '{print $4}' | cut -d/ -f1")
	if err != nil {
		return p.transitionFailed(e, err)
	}
	if guestIP != e.guestIP {
		if err := p.network.detach(ctx, e); err != nil {
			return p.transitionFailed(e, err)
		}
		if _, err := p.runner.guest(ctx, e.vm, "ip link del pod0; iptables -t nat -F OUTPUT"); err != nil {
			return p.transitionFailed(e, err)
		}
		if err := p.network.attach(ctx, e); err != nil {
			return p.transitionFailed(e, err)
		}
	}
	// The snapshot already contains the previous projection. Fetch current
	// sources before admitting wake traffic. Configuration may retain its last
	// good data on API errors; identity projections must refresh successfully.
	e.volumeDigests = nil
	if err := p.refreshVolumeProjections(ctx, e, false, true); err != nil {
		return p.transitionFailed(e, err)
	}
	if err := p.thawApplications(ctx, e); err != nil {
		return p.transitionFailed(e, err)
	}
	e.state = "RUNNING"
	resetProbeProgress(e)
	for _, progress := range e.applications {
		progress.readiness, progress.startup, progress.liveness = probeProgress{}, probeProgress{}, probeProgress{}
		progress.missingSince = time.Time{}
	}
	e.applicationMissingSince = time.Time{}
	if err := p.journal(e); err != nil {
		return err
	}
	wakeMonitor(e)
	return p.retireSnapshots(ctx, e)
}

func wakeMonitor(e *podEntry) {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func (p *provider) retireSnapshots(ctx context.Context, e *podEntry) error {
	for len(e.snapshots) > 1 {
		selector := e.snapshots[0]
		if err := p.deleteSnapshot(ctx, e, selector); err != nil {
			return err
		}
		e.snapshots = e.snapshots[1:]
		delete(e.snapshotIDs, selector)
		if err := p.journal(e); err != nil {
			return err
		}
	}
	return nil
}

func availableTCPPort() (int, error) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

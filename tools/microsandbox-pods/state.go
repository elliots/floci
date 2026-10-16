package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type stateIdentity struct {
	Version                                                      int
	Node, ClusterUID, PeerOwner, Peer, PodCIDR, ProxyBackendCIDR string
}
type journalRecord struct {
	Freeze            *applicationFreeze           `json:"freeze,omitempty"`
	CompletionPhase   corev1.PodPhase              `json:"completionPhase,omitempty"`
	ShutdownDeadline  time.Time                    `json:"shutdownDeadline,omitzero"`
	Applications      map[string]applicationRecord `json:"applications,omitempty"`
	TerminationReason string                       `json:"terminationReason,omitempty"`
	Allocation        podAllocation                `json:"allocation,omitzero"`
	StartupComplete   bool                         `json:"startupComplete,omitzero"`
	ProbeFailure      string                       `json:"probeFailure,omitempty"`
	ProxyPorts        []proxyPortBinding           `json:"proxyPorts,omitempty"`
	BackendIP         string                       `json:"backendIP,omitempty"`
	SnapshotIDs       map[string]string            `json:"snapshotIDs,omitempty"`
	RestoredParent    string                       `json:"restoredParent,omitempty"`
	Version           int                          `json:"version"`
	View              lifecycleView                `json:"view"`
	Pod               *corev1.Pod                  `json:"pod"`
	DeclaredSpec      *corev1.PodSpec              `json:"declaredSpec,omitempty"`
	Slot              int                          `json:"slot"`
	GuestIP           string                       `json:"guestIP"`
	Started           time.Time                    `json:"started"`
	Snapshots         []string                     `json:"snapshots,omitempty"`
}

func (record journalRecord) guestAllocation() (podAllocation, error) {
	if record.Version == 1 && record.Allocation == (podAllocation{}) {
		return validateStoredAllocation(podAllocation{})
	}
	if (record.Version < 2 || record.Version > 5) || record.Allocation.Version == 0 {
		return podAllocation{}, errors.New("pod journal has an invalid version or missing guest allocation")
	}
	if record.Version == 3 && (record.Pod == nil || len(record.Pod.Spec.InitContainers) == 0) {
		return podAllocation{}, errors.New("version 3 journal requires init containers")
	}
	return validateStoredAllocation(record.Allocation)
}

func openState(ctx context.Context, cfg config, client kubernetes.Interface) (*os.File, bool, error) {
	if err := os.MkdirAll(cfg.state, 0700); err != nil {
		return nil, false, err
	}
	cluster, err := client.CoreV1().Namespaces().Get(ctx, "kube-system", metav1.GetOptions{})
	if err != nil {
		return nil, false, err
	}
	wanted := stateIdentity{Version: 1, Node: cfg.nodeName, ClusterUID: string(cluster.UID), PeerOwner: cfg.peerOwner, Peer: cfg.peer, PodCIDR: cfg.podCIDR, ProxyBackendCIDR: cfg.proxyBackendCIDR}
	file, err := os.OpenFile(filepath.Join(cfg.state, "owner"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, false, err
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		file.Close()
		return nil, false, fmt.Errorf("another controller owns this state: %w", err)
	}
	var existing stateIdentity
	err = json.NewDecoder(file).Decode(&existing)
	if errors.Is(err, io.EOF) {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			file.Close()
			return nil, false, err
		}
		if err := json.NewEncoder(file).Encode(wanted); err != nil {
			file.Close()
			return nil, false, err
		}
		if err := file.Sync(); err != nil {
			file.Close()
			return nil, false, err
		}
		return file, false, nil
	}
	if err != nil || !reflect.DeepEqual(existing, wanted) {
		file.Close()
		return nil, false, errors.New("state belongs to a different cluster, node or network peer")
	}
	return file, true, nil
}

func (p *provider) recover(ctx context.Context) error {
	files, err := filepath.Glob(filepath.Join(p.runner.cfg.state, "*.json"))
	if err != nil {
		return err
	}
	var recovered []*podEntry
	usedPorts := map[int]bool{}
	for _, path := range files {
		bytes, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var record journalRecord
		if err := json.Unmarshal(bytes, &record); err != nil {
			return err
		}
		if record.Pod == nil || string(record.Pod.UID) != record.View.UID || filepath.Base(path) != record.View.UID+".json" {
			return fmt.Errorf("invalid ownership journal %s", filepath.Base(path))
		}
		current, err := p.client.CoreV1().Pods(record.View.Namespace).Get(ctx, record.View.Name, metav1.GetOptions{})
		orphan := apierrors.IsNotFound(err) || (err == nil && (current.UID != record.Pod.UID || current.DeletionTimestamp != nil || current.Spec.NodeName != p.runner.cfg.nodeName))
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		lifetime, cancel := context.WithCancel(p.ctx)
		e := &podEntry{freeze: record.Freeze, wake: make(chan struct{}, 1), snapshotIDs: record.SnapshotIDs, restoredParent: record.RestoredParent, pod: record.Pod, vm: record.View.VM, ip: record.View.PodIP, slot: record.Slot, port: record.View.UDPPort,
			httpPort: record.View.HTTPPort, appPort: record.View.ApplicationPort, guestIP: record.GuestIP, applicationProgress: applicationProgress{started: record.Started},
			generation: record.View.Generation, state: record.View.State, snapshots: record.Snapshots, lifetime: lifetime, cancel: cancel, done: make(chan struct{})}
		e.allocation, err = record.guestAllocation()
		if err != nil {
			return fmt.Errorf("invalid allocation for Pod %s: %w", record.View.UID, err)
		}
		if err := validateInitJournal(record); err != nil {
			return fmt.Errorf("invalid init state for Pod %s: %w", record.View.UID, err)
		}
		if err := restoreApplicationRecords(e, record); err != nil {
			return fmt.Errorf("invalid application state for Pod %s: %w", record.View.UID, err)
		}
		if err := validateFreeze(e); err != nil {
			return err
		}
		e.declaredSpec = record.DeclaredSpec
		e.completionPhase, e.shutdownDeadline = record.CompletionPhase, record.ShutdownDeadline
		if e.state == "DRAINING" && (!hasRestartableInit(e.pod) || e.shutdownDeadline.IsZero() || e.completionPhase != corev1.PodSucceeded && e.completionPhase != corev1.PodFailed) {
			return errors.New("sidecar shutdown journal has no valid completion phase or deadline")
		}
		e.startupComplete, e.probeFailure = record.StartupComplete, record.ProbeFailure
		e.terminationReason = record.TerminationReason
		if !orphan {
			if err := validateVolumesWithCA(e.pod, p.runner.guestCAPath()); err != nil {
				return fmt.Errorf("stored Pod volume is incompatible with the configured runtime: %w", err)
			}
			if err := reconcileDeclaredSpec(e, current); err != nil {
				return err
			}
		}
		uidText := stringsWithoutHyphens(record.View.UID)
		e.proxyAvailability, e.idleOwner, e.idleTimeout, err = proxySettings(e.pod)
		if err != nil {
			return err
		}
		e.proxyPorts = record.ProxyPorts
		e.backendIP = record.BackendIP
		if e.proxyAvailability && (e.backendIP != addressAt(p.network.backendPrefix, e.slot) || len(e.proxyPorts) != len(applicationPorts(e.pod))) {
			return errors.New("proxy runtime journal does not match Pod network identity")
		}
		if len(uidText) < 20 || e.vm != "fvp-"+uidText[:20] || e.ip != p.network.address(e.slot) {
			return errors.New("journal runtime identity does not match Pod UID")
		}
		if err := validateJournalPorts(e, usedPorts); err != nil {
			return err
		}
		e.link = "pv" + uidText[:10]
		status, exists, err := p.inspectOwned(ctx, e)
		if err != nil {
			return err
		}
		e.created = exists
		p.pods[e.pod.Namespace+"/"+e.pod.Name] = e
		if orphan {
			if e.proxyAvailability {
				if _, err := p.ensureRelay(ctx); err != nil {
					return err
				}
			}
			close(e.done)
			if err := p.DeletePod(ctx, e.pod); err != nil {
				return err
			}
			continue
		}
		verified := false
		if (e.state == "SLEEPING" || e.state == "CHECKPOINTED") && len(e.snapshots) > 0 {
			selector := e.snapshots[len(e.snapshots)-1]
			if err := p.checkSnapshot(ctx, e, selector); err != nil {
				return err
			}
			if _, err := p.runner.vm(ctx, "snap", "verify", selector); err != nil {
				return err
			}
			verified = true
		}
		action := recoverAction(e.state, status, exists, verified)
		if action == "ROLLBACK" {
			if _, err := p.runner.vm(ctx, "resume", e.vm); err != nil {
				return err
			}
			action = "RUNNING"
		}
		if action == "FINISH_SLEEP" {
			if status == "Paused" {
				if _, err := p.runner.vm(ctx, "stop", "--force", e.vm); err != nil {
					return err
				}
			}
			if err := p.requireVMStatus(ctx, e, "Stopped"); err != nil {
				return err
			}
			action = "SLEEPING"
		}
		e.state = action
		if exists && (status == "Running" || status == "Paused") && action != "SLEEPING" {
			e.allocated = true
			p.activeMemoryMiB += e.allocation.MemoryMiB
		}
		if action == "FAILED" {
			e.mu.Lock()
			e.pod.Status.Phase = corev1.PodFailed
			if record.View.State != "FAILED" || e.pod.Status.Reason == "" || e.pod.Status.Reason == "ControllerRecovering" {
				e.pod.Status.Reason = "NativeRuntimeUncertain"
			}
			if record.View.State != "FAILED" || e.pod.Status.Message == "" {
				e.pod.Status.Message = "Runtime state could not be reconciled; no stale checkpoint or cold replacement was started"
			}
			reason := e.pod.Status.Reason
			e.mu.Unlock()
			p.unavailable(e, reason)
		} else if e.state != "TERMINATED" {
			p.unavailable(e, "ControllerRecovering")
		}
		if e.state == "SLEEPING" {
			p.unavailable(e, "Hibernated")
		}
		if e.state == "COLD" {
			p.unavailable(e, "NativeVMCold")
		}
		if e.state == "WAITINGVOLUMES" {
			p.unavailable(e, "WaitingForVolumes")
		}
		if err := p.journal(e); err != nil {
			return err
		}
		if e.state == "TERMINATED" {
			if err := p.stopTerminal(ctx, e); err != nil {
				return err
			}
		}
		if e.state == "FAILED" || e.state == "TERMINATED" {
			close(e.done)
		} else {
			recovered = append(recovered, e)
		}
	}
	// All journals and active allocations are accounted before any listener can wake a Pod.
	for _, e := range recovered {
		e.op.Lock()
		var err error
		if e.freeze != nil && (e.state == "RUNNING" || e.state == "READY") {
			err = p.refreshAndThaw(ctx, e)
		} else {
			err = p.refreshRecoveredTokens(ctx, e)
		}
		if err == nil {
			err = p.prepareProxy(ctx, e)
		}
		e.op.Unlock()
		if err != nil {
			return err
		}
		waiting := e.state == "WAITINGVOLUMES"
		p.jobs.Go(func() {
			defer close(e.done)
			if waiting {
				if err := p.startStrictPod(e); err != nil {
					return
				}
			}
			p.monitor(e.lifetime, e)
		})
	}
	return nil
}

func stringsWithoutHyphens(value string) string {
	result := ""
	for _, char := range value {
		if char != '-' {
			result += string(char)
		}
	}
	return result
}

func (p *provider) inspectOwned(ctx context.Context, e *podEntry) (string, bool, error) {
	output, err := p.runner.vm(ctx, "ls", "--format", "json")
	if err != nil {
		return "", false, err
	}
	var entries []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(output), &entries); err != nil {
		return "", false, err
	}
	found := false
	for _, entry := range entries {
		if entry.Name == e.vm {
			found = true
		}
	}
	if !found {
		return "", false, nil
	}
	output, err = p.runner.vm(ctx, "inspect", "--format", "json", e.vm)
	if err != nil {
		return "", true, err
	}
	type inspectedConfig struct {
		Labels         map[string]string `json:"labels"`
		SnapshotParent string            `json:"snapshot_parent"`
	}
	var inspection struct {
		Status       string           `json:"status"`
		SnapshotID   string           `json:"snapshot_id"`
		Config       inspectedConfig  `json:"config"`
		ActiveConfig *inspectedConfig `json:"active_config"`
	}
	if err := json.Unmarshal([]byte(output), &inspection); err != nil {
		return "", true, err
	}
	configs := []inspectedConfig{inspection.Config}
	if inspection.ActiveConfig != nil {
		configs = append(configs, *inspection.ActiveConfig)
	}
	for _, config := range configs {
		if e.restoredParent == "" {
			if config.SnapshotParent != "" || config.Labels[ownerLabel] != p.runner.cfg.peerOwner || config.Labels["kubernetes.io/pod-uid"] != string(e.pod.UID) {
				return "", true, errors.New("refusing operation on VM with different ownership labels")
			}
		} else {
			if config.SnapshotParent != e.restoredParent {
				return "", true, errors.New("restored VM snapshot lineage changed")
			}
			if owner, exists := config.Labels[ownerLabel]; exists && owner != p.runner.cfg.peerOwner {
				return "", true, errors.New("restored VM ownership label changed")
			}
			if uid, exists := config.Labels["kubernetes.io/pod-uid"]; exists && uid != string(e.pod.UID) {
				return "", true, errors.New("restored VM Pod UID label changed")
			}
		}
	}
	return inspection.Status, true, nil
}

func (p *provider) snapshotID(ctx context.Context, selector string) (string, error) {
	output, err := p.runner.vm(ctx, "snap", "ls", "--format", "json")
	if err != nil {
		return "", err
	}
	var entries []struct {
		Group string `json:"group"`
		Name  string `json:"name"`
		ID    string `json:"snapshot_id"`
	}
	if err := json.Unmarshal([]byte(output), &entries); err != nil {
		return "", err
	}
	for _, entry := range entries {
		if entry.Group+":"+entry.Name == selector && entry.ID != "" {
			return entry.ID, nil
		}
	}
	return "", nil
}
func (p *provider) rememberSnapshot(ctx context.Context, e *podEntry, selector string) error {
	id, err := p.snapshotID(ctx, selector)
	if err != nil {
		return err
	}
	if id == "" {
		return errors.New("owned snapshot is missing from the runtime index")
	}
	if e.snapshotIDs == nil {
		e.snapshotIDs = make(map[string]string)
	}
	e.snapshotIDs[selector] = id
	return p.journal(e)
}
func (p *provider) checkSnapshot(ctx context.Context, e *podEntry, selector string) error {
	id, err := p.snapshotID(ctx, selector)
	if err != nil {
		return err
	}
	if id == "" || id != e.snapshotIDs[selector] {
		return errors.New("snapshot identity changed; refusing stale or foreign restore")
	}
	return nil
}

func (p *provider) deleteSnapshot(ctx context.Context, e *podEntry, selector string) error {
	id, err := p.snapshotID(ctx, selector)
	if err != nil {
		return err
	}
	if id == "" {
		return nil
	}
	if id != e.snapshotIDs[selector] {
		return errors.New("refusing deletion of replacement snapshot")
	}
	_, err = p.runner.vm(ctx, "snap", "rm", "--force", selector)
	return err
}

func recoverAction(journalState, runtimeStatus string, exists, verified bool) string {
	if journalState == "COLD" || journalState == "WAITINGVOLUMES" {
		if !exists {
			return journalState
		}
		return "FAILED"
	}
	if journalState == "FAILED" || journalState == "TERMINATED" {
		return journalState
	}
	if journalState == "STARTING" || journalState == "RESTARTING" || journalState == "INITSTARTING" || journalState == "APPSTARTING" {
		// A launch can crash before submitting the application, including after
		// removing an earlier exit marker. VM liveness cannot prove task liveness.
		return "FAILED"
	}
	if journalState == "SLEEPING" {
		if exists && runtimeStatus == "Stopped" && verified {
			return "SLEEPING"
		}
		return "FAILED"
	}
	if journalState == "CHECKPOINTED" && exists && verified && (runtimeStatus == "Stopped" || runtimeStatus == "Paused") {
		return "FINISH_SLEEP"
	}
	if journalState == "HIBERNATING" && exists && runtimeStatus == "Paused" {
		return "ROLLBACK"
	}
	if exists && runtimeStatus == "Running" {
		if journalState == "INITIALIZING" || journalState == "DRAINING" {
			return journalState
		}
		return "RUNNING"
	}
	return "FAILED"
}

func validateJournalPorts(e *podEntry, used map[int]bool) error {
	claim := func(port int) error {
		if port < 1 || port > 65535 || used[port] {
			return errors.New("invalid or duplicate journaled host port")
		}
		used[port] = true
		return nil
	}
	if err := claim(e.port); err != nil {
		return err
	}
	if !e.proxyAvailability {
		if len(e.proxyPorts) != 0 || e.backendIP != "" {
			return errors.New("strict Pod has unexpected proxy journal")
		}
		if e.httpPort != 0 {
			return claim(e.httpPort)
		}
		return nil
	}
	for i, binding := range e.proxyPorts {
		if binding.ContainerPort != int(applicationPorts(e.pod)[i].ContainerPort) || (i == 0 && (binding.BackendPort != e.httpPort || binding.ContainerPort != e.appPort)) {
			return errors.New("proxy journal ports do not match Pod declaration")
		}
		if err := claim(binding.BackendPort); err != nil {
			return err
		}
		if err := claim(binding.ProxyPort); err != nil {
			return err
		}
	}
	return nil
}

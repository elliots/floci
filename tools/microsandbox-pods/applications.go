package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/floci-io/floci/tools/microsandbox-runtime/guesttask"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type applicationProgress struct {
	hooks                        lifecycleProgress
	started                      time.Time
	readiness, startup, liveness probeProgress
	startupComplete              bool
	admitted                     bool
	probeFailure                 string
	terminationReason            string
	missingSince                 time.Time
	nextRestart                  time.Time
	nextKill                     time.Time
	killSent                     bool
	ready                        bool
}

type applicationRecord struct {
	Hooks             lifecycleProgress `json:"hooks,omitzero"`
	Started           time.Time         `json:"started"`
	StartupComplete   bool              `json:"startupComplete,omitzero"`
	Admitted          bool              `json:"admitted,omitzero"`
	ProbeFailure      string            `json:"probeFailure,omitempty"`
	TerminationReason string            `json:"terminationReason,omitempty"`
	NextRestart       time.Time         `json:"nextRestart,omitzero"`
	NextKill          time.Time         `json:"nextKill,omitzero"`
	KillSent          bool              `json:"killSent,omitzero"`
}

func concurrentApplications(pod *corev1.Pod) bool {
	return len(pod.Spec.Containers) > 1 || hasRestartableInit(pod)
}

func applicationPorts(pod *corev1.Pod) []corev1.ContainerPort {
	var ports []corev1.ContainerPort
	for _, container := range pod.Spec.Containers {
		ports = append(ports, container.Ports...)
	}
	for _, container := range pod.Spec.InitContainers {
		if restartableInit(container) {
			ports = append(ports, container.Ports...)
		}
	}
	return ports
}

func podNetworkProbes(pod *corev1.Pod) bool {
	for container := range podContainers(pod) {
		if networkProbes(container) {
			return true
		}
	}
	return false
}

func applicationTask(pod *corev1.Pod, index int) string {
	if !concurrentApplications(pod) {
		return "application"
	}
	return "app-" + strconv.Itoa(index)
}

func applicationRoot(pod *corev1.Pod, index int) string {
	if !concurrentApplications(pod) {
		return "/tmp"
	}
	dir, _ := guesttask.Directory(applicationTask(pod, index))
	return dir
}

func (e *podEntry) progress(index int) *applicationProgress {
	return (managedContainer{index: index}).progress(e)
}

func (e *podEntry) applicationRecords() map[string]applicationRecord {
	if !concurrentApplications(e.pod) {
		return nil
	}
	records := make(map[string]applicationRecord, len(e.pod.Spec.Containers))
	for managed := range managedContainers(e.pod) {
		container, progress := managed.container(e), managed.progress(e)
		records[container.Name] = applicationRecord{Hooks: progress.hooks, Started: progress.started, StartupComplete: progress.startupComplete,
			Admitted:     progress.admitted,
			ProbeFailure: progress.probeFailure, TerminationReason: progress.terminationReason, NextRestart: progress.nextRestart,
			NextKill: progress.nextKill, KillSent: progress.killSent}
	}
	return records
}

func restoreApplicationRecords(e *podEntry, record journalRecord) error {
	if !concurrentApplications(e.pod) {
		if record.Version >= 4 && record.Version != 6 && record.Version != 7 && record.Version != 8 || len(record.Applications) != 0 {
			return errors.New("concurrent application journal does not match Pod containers")
		}
		return nil
	}
	count := 0
	for range managedContainers(e.pod) {
		count++
	}
	if record.Version != 6 && record.Version != 7 && record.Version != 8 && record.Version != legacyPodJournalVersion(e.pod) || len(record.Applications) != count || len(e.pod.Status.ContainerStatuses) != len(e.pod.Spec.Containers) {
		return errors.New("concurrent containers require a matching journal version with complete statuses")
	}
	for managed := range managedContainers(e.pod) {
		container := managed.container(e)
		stored, found := record.Applications[container.Name]
		status := managed.status(e)
		if !found || status.Name != container.Name || status.Image != container.Image || status.RestartCount < 0 {
			return errors.New("application status does not match its declared container")
		}
		states := 0
		if status.State.Waiting != nil {
			states++
		}
		if status.State.Running != nil {
			states++
			if stored.Started.IsZero() {
				return errors.New("running application has no journaled start time")
			}
		}
		if status.State.Terminated != nil {
			states++
		}
		if states != 1 {
			return errors.New("application status must have exactly one state")
		}
		if !stored.NextRestart.IsZero() && (status.State.Waiting == nil || status.State.Waiting.Reason != "CrashLoopBackOff" || status.LastTerminationState.Terminated == nil) {
			return errors.New("application backoff has no matching termination")
		}
		if stored.Admitted && (!managed.init || stored.Started.IsZero()) {
			return errors.New("sidecar admission has no matching start")
		}
		progress := managed.progress(e)
		*progress = applicationProgress{hooks: stored.Hooks, started: stored.Started, startupComplete: stored.StartupComplete,
			admitted:     stored.Admitted,
			probeFailure: stored.ProbeFailure, terminationReason: stored.TerminationReason, nextRestart: stored.NextRestart,
			nextKill: stored.NextKill, killSent: stored.KillSent}
	}
	return nil
}

func (p *provider) startApplications(ctx context.Context, e *podEntry) error {
	for index := range e.pod.Spec.Containers {
		if err := p.startNamedApplication(ctx, e, index); err != nil {
			return err
		}
	}
	e.state = "RUNNING"
	return p.journal(e)
}

func (p *provider) startNamedApplication(ctx context.Context, e *podEntry, index int) error {
	return p.startManagedContainer(ctx, e, managedContainer{index: index})
}

func (p *provider) startManagedContainer(ctx context.Context, e *podEntry, managed managedContainer) error {
	if err := p.archiveContainerLogs(ctx, e, logContainer{index: managed.index, init: managed.init}); err != nil {
		return err
	}
	container := managed.container(e)
	root := managed.root(e)
	if _, err := p.runner.guest(ctx, e.vm, "umask 077; mkdir -p "+quote(root)); err != nil {
		return err
	}
	args, oom, err := p.containerLaunch(ctx, e, container, managed.task(e), root)
	if err != nil {
		return p.managedPreparationFailed(e, managed, err)
	}
	request, err := guesttask.StartWithCompletion(guesttask.Request{Action: "start", ID: managed.task(e), Command: args}, oom.Complete)
	if err != nil {
		return err
	}
	// The generation and application-start fence precede submission. Recovery
	// never repeats a command whose submission may already have succeeded.
	previousState := e.state
	e.state = "APPSTARTING"
	if err := p.journal(e); err != nil {
		return err
	}
	if _, err := p.runner.nativeOutput(ctx, append([]string{"exec", "--stream", e.vm, "--"}, request.Args...), request.Stdin); err != nil {
		return fmt.Errorf("submit container %s: %w", container.Name, err)
	}
	progress := managed.progress(e)
	*progress = applicationProgress{started: time.Now(), admitted: progress.admitted}
	e.mu.Lock()
	status := managed.status(e)
	if status.LastTerminationState.Terminated != nil {
		status.RestartCount++
	}
	status.ContainerID = "containerd://" + e.vm + "/" + managed.task(e)
	status.User = nil
	status.Ready, status.Started = false, new(false)
	status.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}
	e.mu.Unlock()
	e.state = previousState
	return p.journal(e)
}

func (p *provider) applicationPreparationFailed(e *podEntry, index int, cause error) error {
	return p.managedPreparationFailed(e, managedContainer{index: index}, cause)
}

func (p *provider) managedPreparationFailed(e *podEntry, managed managedContainer, cause error) error {
	e.mu.Lock()
	status := managed.status(e)
	if status.LastTerminationState.Terminated != nil {
		status.RestartCount++
	}
	e.mu.Unlock()
	return p.managedContainerExited(e, managed, 128, "StartError", cause.Error())
}

// Called with e.op held. A backoff for one application never blocks another
// application's probes, restart, logging, or final completion observation.
func (p *provider) monitorApplications(ctx context.Context, e *podEntry) error {
	if err := p.monitorSidecars(ctx, e); err != nil {
		return err
	}
	for index := range e.pod.Spec.Containers {
		if err := p.monitorApplication(ctx, e, index); err != nil {
			return err
		}
	}
	if time.Since(e.volumesChecked) >= volumeRefreshInterval {
		check, cancel := context.WithTimeout(ctx, 15*time.Second)
		_ = p.refreshVolumes(check, e, false)
		cancel()
	}
	return p.publishApplications(ctx, e)
}

func (p *provider) monitorApplication(ctx context.Context, e *podEntry, index int) error {
	return p.monitorManagedContainer(ctx, e, managedContainer{index: index})
}

func (p *provider) monitorManagedContainer(ctx context.Context, e *podEntry, managed managedContainer) error {
	progress := managed.progress(e)
	if (*managed.status(e)).State.Terminated != nil {
		return nil
	}
	if !progress.nextRestart.IsZero() {
		if time.Now().Before(progress.nextRestart) {
			return nil
		}
		e.generation++
		return p.startManagedContainer(ctx, e, managed)
	}
	root := managed.root(e)
	read := "if test -f " + quote(root+"/application.exit") + "; then if test -f " + quote(root+"/application.oom-error") +
		"; then echo OOMERROR; else printf 'EXIT '; cat " + quote(root+"/application.exit") + "; if test -f " + quote(root+"/application.oom") +
		"; then echo OOMKilled; fi; fi; else " + ctr + " tasks list; fi"
	check, cancel := context.WithTimeout(ctx, 15*time.Second)
	output, err := p.runner.guest(check, e.vm, read)
	cancel()
	if err != nil {
		return err
	}
	if output == "OOMERROR" {
		return fmt.Errorf("container %s memory accounting could not be verified", managed.container(e).Name)
	}
	if strings.HasPrefix(output, "EXIT ") || containerTaskRunning(output, managed.task(e)) {
		if err := p.observeContainerGroups(ctx, e, managed.task(e), managed.status(e)); err != nil {
			return err
		}
	}
	if codeText, exited := strings.CutPrefix(output, "EXIT "); exited {
		reason := ""
		if trimmed, oom := strings.CutSuffix(codeText, "\nOOMKilled"); oom {
			codeText, reason = trimmed, "OOMKilled"
		}
		code, err := strconv.ParseInt(strings.TrimSpace(codeText), 10, 32)
		if err != nil || code < 0 || code > 255 {
			return errors.New("container exit status is invalid")
		}
		if err := p.releaseContainerSubpaths(ctx, e, managed.container(e)); err != nil {
			return err
		}
		if err := p.retainContainerLogs(ctx, e, logContainer{index: managed.index, init: managed.init}); err != nil {
			return err
		}
		if managed.init {
			if err := p.captureInitLogs(ctx, e, managed.index); err != nil {
				return err
			}
		}
		return p.managedContainerExited(e, managed, int32(code), reason, "")
	}
	if !containerTaskRunning(output, managed.task(e)) {
		progress.ready = false
		if progress.missingSince.IsZero() {
			progress.missingSince = time.Now()
		}
		if time.Since(progress.missingSince) >= applicationMissingGrace {
			return fmt.Errorf("container %s is absent without a published exit status for 30 seconds", managed.container(e).Name)
		}
		e.mu.Lock()
		(*managed.status(e)).Ready = false
		e.mu.Unlock()
		return nil
	}
	progress.missingSince = time.Time{}
	if e.state == "DRAINING" && progress.nextKill.IsZero() {
		e.mu.Lock()
		managed.status(e).State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(progress.started)}}
		e.mu.Unlock()
		return nil
	}
	if !progress.nextKill.IsZero() {
		if progress.killSent && time.Since(progress.nextKill) >= applicationMissingGrace {
			return fmt.Errorf("container %s did not terminate within 30 seconds of SIGKILL", managed.container(e).Name)
		}
		if !progress.killSent && !time.Now().Before(progress.nextKill) {
			if _, err := p.runner.guest(ctx, e.vm, "test -f "+quote(root+"/application.exit")+" || "+ctr+" tasks kill --signal SIGKILL "+quote(managed.task(e))+" || test -f "+quote(root+"/application.exit")); err != nil {
				return err
			}
			progress.killSent = true
			progress.nextKill = time.Now()
			return p.journal(e)
		}
		return nil
	}
	container := managed.container(e)
	probe, reason, err := p.containerHealthChecks(ctx, e, container, managed.task(e), progress)
	if err != nil {
		return err
	}
	if reason != "" {
		return p.stopManagedUnhealthy(ctx, e, managed, probe, reason)
	}
	ready := progress.startupComplete
	if managed.init && progress.startupComplete && !progress.admitted {
		progress.admitted = true
		if err := p.journal(e); err != nil {
			return err
		}
	}
	if ready && container.ReadinessProbe != nil {
		p.evaluateContainerProbe(ctx, e, container, managed.task(e), progress.started, container.ReadinessProbe, &progress.readiness)
		ready = progress.readiness.passed
	}
	progress.ready = ready
	e.mu.Lock()
	status := managed.status(e)
	status.State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(progress.started)}}
	status.Ready, status.Started = ready, new(progress.startupComplete)
	e.mu.Unlock()
	return nil
}

func (p *provider) applicationExited(e *podEntry, index int, code int32, reason, message string) error {
	return p.managedContainerExited(e, managedContainer{index: index}, code, reason, message)
}

func (p *provider) managedContainerExited(e *podEntry, managed managedContainer, code int32, reason, message string) error {
	progress := managed.progress(e)
	reason = cmp.Or(reason, progress.probeFailure)
	if reason == "" {
		reason = "Completed"
		if code != 0 {
			reason = "Error"
		}
	}
	e.mu.Lock()
	status := managed.status(e)
	exit := corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code, Reason: reason, Message: message,
		StartedAt: metav1.NewTime(progress.started), FinishedAt: metav1.Now(), ContainerID: status.ContainerID}}
	status.State, status.Ready, status.Started = exit, false, new(false)
	if e.state != "DRAINING" && (managed.init || e.pod.Spec.RestartPolicy == corev1.RestartPolicyAlways || e.pod.Spec.RestartPolicy == corev1.RestartPolicyOnFailure && (code != 0 || progress.probeFailure != "")) {
		status.LastTerminationState = exit
		status.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff", Message: "Container will restart after backoff"}}
		progress.nextRestart = time.Now().Add(min(10*time.Second*time.Duration(1<<min(status.RestartCount, 5)), 300*time.Second))
	}
	progress.missingSince = time.Time{}
	progress.ready = false
	progress.nextKill, progress.killSent = time.Time{}, false
	e.mu.Unlock()
	return p.journal(e)
}

func (p *provider) publishApplications(ctx context.Context, e *podEntry) error {
	ready, complete, failed, waiting := true, true, false, false
	e.applicationMissingSince = time.Time{}
	e.mu.Lock()
	for index, status := range e.pod.Status.ContainerStatuses {
		progress := e.progress(index)
		ready = ready && progress.ready
		complete = complete && status.State.Terminated != nil
		waiting = waiting || status.State.Waiting != nil && status.LastTerminationState.Terminated == nil
		if status.State.Terminated != nil {
			failed = failed || status.State.Terminated.ExitCode != 0 || progress.probeFailure != ""
		}
		if !progress.missingSince.IsZero() && (e.applicationMissingSince.IsZero() || progress.missingSince.Before(e.applicationMissingSince)) {
			e.applicationMissingSince = progress.missingSince
		}
	}
	for index, container := range e.pod.Spec.InitContainers {
		if restartableInit(container) {
			progress := (managedContainer{index: index, init: true}).progress(e)
			ready = ready && progress.ready
			if !progress.missingSince.IsZero() && (e.applicationMissingSince.IsZero() || progress.missingSince.Before(e.applicationMissingSince)) {
				e.applicationMissingSince = progress.missingSince
			}
		}
	}
	e.pod.Status.Phase, e.pod.Status.Reason, e.pod.Status.Message, e.pod.Status.PodIP = corev1.PodRunning, "", "", e.ip
	e.pod.Status.PodIPs, e.pod.Status.HostIP = []corev1.PodIP{{IP: e.ip}}, p.runner.cfg.nodeIP
	if waiting {
		e.pod.Status.Phase = corev1.PodPending
	}
	e.state, e.actualReady = "RUNNING", ready
	if ready {
		e.state = "READY"
	}
	if complete {
		e.state, e.pod.Status.Phase = "TERMINATED", corev1.PodSucceeded
		if failed {
			e.pod.Status.Phase = corev1.PodFailed
		}
	}
	e.pod.Status.Conditions = podConditionsFor(e.pod, ready)
	e.mu.Unlock()
	if complete && hasRestartableInit(e.pod) {
		return p.beginSidecarShutdown(e, e.pod.Status.Phase)
	}
	if err := p.journal(e); err != nil {
		return err
	}
	if complete {
		if err := p.stopTerminal(ctx, e); err != nil {
			return err
		}
	}
	p.publish(e)
	return nil
}

func (p *provider) stopNamedUnhealthy(ctx context.Context, e *podEntry, index int, probe *corev1.Probe, reason string) error {
	return p.stopManagedUnhealthy(ctx, e, managedContainer{index: index}, probe, reason)
}

func (p *provider) stopManagedUnhealthy(ctx context.Context, e *podEntry, managed managedContainer, probe *corev1.Probe, reason string) error {
	progress := managed.progress(e)
	progress.probeFailure = reason
	progress.ready = false
	e.mu.Lock()
	(*managed.status(e)).Ready = false
	e.mu.Unlock()
	if err := p.journal(e); err != nil {
		return err
	}
	grace := int64(corev1.DefaultTerminationGracePeriodSeconds)
	if value := e.pod.Spec.TerminationGracePeriodSeconds; value != nil {
		grace = *value
	}
	if probe != nil && probe.TerminationGracePeriodSeconds != nil {
		grace = *probe.TerminationGracePeriodSeconds
	}
	progress.nextKill = time.Now().Add(time.Duration(grace) * time.Second)
	if e.state == "DRAINING" {
		progress.nextKill = minTime(progress.nextKill, e.shutdownDeadline)
	}
	if err := p.journal(e); err != nil {
		return err
	}
	if err := p.preStop(ctx, e, managed, progress.nextKill); err != nil {
		slog.Warn("preStop hook did not complete", "pod", e.pod.Name, "container", managed.container(e).Name, "error", err)
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil && !time.Now().Before(progress.nextKill) {
			progress.nextKill = progress.nextKill.Add(2 * time.Second)
			if e.state == "DRAINING" && progress.nextKill.After(e.shutdownDeadline) {
				e.shutdownDeadline = progress.nextKill
			}
			if err := p.journal(e); err != nil {
				return err
			}
		}
	}
	p.event(e, corev1.EventTypeNormal, "Killing", "Stopping container %s: %s", managed.container(e).Name, reason)
	root, task := managed.root(e), managed.task(e)
	script := "test -f " + quote(root+"/application.exit") + " || " + ctr + " tasks kill --signal SIGTERM " + quote(task) + " || test -f " + quote(root+"/application.exit")
	if _, err := p.runner.guest(ctx, e.vm, script); err != nil {
		return err
	}
	// Escalation is tracked per container; other containers keep being observed
	// while this container consumes its graceful termination period.
	return p.journal(e)
}

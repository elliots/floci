package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/floci-io/floci/tools/microsandbox-runtime/guesttask"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type applicationProgress struct {
	started                      time.Time
	readiness, startup, liveness probeProgress
	startupComplete              bool
	probeFailure                 string
	terminationReason            string
	missingSince                 time.Time
	nextRestart                  time.Time
	nextKill                     time.Time
	killSent                     bool
	ready                        bool
}

type applicationRecord struct {
	Started           time.Time `json:"started"`
	StartupComplete   bool      `json:"startupComplete,omitzero"`
	ProbeFailure      string    `json:"probeFailure,omitempty"`
	TerminationReason string    `json:"terminationReason,omitempty"`
	NextRestart       time.Time `json:"nextRestart,omitzero"`
	NextKill          time.Time `json:"nextKill,omitzero"`
	KillSent          bool      `json:"killSent,omitzero"`
}

func concurrentApplications(pod *corev1.Pod) bool { return len(pod.Spec.Containers) > 1 }

func applicationPorts(pod *corev1.Pod) []corev1.ContainerPort {
	var ports []corev1.ContainerPort
	for _, container := range pod.Spec.Containers {
		ports = append(ports, container.Ports...)
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
	if !concurrentApplications(e.pod) {
		return &e.applicationProgress
	}
	if e.applications == nil {
		e.applications = make(map[string]*applicationProgress)
	}
	name := e.pod.Spec.Containers[index].Name
	if e.applications[name] == nil {
		e.applications[name] = &applicationProgress{}
	}
	return e.applications[name]
}

func (e *podEntry) applicationRecords() map[string]applicationRecord {
	if !concurrentApplications(e.pod) {
		return nil
	}
	records := make(map[string]applicationRecord, len(e.pod.Spec.Containers))
	for i, container := range e.pod.Spec.Containers {
		progress := e.progress(i)
		records[container.Name] = applicationRecord{Started: progress.started, StartupComplete: progress.startupComplete,
			ProbeFailure: progress.probeFailure, TerminationReason: progress.terminationReason, NextRestart: progress.nextRestart,
			NextKill: progress.nextKill, KillSent: progress.killSent}
	}
	return records
}

func restoreApplicationRecords(e *podEntry, record journalRecord) error {
	if !concurrentApplications(e.pod) {
		if record.Version == 4 || len(record.Applications) != 0 {
			return errors.New("concurrent application journal does not match Pod containers")
		}
		return nil
	}
	if record.Version != 4 || len(record.Applications) != len(e.pod.Spec.Containers) || len(e.pod.Status.ContainerStatuses) != len(e.pod.Spec.Containers) {
		return errors.New("multiple applications require a version 4 journal with complete statuses")
	}
	for index, container := range e.pod.Spec.Containers {
		stored, found := record.Applications[container.Name]
		status := e.pod.Status.ContainerStatuses[index]
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
		progress := e.progress(index)
		*progress = applicationProgress{started: stored.Started, startupComplete: stored.StartupComplete,
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
	container := e.pod.Spec.Containers[index]
	root := applicationRoot(e.pod, index)
	if _, err := p.runner.guest(ctx, e.vm, "umask 077; mkdir -p "+quote(root)); err != nil {
		return err
	}
	args, oom, err := p.containerLaunch(ctx, e, container, applicationTask(e.pod, index), root)
	if err != nil {
		return p.applicationPreparationFailed(e, index, err)
	}
	request, err := guesttask.StartWithCompletion(guesttask.Request{Action: "start", ID: applicationTask(e.pod, index), Command: args}, oom.Complete)
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
	progress := e.progress(index)
	*progress = applicationProgress{started: time.Now()}
	e.mu.Lock()
	status := &e.pod.Status.ContainerStatuses[index]
	if status.LastTerminationState.Terminated != nil {
		status.RestartCount++
	}
	status.ContainerID = "containerd://" + e.vm + "/" + applicationTask(e.pod, index)
	status.Ready, status.Started = false, new(false)
	status.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}
	e.mu.Unlock()
	e.state = previousState
	return p.journal(e)
}

func (p *provider) applicationPreparationFailed(e *podEntry, index int, cause error) error {
	e.mu.Lock()
	status := &e.pod.Status.ContainerStatuses[index]
	if status.LastTerminationState.Terminated != nil {
		status.RestartCount++
	}
	e.mu.Unlock()
	return p.applicationExited(e, index, 128, "StartError", cause.Error())
}

// Called with e.op held. A backoff for one application never blocks another
// application's probes, restart, logging, or final completion observation.
func (p *provider) monitorApplications(ctx context.Context, e *podEntry) error {
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
	progress := e.progress(index)
	if e.pod.Status.ContainerStatuses[index].State.Terminated != nil {
		return nil
	}
	if !progress.nextRestart.IsZero() {
		if time.Now().Before(progress.nextRestart) {
			return nil
		}
		e.generation++
		return p.startNamedApplication(ctx, e, index)
	}
	root := applicationRoot(e.pod, index)
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
		return fmt.Errorf("container %s memory accounting could not be verified", e.pod.Spec.Containers[index].Name)
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
		if err := p.releaseContainerSubpaths(ctx, e, e.pod.Spec.Containers[index]); err != nil {
			return err
		}
		return p.applicationExited(e, index, int32(code), reason, "")
	}
	if !containerTaskRunning(output, applicationTask(e.pod, index)) {
		progress.ready = false
		if progress.missingSince.IsZero() {
			progress.missingSince = time.Now()
		}
		if time.Since(progress.missingSince) >= applicationMissingGrace {
			return fmt.Errorf("container %s is absent without a published exit status for 30 seconds", e.pod.Spec.Containers[index].Name)
		}
		e.mu.Lock()
		e.pod.Status.ContainerStatuses[index].Ready = false
		e.mu.Unlock()
		return nil
	}
	progress.missingSince = time.Time{}
	if !progress.nextKill.IsZero() {
		if progress.killSent && time.Since(progress.nextKill) >= applicationMissingGrace {
			return fmt.Errorf("container %s did not terminate within 30 seconds of SIGKILL", e.pod.Spec.Containers[index].Name)
		}
		if !progress.killSent && !time.Now().Before(progress.nextKill) {
			if _, err := p.runner.guest(ctx, e.vm, "test -f "+quote(root+"/application.exit")+" || "+ctr+" tasks kill --signal SIGKILL "+quote(applicationTask(e.pod, index))+" || test -f "+quote(root+"/application.exit")); err != nil {
				return err
			}
			progress.killSent = true
			progress.nextKill = time.Now()
			return p.journal(e)
		}
		return nil
	}
	container := e.pod.Spec.Containers[index]
	probe, reason, err := p.containerHealthChecks(ctx, e, container, applicationTask(e.pod, index), progress)
	if err != nil {
		return err
	}
	if reason != "" {
		return p.stopNamedUnhealthy(ctx, e, index, probe, reason)
	}
	ready := progress.startupComplete
	if ready && container.ReadinessProbe != nil {
		p.evaluateContainerProbe(ctx, e, container, applicationTask(e.pod, index), progress.started, container.ReadinessProbe, &progress.readiness)
		ready = progress.readiness.passed
	}
	progress.ready = ready
	e.mu.Lock()
	status := &e.pod.Status.ContainerStatuses[index]
	status.State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(progress.started)}}
	status.Ready, status.Started = ready, new(progress.startupComplete)
	e.mu.Unlock()
	return nil
}

func (p *provider) applicationExited(e *podEntry, index int, code int32, reason, message string) error {
	progress := e.progress(index)
	reason = cmp.Or(reason, progress.probeFailure)
	if reason == "" {
		reason = "Completed"
		if code != 0 {
			reason = "Error"
		}
	}
	e.mu.Lock()
	status := &e.pod.Status.ContainerStatuses[index]
	exit := corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code, Reason: reason, Message: message,
		StartedAt: metav1.NewTime(progress.started), FinishedAt: metav1.Now(), ContainerID: status.ContainerID}}
	status.State, status.Ready, status.Started = exit, false, new(false)
	if e.pod.Spec.RestartPolicy == corev1.RestartPolicyAlways || e.pod.Spec.RestartPolicy == corev1.RestartPolicyOnFailure && (code != 0 || progress.probeFailure != "") {
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
	progress := e.progress(index)
	progress.probeFailure = reason
	progress.ready = false
	e.mu.Lock()
	e.pod.Status.ContainerStatuses[index].Ready = false
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
	root, task := applicationRoot(e.pod, index), applicationTask(e.pod, index)
	script := "test -f " + quote(root+"/application.exit") + " || " + ctr + " tasks kill --signal SIGTERM " + quote(task) + " || test -f " + quote(root+"/application.exit")
	if _, err := p.runner.guest(ctx, e.vm, script); err != nil {
		return err
	}
	// Escalation is tracked per container; other containers keep being observed
	// while this container consumes its graceful termination period.
	progress.nextKill = time.Now().Add(time.Duration(grace) * time.Second)
	return p.journal(e)
}

package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func podContainers(pod *corev1.Pod) iter.Seq[corev1.Container] {
	return func(yield func(corev1.Container) bool) {
		for _, containers := range [][]corev1.Container{pod.Spec.InitContainers, pod.Spec.Containers} {
			for _, container := range containers {
				if !yield(container) {
					return
				}
			}
		}
	}
}

func validateInitContainers(pod *corev1.Pod) error {
	names := make(map[string]bool)
	initNames := make(map[string]bool)
	for _, container := range pod.Spec.InitContainers {
		initNames[container.Name] = true
	}
	for container := range podContainers(pod) {
		if names[container.Name] {
			return fmt.Errorf("duplicate container name %q", container.Name)
		}
		names[container.Name] = true
		if len(container.EnvFrom) != 0 {
			return errors.New("unresolved envFrom source")
		}
		if container.RestartPolicy != nil && (!initNames[container.Name] || !restartableInit(container)) || len(container.RestartPolicyRules) != 0 ||
			len(container.VolumeDevices) != 0 || container.Lifecycle != nil || container.Stdin || container.TTY {
			return errors.New("container restart overrides other than Always init sidecars, volume devices, hooks and TTY are not qualified")
		}
		for _, env := range container.Env {
			if env.ValueFrom != nil {
				return errors.New("unresolved environment reference")
			}
		}
		for _, port := range container.Ports {
			if port.HostPort != 0 || port.HostIP != "" {
				return errors.New("host ports are not qualified")
			}
		}
	}
	for _, container := range pod.Spec.InitContainers {
		if restartableInit(container) {
			if err := validateProbes(container); err != nil {
				return err
			}
			continue
		}
		if container.ReadinessProbe != nil || container.StartupProbe != nil || container.LivenessProbe != nil {
			return fmt.Errorf("ordinary init container %s cannot have probes", container.Name)
		}
	}
	return nil
}

func initializeInitStatuses(pod *corev1.Pod) {
	for _, container := range pod.Spec.InitContainers {
		pod.Status.InitContainerStatuses = append(pod.Status.InitContainerStatuses, corev1.ContainerStatus{
			Name: container.Name, Image: container.Image, Started: new(false),
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}},
		})
	}
}

func nextInitContainer(pod *corev1.Pod) int {
	for index, container := range pod.Spec.InitContainers {
		if index >= len(pod.Status.InitContainerStatuses) {
			return index
		}
		status := pod.Status.InitContainerStatuses[index]
		if restartableInit(container) && status.Started != nil && *status.Started {
			continue
		}
		if status.Name != container.Name || status.State.Terminated == nil || status.State.Terminated.ExitCode != 0 {
			return index
		}
	}
	return -1
}

func podConditionsFor(pod *corev1.Pod, ready bool) []corev1.PodCondition {
	conditions := podConditions(ready)
	initialized := false
	for _, condition := range pod.Status.Conditions {
		initialized = initialized || condition.Type == corev1.PodInitialized && condition.Status == corev1.ConditionTrue
	}
	if nextInitContainer(pod) >= 0 && !(hasRestartableInit(pod) && initialized) {
		conditions[0].Status = corev1.ConditionFalse
		conditions[0].Reason = "ContainersNotInitialized"
		conditions[0].Message = "Init containers have not all completed successfully"
	}
	return conditions
}

func podJournalVersion(pod *corev1.Pod) int {
	if hasRestartableInit(pod) {
		return 5
	}
	if concurrentApplications(pod) {
		return 4
	}
	if len(pod.Spec.InitContainers) > 0 {
		return 3
	}
	return 2
}

func validateInitJournal(record journalRecord) error {
	if len(record.Pod.Spec.InitContainers) == 0 {
		return nil
	}
	if hasRestartableInit(record.Pod) {
		return validateSidecarInitJournal(record)
	}
	if record.Version < 3 || record.Version > 4 || len(record.Pod.Spec.InitContainers) != len(record.Pod.Status.InitContainerStatuses) {
		return errors.New("init containers require a supported journal with complete statuses")
	}
	completed := true
	for index, container := range record.Pod.Spec.InitContainers {
		status := record.Pod.Status.InitContainerStatuses[index]
		if status.Name != container.Name || status.Image != container.Image {
			return errors.New("init status does not match its declared container")
		}
		states := 0
		if status.State.Waiting != nil {
			states++
		}
		if status.State.Running != nil {
			states++
		}
		if status.State.Terminated != nil {
			states++
		}
		if states != 1 || status.RestartCount < 0 {
			return errors.New("init status has an invalid state or restart count")
		}
		if !completed && (status.State.Waiting == nil || status.State.Waiting.Reason != "PodInitializing" ||
			status.RestartCount != 0 || status.LastTerminationState.Terminated != nil) {
			return errors.New("only the first incomplete init container may execute")
		}
		if status.State.Terminated != nil && status.State.Terminated.ExitCode == 0 {
			if !completed {
				return errors.New("init completion is not a sequential prefix")
			}
		} else {
			completed = false
		}
	}
	if !completed {
		switch record.View.State {
		case "COLD", "WAITINGVOLUMES", "STARTING", "INITSTARTING", "INITIALIZING", "FAILED", "TERMINATED":
		default:
			return errors.New("application lifecycle requires completed init containers")
		}
	}
	return nil
}

func initTask(index int) string { return "init-" + strconv.Itoa(index) }
func initRoot(index int) string { return "/tmp/floci-" + initTask(index) }

func executingContainerTask(e *podEntry, name string) (string, error) {
	for index, container := range e.pod.Spec.InitContainers {
		if container.Name == name {
			available := e.state == "INITIALIZING" || restartableInit(container) && (e.state == "READY" || e.state == "RUNNING" || e.state == "DRAINING")
			if !available || e.pod.Status.InitContainerStatuses[index].State.Running == nil {
				return "", errdefs.InvalidInput("init container is not executing")
			}
			return initTask(index), nil
		}
	}
	for index, container := range e.pod.Spec.Containers {
		if name == container.Name {
			if e.state != "READY" && e.state != "RUNNING" || concurrentApplications(e.pod) && e.pod.Status.ContainerStatuses[index].State.Running == nil {
				return "", errdefs.InvalidInput("application container is not executing")
			}
			return applicationTask(e.pod, index), nil
		}
	}
	return "", errdefs.NotFound("container not found")
}

// Called under e.op. A single guest supervisor executes init tasks in order.
// Initialization continues in the monitor, without holding a VM-start slot or
// tying a long-running init container to the wake request's deadline.
func (p *provider) advanceInit(ctx context.Context, e *podEntry) error {
	index := nextInitForEntry(e)
	if index < 0 {
		e.mu.Lock()
		e.pod.Status.Conditions = podConditions(false)
		e.mu.Unlock()
		e.state = "STARTING"
		if err := p.journal(e); err != nil {
			return err
		}
		return p.startApplication(ctx, e)
	}
	if restartableInit(e.pod.Spec.InitContainers[index]) {
		managed := managedContainer{index: index, init: true}
		if !managed.progress(e).started.IsZero() || !managed.progress(e).nextRestart.IsZero() {
			return nil
		}
		return p.startManagedContainer(ctx, e, managed)
	}
	status := e.pod.Status.InitContainerStatuses[index]
	if status.State.Running != nil {
		return nil
	}
	if last := status.LastTerminationState.Terminated; last != nil {
		delay := min(10*time.Second*time.Duration(1<<min(status.RestartCount, 5)), 300*time.Second)
		if time.Since(last.FinishedAt.Time) < delay {
			return nil
		}
	}
	container := e.pod.Spec.InitContainers[index]
	root := initRoot(index)
	if _, err := p.runner.guest(ctx, e.vm, "mkdir -p "+quote(root)); err != nil {
		return err
	}
	args, oom, err := p.containerLaunch(ctx, e, container, initTask(index), root)
	if err != nil {
		return p.initPreparationFailed(e, index, err)
	}
	// The journal must distinguish an ambiguous command submission from a
	// confirmed running init. Recovery never guesses whether it already ran.
	e.state = "INITSTARTING"
	if err := p.journal(e); err != nil {
		return err
	}
	cleanup := "rm -f " + quote(path.Join(root, "application.exit")) + "; "
	script := cleanup + applicationRequestScript(containerProcessScript(args, oom.Complete, root))
	if _, err := p.runner.guest(ctx, e.vm, script); err != nil {
		return fmt.Errorf("submit init container %s to guest supervisor: %w", container.Name, err)
	}
	e.mu.Lock()
	current := &e.pod.Status.InitContainerStatuses[index]
	if current.LastTerminationState.Terminated != nil {
		current.RestartCount++
	}
	current.ContainerID = "containerd://" + e.vm + "/" + initTask(index)
	current.User = nil
	current.State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}
	current.Started, current.Ready = new(true), false
	e.pod.Status.Phase, e.pod.Status.Reason, e.pod.Status.Message = corev1.PodPending, "PodInitializing", ""
	for i := range e.pod.Status.ContainerStatuses {
		e.pod.Status.ContainerStatuses[i].State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}
	}
	e.pod.Status.Conditions = podConditionsFor(e.pod, false)
	e.mu.Unlock()
	e.state = "INITIALIZING"
	e.applicationMissingSince = time.Time{}
	if err := p.journal(e); err != nil {
		return err
	}
	p.publish(e)
	return nil
}

func (p *provider) monitorInit(ctx context.Context, e *podEntry) error {
	if time.Since(e.volumesChecked) >= volumeRefreshInterval {
		_ = p.refreshVolumes(ctx, e, false)
	}
	if err := p.monitorSidecars(ctx, e); err != nil {
		return err
	}
	if hasRestartableInit(e.pod) {
		p.publish(e)
	}
	index := nextInitForEntry(e)
	if index >= 0 && restartableInit(e.pod.Spec.InitContainers[index]) {
		return p.advanceInit(ctx, e)
	}
	if index < 0 || e.pod.Status.InitContainerStatuses[index].State.Running == nil {
		return p.advanceInit(ctx, e)
	}
	root := initRoot(index)
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
		return errors.New("init container terminated but its memory accounting could not be verified")
	}
	if strings.HasPrefix(output, "EXIT ") || containerTaskRunning(output, initTask(index)) {
		if err := p.observeContainerGroups(ctx, e, initTask(index), &e.pod.Status.InitContainerStatuses[index]); err != nil {
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
			return errors.New("init container exit status is invalid")
		}
		if err := p.captureInitLogs(ctx, e, index); err != nil {
			return err
		}
		if err := p.releaseContainerSubpaths(ctx, e, e.pod.Spec.InitContainers[index]); err != nil {
			return err
		}
		return p.initExited(e, index, int32(code), reason)
	}
	if containerTaskRunning(output, initTask(index)) {
		missing := !e.applicationMissingSince.IsZero()
		e.applicationMissingSince = time.Time{}
		if missing {
			p.publish(e)
		}
		return nil
	}
	if e.applicationMissingSince.IsZero() {
		e.applicationMissingSince = time.Now()
		p.publish(e)
	}
	if time.Since(e.applicationMissingSince) >= applicationMissingGrace {
		return errors.New("init task is absent or stopped without a published exit status for 30 seconds")
	}
	return nil
}

func (p *provider) initPreparationFailed(e *podEntry, index int, cause error) error {
	e.mu.Lock()
	status := &e.pod.Status.InitContainerStatuses[index]
	if status.LastTerminationState.Terminated != nil {
		status.RestartCount++
	}
	e.mu.Unlock()
	return p.finishInitAttempt(e, index, corev1.ContainerStateTerminated{ExitCode: 128, Reason: "StartError", Message: cause.Error(), FinishedAt: metav1.Now()})
}

func (p *provider) initExited(e *podEntry, index int, code int32, reason string) error {
	status := e.pod.Status.InitContainerStatuses[index]
	if status.State.Running == nil {
		return errors.New("init exit has no matching running attempt")
	}
	if reason == "" {
		reason = "Completed"
		if code != 0 {
			reason = "Error"
		}
	}
	return p.finishInitAttempt(e, index, corev1.ContainerStateTerminated{ExitCode: code, Reason: reason,
		StartedAt: status.State.Running.StartedAt, FinishedAt: metav1.Now(), ContainerID: status.ContainerID})
}

func (p *provider) finishInitAttempt(e *podEntry, index int, exit corev1.ContainerStateTerminated) error {
	e.applicationMissingSince = time.Time{}
	e.mu.Lock()
	status := &e.pod.Status.InitContainerStatuses[index]
	status.State = corev1.ContainerState{Terminated: &exit}
	status.Ready, status.Started = exit.ExitCode == 0, new(false)
	terminal := exit.ExitCode != 0 && e.pod.Spec.RestartPolicy == corev1.RestartPolicyNever
	if exit.ExitCode != 0 && !terminal {
		status.LastTerminationState = status.State
		status.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff", Message: cmp.Or(exit.Message, "Init container failed; retrying with backoff")}}
	}
	e.pod.Status.Conditions = podConditionsFor(e.pod, false)
	e.state = "INITIALIZING"
	if terminal {
		e.state = "TERMINATED"
		e.pod.Status.Phase, e.pod.Status.Reason, e.pod.Status.Message = corev1.PodFailed, "InitContainerFailed", "Init container "+status.Name+" failed: "+exit.Reason
		for i := range e.pod.Status.ContainerStatuses {
			e.pod.Status.ContainerStatuses[i].State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}
		}
	}
	e.mu.Unlock()
	if terminal && hasRestartableInit(e.pod) {
		return p.beginSidecarShutdown(e, corev1.PodFailed)
	}
	if err := p.journal(e); err != nil {
		return err
	}
	p.publish(e)
	if terminal {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return p.stopTerminal(ctx, e)
	}
	return nil
}

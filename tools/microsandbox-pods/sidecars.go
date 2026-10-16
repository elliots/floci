package main

import (
	"context"
	"errors"
	"time"

	corev1 "k8s.io/api/core/v1"
)

func nextInitForEntry(e *podEntry) int {
	for index, container := range e.pod.Spec.InitContainers {
		if restartableInit(container) {
			if !(managedContainer{index: index, init: true}).progress(e).admitted {
				return index
			}
			continue
		}
		status := e.pod.Status.InitContainerStatuses[index]
		if status.State.Terminated == nil || status.State.Terminated.ExitCode != 0 {
			return index
		}
	}
	return -1
}

func (p *provider) monitorSidecars(ctx context.Context, e *podEntry) error {
	for index, container := range e.pod.Spec.InitContainers {
		if !restartableInit(container) {
			continue
		}
		managed := managedContainer{index: index, init: true}
		progress := managed.progress(e)
		if progress.started.IsZero() && progress.nextRestart.IsZero() {
			continue
		}
		if err := p.monitorManagedContainer(ctx, e, managed); err != nil {
			return err
		}
	}
	return nil
}

func validateSidecarInitJournal(record journalRecord) error {
	if record.Version != 5 || len(record.Pod.Spec.InitContainers) != len(record.Pod.Status.InitContainerStatuses) {
		return errors.New("restartable init sidecars require a version 5 journal with complete statuses")
	}
	completed := true
	for index, container := range record.Pod.Spec.InitContainers {
		status := record.Pod.Status.InitContainerStatuses[index]
		if status.Name != container.Name || status.Image != container.Image || status.RestartCount < 0 {
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
		if states != 1 {
			return errors.New("init status must have exactly one state")
		}
		if !completed && (status.State.Waiting == nil || status.State.Waiting.Reason != "PodInitializing" || status.RestartCount != 0 || status.LastTerminationState.Terminated != nil) {
			return errors.New("init stage ran before earlier stages completed startup")
		}
		if restartableInit(container) {
			stored, found := record.Applications[container.Name]
			if !found {
				return errors.New("restartable init sidecar is missing its progress journal")
			}
			completed = completed && stored.Admitted
		} else {
			completed = completed && status.State.Terminated != nil && status.State.Terminated.ExitCode == 0
		}
	}
	if !completed {
		switch record.View.State {
		case "COLD", "WAITINGVOLUMES", "STARTING", "INITSTARTING", "APPSTARTING", "INITIALIZING", "DRAINING", "FAILED", "TERMINATED":
		default:
			return errors.New("application lifecycle requires completed init startup")
		}
	}
	return nil
}

func (p *provider) beginSidecarShutdown(e *podEntry, phase corev1.PodPhase) error {
	if e.state == "DRAINING" {
		return nil
	}
	grace := int64(corev1.DefaultTerminationGracePeriodSeconds)
	if e.pod.Spec.TerminationGracePeriodSeconds != nil {
		grace = max(0, *e.pod.Spec.TerminationGracePeriodSeconds)
	}
	e.state, e.completionPhase, e.actualReady = "DRAINING", phase, false
	e.shutdownDeadline = time.Now().Add(time.Duration(grace) * time.Second)
	for _, progress := range e.applications {
		if !progress.killSent && progress.nextKill.After(e.shutdownDeadline) {
			progress.nextKill = e.shutdownDeadline
		}
	}
	e.mu.Lock()
	e.pod.Status.Phase = corev1.PodRunning
	e.pod.Status.Conditions = podConditionsFor(e.pod, false)
	e.mu.Unlock()
	if err := p.journal(e); err != nil {
		return err
	}
	p.publish(e)
	return nil
}

func (p *provider) drainBeforeDeletion(ctx context.Context, e *podEntry, requested *corev1.Pod) error {
	if err := p.beginSidecarShutdown(e, corev1.PodFailed); err != nil {
		return err
	}
	deadline := e.shutdownDeadline
	if requested.DeletionGracePeriodSeconds != nil {
		deadline = minTime(deadline, time.Now().Add(time.Duration(max(0, *requested.DeletionGracePeriodSeconds))*time.Second))
	}
	if requested.DeletionTimestamp != nil {
		deadline = minTime(deadline, requested.DeletionTimestamp.Time)
	}
	if deadline.Before(e.shutdownDeadline) {
		e.shutdownDeadline = deadline
		for _, progress := range e.applications {
			if !progress.killSent && progress.nextKill.After(deadline) {
				progress.nextKill = deadline
			}
		}
		if err := p.journal(e); err != nil {
			return err
		}
	}
	if !time.Now().Before(deadline) {
		return nil
	}
	// Regular applications get their termination signal before any sidecar.
	// Ordinary init stages may also be active when the Pod is deleted.
	for index := range e.pod.Spec.Containers {
		managed := managedContainer{index: index}
		progress := managed.progress(e)
		if progress.started.IsZero() || !progress.nextRestart.IsZero() || managed.status(e).State.Terminated != nil {
			continue
		}
		if err := p.stopManagedUnhealthy(ctx, e, managed, nil, "PodDeleted"); err != nil {
			return err
		}
	}
	for index, container := range e.pod.Spec.InitContainers {
		if !restartableInit(container) && e.pod.Status.InitContainerStatuses[index].State.Running != nil {
			if _, err := p.runner.guest(ctx, e.vm, ctr+" tasks kill --signal SIGTERM "+quote(initTask(index))+" || test -f "+quote(initRoot(index)+"/application.exit")); err != nil {
				return err
			}
		}
	}
	for time.Now().Before(e.shutdownDeadline) {
		applicationsStopped := true
		for index := range e.pod.Spec.Containers {
			managed := managedContainer{index: index}
			progress := managed.progress(e)
			if progress.started.IsZero() || !progress.nextRestart.IsZero() || managed.status(e).State.Terminated != nil {
				continue
			}
			if err := p.monitorManagedContainer(ctx, e, managed); err != nil {
				return err
			}
			applicationsStopped = applicationsStopped && managed.status(e).State.Terminated != nil
		}
		if applicationsStopped {
			for index, container := range e.pod.Spec.InitContainers {
				if !restartableInit(container) && e.pod.Status.InitContainerStatuses[index].State.Running != nil {
					output, err := p.runner.guest(ctx, e.vm, ctr+" tasks list")
					if err != nil {
						return err
					}
					applicationsStopped = applicationsStopped && !containerTaskRunning(output, initTask(index))
				}
			}
		}
		if applicationsStopped {
			if err := p.drainSidecars(ctx, e); err != nil || e.state == "TERMINATED" {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	// The caller removes this exact owned VM after the shared grace expires,
	// which also terminates tasks that ignored SIGTERM without leaking helpers.
	return nil
}

func minTime(first, second time.Time) time.Time {
	if second.Before(first) {
		return second
	}
	return first
}

// Stop sidecars in reverse declaration order. A single Pod grace deadline is
// journaled so controller recovery cannot grant a fresh grace period per task.
func (p *provider) drainSidecars(ctx context.Context, e *podEntry) error {
	for index := len(e.pod.Spec.InitContainers) - 1; index >= 0; index-- {
		if !restartableInit(e.pod.Spec.InitContainers[index]) {
			continue
		}
		managed := managedContainer{index: index, init: true}
		progress, status := managed.progress(e), managed.status(e)
		if status.State.Terminated != nil || progress.started.IsZero() && status.LastTerminationState.Terminated == nil {
			continue
		}
		if !progress.nextRestart.IsZero() {
			e.mu.Lock()
			status.State = *status.LastTerminationState.DeepCopy()
			status.Ready, status.Started = false, new(false)
			e.mu.Unlock()
			progress.nextRestart = time.Time{}
			if err := p.journal(e); err != nil {
				return err
			}
			continue
		}
		if err := p.monitorManagedContainer(ctx, e, managed); err != nil {
			return err
		}
		if status.State.Terminated != nil {
			continue
		}
		if status.State.Running != nil && progress.nextKill.IsZero() {
			grace := max(int64(0), int64(time.Until(e.shutdownDeadline).Seconds()))
			probe := &corev1.Probe{TerminationGracePeriodSeconds: new(grace)}
			if err := p.stopManagedUnhealthy(ctx, e, managed, probe, "SidecarShutdown"); err != nil {
				return err
			}
		}
		return nil
	}
	e.state, e.actualReady = "TERMINATED", false
	e.mu.Lock()
	e.pod.Status.Phase = e.completionPhase
	e.pod.Status.Conditions = podConditionsFor(e.pod, false)
	e.mu.Unlock()
	if err := p.journal(e); err != nil {
		return err
	}
	if err := p.stopTerminal(ctx, e); err != nil {
		return err
	}
	p.publish(e)
	return nil
}

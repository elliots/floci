package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Hook submission is journaled before invoking user code. An uncertain hook is
// failed after recovery rather than replayed against an already-running app.
type lifecycleProgress struct {
	PostStart string `json:"postStart,omitempty"`
	PreStop   string `json:"preStop,omitempty"`
}

func validateLifecycle(container corev1.Container) error {
	lifecycle := container.Lifecycle
	if lifecycle == nil {
		return nil
	}
	if lifecycle.StopSignal != nil {
		return errors.New("custom lifecycle stop signals are not supported")
	}
	for name, handler := range map[string]*corev1.LifecycleHandler{"postStart": lifecycle.PostStart, "preStop": lifecycle.PreStop} {
		if handler == nil {
			continue
		}
		count := 0
		if handler.Exec != nil {
			count++
			if len(handler.Exec.Command) == 0 {
				return fmt.Errorf("%s exec requires a command", name)
			}
		}
		if handler.HTTPGet != nil {
			count++
			probe := &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: handler.HTTPGet}}
			if err := validateProbes(corev1.Container{Ports: container.Ports, ReadinessProbe: probe}); err != nil {
				return err
			}
		}
		if handler.Sleep != nil {
			count++
			if name != "preStop" || handler.Sleep.Seconds < 0 || handler.Sleep.Seconds > math.MaxInt64/int64(time.Second) {
				return errors.New("sleep lifecycle handlers require preStop and nonnegative seconds")
			}
		}
		if count != 1 || handler.TCPSocket != nil {
			return fmt.Errorf("%s requires exactly one exec, HTTP or sleep handler", name)
		}
	}
	return nil
}

func (p *provider) runLifecycleHook(ctx context.Context, e *podEntry, container corev1.Container, task string, handler *corev1.LifecycleHandler, state *string, kind string) error {
	if handler == nil || *state == "done" {
		return nil
	}
	if *state != "" {
		return fmt.Errorf("%s handler has an incomplete previous invocation", kind)
	}
	*state = "pending"
	if err := p.journal(e); err != nil {
		return err
	}
	var err error
	if handler.Sleep != nil {
		timer := time.NewTimer(time.Duration(handler.Sleep.Seconds) * time.Second)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-timer.C:
		}
	} else {
		timeout := int32(math.MaxInt32)
		if deadline, found := ctx.Deadline(); found {
			timeout = max(1, int32(time.Until(deadline).Seconds()))
		}
		probe := &corev1.Probe{TimeoutSeconds: timeout, ProbeHandler: corev1.ProbeHandler{Exec: handler.Exec, HTTPGet: handler.HTTPGet}}
		var success bool
		success, err = p.executeContainerCheck(ctx, e, container, task, probe, true)
		if err == nil && !success {
			err = fmt.Errorf("%s handler failed", kind)
		}
	}
	if err != nil {
		*state = "failed"
		p.event(e, corev1.EventTypeWarning, "Failed"+kind+"Hook", "Container %s %s hook failed", container.Name, kind)
	} else {
		*state = "done"
	}
	return errors.Join(err, p.journal(e))
}

func (p *provider) postStart(ctx context.Context, e *podEntry, container corev1.Container, task string, progress *applicationProgress) error {
	if container.Lifecycle == nil {
		return nil
	}
	return p.runLifecycleHook(ctx, e, container, task, container.Lifecycle.PostStart, &progress.hooks.PostStart, "PostStart")
}

func (p *provider) preStop(ctx context.Context, e *podEntry, managed managedContainer, deadline time.Time) error {
	container := managed.container(e)
	if container.Lifecycle == nil || container.Lifecycle.PreStop == nil || !time.Now().Before(deadline) {
		return nil
	}
	hook, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	return p.runLifecycleHook(hook, e, container, managed.task(e), container.Lifecycle.PreStop, &managed.progress(e).hooks.PreStop, "PreStop")
}

func hasLifecycle(pod *corev1.Pod) bool {
	for container := range podContainers(pod) {
		if container.Lifecycle != nil {
			return true
		}
	}
	return false
}

func (p *provider) checkResumePod(ctx context.Context, e *podEntry, terminating bool) error {
	if !terminating {
		return p.checkCurrentPod(ctx, e)
	}
	if p.client == nil {
		return nil
	}
	current, err := p.client.CoreV1().Pods(e.pod.Namespace).Get(ctx, e.pod.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if current.UID != e.pod.UID || current.Spec.NodeName != p.runner.cfg.nodeName {
		return errors.New("terminating Pod identity or assignment changed")
	}
	return p.checkNetworkPolicy(ctx, current)
}

func deletionDeadline(pod *corev1.Pod) time.Time {
	grace := int64(corev1.DefaultTerminationGracePeriodSeconds)
	if pod.Spec.TerminationGracePeriodSeconds != nil {
		grace = max(0, *pod.Spec.TerminationGracePeriodSeconds)
	}
	if pod.DeletionGracePeriodSeconds != nil {
		grace = min(grace, max(0, *pod.DeletionGracePeriodSeconds))
	}
	deadline := time.Now().Add(time.Duration(grace) * time.Second)
	if pod.DeletionTimestamp != nil {
		deadline = minTime(deadline, pod.DeletionTimestamp.Time)
	}
	return deadline
}

func needsTerminationWake(e *podEntry, pod *corev1.Pod) bool {
	deadline := deletionDeadline(pod)
	if e.state != "SLEEPING" || !time.Now().Before(deadline) {
		return false
	}
	for container := range podContainers(e.pod) {
		if container.Lifecycle != nil && container.Lifecycle.PreStop != nil {
			return true
		}
	}
	return false
}

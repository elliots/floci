package main

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const vmMemoryMiB = 768

func (p *provider) acquireStart(ctx context.Context, e *podEntry) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case p.starts <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	release := func() { <-p.starts }
	if err := p.reserveMemory(ctx, e); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

func (p *provider) reserveMemory(ctx context.Context, e *podEntry) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		p.budgetMu.Lock()
		if e.allocated {
			p.budgetMu.Unlock()
			return nil
		}
		if p.activeMemoryMiB+vmMemoryMiB <= p.runner.cfg.maxActiveMemoryMiB {
			p.activeMemoryMiB += vmMemoryMiB
			e.allocated = true
			p.budgetMu.Unlock()
			return nil
		}
		p.budgetMu.Unlock()
		select {
		case <-ctx.Done():
			return fmt.Errorf("native pod memory admission: %w", ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}
func (p *provider) releaseMemory(e *podEntry) {
	p.budgetMu.Lock()
	defer p.budgetMu.Unlock()
	if e.allocated {
		p.activeMemoryMiB -= vmMemoryMiB
		e.allocated = false
	}
}

func (p *provider) restart(ctx context.Context, e *podEntry, code int32) error {
	e.generation++
	e.state = "RESTARTING"
	if err := p.journal(e); err != nil {
		return err
	}
	p.unavailable(e, "ContainerRestarting")
	e.mu.Lock()
	status := &e.pod.Status.ContainerStatuses[0]
	status.LastTerminationState = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
		ExitCode: code, StartedAt: metav1.NewTime(e.started), FinishedAt: metav1.Now()}}
	count := status.RestartCount
	e.mu.Unlock()
	delay := min(10*time.Second*time.Duration(1<<min(count, 5)), 300*time.Second)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
	}
	if _, err := p.runner.guest(ctx, e.vm, "mv /tmp/application.log /tmp/application.previous.log; rm /tmp/application.exit"); err != nil {
		return err
	}
	e.mu.Lock()
	e.pod.Status.ContainerStatuses[0].RestartCount++
	e.mu.Unlock()
	e.probeAt = time.Time{}
	e.probeSuccesses = 0
	e.probeFailures = 0
	return p.startApplication(ctx, e)
}

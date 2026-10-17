package main

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const vmMemoryMiB = 768

func (p *provider) acquireStart(ctx context.Context, e *podEntry) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p.budgetMu.Lock()
		allocation, err := p.memoryAllocationLocked(e)
		if err != nil {
			p.budgetMu.Unlock()
			return nil, err
		}
		if p.activeMemoryMiB < 0 {
			p.budgetMu.Unlock()
			return nil, fmt.Errorf("native pod memory accounting is negative")
		}
		budget := p.runner.cfg.maxActiveMemoryMiB
		if e.allocated || p.activeMemoryMiB <= budget && allocation.MemoryMiB <= budget-p.activeMemoryMiB {
			// Claim both resources together. A large Pod waiting for RAM must
			// not occupy the start permits needed by smaller Pods that fit.
			select {
			case p.starts <- struct{}{}:
				if !e.allocated {
					p.activeMemoryMiB += allocation.MemoryMiB
					e.allocated = true
				}
				p.budgetMu.Unlock()
				return func() { <-p.starts }, nil
			default:
			}
		}
		p.budgetMu.Unlock()
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("native pod start admission: %w", ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func (p *provider) reserveMemory(ctx context.Context, e *podEntry) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		p.budgetMu.Lock()
		allocation, err := p.memoryAllocationLocked(e)
		if err != nil {
			p.budgetMu.Unlock()
			return err
		}
		if e.allocated {
			p.budgetMu.Unlock()
			return nil
		}
		if p.activeMemoryMiB < 0 {
			p.budgetMu.Unlock()
			return fmt.Errorf("native pod memory accounting is negative")
		}
		budget := p.runner.cfg.maxActiveMemoryMiB
		if p.activeMemoryMiB <= budget && allocation.MemoryMiB <= budget-p.activeMemoryMiB {
			p.activeMemoryMiB += allocation.MemoryMiB
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

// Callers hold budgetMu while normalizing legacy entries or reading admission.
func (p *provider) memoryAllocationLocked(e *podEntry) (podAllocation, error) {
	allocation, err := validateStoredAllocation(e.allocation)
	if err != nil {
		return podAllocation{}, err
	}
	e.allocation = allocation
	if !e.allocated && allocation.MemoryMiB > p.runner.cfg.maxActiveMemoryMiB {
		return podAllocation{}, fmt.Errorf("native pod requires %d MiB, exceeding the %d MiB active memory budget",
			allocation.MemoryMiB, p.runner.cfg.maxActiveMemoryMiB)
	}
	return allocation, nil
}

func (p *provider) releaseMemory(e *podEntry) {
	p.budgetMu.Lock()
	defer p.budgetMu.Unlock()
	if e.allocated {
		allocation, err := validateStoredAllocation(e.allocation)
		if err != nil || allocation.MemoryMiB > p.activeMemoryMiB {
			slog.Error("cannot release inconsistent native pod memory allocation", "vm", e.vm,
				"allocation", e.allocation, "activeMemoryMiB", p.activeMemoryMiB, "error", err)
			return
		}
		e.allocation = allocation
		p.activeMemoryMiB -= allocation.MemoryMiB
		e.allocated = false
	}
}

// Callers hold e.op. An uncertain native command can still create or restore a
// guest, so keep its reservation until both the command fence and runtime agree.
func (p *provider) releaseInactiveMemory(ctx context.Context, e *podEntry) error {
	if e.state != "FAILED" {
		return nil
	}
	p.budgetMu.Lock()
	allocated := e.allocated
	p.budgetMu.Unlock()
	if !allocated {
		return nil
	}
	if !nativeMutations.TryLock() {
		return fmt.Errorf("native command is in progress; retain failed pod memory")
	}
	defer nativeMutations.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.runner.checkPending(); err != nil {
		return err
	}
	status, exists, err := p.inspectOwned(ctx, e)
	if err != nil {
		return err
	}
	if exists && status != "Stopped" {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.runner.checkPending(); err != nil {
		return err
	}
	p.releaseMemory(e)
	return nil
}

func (p *provider) restart(ctx context.Context, e *podEntry, code int32) error {
	if err := stopContainerExecs(ctx, e, "application"); err != nil {
		return err
	}
	e.generation++
	e.state = "RESTARTING"
	e.mu.Lock()
	status := &e.pod.Status.ContainerStatuses[0]
	status.LastTerminationState = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
		ExitCode: code, Reason: cmp.Or(e.terminationReason, e.probeFailure), StartedAt: metav1.NewTime(e.started), FinishedAt: metav1.Now()}}
	count := status.RestartCount
	e.mu.Unlock()
	if err := p.journal(e); err != nil {
		return err
	}
	p.unavailable(e, "ContainerRestarting")
	delay := min(10*time.Second*time.Duration(1<<min(count, 5)), 300*time.Second)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
	}
	if err := p.archiveContainerLogs(ctx, e, logContainer{}); err != nil {
		return err
	}
	if _, err := p.runner.guest(ctx, e.vm, "mv /tmp/application.log /tmp/application.previous.log; rm /tmp/application.exit"); err != nil {
		return err
	}
	e.mu.Lock()
	e.pod.Status.ContainerStatuses[0].RestartCount++
	e.mu.Unlock()
	resetProbeProgress(e)
	return p.startApplication(ctx, e)
}

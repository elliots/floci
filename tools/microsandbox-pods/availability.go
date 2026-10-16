package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
)

type proxyPortBinding struct {
	ContainerPort int `json:"containerPort"`
	BackendPort   int `json:"backendPort"`
	ProxyPort     int `json:"proxyPort"`
}

func proxySettings(pod *corev1.Pod) (bool, string, time.Duration, error) {
	mode := pod.Annotations["microsandbox.dev/availability"]
	if mode != "" && mode != "strict" && mode != "proxy" {
		return false, "", 0, errors.New("availability must be strict or proxy")
	}
	if mode != "proxy" {
		return false, "external", 0, nil
	}
	owner := pod.Annotations["microsandbox.dev/idle-owner"]
	if owner == "" {
		owner = "controller"
	}
	if owner != "controller" && owner != "external" {
		return false, "", 0, errors.New("idle-owner must be controller or external")
	}
	seconds := 120
	if value := pod.Annotations["microsandbox.dev/idle-timeout-seconds"]; value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 86400 {
			return false, "", 0, errors.New("idle-timeout-seconds must be 1..86400")
		}
		seconds = parsed
	}
	ports := map[int32]bool{}
	for _, port := range pod.Spec.Containers[0].Ports {
		if port.Protocol != "" && port.Protocol != corev1.ProtocolTCP {
			return false, "", 0, errors.New("proxy availability requires TCP ports only")
		}
		if ports[port.ContainerPort] {
			return false, "", 0, errors.New("duplicate proxy container port")
		}
		ports[port.ContainerPort] = true
	}
	if len(ports) < 1 || len(ports) > 16 {
		return false, "", 0, errors.New("proxy availability requires 1..16 declared TCP ports")
	}
	return true, owner, time.Duration(seconds) * time.Second, nil
}

func (p *provider) prepareProxy(ctx context.Context, e *podEntry) error {
	if !e.proxyAvailability {
		return nil
	}
	release, err := p.acquireRegistration(ctx)
	if err != nil {
		return err
	}
	defer release()
	relay, err := p.ensureRelay(ctx)
	if err != nil {
		return err
	}
	if err := p.network.prepareProxy(ctx, e); err != nil {
		return err
	}
	for _, binding := range e.proxyPorts {
		_, err := p.proxy.listen(string(e.pod.UID), binding.ContainerPort, "127.0.0.1", binding.ProxyPort, func(ctx context.Context) (string, func(), error) { return p.acquireProxy(ctx, e, binding.BackendPort) })
		if err != nil {
			return err
		}
		if err := relay.bind(ctx, e, binding, p.network.hostIP); err != nil {
			return err
		}
	}
	e.proxyRoutesReady = true
	e.lastActivity = time.Now()
	if err := p.journal(e); err != nil {
		return err
	}
	p.publish(e)
	return nil
}

func (p *provider) acquireProxy(ctx context.Context, e *podEntry, backendPort int) (string, func(), error) {
	e.op.Lock()
	if err := ctx.Err(); err != nil {
		e.op.Unlock()
		return "", nil, err
	}
	if e.deleted || e.lifetime.Err() != nil || !e.proxyRoutesReady || e.proxyRouteFailed.Load() {
		e.op.Unlock()
		return "", nil, errors.New("pod availability route is unavailable")
	}
	e.activeConnections++
	e.lastActivity = time.Now()
	refreshView(e)
	e.op.Unlock()
	var once sync.Once
	release := func() {
		once.Do(func() {
			e.op.Lock()
			defer e.op.Unlock()
			e.activeConnections--
			e.lastActivity = time.Now()
			refreshView(e)
		})
	}
	for {
		e.op.Lock()
		if err := ctx.Err(); err != nil {
			e.op.Unlock()
			return "", release, err
		}
		if e.deleted || e.lifetime.Err() != nil || e.proxyRouteFailed.Load() {
			e.op.Unlock()
			return "", release, errors.New("pod availability route is unavailable")
		}
		if e.state == "COLD" || e.state == "SLEEPING" {
			if err := p.resume(ctx, e); err != nil {
				e.op.Unlock()
				return "", release, err
			}
		}
		if e.state == "READY" && e.actualReady {
			e.op.Unlock()
			return fmt.Sprintf("127.0.0.1:%d", backendPort), release, nil
		}
		if e.state == "FAILED" || e.state == "TERMINATED" {
			e.op.Unlock()
			return "", release, errors.New("application is unavailable")
		}
		e.op.Unlock()
		select {
		case <-ctx.Done():
			return "", release, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (p *provider) projectProxyAvailability(e *podEntry) {
	if !e.proxyAvailability {
		return
	}
	ready := e.proxyRoutesReady && !e.proxyRouteFailed.Load() && !e.deleted && e.lifetime.Err() == nil && e.state != "FAILED" && e.state != "TERMINATED" && e.applicationMissingSince.IsZero()
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pod.Status.PodIP = e.ip
	e.pod.Status.PodIPs = []corev1.PodIP{{IP: e.ip}}
	e.pod.Status.HostIP = p.runner.cfg.nodeIP
	if ready {
		e.pod.Status.Phase = corev1.PodRunning
	}
	e.pod.Status.Conditions = podConditionsFor(e.pod, ready)
	e.pod.Status.ContainerStatuses[0].Ready = ready
}

func (p *provider) watchAvailability(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		p.relayMu.Lock()
		relay := p.relay
		p.relayMu.Unlock()
		if relay == nil {
			continue
		}
		check, cancel := context.WithTimeout(ctx, 5*time.Second)
		inventory, err := relay.inventory(check)
		healthy := err == nil
		cancel()
		p.mu.Lock()
		entries := make([]*podEntry, 0, len(p.pods))
		for _, e := range p.pods {
			if e.proxyAvailability {
				entries = append(entries, e)
			}
		}
		p.mu.Unlock()
		for _, e := range entries {
			if !p.observeProxyRoutes(e, inventory, healthy) || !e.op.TryLock() {
				continue
			}
			if !e.proxyRoutesReady || e.proxyRouteFailed.Load() {
				e.proxyRoutesReady = true
				e.proxyRouteFailed.Store(false)
				p.publish(e)
			}
			if idleEligible(e) && !e.idleScheduled {
				select {
				case p.idleJobs <- struct{}{}:
					e.idleScheduled = true
					p.jobs.Go(func() {
						defer func() { <-p.idleJobs }()
						e.op.Lock()
						defer e.op.Unlock()
						defer func() { e.idleScheduled = false }()
						if !idleEligible(e) || e.deleted || e.lifetime.Err() != nil {
							return
						}
						operation, cancel := context.WithTimeout(ctx, 4*time.Minute)
						defer cancel()
						if err := p.hibernate(operation, e); err != nil {
							slog.Error("proxy idle hibernation failed", "pod", e.pod.Name, "error", err)
						}
					})
				default:
				}
			}
			e.op.Unlock()
		}
	}
}

func (p *provider) observeProxyRoutes(e *podEntry, inventory map[string]relayListenerView, healthy bool) bool {
	e.mu.Lock()
	ready := healthy && p.proxy.ready(string(e.pod.UID))
	if ready {
		for _, binding := range e.proxyPorts {
			if !readyRelayBinding(inventory[relayBindingID(e, binding.ContainerPort)], e, binding, p.network.hostIP) {
				ready = false
				break
			}
		}
	}
	e.mu.Unlock()
	if !ready {
		p.withdrawProxyAvailability(e)
	}
	return ready
}

func (p *provider) withdrawProxyAvailability(e *podEntry) {
	if e.proxyRouteFailed.Swap(true) {
		return
	}
	e.publication.Lock()
	defer e.publication.Unlock()
	e.mu.Lock()
	e.pod.Status.ContainerStatuses[0].Ready = false
	e.pod.Status.Conditions = podConditionsFor(e.pod, false)
	pod := e.pod.DeepCopy()
	e.mu.Unlock()
	p.notifySnapshot(pod)
}

func idleEligible(e *podEntry) bool {
	return e.idleOwner == "controller" && (e.state == "READY" || e.state == "RUNNING") && e.applicationMissingSince.IsZero() && e.activeConnections == 0 && time.Since(e.lastActivity) >= e.idleTimeout
}

func (p *provider) removeProxy(ctx context.Context, e *podEntry) error {
	if !e.proxyAvailability {
		return nil
	}
	relay, err := p.ensureRelay(ctx)
	if err != nil {
		return err
	}
	inventory, err := relay.inventory(ctx)
	if err != nil {
		return err
	}
	for _, binding := range e.proxyPorts {
		current, exists := inventory[relayBindingID(e, binding.ContainerPort)]
		if !exists {
			continue
		}
		if !matchingRelayBinding(current, e, binding, p.network.hostIP) {
			return errors.New("refusing deletion of changed relay listener")
		}
		if _, err := relay.request(ctx, "DELETE", "/listeners/"+relayBindingID(e, binding.ContainerPort), nil); err != nil {
			return err
		}
	}
	e.proxyRoutesReady = false
	return p.network.removeProxy(ctx, e)
}

func (p *provider) acquireRegistration(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case p.registrations <- struct{}{}:
		return func() { <-p.registrations }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

type networkPolicyController struct {
	provider *provider
	stores   [3]cache.Store
	sources  [3]policyInventorySource
	revision atomic.Uint64
	healthy  [3]atomic.Bool
	changed  chan struct{}
	mu       sync.Mutex
	peerMu   sync.Mutex
	applied  map[string]string
	peerHash string
}

type policyInventorySource struct {
	list  func(context.Context) (runtime.Object, error)
	watch func(context.Context, string) (watch.Interface, error)
}

func (p *provider) configureNetworkPolicies(ctx context.Context) error {
	if p.runner.cfg.networkPolicy && p.runner.cfg.relayBinary == "" {
		return errors.New("native NetworkPolicy requires --network-relay-binary for the one-shot guest socket hooks")
	}
	c := &networkPolicyController{provider: p, changed: make(chan struct{}, 1), applied: map[string]string{}}
	p.policies = c
	for index := range c.stores {
		c.stores[index] = cache.NewStore(cache.MetaNamespaceKeyFunc)
		c.sources[index] = policyInventorySource{
			list: func(ctx context.Context) (runtime.Object, error) {
				switch index {
				case 0:
					return p.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
				case 1:
					return p.client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
				default:
					return p.client.NetworkingV1().NetworkPolicies("").List(ctx, metav1.ListOptions{})
				}
			},
			watch: func(ctx context.Context, version string) (watch.Interface, error) {
				options := metav1.ListOptions{ResourceVersion: version, TimeoutSeconds: new(int64(60)), AllowWatchBookmarks: true}
				switch index {
				case 0:
					return p.client.CoreV1().Pods("").Watch(ctx, options)
				case 1:
					return p.client.CoreV1().Namespaces().Watch(ctx, options)
				default:
					return p.client.NetworkingV1().NetworkPolicies("").Watch(ctx, options)
				}
			},
		}
		p.jobs.Go(func() { c.watchInventory(p.ctx, index) })
	}
	initial, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for !c.ready() {
		select {
		case <-initial.Done():
			return errors.New("NetworkPolicy inventory did not synchronize")
		case <-c.changed:
		}
	}
	return p.installPeerPolicy(ctx, nil)
}

func (c *networkPolicyController) watchHealth(index int, healthy bool) {
	if healthy {
		c.revision.Add(1)
		c.healthy[index].Store(true)
	} else {
		c.healthy[index].Store(false)
		c.revision.Add(1)
	}
	c.notify()
}

func (c *networkPolicyController) watchInventory(ctx context.Context, index int) {
	delay := time.Second
	for ctx.Err() == nil {
		cycle, cancel := context.WithTimeout(ctx, 90*time.Second)
		err := c.refreshPolicyWatch(cycle, index)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			delay = time.Second
			continue
		}
		slog.Warn("native NetworkPolicy watch unavailable", "resource", index, "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		delay = min(2*delay, 15*time.Second)
	}
}

// A reconnect replaces the store from a fresh, consistent List before it can
// advertise health. Incremental events are then applied synchronously, without
// an informer queue that could reopen access before replaying lost updates.
func (c *networkPolicyController) refreshPolicyWatch(ctx context.Context, index int) error {
	c.watchHealth(index, false)
	defer c.watchHealth(index, false)
	listed, err := c.sources[index].list(ctx)
	if err != nil {
		return err
	}
	metadata, err := meta.ListAccessor(listed)
	if err != nil || metadata.GetResourceVersion() == "" {
		return errors.New("NetworkPolicy inventory List has no resource version")
	}
	objects, err := meta.ExtractList(listed)
	if err != nil {
		return err
	}
	values := make([]any, len(objects))
	for i, object := range objects {
		if !policyInventoryObject(index, object) {
			return errors.New("NetworkPolicy inventory List contains an unexpected resource")
		}
		values[i] = object
	}
	source, err := c.sources[index].watch(ctx, metadata.GetResourceVersion())
	if err != nil {
		return err
	}
	defer source.Stop()
	if err := c.stores[index].Replace(values, metadata.GetResourceVersion()); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.watchHealth(index, true)
	return c.consumePolicyWatch(ctx, index, source)
}

func policyInventoryObject(index int, object runtime.Object) bool {
	switch index {
	case 0:
		_, ok := object.(*corev1.Pod)
		return ok
	case 1:
		_, ok := object.(*corev1.Namespace)
		return ok
	default:
		_, ok := object.(*networkingv1.NetworkPolicy)
		return ok
	}
}

func (c *networkPolicyController) consumePolicyWatch(ctx context.Context, index int, source watch.Interface) error {
	defer c.watchHealth(index, false)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-source.ResultChan():
			if !ok {
				return nil
			}
			if event.Type == watch.Error {
				return apierrors.FromObject(event.Object)
			}
			if event.Type == watch.Bookmark {
				continue
			}
			if !policyInventoryObject(index, event.Object) {
				return errors.New("NetworkPolicy watch contains an unexpected resource")
			}
			previous, exists, err := c.stores[index].Get(event.Object)
			if err != nil {
				return err
			}
			switch event.Type {
			case watch.Added, watch.Modified:
				err = c.stores[index].Update(event.Object)
			case watch.Deleted:
				err = c.stores[index].Delete(event.Object)
			default:
				return errors.New("NetworkPolicy watch has an unexpected event type")
			}
			if err != nil {
				return err
			}
			if event.Type == watch.Deleted || !exists || policyObjectChanged(previous, event.Object) {
				c.notify()
			}
		}
	}
}

func policyObjectChanged(old, current any) bool {
	switch before := old.(type) {
	case *corev1.Pod:
		after, ok := current.(*corev1.Pod)
		return !ok || before.UID != after.UID || !reflect.DeepEqual(before.Labels, after.Labels) || !reflect.DeepEqual(before.Spec.Containers, after.Spec.Containers) || !reflect.DeepEqual(before.Spec.InitContainers, after.Spec.InitContainers) || !reflect.DeepEqual(before.Status.PodIPs, after.Status.PodIPs) || before.Status.PodIP != after.Status.PodIP || before.Spec.HostNetwork != after.Spec.HostNetwork
	default:
		return !reflect.DeepEqual(old, current)
	}
}

func (c *networkPolicyController) notify() {
	select {
	case c.changed <- struct{}{}:
	default:
	}
}

func (c *networkPolicyController) ready() bool {
	for i := range c.healthy {
		if !c.healthy[i].Load() {
			return false
		}
	}
	return true
}

func (c *networkPolicyController) inventory() policyInventory {
	result := policyInventory{namespaces: map[string]labels.Set{}, addresses: map[string][]string{},
		observed: true, revision: c.revision.Load(), available: c.ready()}
	for _, object := range c.stores[0].List() {
		result.pods = append(result.pods, object.(*corev1.Pod))
	}
	for _, object := range c.stores[1].List() {
		namespace := object.(*corev1.Namespace)
		result.namespaces[namespace.Name] = labels.Set(namespace.Labels)
	}
	for _, object := range c.stores[2].List() {
		result.policies = append(result.policies, object.(*networkingv1.NetworkPolicy))
	}
	c.provider.mu.Lock()
	for _, entry := range c.provider.pods {
		entry.mu.Lock()
		result.addresses[string(entry.pod.UID)] = []string{entry.ip, entry.backendIP}
		entry.mu.Unlock()
	}
	c.provider.mu.Unlock()
	return result
}

func (c *networkPolicyController) plan(pod *corev1.Pod, inventory policyInventory) podPolicyPlan {
	for _, current := range inventory.pods {
		if current.UID == pod.UID {
			pod = current
			break
		}
	}
	plan := compilePodPolicy(pod, inventory)
	if !c.provider.runner.cfg.networkPolicy && (plan.ingressIsolated || plan.egressIsolated) {
		plan.ingress, plan.egress = nil, nil
		plan.ingressIsolated, plan.egressIsolated = true, true
		plan.unsupported = "NetworkPolicy requires explicit controller --network-policy=true and safe capabilities for every native Pod on its network peer"
	}
	if !c.ready() || inventory.observed && (!inventory.available || inventory.revision != c.revision.Load()) {
		plan.stale = true
		if c.provider.runner.cfg.networkPolicy {
			plan.ingress, plan.egress = nil, nil
			plan.ingressIsolated, plan.egressIsolated = true, true
			if err := policyCapabilities(pod); err != nil {
				plan.unsupported = "NetworkPolicy inventory unavailable; cannot fence raw/administrative sockets: " + err.Error()
			}
		}
	}
	return plan
}

func (c *networkPolicyController) entries() []*podEntry {
	c.provider.mu.Lock()
	defer c.provider.mu.Unlock()
	result := make([]*podEntry, 0, len(c.provider.pods))
	for _, entry := range c.provider.pods {
		result = append(result, entry)
	}
	return result
}

func (c *networkPolicyController) installPeer(ctx context.Context, inventory policyInventory) error {
	c.peerMu.Lock()
	defer c.peerMu.Unlock()
	var endpoints []policyEndpoint
	entries := c.entries()
	c.mu.Lock()
	defer c.mu.Unlock()
	current := make(map[string]bool, len(entries))
	for _, entry := range entries {
		entry.mu.Lock()
		pod := entry.pod.DeepCopy()
		entry.mu.Unlock()
		plan := c.plan(pod, inventory)
		plan = policyForEntry(entry, plan)
		current[string(pod.UID)] = true
		pending := false
		if view := entry.publishedView.Load(); view != nil && view.State != "COLD" && view.State != "SLEEPING" && view.State != "WAITINGVOLUMES" {
			pending = (plan.ingressIsolated || plan.egressIsolated) && c.applied[string(pod.UID)] != policyPlanHash(plan)
		}
		endpoints = append(endpoints, policyEndpoint{uid: string(pod.UID), address: entry.ip, backend: entry.backendIP, link: entry.link, plan: plan, pending: pending})
	}
	for uid := range c.applied {
		if !current[uid] {
			delete(c.applied, uid)
		}
	}
	rules := peerPolicyRules(c.provider.runner.cfg.peerOwner, c.provider.network.peerIP, c.provider.network.prefix.String(), c.provider.network.backendPrefix.String(), endpoints)
	if rules == c.peerHash {
		return nil
	}
	if err := c.provider.installPeerPolicy(ctx, endpoints); err != nil {
		return err
	}
	c.peerHash = rules
	return nil
}

// The caller holds the Pod operation lock. No controller lock is held while
// waiting for that lock, so an activation cannot deadlock a policy update.
func (p *provider) applyNetworkPolicy(ctx context.Context, e *podEntry) error {
	if p.policies == nil {
		return nil
	}
	c := p.policies
	inventory := c.inventory()
	if err := c.installPeer(ctx, inventory); err != nil {
		return err
	}
	e.mu.Lock()
	pod := e.pod.DeepCopy()
	e.mu.Unlock()
	plan := c.plan(pod, inventory)
	if err := p.applyGuestPolicy(ctx, e, plan); err != nil {
		return err
	}
	return c.installPeer(ctx, inventory)
}

func (p *provider) applyGuestPolicy(ctx context.Context, e *podEntry, plan podPolicyPlan) error {
	c := p.policies
	plan = policyForEntry(e, plan)
	hash := policyPlanHash(plan)
	c.mu.Lock()
	applied := c.applied[string(e.pod.UID)]
	c.mu.Unlock()
	// Ordinary unselected guests keep their networking through a watch outage;
	// readiness and new activation remain blocked until inventory returns. They
	// have no trusted-node socket hook, so installing a deny rule would also
	// break node operations and cannot safely protect their raw-capable apps.
	if applied != hash && (e.networkPolicyVersion == 1 || plan.unsupported != "") {
		if _, err := p.runner.guest(ctx, e.vm, guestPolicyScript(plan, p.runner.cfg.clusterCIDR, p.runner.cfg.serviceCIDR, p.network.peerPort)); err != nil {
			return fmt.Errorf("install guest NetworkPolicy: %w", err)
		}
		c.mu.Lock()
		c.applied[string(e.pod.UID)] = hash
		c.mu.Unlock()
	}
	wasUnavailable := e.policyUnavailable.Swap(plan.unsupported != "" || plan.stale)
	if wasUnavailable != e.policyUnavailable.Load() {
		if e.policyUnavailable.Load() {
			p.event(e, corev1.EventTypeWarning, "NetworkPolicyUnavailable", "Policy inventory or enforcement is unavailable; application activation is blocked")
		}
		p.publish(e)
	}
	if plan.unsupported != "" {
		return errors.New(plan.unsupported)
	}
	return nil
}

func policyForEntry(e *podEntry, plan podPolicyPlan) podPolicyPlan {
	if e.networkPolicyVersion != 1 && (plan.ingressIsolated || plan.egressIsolated) {
		plan.ingressIsolated, plan.egressIsolated = true, true
		plan.ingress, plan.egress = nil, nil
		plan.unsupported = "NetworkPolicy requires a new Pod UID created by this controller; preserve PVCs and recreate the legacy Pod"
	}
	return plan
}

func (c *networkPolicyController) run(ctx context.Context) {
	retry := time.NewTicker(5 * time.Second)
	defer retry.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.changed:
		case <-retry.C:
		}
		// Coalesce label/IP/policy bursts into one atomic peer update.
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
		inventory := c.inventory()
		operation, cancel := context.WithTimeout(ctx, 30*time.Second)
		peerFailure := c.installPeer(operation, inventory)
		if peerFailure != nil {
			slog.Error("cannot reconcile peer NetworkPolicy", "error", peerFailure)
		}
		cancel()
		for _, entry := range c.entries() {
			if !entry.op.TryLock() {
				continue
			}
			if !entry.deleted && (entry.state == "RUNNING" || entry.state == "READY" || entry.state == "INITIALIZING") {
				operation, cancel := context.WithTimeout(ctx, 15*time.Second)
				entry.mu.Lock()
				pod := entry.pod.DeepCopy()
				entry.mu.Unlock()
				plan := c.plan(pod, inventory)
				var err error
				if peerFailure != nil && (plan.ingressIsolated || plan.egressIsolated) {
					err = peerFailure
				} else {
					err = c.provider.applyGuestPolicy(operation, entry, plan)
				}
				cancel()
				if err != nil {
					entry.policyUnavailable.Store(true)
					// A guest whose firewall could not be installed must not keep
					// using direct eth0 egress. Pause preserves RAM and PVC data.
					err = errors.Join(err, c.provider.fencePolicyFailure(ctx, entry))
					c.provider.fail(entry, fmt.Errorf("NetworkPolicy enforcement unavailable: %w", err))
				}
			}
			entry.op.Unlock()
		}
		operation, cancel = context.WithTimeout(ctx, 30*time.Second)
		if err := c.installPeer(operation, inventory); err != nil {
			slog.Error("cannot release reconciled peer NetworkPolicy", "error", err)
		}
		cancel()
	}
}

func (p *provider) fencePolicyFailure(ctx context.Context, e *podEntry) error {
	freeze, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	freezeErr := p.freezeApplications(freeze, e)
	cancel()
	pause, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, pauseErr := p.runner.vm(pause, "pause", e.vm)
	if pauseErr == nil {
		pauseErr = p.requireVMStatus(pause, e, "Paused")
	}
	if pauseErr == nil || freezeErr == nil && e.freeze != nil && e.freeze.Phase == "FROZEN" {
		p.event(e, corev1.EventTypeWarning, "NetworkPolicyFenced", "Applications were frozen or the owned VM was confirmed paused after policy enforcement failed")
		return nil
	}
	err := errors.Join(errors.New("direct egress could not be confirmed fenced; stop the owned VM before restoring access"), freezeErr, pauseErr)
	p.event(e, corev1.EventTypeWarning, "NetworkPolicyUncertain", "%s", err.Error())
	return err
}

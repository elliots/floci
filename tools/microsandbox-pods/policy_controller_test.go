package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

func healthyPolicyController(p *provider) *networkPolicyController {
	p.runner.cfg.networkPolicy = true
	c := &networkPolicyController{provider: p, changed: make(chan struct{}, 1), applied: map[string]string{}}
	for i := range c.healthy {
		c.stores[i] = cache.NewStore(cache.MetaNamespaceKeyFunc)
		c.healthy[i].Store(true)
	}
	p.policies = c
	return c
}

func TestPolicyWatchLossQuarantinesPreviouslyUnselectedPods(t *testing.T) {
	for unavailable := range 3 {
		c := healthyPolicyController(&provider{})
		pod := policyPod("apps", "api", "10.0.0.1")
		if plan := c.plan(pod, policyInventory{}); plan.ingressIsolated || plan.egressIsolated || plan.stale {
			t.Fatalf("healthy inventory must preserve ordinary unselected traffic: %#v", plan)
		}
		c.healthy[unavailable].Store(false)
		plan := c.plan(pod, policyInventory{})
		if !plan.stale || !plan.ingressIsolated || !plan.egressIsolated || len(plan.ingress) != 0 || len(plan.egress) != 0 {
			t.Fatalf("missing watcher %d must fence newly selected policies too: %#v", unavailable, plan)
		}
		pod.Spec.Containers[0].SecurityContext = nil
		if plan := c.plan(pod, policyInventory{}); !strings.Contains(plan.unsupported, "NET_RAW") {
			t.Fatalf("raw sockets must not be claimed fenced by IP rules: %#v", plan)
		}
		c.healthy[unavailable].Store(true)
		if plan := c.plan(pod, policyInventory{}); plan.stale || plan.unsupported != "" || plan.ingressIsolated || plan.egressIsolated {
			t.Fatalf("healthy unselected Pod should recover its ordinary capabilities: %#v", plan)
		}
	}
}

func TestOrdinaryModeWatchReconnectDoesNotFailOrFreezeUnselectedPods(t *testing.T) {
	pod := policyPod("apps", "ordinary", "10.0.0.1")
	pod.Spec.Containers[0].SecurityContext = nil
	e := &podEntry{pod: pod, state: "READY", proxyAvailability: true, proxyRoutesReady: true, lifetime: t.Context()}
	p := &provider{runner: commandRunner{runtime: stubNativeRuntime{execute: func(context.Context, []string, []byte) (msbruntime.Result, error) {
		t.Fatal("ordinary watch reconnect must not mutate an unselected guest")
		return msbruntime.Result{}, nil
	}}}}
	c := healthyPolicyController(p)
	p.runner.cfg.networkPolicy = false
	c.healthy[2].Store(false)
	plan := c.plan(pod, policyInventory{})
	if !plan.stale || plan.unsupported != "" || plan.ingressIsolated || plan.egressIsolated {
		t.Fatalf("ordinary watch outage changed the networking contract: %#v", plan)
	}
	if err := p.applyGuestPolicy(t.Context(), e, plan); err != nil {
		t.Fatal(err)
	}
	if !e.policyUnavailable.Load() || e.state != "READY" {
		t.Fatal("watch outage must block activation without failing the running application")
	}
	c.healthy[2].Store(true)
	if err := p.applyGuestPolicy(t.Context(), e, c.plan(pod, policyInventory{})); err != nil {
		t.Fatal(err)
	}
	if e.policyUnavailable.Load() || e.state != "READY" {
		t.Fatal("ordinary workload did not recover after the watch reconnected")
	}
}

func TestPolicyWatchCloseErrorAndCancellationInvalidateInventory(t *testing.T) {
	for _, loss := range []string{"close", "error", "cancel", "deadline"} {
		t.Run(loss, func(t *testing.T) {
			c := healthyPolicyController(&provider{})
			ctx, cancel := context.WithCancel(t.Context())
			if loss == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 20*time.Millisecond)
			}
			defer cancel()
			source := watch.NewRaceFreeFake()
			defer source.Stop()
			done := make(chan error, 1)
			go func() { done <- c.consumePolicyWatch(ctx, 1, source) }()
			switch loss {
			case "close":
				source.Stop()
			case "error":
				source.Error(&metav1.Status{Status: "Failure", Reason: metav1.StatusReasonExpired, Code: 410})
			case "cancel":
				cancel()
			}
			select {
			case err := <-done:
				if loss != "close" && err == nil {
					t.Fatal("watch failure was lost")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("watch consumer did not stop")
			}
			if c.ready() {
				t.Fatal("watch loss left policy inventory healthy")
			}
		})
	}
}

type blockedPolicyStore struct {
	cache.Store
	replacing chan struct{}
	release   chan struct{}
}

func (s *blockedPolicyStore) Replace(values []any, version string) error {
	close(s.replacing)
	<-s.release
	return s.Store.Replace(values, version)
}

func waitPolicyCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("policy condition did not converge")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPolicyReconnectReplacesOldRulesBeforeHealthyAndRejectsOldSnapshots(t *testing.T) {
	c := healthyPolicyController(&provider{})
	pod := policyPod("apps", "api", "10.0.0.1")
	allow := policyFor(pod)
	allow.Spec.Ingress = []networkingv1.NetworkPolicyIngressRule{{}}
	if err := c.stores[2].Add(allow); err != nil {
		t.Fatal(err)
	}
	before := c.inventory()
	store := &blockedPolicyStore{Store: c.stores[2], replacing: make(chan struct{}), release: make(chan struct{})}
	c.stores[2] = store
	deny := allow.DeepCopy()
	deny.Spec.Ingress = nil
	source := watch.NewRaceFreeFake()
	defer source.Stop()
	c.sources[2] = policyInventorySource{
		list: func(context.Context) (runtime.Object, error) {
			return &networkingv1.NetworkPolicyList{ListMeta: metav1.ListMeta{ResourceVersion: "fresh-2"}, Items: []networkingv1.NetworkPolicy{*deny}}, nil
		},
		watch: func(_ context.Context, version string) (watch.Interface, error) {
			if version != "fresh-2" {
				t.Error("watch did not continue from its fresh List")
			}
			return source, nil
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.refreshPolicyWatch(ctx, 2) }()
	select {
	case <-store.replacing:
	case <-time.After(3 * time.Second):
		t.Fatal("fresh store replacement was not reached")
	}
	during := c.inventory()
	if c.ready() || !c.plan(pod, before).stale || !c.plan(pod, during).stale {
		t.Fatal("starting a watch reopened stale cached rules before Replace completed")
	}
	close(store.release)
	waitPolicyCondition(t, c.ready)
	plan := c.plan(pod, c.inventory())
	if plan.stale || !plan.ingressIsolated || len(plan.ingress) != 0 {
		t.Fatalf("fresh deny rules were not visible when the watch became healthy: %#v", plan)
	}
	if !c.plan(pod, before).stale || !c.plan(pod, during).stale {
		t.Fatal("a snapshot copied before recovery reopened access after health returned")
	}
	source.Modify(allow)
	waitPolicyCondition(t, func() bool { return len(c.plan(pod, c.inventory()).ingress) == 1 })
	source.Delete(allow)
	waitPolicyCondition(t, func() bool { return len(c.inventory().policies) == 0 })
	source.Stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reconnected watch failed to stop")
	}
	if c.ready() {
		t.Fatal("closed replacement watch remained healthy")
	}
}

func TestPolicyFailedRelistCannotRecoverOldAllowInventory(t *testing.T) {
	c := healthyPolicyController(&provider{})
	c.sources[2].list = func(context.Context) (runtime.Object, error) { return nil, errors.New("API unavailable") }
	if err := c.refreshPolicyWatch(t.Context(), 2); err == nil || c.ready() {
		t.Fatal("failed fresh List reopened cached policies")
	}
}

func TestPolicyPlanUsesObservedLabelsAndRevokesRemovedPolicies(t *testing.T) {
	c := healthyPolicyController(&provider{})
	original := policyPod("apps", "api", "10.0.0.1")
	policy := policyFor(original)
	policy.Spec.Ingress = []networkingv1.NetworkPolicyIngressRule{{}}
	inventory := policyInventory{policies: []*networkingv1.NetworkPolicy{policy}}
	if plan := c.plan(original, inventory); !plan.ingressIsolated || len(plan.ingress) != 1 {
		t.Fatalf("selected policy was not applied: %#v", plan)
	}
	current := original.DeepCopy()
	current.Labels = map[string]string{"app": "different"}
	inventory.pods = []*corev1.Pod{current}
	if plan := c.plan(original, inventory); plan.ingressIsolated {
		t.Fatalf("cached workload labels overrode current watched labels: %#v", plan)
	}
	inventory.pods = []*corev1.Pod{original}
	inventory.policies = nil
	if plan := c.plan(original, inventory); plan.ingressIsolated || plan.egressIsolated {
		t.Fatalf("deleted policy left stale isolation: %#v", plan)
	}
}

func TestGuestPolicyRefusesLegacySelectedGuestsAndCachesOnlySuccessfulInstalls(t *testing.T) {
	pod := policyPod("apps", "api", "10.0.0.1")
	entry := &podEntry{pod: pod, vm: "owned", state: "READY", networkPolicyVersion: 1}
	p := &provider{network: &network{peerPort: 4789}}
	c := healthyPolicyController(p)
	plan := podPolicyPlan{ingressIsolated: true, egressIsolated: true}
	calls := 0
	installFailure := true
	p.runner.runtime = stubNativeRuntime{execute: func(_ context.Context, args []string, _ []byte) (msbruntime.Result, error) {
		calls++
		if args[0] != "exec" || !strings.Contains(args[len(args)-1], "-j DROP") {
			t.Fatalf("expected actual guest deny installation: %v", args)
		}
		if installFailure {
			return msbruntime.Result{}, errors.New("injected firewall failure")
		}
		return msbruntime.Result{}, nil
	}}
	if err := p.applyGuestPolicy(t.Context(), entry, plan); err == nil || len(c.applied) != 0 {
		t.Fatalf("failed firewall install was treated as applied: %v, %v", err, c.applied)
	}
	installFailure = false
	for range 2 {
		if err := p.applyGuestPolicy(t.Context(), entry, plan); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 || entry.policyUnavailable.Load() {
		t.Fatalf("identical confirmed plan should not execute again: calls=%d unavailable=%v", calls, entry.policyUnavailable.Load())
	}
	entry.networkPolicyVersion = 0
	if err := p.applyGuestPolicy(t.Context(), entry, plan); err == nil || !strings.Contains(err.Error(), "new Pod UID") || !entry.policyUnavailable.Load() {
		t.Fatalf("legacy selected guest must remain unavailable: %v", err)
	}
	before := calls
	if err := p.applyGuestPolicy(t.Context(), entry, podPolicyPlan{}); err != nil || calls != before {
		t.Fatalf("unselected legacy guest must not receive unsupported socket hooks: calls=%d error=%v", calls, err)
	}
}

func TestGuestPolicyWatchOutageRemovesProxyReadinessAndRecovers(t *testing.T) {
	pod := policyPod("apps", "api", "10.0.0.1")
	entry := &podEntry{pod: pod, vm: "owned", ip: "10.0.0.1", state: "READY", networkPolicyVersion: 1,
		proxyAvailability: true, proxyRoutesReady: true, lifetime: t.Context()}
	p := &provider{network: &network{peerPort: 4789}, runner: commandRunner{runtime: stubNativeRuntime{execute: func(context.Context, []string, []byte) (msbruntime.Result, error) {
		return msbruntime.Result{}, nil
	}}}}
	c := healthyPolicyController(p)
	c.healthy[2].Store(false)
	if err := p.applyGuestPolicy(t.Context(), entry, c.plan(pod, policyInventory{})); err != nil {
		t.Fatal(err)
	}
	if !entry.policyUnavailable.Load() || policyPodReady(entry.pod) {
		t.Fatal("stale policy inventory left the wake proxy Ready")
	}
	c.healthy[2].Store(true)
	if err := p.applyGuestPolicy(t.Context(), entry, c.plan(pod, policyInventory{})); err != nil {
		t.Fatal(err)
	}
	if entry.policyUnavailable.Load() || !policyPodReady(entry.pod) {
		t.Fatal("confirmed current policy failed to restore proxy readiness")
	}
}

func policyPodReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

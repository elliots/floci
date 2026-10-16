package main

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func availabilityEntry(t *testing.T) *podEntry {
	t.Helper()
	pod := validPod()
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app"}}
	return &podEntry{pod: pod, lifetime: t.Context(), state: "COLD", generation: 9,
		proxyAvailability: true, proxyRoutesReady: true, ip: "10.42.200.2"}
}

func TestProxyRegistrationBoundsSetupAndReusesReleasedSlot(t *testing.T) {
	p := &provider{registrations: make(chan struct{}, 8)}
	releases := make([]func(), 0, 8)
	for range 8 {
		release, err := p.acquireRegistration(t.Context())
		if err != nil || release == nil {
			t.Fatalf("setup slot was unavailable: %v", err)
		}
		releases = append(releases, release)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if release, err := p.acquireRegistration(ctx); !errors.Is(err, context.DeadlineExceeded) || release != nil {
		t.Fatalf("ninth registration bypassed bounded admission: %v", err)
	}
	if len(p.registrations) != 8 {
		t.Fatal("cancelled registration consumed or released another setup slot")
	}
	releases[0]()
	replacement, err := p.acquireRegistration(t.Context())
	if err != nil || replacement == nil || len(p.registrations) != 8 {
		t.Fatalf("released setup slot was not reusable: %v", err)
	}
	replacement()
	for _, release := range releases[1:] {
		release()
	}
	if len(p.registrations) != 0 {
		t.Fatal("setup registrations did not drain")
	}
}

func TestProxyProjectionDoesNotMakeApplicationReady(t *testing.T) {
	for _, state := range []string{"COLD", "SLEEPING", "RUNNING"} {
		t.Run(state, func(t *testing.T) {
			entry := availabilityEntry(t)
			entry.state = state
			entry.started = time.Now().Add(-time.Hour)
			entry.readiness.at = time.Now()
			entry.pod.Spec.Containers[0].ReadinessProbe = &corev1.Probe{PeriodSeconds: 60}
			p := &provider{}
			p.projectProxyAvailability(entry)
			if !entry.pod.Status.ContainerStatuses[0].Ready || entry.pod.Status.Phase != corev1.PodRunning {
				t.Fatal("working proxy did not project availability")
			}
			if entry.state != state || entry.actualReady || p.probe(t.Context(), entry) {
				t.Fatal("projected availability changed actual application readiness")
			}
		})
	}
}

func TestProbeThresholdDoesNotReuseProjectedReadiness(t *testing.T) {
	entry := availabilityEntry(t)
	entry.state = "RUNNING"
	entry.started = time.Now().Add(-time.Hour)
	entry.pod.Spec.Containers[0].ReadinessProbe = &corev1.Probe{
		ProbeHandler:     corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"false"}}},
		FailureThreshold: 3,
	}
	script := filepath.Join(t.TempDir(), "msb")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	p := &provider{runner: commandRunner{cfg: config{msb: script}}}
	p.projectProxyAvailability(entry)
	if p.probe(t.Context(), entry) || entry.readiness.failures != 1 {
		t.Fatal("readiness hysteresis treated proxy availability as a successful app probe")
	}
}

func TestMissingApplicationWithdrawsProxyAvailabilityUntilObservationResumes(t *testing.T) {
	entry := availabilityEntry(t)
	entry.state = "RUNNING"
	entry.applicationMissingSince = time.Now()
	p := &provider{}
	p.projectProxyAvailability(entry)
	if entry.pod.Status.ContainerStatuses[0].Ready {
		t.Fatal("proxy hid the missing application task")
	}
	for _, condition := range entry.pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			t.Fatal("missing application still had a Ready condition")
		}
	}
	entry.applicationMissingSince = time.Time{}
	p.projectProxyAvailability(entry)
	if !entry.pod.Status.ContainerStatuses[0].Ready {
		t.Fatal("availability did not recover after the missing-task observation cleared")
	}
}

func TestMissingApplicationCannotHideThroughIdleHibernation(t *testing.T) {
	entry := availabilityEntry(t)
	entry.state = "RUNNING"
	entry.idleOwner = "controller"
	entry.idleTimeout = time.Second
	entry.lastActivity = time.Now().Add(-time.Hour)
	entry.applicationMissingSince = time.Now()
	if idleEligible(entry) {
		t.Fatal("missing application was eligible for automatic hibernation")
	}
	p := &provider{}
	if err := p.hibernate(t.Context(), entry); err == nil || !strings.Contains(err.Error(), "missing application task") {
		t.Fatalf("explicit hibernation did not reject the missing task: %v", err)
	}
	if entry.applicationMissingSince.IsZero() || entry.state != "RUNNING" {
		t.Fatal("hibernation cleared the missing-task evidence")
	}
	entry.applicationMissingSince = time.Time{}
	if !idleEligible(entry) {
		t.Fatal("normal idle eligibility did not return when the task recovered")
	}
}

func TestProxyProjectionPreservesStrictModeAndWithdrawsUnavailableRoutes(t *testing.T) {
	p := &provider{}
	strict := availabilityEntry(t)
	strict.proxyAvailability = false
	p.projectProxyAvailability(strict)
	if strict.pod.Status.ContainerStatuses[0].Ready {
		t.Fatal("strict cold pod became Ready")
	}
	for _, scenario := range []string{"route failure", "deleted", "cancelled", "FAILED", "TERMINATED"} {
		t.Run(scenario, func(t *testing.T) {
			entry := availabilityEntry(t)
			p.projectProxyAvailability(entry)
			switch scenario {
			case "route failure":
				entry.proxyRoutesReady = false
			case "deleted":
				entry.deleted = true
			case "cancelled":
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				entry.lifetime = ctx
			default:
				entry.state = scenario
			}
			p.projectProxyAvailability(entry)
			if entry.pod.Status.ContainerStatuses[0].Ready {
				t.Fatal("unavailable proxy retained projected readiness")
			}
			for _, condition := range entry.pod.Status.Conditions {
				if (condition.Type == corev1.PodReady || condition.Type == corev1.ContainersReady) && condition.Status != corev1.ConditionFalse {
					t.Fatal("unavailable proxy retained a Ready condition")
				}
			}
		})
	}
}

func TestProxyRouteFailureWithdrawsReadinessWhileLifecycleIsBusy(t *testing.T) {
	for _, scenario := range []string{"relay unavailable", "listener missing", "listener stopped"} {
		t.Run(scenario, func(t *testing.T) {
			entry := availabilityEntry(t)
			p := &provider{network: &network{hostIP: "192.168.1.1"}}
			p.proxy = testWakeProxy(t, 1, time.Minute)
			port, err := p.proxy.listen(string(entry.pod.UID), 8080, "127.0.0.1", 0,
				func(context.Context) (string, func(), error) { return "", nil, errors.New("unexpected activation") })
			if err != nil {
				t.Fatal(err)
			}
			binding := proxyPortBinding{ContainerPort: 8080, BackendPort: 18080, ProxyPort: port}
			entry.proxyPorts = []proxyPortBinding{binding}
			listener := relayListenerView{Ready: true, ID: relayBindingID(entry, 8080),
				ListenAddress: "10.42.200.2:8080", TargetAddress: net.JoinHostPort("192.168.1.1", strconv.Itoa(port))}
			inventory := map[string]relayListenerView{listener.ID: listener}
			if !p.observeProxyRoutes(entry, inventory, true) {
				t.Fatal("healthy owned route was unavailable")
			}
			notifications := make(chan *corev1.Pod, 3)
			p.notify = func(pod *corev1.Pod) { notifications <- pod }
			p.publish(entry)
			if !((<-notifications).Status.ContainerStatuses[0].Ready) {
				t.Fatal("healthy route was not Ready")
			}
			healthy := true
			switch scenario {
			case "relay unavailable":
				healthy = false
			case "listener missing":
				delete(inventory, listener.ID)
			case "listener stopped":
				listener.Ready = false
				inventory[listener.ID] = listener
			}
			entry.op.Lock()
			unlock := sync.OnceFunc(entry.op.Unlock)
			defer unlock()
			observed := make(chan bool, 1)
			go func() { observed <- p.observeProxyRoutes(entry, inventory, healthy) }()
			select {
			case ready := <-observed:
				if ready {
					t.Fatal("failed route remained available")
				}
			case <-time.After(time.Second):
				t.Fatal("route withdrawal waited for the busy VM lifecycle lock")
			}
			if (<-notifications).Status.ContainerStatuses[0].Ready || !entry.proxyRouteFailed.Load() {
				t.Fatal("failed route retained Ready while startup was busy")
			}
			p.publish(entry)
			if (<-notifications).Status.ContainerStatuses[0].Ready {
				t.Fatal("lifecycle publication restored readiness despite route failure")
			}
			if entry.state != "COLD" || entry.generation != 9 || !entry.proxyRoutesReady {
				t.Fatal("route withdrawal mutated lifecycle-owned state")
			}
			unlock()
			backend, release, err := p.acquireProxy(t.Context(), entry, binding.BackendPort)
			if err == nil || backend != "" || release != nil || entry.activeConnections != 0 {
				t.Fatalf("failed route admitted an activation lease: %q, %v", backend, err)
			}
		})
	}
}

func TestProxyRouteWithdrawalOrdersStatusAfterInflightReadyPublication(t *testing.T) {
	entry := availabilityEntry(t)
	readyCallback, allowReady := make(chan struct{}), make(chan struct{})
	releaseReady := sync.OnceFunc(func() { close(allowReady) })
	t.Cleanup(releaseReady)
	notifications := make(chan bool, 3)
	p := &provider{notify: func(pod *corev1.Pod) {
		ready := pod.Status.ContainerStatuses[0].Ready
		if ready {
			close(readyCallback)
			<-allowReady
		}
		notifications <- ready
	}}
	published := make(chan struct{})
	go func() {
		p.publish(entry)
		close(published)
	}()
	awaitSignal(t, readyCallback)
	withdrawn := make(chan struct{})
	go func() {
		p.observeProxyRoutes(entry, nil, false)
		close(withdrawn)
	}()
	deadline := time.Now().Add(time.Second)
	for !entry.proxyRouteFailed.Load() {
		if time.Now().After(deadline) {
			t.Fatal("blocked status delivery prevented route admission fencing")
		}
		time.Sleep(time.Millisecond)
	}
	releaseReady()
	awaitSignal(t, published)
	awaitSignal(t, withdrawn)
	if !<-notifications || <-notifications {
		t.Fatal("an older Ready callback was delivered after route withdrawal")
	}
	p.publish(entry)
	if <-notifications {
		t.Fatal("subsequent lifecycle publication ignored the route failure")
	}
}

func TestProxyLeaseBlocksHibernateAndPublishesActivity(t *testing.T) {
	entry := availabilityEntry(t)
	entry.state, entry.actualReady = "READY", true
	p := &provider{}
	backend, release, err := p.acquireProxy(t.Context(), entry, 18080)
	if err != nil || backend != "127.0.0.1:18080" || release == nil {
		t.Fatalf("acquire ready backend: %q, %v", backend, err)
	}
	t.Cleanup(release)
	if entry.activeConnections != 1 || entry.publishedView.Load().ActiveConnections != 1 {
		t.Fatal("active lease was not visible to external idle ownership")
	}
	entry.op.Lock()
	err = p.hibernate(t.Context(), entry)
	entry.op.Unlock()
	if err == nil || !strings.Contains(err.Error(), "connections are active") || entry.state != "READY" || entry.generation != 9 {
		t.Fatalf("active connection did not fence hibernation: %v", err)
	}
	release()
	release()
	if entry.activeConnections != 0 || entry.publishedView.Load().ActiveConnections != 0 {
		t.Fatal("lease release was not idempotent and visible")
	}
}

func TestProxyWaitsForActualReadinessDespiteProjectedReady(t *testing.T) {
	entry := availabilityEntry(t)
	entry.state = "RUNNING"
	p := &provider{}
	p.projectProxyAvailability(entry)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	backend, release, err := p.acquireProxy(ctx, entry, 18080)
	if release != nil {
		release()
	}
	if !errors.Is(err, context.DeadlineExceeded) || backend != "" || release == nil || entry.activeConnections != 0 {
		t.Fatalf("unready application was forwarded or leaked its lease: %q, %v", backend, err)
	}
}

func TestCancelledProxyAdmissionDoesNotTakeLease(t *testing.T) {
	entry := availabilityEntry(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	backend, release, err := (&provider{}).acquireProxy(ctx, entry, 18080)
	if !errors.Is(err, context.Canceled) || release != nil || backend != "" || entry.activeConnections != 0 {
		t.Fatalf("cancelled request admitted a lease: %q, %v", backend, err)
	}
}

func TestDeletionCancelsProxyAdmissionBeforeWaitingForMonitor(t *testing.T) {
	entry := availabilityEntry(t)
	entry.lifetime, entry.cancel = context.WithCancel(t.Context())
	defer entry.cancel()
	entry.done = make(chan struct{})
	directory := t.TempDir()
	p := &provider{client: fake.NewClientset(entry.pod.DeepCopy()), starts: make(chan struct{}, 1),
		pods: map[string]*podEntry{"default/app": entry}, activeMemoryMiB: vmMemoryMiB,
		runner: commandRunner{cfg: config{state: directory, maxActiveMemoryMiB: vmMemoryMiB}}}
	p.proxy = testWakeProxy(t, 1, time.Minute)
	port, err := p.proxy.listen(string(entry.pod.UID), 8080, "127.0.0.1", 0,
		func(ctx context.Context) (string, func(), error) { return p.acquireProxy(ctx, entry, 18080) })
	if err != nil {
		t.Fatal(err)
	}
	client, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	deadline := time.Now().Add(time.Second)
	for {
		p.budgetMu.Lock()
		// Normalizing the legacy allocation confirms that the wake reached
		// admission. Waiting for RAM must retain neither RAM nor a start permit.
		waiting := entry.allocation.Version != 0 && !entry.allocated && p.activeMemoryMiB == vmMemoryMiB && len(p.starts) == 0
		p.budgetMu.Unlock()
		if current := entry.publishedView.Load(); waiting && current != nil && current.ActiveConnections == 1 {
			if !entry.op.TryLock() {
				break
			}
			entry.op.Unlock()
		}
		if time.Now().After(deadline) {
			t.Fatal("proxy request did not enter admission")
		}
		time.Sleep(time.Millisecond)
	}
	go func() {
		<-entry.lifetime.Done()
		entry.op.Lock()
		entry.op.Unlock()
		close(entry.done)
	}()
	// Stop cleanup at its existing ambiguity fence after cancellation has drained.
	if err := os.WriteFile(filepath.Join(directory, "native-command.pending"), []byte("run"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	err = p.DeletePod(ctx, entry.pod)
	if err == nil || !strings.Contains(err.Error(), "uncertain native command") {
		t.Fatalf("deletion waited on admission instead of cancelling it: %v", err)
	}
	if entry.activeConnections != 0 || len(p.starts) != 0 || entry.allocated || p.activeMemoryMiB != vmMemoryMiB {
		t.Fatal("cancelled proxy admission retained a lease, permit or RAM reservation")
	}
	if entry.state != "COLD" || entry.generation != 9 {
		t.Fatal("cancelled queued wake changed the Pod's cold lifecycle identity")
	}
}

func TestControllerIdleRequiresOwnershipAndNoActiveConnection(t *testing.T) {
	for _, scenario := range []struct {
		name, owner, state string
		connections        int
		recent             bool
		want               bool
	}{
		{name: "controller idle", owner: "controller", state: "READY", want: true},
		{name: "external queue owner", owner: "external", state: "READY"},
		{name: "open TCP connection", owner: "controller", state: "READY", connections: 1},
		{name: "recent activity", owner: "controller", state: "READY", recent: true},
		{name: "cold", owner: "controller", state: "COLD"},
		{name: "unready app after failed request", owner: "controller", state: "RUNNING", want: true},
		{name: "unready external owner", owner: "external", state: "RUNNING"},
		{name: "pending request to unready app", owner: "controller", state: "RUNNING", connections: 1},
		{name: "starting", owner: "controller", state: "STARTING"},
		{name: "restarting", owner: "controller", state: "RESTARTING"},
		{name: "uncertain failure", owner: "controller", state: "FAILED"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			entry := &podEntry{idleOwner: scenario.owner, state: scenario.state, activeConnections: scenario.connections,
				idleTimeout: time.Minute, lastActivity: time.Now().Add(-time.Hour)}
			if scenario.recent {
				entry.lastActivity = time.Now()
			}
			if idleEligible(entry) != scenario.want {
				t.Fatal("controller idle policy ignored ownership or activity")
			}
		})
	}
}

func TestColdBootAdmissionFailureKeepsIdentityAndBudgetRetryable(t *testing.T) {
	for _, scenario := range []string{"cancelled", "boot permits exhausted", "RAM exhausted"} {
		t.Run(scenario, func(t *testing.T) {
			entry := availabilityEntry(t)
			p := &provider{client: fake.NewClientset(entry.pod.DeepCopy()), starts: make(chan struct{}, 1),
				runner: commandRunner{cfg: config{state: t.TempDir(), maxActiveMemoryMiB: vmMemoryMiB}}}
			wantedPermits, wantedMemory := 0, 0
			ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
			defer cancel()
			switch scenario {
			case "cancelled":
				cancel()
			case "boot permits exhausted":
				p.starts <- struct{}{}
				wantedPermits = 1
			case "RAM exhausted":
				p.activeMemoryMiB = vmMemoryMiB
				wantedMemory = vmMemoryMiB
			}
			if err := p.resume(ctx, entry); err == nil || ctx.Err() == nil {
				t.Fatalf("expected admission cancellation, got %v", err)
			}
			if entry.state != "COLD" || entry.generation != 9 || entry.allocated ||
				p.activeMemoryMiB != wantedMemory || len(p.starts) != wantedPermits {
				t.Fatal("pre-boot admission failure consumed identity, memory or a boot permit")
			}
			files, err := os.ReadDir(p.runner.cfg.state)
			if err != nil || len(files) != 0 {
				t.Fatalf("admission failure wrote lifecycle state: %v", err)
			}
		})
	}
}

func TestColdWakeChecksCurrentPodLabelsForNetworkPolicies(t *testing.T) {
	entry := availabilityEntry(t)
	current := entry.pod.DeepCopy()
	current.Labels = map[string]string{"role": "isolated"}
	policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: "deny", Namespace: "default"},
		Spec: networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{MatchLabels: current.Labels}}}
	p := &provider{client: fake.NewClientset(current, policy)}
	err := p.resume(t.Context(), entry)
	if err == nil || !strings.Contains(err.Error(), "NetworkPolicy") || entry.state != "COLD" || entry.generation != 9 || entry.allocated {
		t.Fatalf("wake ignored policy selecting new Pod labels: %v", err)
	}
}

func TestProxySettingsRequireDeclaredTCPRoutes(t *testing.T) {
	for _, scenario := range []string{"no ports", "UDP", "duplicate port", "invalid owner", "invalid timeout"} {
		t.Run(scenario, func(t *testing.T) {
			pod := validPod()
			pod.Annotations = map[string]string{"microsandbox.dev/availability": "proxy"}
			pod.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 8080}}
			switch scenario {
			case "no ports":
				pod.Spec.Containers[0].Ports = nil
			case "UDP":
				pod.Spec.Containers[0].Ports[0].Protocol = corev1.ProtocolUDP
			case "duplicate port":
				pod.Spec.Containers[0].Ports = append(pod.Spec.Containers[0].Ports, pod.Spec.Containers[0].Ports[0])
			case "invalid owner":
				pod.Annotations["microsandbox.dev/idle-owner"] = "both"
			case "invalid timeout":
				pod.Annotations["microsandbox.dev/idle-timeout-seconds"] = "0"
			}
			if _, _, _, err := proxySettings(pod); err == nil {
				t.Fatal("unsupported proxy availability accepted")
			}
		})
	}
	pod := validPod()
	if enabled, _, _, err := proxySettings(pod); err != nil || enabled {
		t.Fatalf("strict queue-only pod required a proxy: %v", err)
	}
	pod.Annotations = map[string]string{"microsandbox.dev/availability": "proxy", "microsandbox.dev/idle-owner": "external"}
	pod.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 8080}, {ContainerPort: 8443, Protocol: corev1.ProtocolTCP}}
	if enabled, owner, _, err := proxySettings(pod); err != nil || !enabled || owner != "external" {
		t.Fatalf("external idle ownership was not retained: %v", err)
	}
}

func TestRelayOwnershipRequiresExactPodPortAndTarget(t *testing.T) {
	entry := availabilityEntry(t)
	binding := proxyPortBinding{ContainerPort: 8080, BackendPort: 18080, ProxyPort: 28080}
	owned := relayListenerView{Ready: true, ID: relayBindingID(entry, 8080), ListenAddress: "10.42.200.2:8080", TargetAddress: "192.168.1.1:28080"}
	if !matchingRelayBinding(owned, entry, binding, "192.168.1.1") {
		t.Fatal("owned listener was not recognized")
	}
	if !readyRelayBinding(owned, entry, binding, "192.168.1.1") {
		t.Fatal("healthy owned listener was not available")
	}
	unhealthy := owned
	unhealthy.Ready = false
	if readyRelayBinding(unhealthy, entry, binding, "192.168.1.1") || !matchingRelayBinding(unhealthy, entry, binding, "192.168.1.1") {
		t.Fatal("unhealthy listener must withdraw availability while retaining cleanup ownership")
	}
	for _, scenario := range []string{"UID", "pod address", "container port", "host address", "proxy port"} {
		t.Run(scenario, func(t *testing.T) {
			changed := owned
			switch scenario {
			case "UID":
				changed.ID = "another-pod-8080"
			case "pod address":
				changed.ListenAddress = "10.42.200.3:8080"
			case "container port":
				changed.ListenAddress = "10.42.200.2:8443"
			case "host address":
				changed.TargetAddress = "192.168.1.2:28080"
			case "proxy port":
				changed.TargetAddress = "192.168.1.1:28081"
			}
			if matchingRelayBinding(changed, entry, binding, "192.168.1.1") {
				t.Fatal("changed listener was accepted as owned")
			}
		})
	}
}

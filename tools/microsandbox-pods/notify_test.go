package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestNotifyPodsReplays500RecoveredStatusesWithoutWakingGuests(t *testing.T) {
	p := &provider{pods: make(map[string]*podEntry)}
	expected := make(map[types.UID]bool)
	for i := range 500 {
		entry := availabilityEntry(t)
		entry.pod.Name = fmt.Sprintf("recovered-%d", i)
		entry.pod.UID = types.UID(fmt.Sprintf("550e8400-e29b-41d4-a716-%012d", i))
		if i == 499 {
			entry.state = "SLEEPING"
		}
		if i == 250 {
			entry.proxyRoutesReady = false
		}
		p.pods["default/"+entry.pod.Name] = entry
		p.publish(entry)
		expected[entry.pod.UID] = entry.pod.Status.ContainerStatuses[0].Ready
		entry.op.Lock()
		t.Cleanup(entry.op.Unlock)
	}
	notifications := make(chan *corev1.Pod, 500)
	p.NotifyPods(t.Context(), func(pod *corev1.Pod) { notifications <- pod })
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	seen := make(map[types.UID]bool)
	for range 500 {
		select {
		case pod := <-notifications:
			want, exists := expected[pod.UID]
			if !exists || seen[pod.UID] {
				t.Fatalf("replay returned an unknown or duplicate Pod: %s", pod.UID)
			}
			seen[pod.UID] = true
			if pod.Status.ContainerStatuses[0].Ready != want {
				t.Fatalf("replay changed recovered availability for %s", pod.Name)
			}
			pod.Status.ContainerStatuses[0].Ready = !want
		case <-deadline.C:
			t.Fatalf("only %d of 500 recovered Pods were replayed", len(seen))
		}
	}
	p.jobs.Wait()
	for _, entry := range p.pods {
		if entry.state != "COLD" && entry.state != "SLEEPING" || entry.generation != 9 ||
			entry.allocated || entry.activeConnections != 0 || entry.actualReady {
			t.Fatalf("replay activated or changed recovered lifecycle: %s", entry.pod.Name)
		}
		if entry.pod.Status.ContainerStatuses[0].Ready != expected[entry.pod.UID] {
			t.Fatalf("callback received mutable provider state: %s", entry.pod.Name)
		}
	}
	if p.activeMemoryMiB != 0 {
		t.Fatal("status replay allocated guest RAM")
	}
}

func TestNotifyPodsRegistrationReturnsBeforeConsumerIsReady(t *testing.T) {
	entry := availabilityEntry(t)
	p := &provider{pods: map[string]*podEntry{"default/app": entry}}
	p.publish(entry)
	entered, allowCallback := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(allowCallback) })
	t.Cleanup(release)
	registered := make(chan struct{})
	go func() {
		p.NotifyPods(t.Context(), func(*corev1.Pod) {
			close(entered)
			<-allowCallback
		})
		close(registered)
	}()
	awaitSignal(t, registered)
	awaitSignal(t, entered)
	release()
	p.jobs.Wait()
}

func TestNotifyPodsCopiesCurrentStatusAfterPublicationLock(t *testing.T) {
	entry := availabilityEntry(t)
	p := &provider{pods: map[string]*podEntry{"default/app": entry}}
	p.publish(entry)
	entry.publication.Lock()
	unlock := sync.OnceFunc(entry.publication.Unlock)
	defer unlock()
	notifications := make(chan *corev1.Pod, 1)
	p.NotifyPods(t.Context(), func(pod *corev1.Pod) { notifications <- pod })
	entry.mu.Lock()
	entry.pod.Status.ContainerStatuses[0].Ready = false
	entry.pod.Status.Conditions = podConditions(false)
	entry.mu.Unlock()
	unlock()
	select {
	case pod := <-notifications:
		if pod.Status.ContainerStatuses[0].Ready {
			t.Fatal("replay delivered stale Ready captured before serialized publication")
		}
	case <-time.After(time.Second):
		t.Fatal("replay did not publish recovered status")
	}
	p.jobs.Wait()
}

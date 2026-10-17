package main

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
)

func (p *provider) configureEvents() {
	broadcaster := record.NewBroadcaster(record.WithContext(p.ctx), record.WithCorrelatorOptions(record.CorrelatorOptions{LRUCacheSize: 1024, BurstSize: 4, QPS: 0.1}))
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: p.client.CoreV1().Events("")})
	p.events = broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "microsandbox-pods", Host: p.runner.cfg.nodeName})
}

func (p *provider) event(e *podEntry, eventType, reason, message string, args ...any) {
	if p.events == nil {
		return
	}
	e.mu.Lock()
	pod := e.pod.DeepCopy()
	e.mu.Unlock()
	p.events.Eventf(pod, eventType, reason, message, args...)
}

package main

import (
	"iter"

	"github.com/floci-io/floci/tools/microsandbox-runtime/guesttask"
	corev1 "k8s.io/api/core/v1"
)

// Regular applications and restartable init sidecars share task supervision.
// Ordinary sequential init stages keep their existing completion protocol.
type managedContainer struct {
	index int
	init  bool
}

func restartableInit(container corev1.Container) bool {
	return container.RestartPolicy != nil && *container.RestartPolicy == corev1.ContainerRestartPolicyAlways
}

func hasRestartableInit(pod *corev1.Pod) bool {
	for _, container := range pod.Spec.InitContainers {
		if restartableInit(container) {
			return true
		}
	}
	return false
}

func managedContainers(pod *corev1.Pod) iter.Seq[managedContainer] {
	return func(yield func(managedContainer) bool) {
		for index := range pod.Spec.Containers {
			if !yield(managedContainer{index: index}) {
				return
			}
		}
		for index, container := range pod.Spec.InitContainers {
			if restartableInit(container) && !yield(managedContainer{index: index, init: true}) {
				return
			}
		}
	}
}

func (m managedContainer) container(e *podEntry) corev1.Container {
	if m.init {
		return e.pod.Spec.InitContainers[m.index]
	}
	return e.pod.Spec.Containers[m.index]
}

func (m managedContainer) status(e *podEntry) *corev1.ContainerStatus {
	if m.init {
		return &e.pod.Status.InitContainerStatuses[m.index]
	}
	return &e.pod.Status.ContainerStatuses[m.index]
}

func (m managedContainer) task(e *podEntry) string {
	if m.init {
		return initTask(m.index)
	}
	return applicationTask(e.pod, m.index)
}

func (m managedContainer) root(e *podEntry) string {
	if m.init {
		root, _ := guesttask.Directory(m.task(e))
		return root
	}
	return applicationRoot(e.pod, m.index)
}

func (m managedContainer) progress(e *podEntry) *applicationProgress {
	if !m.init && !concurrentApplications(e.pod) {
		return &e.applicationProgress
	}
	if e.applications == nil {
		e.applications = make(map[string]*applicationProgress)
	}
	name := m.container(e).Name
	if e.applications[name] == nil {
		e.applications[name] = &applicationProgress{}
	}
	return e.applications[name]
}

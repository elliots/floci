package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
	"github.com/prometheus/common/expfmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

func TestResourceMetricsNeverWakeOrExtendActivity(t *testing.T) {
	for _, state := range []string{"COLD", "SLEEPING", "TERMINATED", "FAILED"} {
		t.Run(state, func(t *testing.T) {
			e := &podEntry{pod: validPod(), state: state, vm: "owned", lastActivity: time.Unix(100, 0)}
			p := &provider{pods: map[string]*podEntry{"default/app": e}, runner: commandRunner{runtime: stubNativeRuntime{execute: func(context.Context, []string, []byte) (msbruntime.Result, error) {
				t.Fatal("metrics contacted a non-running guest")
				return msbruntime.Result{}, nil
			}}}}
			families, err := p.GetMetricsResource(t.Context())
			if err != nil || len(families) != 1 || families[0].GetName() != "scrape_error" || families[0].Metric[0].Gauge.GetValue() != 0 {
				t.Fatalf("sleeping guest metrics: %v, %v", families, err)
			}
			for _, family := range families {
				if _, err := expfmt.MetricFamilyToText(io.Discard, family); err != nil {
					t.Fatalf("kubelet cannot encode metrics: %v", err)
				}
			}
			if e.activeRequests != 0 || e.activeConnections != 0 || e.lastActivity != time.Unix(100, 0) {
				t.Fatal("metrics changed application activity")
			}
		})
	}
}

func TestResourceMetricsUseCgroupCountersAndExposePartialFailure(t *testing.T) {
	pod := validPod()
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(time.Unix(50, 0))}}}}
	e := &podEntry{pod: pod, state: "READY", vm: "owned", lastActivity: time.Unix(100, 0)}
	failure := false
	p := &provider{pods: map[string]*podEntry{"default/app": e}, runner: commandRunner{runtime: stubNativeRuntime{execute: func(_ context.Context, args []string, _ []byte) (msbruntime.Result, error) {
		if args[0] != "exec" || !strings.Contains(args[len(args)-1], "memory.current") {
			t.Errorf("unexpected metric operation %v", args)
		}
		if failure {
			return msbruntime.Result{}, errors.New("private guest error")
		}
		return msbruntime.Result{Stdout: "application 1234500 8192 3072 2048\n"}, nil
	}}}}
	families, err := p.GetMetricsResource(t.Context())
	if err != nil || families[0].Metric[0].Counter.GetValue() != 1.2345 || families[1].Metric[0].Gauge.GetValue() != 5120 || families[2].Metric[0].Gauge.GetValue() != 50 {
		t.Fatalf("wrong metrics: %v, %v", families, err)
	}
	if e.lastActivity != time.Unix(100, 0) || e.activeRequests != 0 {
		t.Fatal("metrics kept Pod awake")
	}
	summary, err := p.GetStatsSummary(t.Context())
	if err != nil || len(summary.Pods) != 1 || *summary.Pods[0].Containers[0].CPU.UsageCoreNanoSeconds != 1234500000 {
		t.Fatalf("summary: %+v, %v", summary, err)
	}
	// A busy lifecycle does not block a scrape or invent current readings.
	e.op.Lock()
	families, err = p.GetMetricsResource(t.Context())
	e.op.Unlock()
	if err != nil || len(families) != 1 || families[0].GetName() != "scrape_error" || families[0].Metric[0].Gauge.GetValue() != 1 {
		t.Fatal("busy guest failed to report partial scrape")
	}
	failure = true
	if _, err := p.GetStatsSummary(t.Context()); err == nil || strings.Contains(err.Error(), "private") {
		t.Fatalf("failed summary must not report zero usage or leak runtime error: %v", err)
	}
}

func TestResourceStatsRejectIncompleteAndInvalidCounters(t *testing.T) {
	for _, output := range []string{"", "other 1 2 3 4", "application -1 2 3 4", "application 18446744073709552 2 3 4", "application 1 2 3 4\napplication 1 2 3 4", "application 1 2 3"} {
		if err := parseResourceStats(output, []string{"application"}, make([]stats.ContainerStats, 1), metav1.Now()); err == nil {
			t.Errorf("accepted %q", output)
		}
	}
	containers := make([]stats.ContainerStats, 1)
	if err := parseResourceStats("application 1 2 3 1", []string{"application"}, containers, metav1.Now()); err != nil || *containers[0].Memory.WorkingSetBytes != 0 {
		t.Fatalf("racing inactive-file count must clamp working set: %+v %v", containers, err)
	}
}

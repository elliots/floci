package main

import (
	"context"
	"errors"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// Scrapes never wake guests or acquire application activity. Missing or busy
// guests are omitted, not represented by fabricated zero usage or stale samples.
func (p *provider) collectResourceStats(ctx context.Context) (*stats.Summary, bool) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	p.mu.Lock()
	entries := make([]*podEntry, 0, len(p.pods))
	for _, entry := range p.pods {
		entries = append(entries, entry)
	}
	p.mu.Unlock()
	summary := &stats.Summary{Node: stats.NodeStats{NodeName: p.runner.cfg.nodeName}, Pods: []stats.PodStats{}}
	var mu sync.Mutex
	failed := false
	jobs := make(chan *podEntry, len(entries))
	for _, entry := range entries {
		jobs <- entry
	}
	close(jobs)
	var workers sync.WaitGroup
	for range min(4, len(entries)) {
		workers.Go(func() {
			for entry := range jobs {
				pod, err := p.collectPodStats(ctx, entry)
				mu.Lock()
				failed = failed || err != nil
				if pod != nil {
					summary.Pods = append(summary.Pods, *pod)
				}
				mu.Unlock()
			}
		})
	}
	workers.Wait()
	slices.SortFunc(summary.Pods, func(a, b stats.PodStats) int {
		return strings.Compare(a.PodRef.Namespace+"/"+a.PodRef.Name, b.PodRef.Namespace+"/"+b.PodRef.Name)
	})
	return summary, failed
}

func (p *provider) collectPodStats(ctx context.Context, entry *podEntry) (*stats.PodStats, error) {
	if !entry.op.TryLock() {
		return nil, errors.New("Pod lifecycle is busy")
	}
	defer entry.op.Unlock()
	entry.mu.Lock()
	state, deleted := entry.state, entry.deleted
	pod := entry.pod.DeepCopy()
	entry.mu.Unlock()
	if deleted || state == "COLD" || state == "SLEEPING" || state == "TERMINATED" || state == "FAILED" {
		return nil, nil
	}
	if state != "READY" && state != "RUNNING" {
		return nil, errors.New("Pod is transitioning")
	}
	var containers []stats.ContainerStats
	var tasks []string
	for managed := range managedContainers(pod) {
		var status corev1.ContainerStatus
		if managed.init {
			if managed.index >= len(pod.Status.InitContainerStatuses) {
				continue
			}
			status = pod.Status.InitContainerStatuses[managed.index]
		} else {
			if managed.index >= len(pod.Status.ContainerStatuses) {
				continue
			}
			status = pod.Status.ContainerStatuses[managed.index]
		}
		if status.State.Running == nil {
			continue
		}
		containers = append(containers, stats.ContainerStats{Name: status.Name, StartTime: status.State.Running.StartedAt})
		tasks = append(tasks, managed.task(entry))
	}
	if len(tasks) == 0 {
		return nil, nil
	}
	read, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	output, err := p.runner.guest(read, entry.vm, resourceStatsScript(tasks))
	if err != nil {
		return nil, err
	}
	now := metav1.Now()
	if err := parseResourceStats(output, tasks, containers, now); err != nil {
		return nil, err
	}
	result := &stats.PodStats{PodRef: stats.PodReference{Name: pod.Name, Namespace: pod.Namespace, UID: string(pod.UID)}, Containers: containers}
	if pod.Status.StartTime != nil {
		result.StartTime = *pod.Status.StartTime
	}
	return result, nil
}

func resourceStatsScript(tasks []string) string {
	var script strings.Builder
	script.WriteString("set -eu\ntasks=$(" + ctr + " tasks list)\n")
	for _, task := range tasks {
		script.WriteString("pid=$(printf '%s\\n' \"$tasks\" | awk -v task=" + quote(task) + " '$1 == task && $3 == \"RUNNING\" { print $2 }')\n")
		script.WriteString(`case "$pid" in ''|*[!0-9]*) exit 1;; esac
group=$(awk -F: '$1 == "0" { print $3 }' "/proc/$pid/cgroup")
case "$group" in /floci-application-*/application) ;; *) exit 1;; esac
case "$group" in *..*|*[!a-zA-Z0-9_/-]*) exit 1;; esac
root="/sys/fs/cgroup$group"
cpu=$(awk '$1 == "usage_usec" { print $2 }' "$root/cpu.stat")
memory=$(cat "$root/memory.current")
inactive=$(awk '$1 == "inactive_file" { print $2 }' "$root/memory.stat")
rss=$(awk '$1 == "anon" { print $2 }' "$root/memory.stat")
`)
		script.WriteString("printf '%s %s %s %s %s\\n' " + quote(task) + " \"$cpu\" \"$memory\" \"$inactive\" \"$rss\"\n")
	}
	return script.String()
}

func parseResourceStats(output string, tasks []string, containers []stats.ContainerStats, now metav1.Time) error {
	if len(output) > 64<<10 || len(tasks) != len(containers) {
		return errors.New("invalid container metrics response")
	}
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != len(tasks) {
		return errors.New("incomplete container metrics response")
	}
	for i, line := range lines {
		fields := strings.Fields(line)
		if len(fields) != 5 || fields[0] != tasks[i] {
			return errors.New("container metrics identity differs")
		}
		values := [4]uint64{}
		for n := range values {
			value, err := strconv.ParseUint(fields[n+1], 10, 64)
			if err != nil {
				return errors.New("container metrics counter is invalid")
			}
			values[n] = value
		}
		cpu, memory, inactive, rss := values[0], values[1], values[2], values[3]
		if cpu > math.MaxUint64/1000 {
			return errors.New("container CPU counter overflows")
		}
		containers[i].CPU = &stats.CPUStats{Time: now, UsageCoreNanoSeconds: new(cpu * 1000)}
		containers[i].Memory = &stats.MemoryStats{Time: now, UsageBytes: new(memory), WorkingSetBytes: new(memory - min(memory, inactive)), RSSBytes: new(rss)}
	}
	return nil
}

func (p *provider) GetStatsSummary(ctx context.Context) (*stats.Summary, error) {
	summary, failed := p.collectResourceStats(ctx)
	if failed {
		return nil, errors.New("one or more active Pod metrics are unavailable; retry after lifecycle transitions")
	}
	return summary, nil
}

func (p *provider) GetMetricsResource(ctx context.Context) ([]*dto.MetricFamily, error) {
	summary, failed := p.collectResourceStats(ctx)
	cpu := &dto.MetricFamily{Name: new("container_cpu_usage_seconds_total"), Help: new("Cumulative container CPU time in seconds."), Type: new(dto.MetricType_COUNTER)}
	memory := &dto.MetricFamily{Name: new("container_memory_working_set_bytes"), Help: new("Container memory working set in bytes."), Type: new(dto.MetricType_GAUGE)}
	started := &dto.MetricFamily{Name: new("container_start_time_seconds"), Help: new("Container start time in Unix seconds."), Type: new(dto.MetricType_GAUGE)}
	for _, pod := range summary.Pods {
		for _, container := range pod.Containers {
			labels := []*dto.LabelPair{{Name: new("namespace"), Value: new(pod.PodRef.Namespace)}, {Name: new("pod"), Value: new(pod.PodRef.Name)}, {Name: new("container"), Value: new(container.Name)}}
			timestamp := container.CPU.Time.UnixMilli()
			cpu.Metric = append(cpu.Metric, &dto.Metric{Label: labels, TimestampMs: new(timestamp), Counter: &dto.Counter{Value: new(float64(*container.CPU.UsageCoreNanoSeconds) / 1e9)}})
			memory.Metric = append(memory.Metric, &dto.Metric{Label: labels, TimestampMs: new(timestamp), Gauge: &dto.Gauge{Value: new(float64(*container.Memory.WorkingSetBytes))}})
			started.Metric = append(started.Metric, &dto.Metric{Label: labels, TimestampMs: new(timestamp), Gauge: &dto.Gauge{Value: new(float64(container.StartTime.UnixNano()) / 1e9)}})
		}
	}
	errorValue := float64(0)
	if failed {
		errorValue = 1
	}
	scrape := &dto.MetricFamily{Name: new("scrape_error"), Help: new("Whether one or more active Pod metrics could not be collected."), Type: new(dto.MetricType_GAUGE), Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: new(errorValue)}}}}
	return []*dto.MetricFamily{cpu, memory, started, scrape}, nil
}

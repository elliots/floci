# Workload resource metrics

The existing authenticated kubelet TLS server exposes `/metrics/resource` and
`/stats/summary`. Running application containers and restartable init sidecars
report cumulative cgroup-v2 CPU time, memory usage and working-set memory. The
working set subtracts `inactive_file` from `memory.current`, clamped to zero.
The resource endpoint uses the standard container metric names and labels read
by metrics-server, allowing `kubectl top pods` when metrics-server is installed.

Collection uses at most four concurrent guest reads and a ten-second scrape
deadline. It does not acquire HTTP activity, reset idle time or wake a guest.
Cold, sleeping and completed containers have no current resource sample. They
are omitted rather than represented as running containers consuming zero RAM.
Busy or failed active guests set `scrape_error` and omit their samples; the
summary endpoint instead returns an error for an incomplete scrape. Metrics are
not a reason to keep workloads awake.

Host-wide node CPU and working-set metrics are deliberately absent. Host usage
includes other workloads, and summing guest cumulative counters would create a
misleading node counter whenever guests disappear. Use the authenticated
`/v1/capacity` management endpoint for admitted VM RAM and outstanding capacity
reservations. Application cgroup memory is distinct from guest allocation and
host resident memory. Filesystem, network and PSI metrics remain unsupported.

HPA still uses Kubernetes requests and normal metrics-server behavior when
samples are unavailable. Proxy availability does not fabricate a resource
sample for a sleeping or never-started container.

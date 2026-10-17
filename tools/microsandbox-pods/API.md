# Experimental host lifecycle API, contract 1

This generic API is separate from the released sidecar contract. It binds an
explicit loopback IP and requires `Authorization: Bearer <token>` on every route.
`--control-token-file` must contain at least 32 bytes. Access grants control over
all owned Pods. Kubelet logs/exec use a separate mTLS/RBAC listener.

| Request | Result |
| --- | --- |
| `GET /v1/health` | status, name, contract, clusterName, activeMemoryMiB, maxActiveMemoryMiB, maxPods |
| `GET /v1/capacity` | Verified shared runtime RAM, pending starts, reservations, limits and enabled disk admission accounting; 503 if unverifiable |
| `GET /v1/pods/{uid}` | Exact owned Pod view |
| `POST /v1/pods/{uid}/hibernate` | Updated Pod view |
| `POST /v1/pods/{uid}/resume` | Updated Pod view |
| `GET /v1/deployments` | Batch `{deployments:[view or {namespace,name,error}]}` |
| `GET /v1/deployments/{namespace}/{name}` | Actual Deployment identity and owned Pod views |

POST requires `{"expectedGeneration":4}`. Stale generations conflict: refresh and
reconcile before retrying. Requests may finish after client disconnect. Resume
boots COLD or restores SLEEPING. Hibernate leaves COLD untouched. Hibernate also
accepts `expectedActivitySequence` from the last observed Pod view. A mismatch
returns 409 before mutation, protecting against requests completed between the
idle decision and hibernation. Older clients can omit it; new coordinators should
send it when the observation includes `activitySequence`.

Pod views contain `uid`, `namespace`, `name`, `state`, `generation`, `podIP`, `vm`,
`vcpus`, `memoryMiB`, `rootDiskMiB` (the persisted guest allocation),
`udpPort`, optional `snapshot`, and optional `backendUrl`, `httpPort`,
`applicationPort`, `proxyAvailability`, optional `proxyProtocol` (`http` or `tcp`),
`idleOwner`, `activeRequests`,
`activeConnections`, `activitySequence`, and
optional `proxyUrl` (first host wake listener). The backend is `http://127.0.0.1:<published-port>` for the first
declared TCP container port, assigned before cold boot. Neither endpoint load
balances replicas. Floci's default pod routing chooses one owned pod and uses
its HTTP `proxyUrl` for application requests, with `backendUrl` for running
health checks. A loopback advertisement is resolved on the configured controller
host without changing its port. The listeners must actually be reachable there.
Explicit Service routing preserves custom origins but can wake other replicas
through Service balancing. A missing `proxyProtocol` cannot prove that a proxy
speaks HTTP; rebuild the controller or explicitly use Service routing.

States: COLD, WAITINGVOLUMES, STARTING, INITSTARTING, INITIALIZING, RUNNING, READY,
HIBERNATING, CHECKPOINTED, SLEEPING, RESUMING, RESTARTING, TERMINATED, FAILED.
Only READY confirms guest readiness. INITIALIZING includes running init tasks
and retry backoff. INITSTARTING fences a submitted init launch whose outcome
cannot safely be replayed after an interrupted controller operation. Coordinators
must wait through both states and must not attempt hibernation during initialization.
WAITINGVOLUMES is a strict pod awaiting required ConfigMap/Secret volume sources
before VM allocation. It remains Pending and resumes that wait after controller
restart; provisioning continues automatically when its sources become available.
Sleeping Pods keep their UID and replicas. Strict Pods remain not Ready; opted-in
proxy Pods can remain Ready through their actual shared wake route. Shared node
leases continue. `proxyAvailability` identifies the opted-in mode, not the guest's
readiness. External coordinators must require `idleOwner: "external"`, adopt
actual direct activations and honor both `activeRequests` and `activeConnections`
before idle hibernation. `activeRequests` counts HTTP requests through the shared
pod proxy, including their streaming responses and upgraded streams until they
finish. `activeConnections` counts legacy TCP-mode proxy and port-forward sessions.
Idle frontend HTTP keepalive connections do not contribute to either counter and
do not wake the pod. The controller also rejects hibernation atomically while
either kind of activity is present.

`activitySequence` advances when activity is acquired or released. External
coordinators must observe it as well as the current counts so a short request
completed between polls still resets their idle interval. It is nonnegative and
monotonic within a controller entry, not a durable count across controller
restarts; establish a new observation baseline after recovery.

The Pod annotation `microsandbox.dev/proxy-protocol` selects `http` (the default)
or `tcp` for every declared proxy port. HTTP mode accepts HTTP/1 and cleartext
HTTP/2 (prior-knowledge h2c/gRPC). The backend must support the incoming protocol;
HTTP/2-to-HTTP/1 translation is not provided. A request from any reachable
Service/PodIP client acquires activity before wake and retains it until its response or upgraded stream finishes. An idle
frontend connection can remain open across hibernation; the next request wakes
the pod. TCP mode retains connection-based activation for raw TCP/TLS, including
idle connections. Terminate HTTPS before an HTTP-mode listener or use TCP mode.
These counters do not replace the external coordinator's gateway request or
SQS/RabbitMQ message accounting. Kubernetes probes do not renew normal idle time;
direct HTTP health requests count unless handled separately by a configured gateway.
The shared proxy bounds open HTTP frontend connections and in-flight requests
separately at 1024 each. Idle HTTP keepalive expires after ten minutes; keeping
an idle connection consumes a frontend slot but no guest activity lease.

Deployment fields: namespace, name, uid, generation, replicas, clusterName, pods.
Discovery checks the real selector and Pod to ReplicaSet to Deployment owner
chain. Zero replicas, no Pods or another runtime's nonterminating Pod fail.
Callers require a stable Pod set equal to replicas before coordinating changes.
Batch discovery performs one list each for Deployments, ReplicaSets and Pods,
then joins immutable lifecycle snapshots without waiting behind guest operations.
Individual discovery errors do not hide healthy unrelated Deployments. Clients
can briefly cache the batch (Floci uses one second) rather than issue one query
per workload per reconciliation tick.

Errors use `{"error":"description"}`: 400 invalid input, 401 authorization,
404 unknown UID/route, 409 lifecycle conflict/failure. These are host-control
errors. Captured guest output is excluded from errors and Pod status; authorized
logs and exec are the app-output paths.

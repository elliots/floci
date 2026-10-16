# Experimental host lifecycle API, contract 1

This generic API is separate from the released sidecar contract. It binds an
explicit loopback IP and requires `Authorization: Bearer <token>` on every route.
`--control-token-file` must contain at least 32 bytes. Access grants control over
all owned Pods. Kubelet logs/exec use a separate mTLS/RBAC listener.

| Request | Result |
| --- | --- |
| `GET /v1/health` | status, name, contract, clusterName, activeMemoryMiB, maxActiveMemoryMiB, maxPods |
| `GET /v1/pods/{uid}` | Exact owned Pod view |
| `POST /v1/pods/{uid}/hibernate` | Updated Pod view |
| `POST /v1/pods/{uid}/resume` | Updated Pod view |
| `GET /v1/deployments` | Batch `{deployments:[view or {namespace,name,error}]}` |
| `GET /v1/deployments/{namespace}/{name}` | Actual Deployment identity and owned Pod views |

POST requires `{"expectedGeneration":4}`. Stale generations conflict: refresh and
reconcile before retrying. Requests may finish after client disconnect. Resume
boots COLD or restores SLEEPING. Hibernate leaves COLD untouched.

Pod views contain `uid`, `namespace`, `name`, `state`, `generation`, `podIP`, `vm`,
`vcpus`, `memoryMiB` (the persisted guest allocation),
`udpPort`, optional `snapshot`, and optional `backendUrl`, `httpPort`,
`applicationPort`, `proxyAvailability`, `idleOwner`, `activeConnections`, and
optional `proxyUrl` (first host wake listener). The backend is `http://127.0.0.1:<published-port>` for the first
declared TCP container port, assigned before cold boot. It does not load balance
replicas; coordinators should explicitly configure a Service/backend for that.

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
actual direct activations and honor `activeConnections` before idle hibernation.
The controller also rejects hibernation atomically while a connection is active.

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

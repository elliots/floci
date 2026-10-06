# On-demand application runtime

Floci can start explicitly configured Kubernetes deployments, ECS services, and Docker-backed EC2
instances when application requests or SQS messages arrive, then stop them when idle. The feature
is disabled by default and extends emulator runtime behavior. AWS management request and response formats remain
unchanged.

Your existing tooling can deploy workloads and generate their configuration. Floci owns host-based
application routing, activation, readiness checks, and idle shutdown.

Floci's EKS service can provision the Kubernetes control plane itself as a k3s container. Set
`runtime.cluster-name` to target one of those clusters. Its endpoint and Kubernetes credentials
are resolved internally, without exporting or changing your machine's kubeconfig. The control
plane stays running while application deployments scale to zero.

## Enable the runtime

```bash
export FLOCI_ON_DEMAND_ENABLED=true
export FLOCI_ON_DEMAND_CONFIG_FILE=/path/to/on-demand.yaml
export JAVA_HOME=/path/to/jdk-25
./mvnw quarkus:dev
```

The on-demand configuration file accepts YAML or JSON. Floci validates and reads it at startup.
Restart Floci after changing the file. Resource identifiers can refer to resources created later
by your normal deployment workflow; a missing resource fails activation and can be retried after
provisioning.

Configure [network isolation and simulated third-party origins](workload-network.md) with
`FLOCI_NETWORK_CONFIG_FILE`. Network routes can call this gateway through a backend URL.

```yaml
workloads:
  orders:
    host: orders.dev.test
    account-id: '000000000000'
    region: us-east-1
    idle-timeout-seconds: 120
    startup-timeout-seconds: 90
    runtime:
      type: kubernetes
      cluster-name: integration-test
      namespace: integration-test
      deployment: orders
      replicas: 1
      backend-url: http://orders-backend.integration-test.svc.cluster.local:8080
    queues: [orders-incoming]
    health:
      requests:
        - method: GET
          path: /health
        - method: HEAD
          path: /health
      sleeping-response:
        status: 200
        body: '{"status":"UP"}'
        content-type: application/json
    readiness:
      path: /ready
      status: 200

  worker:
    queues: [background-jobs]
    runtime:
      type: kubernetes
      cluster-name: integration-test
      namespace: integration-test
      deployment: worker

  legacy:
    host: legacy.dev.test
    runtime:
      type: ec2
      instance-id: i-0123456789abcdef0
      backend-port: 8080
    readiness:
      path: /ready
```

Account and region default to Floci's configured defaults. Each HTTP hostname and runtime target
must have one owner. Queue names can have multiple configured consumers, which all activate when
the shared queue has work. Use queue names, including the `.fifo` suffix where applicable, rather
than URLs or ARNs.

Queue-only Kubernetes workers need no HTTP host, backend URL, or HTTP readiness endpoint. They
use the deployment's observed generation and available replicas for readiness. An EC2 worker
without HTTP readiness uses the instance's running state; configure an HTTP readiness endpoint
if its application requires additional boot validation.

HTTP workloads default to `GET /ready` expecting status 200. Set `readiness.enabled: false` to rely
only on runtime readiness, or explicitly configure a different path and successful status.

## ECS services

Use `runtime.type: ecs`, `runtime.service`, and `runtime.cluster-name` (default `default`).
`runtime.replicas` sets the awake desired count, defaulting to one. ECS service names and cluster
names are scoped to the configured account and region. This controls Floci's Docker-backed ECS
emulator, including its EC2 and Fargate launch types. It does not control an actual Amazon ECS
service or suspend EC2 capacity-provider hosts.

Only active `REPLICA` services using the `ECS` deployment controller are supported. Floci updates
only `desiredCount`, preserving the task definition, networking, capacity providers, and other
service settings. Startup waits for the current deployment's running tasks and configured container
health checks. Shutdown sets desired count to zero and waits for every owned task to reach `STOPPED`,
including container removal. Other services and standalone tasks remain independent.

HTTP and gRPC services require a stable `backend-url`, such as a backend load balancer or a fixed
published port. The existing ECS load-balancer registrar can update targets as tasks are replaced.
That backend must bypass the activation gateway. A task's direct IP or dynamic port is unsuitable:
it can change on each start or during task replacement. Queue-only ECS services need no backend URL.

Tasks holding active ECS task scale-in protection keep the workload awake, and the ECS scheduler
respects protection during scale-in and deployment replacement. Use the standard
`UpdateTaskProtection` API for work continuing after an RPC response or queue acknowledgement;
clear protection when that work finishes. Protection defaults to 120 minutes and honors its expiry.
A deliberate `StopTask` or service deletion can still stop a protected task. ECS restarts create new tasks and containers;
keep durable state in mounted volumes or external stores.

```yaml
workloads:
  integrations:
    host: integrations.dev.test
    runtime:
      type: ecs
      cluster-name: customer-tests
      service: integration-api
      replicas: 1
      backend-url: http://integration-backend:50051
      backend-protocol: http2
    queues: [integration-events]
    readiness:
      enabled: false
```

This example relies on ECS runtime and container health checks. Configure an ECS container health
check that verifies the gRPC server is serving, or provide a real HTTP readiness endpoint. A pure
gRPC server usually does not implement the default HTTP `GET /ready` probe.

## HTTP/2 and gRPC

The gateway accepts cleartext HTTP/2 with prior knowledge (h2c), HTTP/1.1 upgrades to h2c, and
ordinary HTTP/1.1 on the same listener. HTTP/2 routing uses `:authority`, equivalent to the HTTP/1.1
Host header. Set `runtime.backend-protocol: http2` on any runtime type to use HTTP/2 to the backend;
the default remains `http1`. An `http://` backend uses h2c with prior knowledge. An `https://` backend
uses TLS and ALPN with normal certificate verification. Inbound TLS still terminates at your ingress,
which must forward gRPC over HTTP/2 and preserve the application's authority.

Unary, client-streaming, server-streaming, and bidirectional gRPC calls are proxied without decoding
messages. Binary metadata, compression headers, `grpc-timeout`, response trailers, status, status
details, and error messages pass through. Uploads and downloads stream independently with
backpressure; a stream's lease remains active until response completion or cancellation. Resetting
one HTTP/2 stream does not close other streams on the connection. Idle HTTP/2 connections alone do
not keep containers awake. Before response headers are sent, backend transport failures and startup
failures return gRPC `UNAVAILABLE`; a gateway startup timeout returns `DEADLINE_EXCEEDED`. A failure
after response headers resets the affected stream, leaving the connection usable. A cold start
consumes the caller's deadline, so clients should allow for startup time.

Configured synthetic health exclusions still match only exact GET/HEAD paths. gRPC health RPCs are
POST requests and count as application traffic, so periodic gRPC health checks through the gateway
will wake the workload. Route only deliberate activation traffic through those RPCs or use the
configured HTTP sleeping-health endpoint for ingress probes. Generic request trailers, HTTP/2 server
push, extended CONNECT, and gRPC-Web translation are outside this proxy's supported contract.

## Route application traffic through Floci

The application gateway has its own listener, defaulting to `127.0.0.1:8080`. AWS APIs continue
using port 4566. The gateway uses the HTTP Host header to select a configured workload, preserves
methods, query strings, authorization headers, binary bodies, response status, and duplicate
headers, and streams requests and responses with backpressure. Standard proxy hop-by-hop headers
are removed. WebSocket upgrades are relayed and hold the workload awake for the connection's
lifetime. The gateway never automatically replays a request after a backend failure.

For a local request without changing DNS:

```bash
curl --resolve orders.dev.test:8080:127.0.0.1 http://orders.dev.test:8080/orders
```

For internal Kubernetes calls, deploy a Service pointing to Floci's gateway and arrange the
application hostname and port through your existing manifests, ingress, DNS, or proxy. Preserve
the original application hostname when an ingress forwards traffic. Keep a separate backend
Service pointing directly to application pods; `backend-url` must reach that Service rather than
the gateway. Floci must be able to reach the backend from where it runs. A host process cannot
normally resolve cluster Service DNS names without additional networking.

For local and container networks, the same rule applies: application names reach Floci, while
Floci connects to distinct backend addresses. Calls directly to pod IPs, EC2 addresses, or container
ports bypass activation. Floci does not rewrite existing Services or install DNS rules.

Inbound TLS terminates at your existing ingress/proxy. HTTPS backend origins are supported with
normal certificate validation. This gateway serves HTTP, gRPC, and WebSocket application traffic.
Raw TCP database protocols are not provided by this facility.

## Health and readiness

Health exclusions match the method and path exactly. Only GET and HEAD exclusions are accepted.
Query parameters do not change a matching health path. A POST to `/health`, or a GET to a different
path, is ordinary application traffic unless explicitly configured otherwise.

| Runtime state | Configured health request |
|---|---|
| Deliberately sleeping or draining after successful execution | Configured synthetic success, without activation |
| Running and ready | Forwarded to the real application without refreshing the idle clock |
| Starting, disabled, unknown, or failed | HTTP 503 |

Synthetic health means the endpoint is available for on-demand activation. It does not prove a
stopped process is healthy. Actual startup readiness always goes directly to the backend and does
not use the synthetic health response. Failed startups remain unhealthy even after cleanup.
Synthetic and gateway error responses use `Cache-Control: no-store`.

A running health probe holds the backend only until that probe completes. It does not restart
the idle interval. Kubernetes startup, readiness, and liveness probes keep their existing behavior
inside running pods. Deep application probes that call other services can still activate those
services; use shallow probes where these calls would prevent the intended idle behavior.

## Queue activity and shutdown

Successful SQS enqueues notify the runtime asynchronously through the shared SQS service, including
SNS-to-SQS delivery. Reconciliation reads visible and in-flight counts without receiving messages.
It covers delayed messages becoming visible, visibility-timeout retries, redrive, and persisted
messages after restart. FIFO ordering, deduplication, visibility, and acknowledgement stay with SQS
and the application consumer.

Visible or in-flight messages keep configured consumers running. Empty consumer polling does not
keep them awake. The idle interval begins after queue activity is confirmed empty and the last
application request finishes. Failed queue reads hold workers awake until queue state is known
again. A consumer startup failure does not fail an already successful SendMessage or remove its
message. Consumers must acknowledge after completing work; tasks continuing after acknowledgement
need another activity signal or must remain outside this facility.

Ordinary requests, response streams, WebSockets, and queue activity prevent idle shutdown. A
request arriving during shutdown waits for shutdown to finish and the next startup to become
ready. Concurrent requests share startup, and readiness and pending-request capacity are bounded.
The concurrent-start limit applies only while submitting start or scale commands. Runtime and
application readiness checks run after the slot is released, so a starting application can call another
sleeping workload even when the limit is one. No dependency declarations are required. More
workloads than this limit may be warming up simultaneously. One startup deadline covers lifecycle
preparation, waiting for a launch slot, the start command, and both readiness checks.
Gateway startup failures return 503; a request exceeding its startup deadline returns 504. Backend
connection/stream failures return 502, or close a response whose headers were already sent.

Reconciliation also checks whether a ready workload is still running. If it has stopped, Floci
discards its backend address and marks it failed. Queue work or the next application request
activates it again and resolves a fresh backend address. Health probes do not trigger recovery,
and requests already sent to the old backend are not replayed. A failed runtime inspection defers
idle shutdown until its state can be checked again.

Kubernetes deployments scale to zero, retaining Services and persistent volumes. Shutdown waits
for matching pods to disappear, including terminating pods, before a subsequent activation starts.
EC2 uses Floci's
existing StopInstances/StartInstances lifecycle, including truthful instance states and refreshed
container addresses. EC2 applications must have a boot mechanism that runs again after stop/start;
a process launched only once by user data may not restart. EKS backing nodes cannot be managed as
standalone EC2 workloads.

On restart, Floci inspects configured resources, adopts running workloads through readiness, and
leaves dormant workloads asleep. It reconstructs queue activity from the existing StorageFactory
stores. Emulator reset quiesces the coordinator and reconstructs it after resources are cleared.
Definitions remain in the external file. Shutdown closes the gateway and coordinator; existing
EC2 teardown follows Floci's lifecycle, while external Kubernetes deployments are left at their
current replica count for adoption on the next startup.

## Configuration and deployment requirements

| Variable | Default | Meaning |
|---|---|---|
| `FLOCI_ON_DEMAND_ENABLED` | `false` | Enable the runtime and application gateway |
| `FLOCI_ON_DEMAND_CONFIG_FILE` | unset | Required on-demand YAML/JSON configuration file |
| `FLOCI_ON_DEMAND_GATEWAY_HOST` | `127.0.0.1` | Application gateway bind address |
| `FLOCI_ON_DEMAND_GATEWAY_PORT` | `8080` | Application gateway port, separate from AWS API port |
| `FLOCI_ON_DEMAND_RECONCILE_INTERVAL_MILLIS` | `1000` | Queue and idle reconciliation cadence, minimum 100 ms |
| `FLOCI_ON_DEMAND_MAX_CONCURRENT_STARTS` | `4` | Concurrent start/scale commands; readiness waits do not hold a slot |
| `FLOCI_ON_DEMAND_MAX_PENDING_REQUESTS` | `1024` | Global maximum requests waiting for startup |
| `FLOCI_ON_DEMAND_MAX_PENDING_REQUESTS_PER_WORKLOAD` | `64` | Maximum pending requests per workload |

To bind beyond loopback, explicitly enable `FLOCI_SECURITY_ALLOW_UNSAFE_NETWORK_EXPOSURE`, as with
Floci's other listeners. Put customer-facing authentication, TLS, and environment isolation at
your existing edge. Emulated account namespaces are not a security boundary between untrusted
customers.

With `runtime.cluster-name`, the Kubernetes adapter resolves the named Floci EKS cluster in the
configured account and region and uses its internal endpoint and k3s administrator credentials.
The cluster must be active and have a real k3s container; metadata-only mock clusters cannot run
applications. The runtime shares a client between workloads in a cluster. Your normal EKS/IaC
workflow still creates the cluster, deployments, and backend Services.

Omit `runtime.cluster-name` to use an existing external cluster through mounted service-account
credentials or the current kubeconfig context, including OrbStack Kubernetes. That identity needs
get access to Deployments, list access to Pods, and get and update access to their
`deployments/scale` subresource.
The existing client's kubeconfig authentication support and limitations apply. Cluster selection
does not make backend Service DNS or pod addresses reachable from outside the cluster; configure
a backend origin reachable by Floci in either mode.

Configured workloads belong to this runtime. Disable competing HPA/KEDA/GitOps replica reconciliation
and avoid Auto Scaling-managed EC2 targets. Manual scale/stop operations are not durable disable
signals: set the workload's `enabled: false` and restart Floci to prevent activation. Run one active
Floci runtime owner per environment; cross-process activation locking is not implemented.

Keep Floci, gateway networking, Kubernetes control-plane components, essential databases, and storage
available. This feature releases application compute resources; reducing cloud instance costs also
requires your cluster's worker capacity to shrink. Database suspension and arbitrary background
schedulers are separate policies.

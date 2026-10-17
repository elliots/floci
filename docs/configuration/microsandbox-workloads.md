# Experimental microsandbox workloads

This branch can run an OCI application in a native microsandbox microVM, save its
RAM and disk when idle, and resume it for HTTP or queued work. Docker remains the
default for existing AWS services. This local workload configuration is separate
from AWS management APIs; it does not turn an arbitrary VM into an EC2 instance
or Kubernetes pod.

Use microsandbox 0.7.8 and the authenticated native host helper described in
the repository's `tools/microsandbox/MIGRATION.md`. Run the helper on
the hypervisor host and retain its runtime home and Floci's persistent storage.
Only one Floci controller may own that runtime home. The helper is experimental
and is built from `tools/microsandbox-host` in this checkout.

```sh
export FLOCI_ON_DEMAND_ENABLED=true
export FLOCI_ON_DEMAND_CONFIG_FILE=/absolute/path/workloads.yml
export FLOCI_ON_DEMAND_MICROSANDBOX_URL=http://127.0.0.1:8186
export FLOCI_ON_DEMAND_MICROSANDBOX_TOKEN=your-local-runtime-token
export FLOCI_STORAGE_MODE=persistent
```

Example `workloads.yml`:

```yaml
workloads:
  worker:
    host: worker.localhost
    account-id: "000000000000"
    region: us-east-1
    runtime:
      type: microsandbox
      image: your-worker:1.0.0
      backend-port: 8080
      cpus: 1
      memory-mib: 512
      root-disk-mib: 4096
      environment:
        AWS_REGION: us-east-1
        AWS_ACCESS_KEY_ID: "000000000000"
        AWS_SECRET_ACCESS_KEY: test
        AWS_ENDPOINT_URL: http://host.microsandbox.internal:4566
    queues: [jobs]
    rabbitmq-queues:
      - broker-id: b-replace-with-your-broker-id
        virtual-host: /
        queue: jobs
        username-env: WORKER_RABBITMQ_USER
        password-env: WORKER_RABBITMQ_PASSWORD
    idle-timeout-seconds: 120
    startup-timeout-seconds: 240
    readiness:
      path: /ready
      status: 200
    health:
      requests:
        - method: GET
          path: /health
      sleeping-response:
        status: 200
        body: sleeping
        content-type: text/plain
```

Set the RabbitMQ credential variables in Floci's environment for its management
probe. Configure the application's broker address and credentials separately;
the probe's environment variables are not automatically passed to the guest. Remove queue
entries you do not use. An application needs at least one HTTP hostname or queue
source. Queue-only workers can omit `host` and set `readiness.enabled: false`.
`runtime.command` optionally supplies a list of command arguments. Environment
values are literal strings, not shell expressions or environment interpolation.
Protect the workload file and the native runtime home when they contain secrets.
Standalone application VMs permit outbound access to the host, private networks
and public networks, with ingress through their published port. This local
profile does not apply EC2 security groups or Kubernetes NetworkPolicy.

Send HTTP requests to the gateway, which defaults to `127.0.0.1:8080`, using the
configured Host header. The first request waits for the VM and its readiness
check. Active requests, streaming responses and WebSockets hold the VM awake.
The configured health requests receive their synthetic response while sleeping
and do not wake the VM or extend its idle timer. The gateway does not replay a
request after forwarding it to the application.

## Kubernetes workloads

For opted-in Kubernetes Deployments, see
[native Kubernetes pods](microsandbox-kubernetes.md). Their wake rules use the
same HTTP and queue coordinator, while the pod controller preserves Pod UIDs and
replicas across hibernation. Their shared pod proxy defaults to HTTP request
accounting, so direct Service/PodIP requests from other pods also wake the selected
application. Responses and upgraded streams hold it awake; idle frontend HTTP
keepalive connections do not. Explicit `microsandbox.dev/proxy-protocol: tcp`
retains the previous connection-based behavior for raw TCP/TLS listeners. HTTPS
needs TLS termination before an HTTP-mode listener or TCP mode. This changes
neither the Floci gateway's request accounting nor the queue rules below.
EC2 wake integration is parked separately and is not available in this branch.

## Queue and MQTT behavior

SQS visible and in-flight messages keep a workload awake. Floci checks queue
attributes in the configured AWS account and region; it does not receive or
delete messages as a wake probe. Delayed messages wake the worker when visible,
and unacknowledged messages remain subject to the queue's visibility timeout.

For Amazon MQ RabbitMQ brokers, Floci reads the management API's ready and
unacknowledged counts. The application consumes and acknowledges its own
messages. An unavailable broker or unreadable queue count prevents an already
running workload from being hibernated. Queue observations do not provide
exactly-once processing, and applications should retain their normal retry and
idempotency behavior.

For AWS IoT MQTT, configure an ordinary IoT topic rule with an SQS action and
include that queue in `queues`. The rule places the published message in SQS;
that durable handoff wakes the worker. A direct MQTT subscription in a sleeping
application is not a durable wake source in this implementation.

## Resume and recovery

Hibernation preserves process memory and the private disk. Existing network
connections can break across restore, so applications must reconnect to SQS,
RabbitMQ, databases and other services. Configure retry and visibility timeouts
for the expected resume interval. A changed image, command, environment,
resource allocation or published-port configuration is rejected for an existing
journaled workload; migrate or explicitly reset it before replacing its runtime.

Floci records lifecycle transitions through `StorageFactory`. When helper
completion is confirmed, it can adopt a previously restored generation even if
the original caller timed out. It refuses to replay a
checkpoint after an ambiguous restore has consumed its source. This matters for
queue consumers: a replay could repeat acknowledgements or external effects.
Missing state causes an error rather than launching an empty replacement.
If the helper's completion response is lost or Floci exits while a command is
pending, the persisted command record blocks further mutations and reset until
manual recovery. Follow the command-journal recovery procedure in
`tools/microsandbox/MIGRATION.md`.

Ordinary Floci shutdown retains application guests for adoption on restart.
Reset removes this deployment's recorded guests and checkpoints. Keep independent
backups; the experiment does not promise power-loss durability, live network
connection migration, or cross-host snapshot portability.

## Settings

`FLOCI_MICROSANDBOX_MAX_ACTIVE_MEMORY_MIB` limits admitted guest RAM across
the native RDS and generic standalone application providers to 8192 MiB by default.
A separate pod controller has its own budget. A wake waits for capacity;
hibernation releases its allocation only after the guest is confirmed stopped.
An uncertain runtime operation retains its reservation. Set zero to disable
the limit. This accounts for configured guest RAM, not all host memory: allow
additional space for Floci, Docker, brokers, the control plane and VM overhead.

| Variable | Default | Purpose |
| --- | --- | --- |
| `FLOCI_ON_DEMAND_ENABLED` | `false` | Enable the local workload coordinator |
| `FLOCI_ON_DEMAND_CONFIG_FILE` | unset | YAML or JSON workload definitions |
| `FLOCI_ON_DEMAND_GATEWAY_HOST` | `127.0.0.1` | HTTP gateway bind address |
| `FLOCI_ON_DEMAND_GATEWAY_PORT` | `8080` | HTTP gateway port |
| `FLOCI_ON_DEMAND_RECONCILE_INTERVAL_MILLIS` | `1000` | Queue and idle polling interval |
| `FLOCI_ON_DEMAND_MAX_CONCURRENT_STARTS` | `4` | Concurrent runtime activation limit |
| `FLOCI_ON_DEMAND_MAX_PENDING_REQUESTS` | `1024` | Total waiting HTTP request limit |
| `FLOCI_ON_DEMAND_MAX_PENDING_REQUESTS_PER_WORKLOAD` | `64` | Waiting HTTP requests per workload |
| `FLOCI_ON_DEMAND_MICROSANDBOX_URL` | unset | Native helper endpoint |
| `FLOCI_ON_DEMAND_MICROSANDBOX_TOKEN` | unset | Native helper bearer token |
| `FLOCI_ON_DEMAND_MICROSANDBOX_BACKEND_HOST` | `127.0.0.1` | Helper host address reachable from Floci |
| `FLOCI_ON_DEMAND_MICROSANDBOX_BIND_HOST` | `127.0.0.1` | Guest port bindings on the helper host |
| `FLOCI_ON_DEMAND_POD_CONTROLLER_URL` | unset | Native Kubernetes controller lifecycle endpoint |
| `FLOCI_ON_DEMAND_POD_CONTROLLER_TOKEN` | unset | Controller bearer token, at least 32 characters |

When Floci runs in Docker, use a host address reachable from that container and
bind guest ports accordingly. The helper's `127.0.0.1` is the hypervisor host,
not the Floci container. Gateway exposure follows Floci's existing
`FLOCI_SECURITY_ALLOW_UNSAFE_NETWORK_EXPOSURE` setting.

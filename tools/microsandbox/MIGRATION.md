# Experimental native microVM runtimes

This branch starts from main. The reusable HTTP and queue wake coordinator is
ported from `feat/on-demand-runtime`, without importing that branch's unrelated
service and network changes.
Docker remains the default. No AWS request or response shapes change.

## Managed controller and certificate renewal

`tools/microsandbox-pods/manage.py run` now supervises its controller child and
renews the 30-day client and kubelet certificates within seven days of expiry.
The optional `start`, `stop` and `renew` commands use the same supervisor through
a private authenticated Unix socket. `stop` retains all workloads and journals.
No host-specific service manager or automatic login startup is installed.

New setup credentials live in complete private `credentials/<generation>`
directories, selected atomically by `cluster.json`. The generated administrator
kubeconfig stays at its existing path. Old flat controller credentials remain
readable until their next renewal or `prepare`; custom scripts should use
`manage.py run/start` instead of assuming a flat controller certificate path.
Preparation failures preserve the existing process and credential selection.
An interrupted controller can retry existing journal recovery up to three times
in five minutes. Pending or ambiguous native mutations still fail closed.
A supervisor crash does not authorize adoption or signalling of an orphaned
controller PID. Logs and `status` identify what needs inspection before restart.

## Reconciliation and capacity diagnostics

The native pod controller now defaults to eight Kubernetes reconciliation workers
instead of two. `--pod-workers` accepts 1 through 64; the setup helper's `run`
command exposes the same option. This bounds concurrent cold-Pod cleanup without
raising the separate VM-start or memory admission limits. Wait for the private
Pod journals to disappear before removing a cluster; Kubernetes object deletion
can finish first.

`manage.py status` also reports shared native capacity and outstanding
reservations, including disk accounting when enabled. Unknown inventory remains
explicitly unavailable. No workload or AWS protocol changes are required.

HTTP wake now survives the requesting client's disconnect after native admission
has succeeded. Capacity and preflight waits still cancel with the request; an
accepted VM transition has a separate four-minute deadline and remains bound to
Pod deletion and controller shutdown. Rebuild the Pod controller to pick up this
fix. No annotations or application changes are required. Existing unknown native
operation fences still require reconciliation before another lifecycle mutation.

## Native local persistent volumes

Existing storage classes keep their meaning. To place persistent data in native
Pod disks, add the explicit `microsandbox.dev/local` StorageClass from the
[storage guide](../microsandbox-pods/STORAGE.md) and select it in the application's
PVCs or Helm values. It requires `WaitForFirstConsumer`, Filesystem and RWO.
It is local storage rather than an EBS or CSI implementation.

One Pod can attach a disk at a time, including while sleeping. For a
single-replica Deployment sharing one PVC, use `strategy.type: Recreate` to avoid
a rolling surge waiting for the old Pod's attachment. Normal replacement Pods
retain data in the same PVC. Disk expansion is unsupported; select its size at
creation. Use a fresh PVC and application-level data migration when changing
storage backends. Existing Docker volumes are not imported automatically.

Rebuild the controller and Linux `guestfs` helper together; local PVCs require
the helper even without `fsGroup`, for verified filesystem writeback. With the
controller stopped, rerun `manage.py prepare` to update its role with
PVC/StorageClass reads and PV management permissions. Keep the generated cluster
and runtime directories. The controller adds no per-Pod storage service. PVC Pods
use journal format 7 to prevent destructive cleanup by older controllers; other
Pods retain format 6.
Do not downgrade while PVC Pods or their checkpoints remain.

## Checkout consolidation

Use `feat/microsandbox` in the main Floci checkout. The host helper now builds
with Go from `tools/microsandbox-host`. The pod controller
and Linux relay build from `tools/microsandbox-pods` in the same repository.
Update scripts that refer to the former `floci-sidecars` checkout. Runtime
configuration, tokens, state directories and checkpoint formats are unchanged
by this source relocation. The original on-demand branch remains separate.

## Go SDK migration

The host helper and pod controller now use the official microsandbox Go SDK
`v0.7.8` through `tools/microsandbox-runtime`. Rebuild both binaries together.
The host helper no longer needs Java or its former Maven/JAR launch command:

```sh
(cd tools/microsandbox-host && go build -o /tmp/floci-microsandbox-host .)
(cd tools/microsandbox-pods && go build -o /tmp/floci-microsandbox-pods .)
```

Use Go 1.26+ with CGO enabled and a C compiler. Keep native microsandbox 0.7.8
and its matching runtime libraries installed. Linux needs glibc 2.28+;
build its controller and helper with a Linux C toolchain. `MICROSANDBOX_HOME`,
`MICROSANDBOX_TOKEN`, `MICROSANDBOX_EXECUTABLE`, `PORT`, `QUARKUS_HTTP_PORT` and
`QUARKUS_HTTP_HOST` retain their meanings. Run the host executable instead of
`java -jar`; Floci's helper URL, token, HTTP contract and runtime journals do not
change. Existing guest disks and RAM checkpoints stay in the same runtime home.

Native lifecycle, snapshots and guest execution use the SDK. `run` retains the
CLI's image launch mode: replacing it with an SDK managed job prevents full RAM
capture. `inspect`, `metrics`, `doctor` and `--version` also retain the pinned CLI
to preserve ownership fields and diagnostic reports absent from the Go binding.
These are explicit fallbacks; unsupported commands still fail. The Linux
network relay remains a separate Go binary that can be built with CGO disabled.

Database cold starts now launch their daemon outside a managed exec job, so
Aurora can hibernate again after an ordinary stop/start. The existing tmpfs lock
still prevents duplicate starts. Cold-start logs are written privately inside
the guest at `/tmp/floci-pg-start.log` or `/tmp/floci-mysql-start.log`.
Shutdown can stop a guest before its exec stream returns an exit status. Floci
now resolves that specific shutdown operation only after a completed helper
error and confirmation that the same native VM ID is stopped. Timeouts, lost
responses and unrelated pending commands remain fenced. This identity check
requires the rebuilt Go helper; the former Java helper cannot supply it.

## Shared runtime checks

The same RDS workload scenarios run with
`FLOCI_TEST_RUNTIME=docker` or `FLOCI_TEST_RUNTIME=microsandbox`. Existing AWS
test classes retain their scenarios; runtime setup, guest access and cleanup live
in shared fixtures. See the [matrix and remaining exclusions](../runtime-tests/README.md).
The converted suites use the `runtime` test tag. Update scripts that selected
them with `-Dgroups=docker` to use the matrix runner or `-Dgroups=runtime`.
Ordinary Maven test runs still select Docker by default.
The shared database checks also exposed MySQL remote root password rotation,
which is fixed in this branch. EC2 work and its new regression fixtures are parked
separately; the baseline EC2 implementation is unchanged.

## Main rebase: standalone native data directory

Current main changes the default data directory for a standalone native Floci
executable outside a container. On macOS it is
`~/Library/Application Support/floci-aws`; on Linux it is
`$XDG_DATA_HOME/floci-aws`, or `~/.local/share/floci-aws` when unset. Windows uses
`%LOCALAPPDATA%\floci-aws`, or `%USERPROFILE%\AppData\Local\floci-aws` when unset.
This applies independently of choosing microsandbox. JVM, dev and container
defaults are unchanged.

To retain an existing persistent, hybrid or WAL installation, explicitly keep
`FLOCI_STORAGE_PERSISTENT_PATH` and `FLOCI_STORAGE_HOST_PERSISTENT_PATH` at their
previous effective values before restarting. Floci's stored resource metadata,
runtime journals and TLS files follow these settings. The helper's `MSB_HOME`
and the pod controller's `--state` directory do not move automatically.

## Enable the provider

1. Build `tools/microsandbox-host` in this checkout with
   `go build` from that directory. Follow its
   [host helper instructions](../microsandbox-host/API.md).
2. Install native microsandbox 0.7.8 with matching runtime libraries. Run its
   authenticated helper on the hypervisor host, with a dedicated, short,
   durable `MSB_HOME`. On macOS this is a host process, outside Docker's Linux VM.
3. Start Floci with fresh persistent metadata storage and these settings:

```sh
export FLOCI_SERVICES_RDS_RUNTIME=microsandbox
export FLOCI_SERVICES_RDS_MICROSANDBOX_URL=http://127.0.0.1:8186
export FLOCI_SERVICES_RDS_MICROSANDBOX_TOKEN=your-local-runtime-token
export FLOCI_STORAGE_MODE=persistent
```

Use the same token in the helper. Persist both Floci's storage directory and the
helper's runtime home. See [RDS configuration](../../docs/services/rds.md#experimental-microsandbox-runtime)
for container-to-host networking and optional RAM settings. There is no published
helper release or automatic helper-container startup in this experiment.

## Scheduling hundreds of cold pods

Rebuild `tools/microsandbox-pods` and set `--scheduling-cpu` and
`--scheduling-memory` when your lazy deployments reserve more resources than
their active working set. For example, use `--scheduling-cpu 500
--scheduling-memory 384Gi --max-active-memory-mib 4096` for 500 one-CPU/768-MiB
pods with a separate 4 GiB active budget. Kubernetes requests remain unchanged
while pods sleep. Existing scheduling defaults are unchanged, and no manual Pod
journal migration is needed. The [resource update](#pod-resource-sizes-and-limits)
automatically upgrades journals and limits controller downgrades.
An exhausted active budget delays wake requests, so size it for the
applications and dependencies that need to run together.

## Changes your project may need

- Use PostgreSQL, Aurora PostgreSQL, MySQL, or Aurora MySQL with official
  PostgreSQL/MySQL image conventions. Other database engines are rejected.
- Migrate existing data with logical SQL export/import into a fresh installation.
  Docker volumes and microsandbox disks are different formats. Existing resources
  without a microsandbox journal are rejected. Do not switch providers in place
  in either direction; retain the original installation until migration is verified.
- Aurora Serverless v2 clusters with `MinCapacity=0` use memory-and-disk
  hibernation after the existing AWS-compatible idle interval. Active connections
  prevent auto-pause. The next connection waits for restore. Ordinary instances
  do not gain automatic scale-to-zero.
- `StopDBInstance` and `StopDBCluster` retain data but start fresh PostgreSQL
  or MySQL processes when started again. PostgreSQL AWS database snapshots remain
  logical backups, separate from internal RAM checkpoints. MySQL AWS database
  snapshots are unsupported, as with Floci's Docker provider. Use SQL export/import
  for MySQL backups; internal hibernation still preserves its RAM and disk state.
- Allow longer SDK read timeouts for initial image pulls. The measured tiny
  PostgreSQL 512 MiB guest takes about 1.2 seconds to hibernate and 0.6 seconds to
  resume
  through Floci, with the configured artificial resume delay set to zero.
  These are Apple Silicon observations, not guarantees for larger databases.
- The provider defaults to one vCPU, a 4 GiB root disk and 512 MiB RAM per
  runtime. Set `FLOCI_SERVICES_RDS_MICROSANDBOX_ROOT_DISK_MIB` before creating a
  database to choose its native disk size. Existing guests retain their recorded
  geometry. The AWS allocated-storage field does not resize that experimental
  disk. SQL backup transport is limited to 64 MiB and commands to
  180 seconds. Guest CloudWatch log forwarding is not implemented.

## Standard pod image startup and security

Rebuild the pod controller to use an image's own ENTRYPOINT and CMD without an
explicit `command`. Pod `command`/`args`, working directory and environment follow
Kubernetes override rules. Command/argument `$(NAME)` references now expand from
the resolved Pod environment. Escape them as `$$(NAME)` when literal text is
intended; unresolved references stay literal. Image command defaults are not
expanded by the controller.

Common security contexts are now accepted: UID/GID, non-root requirements,
read-only root filesystem, privilege escalation, capability add/drop and seccomp
`RuntimeDefault`/`Unconfined`. Omitting the seccomp profile leaves it unconfined.
Omitting
`allowPrivilegeEscalation` now follows Kubernetes's default of allowing it. The
earlier direct ctr launch implicitly prevented escalation. Set
`allowPrivilegeEscalation: false` explicitly if you relied on that restriction.
No existing running or sleeping process is changed in place; replace the Pod to
apply updated startup semantics. Existing Pod journals remain readable.

## Pod resource sizes and limits

New pods can declare CPU/memory requests and limits above the original fixed
profile. Guest sizing uses the larger declaration, reserves 256 MiB for the
guest runtime and aligns memory to 2 MiB, with a one-vCPU/768-MiB minimum.
For example, a 768 MiB request now gets a 1024 MiB VM; adjust
`--max-active-memory-mib` for the number of these guests that must run together.
CPU/memory limits are enforced by guest cgroups and actual memory-limit kills
report `OOMKilled`. Applications see their own limits through a read-only cgroup
mount. Requests remain scheduler reservations, not hard limits.

Existing pods keep their original one-vCPU/768-MiB allocation. Recreate a Pod to
change its size. Journal version 2 introduced persisted allocations; the current
controller reads legacy records and writes format 6, or 7 for PVC Pods. Older controllers reject
the upgraded format. Do not
downgrade against upgraded state; keep a backup if you need to revert the runtime.
The private lifecycle API adds `vcpus` and `memoryMiB`; its contract remains 1 and
the existing Floci client tolerates these additive fields.

## Kubernetes API throughput

For larger clusters, the shared Kubernetes client now defaults to 50 requests
per second with a burst of 100. Use `--kube-api-qps` and `--kube-api-burst` to tune
this without changing VM admission. The previous client defaults could delay
readiness replay for hundreds of cold pods after recovery.

## Recovery and scope

Pod-controller recovery now reports interrupted initial starts and container
restarts as failed when application startup cannot be proved. Replace those Pods
to recover. Existing failure diagnostics and completed exit details, including
`OOMKilled`, survive a controller restart. A disappearing task cannot remain
Ready indefinitely: absent exit status gets a 30-second handoff window, then the
Pod fails explicitly. Hibernation is blocked while that handoff is unresolved.

The provider journals transitions through `StorageFactory`. When helper command
completion is confirmed, it can adopt an already-restored generation after its
caller timed out. It refuses to replay an old
checkpoint when its source and target have disappeared. Missing durable data
causes an error rather than an empty replacement database. An ambiguous initial
creation without its guest also fails closed; inspect and explicitly delete the
failed resource before attempting a replacement.

Keep one Floci controller and one helper per runtime home. The journal uses the
existing storage backend's durability guarantees; this experiment does not claim
power-loss durability or cross-host/cross-architecture snapshot portability.
Back up data independently before relying on experimental recovery.

The shared helper transport now records opaque pending mutation IDs through
`StorageFactory` in `microsandbox-commands.json`. A caller timing out does not
cancel the accepted native command. Confirmed completion removes its record;
uncertain completion survives restart and prevents further mutations or reset.
For manual recovery, stop Floci and the native helper, confirm no helper-launched
command remains, reconcile owned guests and checkpoints, then remove only the
confirmed stale entries from the configured storage backend. Keep the runtime
journals, guests and snapshots together. Deleting command records while a native
operation is running can permit cleanup to race with late VM creation.

The Kubernetes controller's separate `native-command.pending` file now contains
a private intent and, after confirmed native completion, a durable terminal
receipt. Startup automatically clears only terminal receipts whose controller
owner, Pod UID/generation and immutable VM/checkpoint identities still match
read-only runtime inspection. This closes the crash window between persisting a
verified terminal result and removing its fence. It does not recover commands
whose completion was never recorded, replay mutations, or infer completion from
a matching name or an old timestamp. Existing plaintext fences and uncertain
intents still require manual reconciliation; interrupted initial Pod startup can
still become failed under the existing lifecycle recovery rules. No command
arguments, environment values or guest configuration are persisted in the receipt.

The database provider covers RDS PostgreSQL and MySQL. An additional local
application provider uses the same host helper for HTTP and queue-driven
hibernation. See [native workloads](../../docs/configuration/microsandbox-workloads.md)
for its configuration and recovery limits. The RDS runtime setting does not
enable that separate application coordinator.
EC2 retains its Docker provider. See [compute runtime design](COMPUTE.md) for
Kubernetes and other service boundaries. Those paths are not enabled by the RDS setting.

## Moving application wake rules from the on-demand branch

- `host`, `queues`, `rabbitmq-queues`, readiness and sleeping health rules keep
  their existing configuration shape. The local gateway defaults to port 8080.
- A standalone OCI application uses `runtime.type: microsandbox`, its image,
  optional command and environment, and a guest `backend-port`. This definition
  does not create an AWS resource or a Kubernetes pod.
- EC2 wake rules are parked in `parked/microsandbox-ec2-20261017`. Remove
  `runtime.type: ec2` entries before using this branch; only the existing baseline
  EC2 lifecycle is available here.
- Kubernetes wake rules target an opted-in Deployment through the separate native
  pod controller. Its replica count and Pod UIDs remain unchanged while sleeping.
  The old `runtime.replicas` setting no longer controls scale-to-zero for this
  backend. See [Kubernetes migration and feature gaps](../../docs/configuration/microsandbox-kubernetes.md).
  Strict-mode Service traffic needs an available wake gateway; it cannot wake
  a VM by itself. Existing unsupported pod features must remain on ordinary workers.
- Kubernetes rules currently share one configured native controller. Account,
  region and optional cluster hints cannot create independent rules for the same
  namespace/Deployment. Use distinct namespaces for separate applications; a
  cluster hint validates the controller's identity rather than selecting a cluster.
- Set `microsandbox.dev/availability: proxy` for lazy deployment: the shared wake
  listener makes the pod available to Services and rollout readiness checks
  before its first VM boot. This local mode defines Ready as proxy availability;
  an application startup failure is discovered on first use. Declared TCP ports
  are required. Applications see the proxy's source address for those connections.
  Omit the annotation, or select `strict`, for ordinary application readiness.
- Proxy mode needs the controller's Linux relay binary supplied through
  `--network-relay-binary`, matching the Docker peer's architecture. Reserve a
  separate `--proxy-backend-cidr` for guests, default `10.42.204.0/22`, as well as
  the public native Pod CIDR. Both must be unused ranges inside the cluster CIDR.
- With a Floci Kubernetes wake rule, proxy pods must set
  `microsandbox.dev/idle-owner: external`. Standalone proxy pods default to the
  controller's activity-based idle timer, which does not inspect queues.
  Keeping these owners separate prevents HTTP idleness from suspending queue work.
- Floci's gateway request accounting is unchanged. The native pod proxy now
  defaults to HTTP request activity, allowing idle frontend keepalive connections
  to remain open while the VM sleeps. Explicit `proxy-protocol: tcp` retains the
  previous connection-based behavior. See the HTTP proxy migration below before
  upgrading existing raw TCP/TLS workloads.
- Existing pods retain their resolved environment across hibernation and controller
  restarts. Roll replacement pods to consume changed ConfigMap, Secret or Service
  environment values. Consider `enableServiceLinks: false` when applications use
  DNS discovery, to avoid injecting hundreds of Service variables into every pod.
- The advanced `microsandbox.dev/on-demand: "true"` annotation can defer first
  boot in strict mode too. Such cold pods remain Pending and not Ready, requiring
  an initial wake before rollout readiness waits or Helm's `--wait` can complete.
- ECS workload definitions from the older on-demand branch are not implemented
  in this runtime branch. Use the existing ECS provider or an explicit standalone
  microsandbox workload until an ECS runtime adapter is added. Lambda retains
  its existing execution providers and warm-pool behaviour.
- SQS enqueue notifications trigger immediate wake, with visible and in-flight
  count polling as a fallback. Wake detection does not consume messages or
  require application changes to `SendMessage`. RabbitMQ uses management counts
  while the application keeps ownership of AMQP consumption and acknowledgements.
- Route IoT MQTT messages through an ordinary IoT rule's SQS action. Direct MQTT
  subscriptions inside a sleeping guest are not a durable wake mechanism.
- Applications must reconnect outbound network clients after restore. RAM resumes,
  but backend TCP connections are not migrated with the checkpoint. Idle frontend
  HTTP connections owned by the shared pod proxy can survive VM hibernation.
- Existing Docker application filesystems are not converted into VM disks.
  Migrate durable data before switching runtime providers.
- The shared `FLOCI_MICROSANDBOX_MAX_ACTIVE_MEMORY_MIB` setting defaults to
  8192 MiB of admitted guest RAM. It counts configured guest allocations, not
  total host RSS. Leave additional RAM for Floci, the Kubernetes control plane,
  Docker, brokers and native runtime overhead. Zero disables admission limiting.
  The Java RDS and generic standalone application providers share this budget.
  The native pod controller also has its own allocation limit. Both now use the
  shared SDK adapter ledger when configured with the same canonical runtime home;
  set `FLOCI_MICROSANDBOX_MEMORY_LIMIT_MIB` and
  `FLOCI_MICROSANDBOX_START_LIMIT` for the combined native budget. The Java and
  controller limits remain additional bounds.
- A deployment with hundreds of applications is the design target, not a measured
  capacity claim. Most applications must be idle to fit within the host memory budget. Snapshot
  storage and the working set of simultaneously active applications still matter.

## Kubernetes image pull policy

Container starts now honor `imagePullPolicy`. `IfNotPresent` reuses complete
images cached in the guest, including after RAM resume or an application crash;
`Always` still contacts the registry. `Never` requires an existing complete cache,
which fresh Pod VMs do not have. This changes the previous unconditional image
pull on every restart and allows cached applications to restart without public
registry access. Registry credentials are now supported as described below. Image-default commands
are now supported as described above.

## Kubernetes mounted configuration and temporary storage

Native pods now accept ConfigMap, Secret and `emptyDir` volumes. Existing
directory mounts can retain their Kubernetes manifests, including ConfigMap
`binaryData`, Secret binary values, `items`, optional references and file modes.
ConfigMap and Secret mounts are read-only even when the mount omits `readOnly`.

Rebuild Floci and the pod controller together for this update. The controller
now reports `WAITINGVOLUMES` for strict pods awaiting required mounted sources
before VM allocation. Older Floci clients reject that state. It is an additive
state in controller contract version 1; no manual configuration or journal
migration is required. The current controller reads legacy resource journals
and writes format 6, or 7 for PVC Pods; older controllers cannot read upgraded state.

Mounted source values are fetched when a cold pod first starts, refreshed
periodically while awake, and refreshed after RAM restore before wake-proxy
traffic is released. Missing required sources prevent first VM allocation;
strict pods wait Pending and retry, while a cold proxy pod retries on a later
wake request. Sleeping pods do not need to wake for a source update.
Projection updates replace each volume's contents atomically. Applications must
reopen files to observe updates; an open descriptor can keep old content. This
does not change environment-variable behavior: roll replacement pods to refresh
environment references. The controller kubeconfig needs `get` access to the
ConfigMaps and Secrets referenced by the pod.

Configuration source/API errors during refresh preserve the last good mounted
contents and are retried. Pods without renewable identity can resume background
processes before the post-restore refresh and briefly see previous configuration.
Identity-bearing Pods now freeze before capture and refresh before thaw, as
described in the identity migration below.

Disk-backed and `medium: Memory` `emptyDir` volumes retain files across container
restarts and RAM sleep. They are scoped to the Pod UID and are removed when that
pod is deleted. Disk-backed `emptyDir` shares the configurable VM root disk
(default 4 GiB) and does not yet
support `sizeLimit`. Memory-backed `emptyDir` uses guest tmpfs, with an optional
positive size limit no greater than the guest RAM allocation. Its contents
still consume RAM and checkpoint storage, so hibernation does not make large
temporary datasets free to store.

`subPath` and `subPathExpr` mounts can now retain their Kubernetes declarations.
File and directory mounts are supported for the existing volume types, with
container environment expansion for `subPathExpr`. ConfigMap/Secret subPath
mounts are pinned and do not receive live source updates. Regular directory
mounts continue to refresh. Combined projections and service-account tokens
are supported, as are opt-in [local PVCs](../microsandbox-pods/STORAGE.md).
CSI, host paths, mount propagation and recursive read-only options remain
unsupported and are rejected. Alpha `emptyDir.mode`, volume `defaultUser` and
per-item ownership overrides are also unsupported. Secret files live on guest tmpfs, but their bytes
and secrets held in process memory are included in RAM checkpoints. Keep the
runtime home and checkpoint backups private; the controller does not provide
checkpoint encryption.

## Init containers and private images

Rebuild Floci and the pod controller together. Sequential init containers are
now supported alongside one regular application container. Their lifecycle adds
`INITSTARTING` and `INITIALIZING` states to the private controller contract;
older Floci clients reject those states. Journal version 3 introduced init
completion and retry status. The current controller reads those records and
writes format 6, or 7 for PVC Pods; older controllers cannot read upgraded journals. Do not downgrade
against upgraded state.

Init containers run on first activation. A cold proxy Pod can be Ready with
`Initialized=False`; this does not execute initialization during deployment.
`kubectl get pods` can therefore show `Init:0/N` even while the Pod's `Ready`
condition is true and its Deployment counts it as available.
The first request waits for successful init completion and actual app readiness.
Regular application restarts and RAM resumes do not repeat completed init
containers. A failed init retries for Pod `Always`/`OnFailure` policies and fails
the Pod for `Never`. Restartable init sidecars remain rejected. Multiple regular
containers are supported as described below.

The guest allocation now accommodates the largest sequential init container as
well as the application. A large migration init container can therefore increase
the Pod's admitted RAM even after initialization finishes. Scheduling still uses
the manifest's real resource requests.

`imagePullSecrets` can reference Docker config Secrets in the Pod's namespace.
The controller needs `get` access to those Secrets. A required pull reads the
current credentials, while cached `IfNotPresent`/`Never` starts do not require
secret lookup. Missing or malformed referenced Secrets fail with a sanitized
error. Credential helpers and token-only entries remain unsupported. Use
username/password entries, including ordinary registry access tokens used as
passwords.

Credentials are sent to the guest through stdin without controller journal or
host command-line copies. The guest's `ctr` process briefly carries credentials
in its arguments during the pull. Application containers use separate PID
namespaces. TLS verification remains enabled; this change does not add insecure
registry defaults.

## Immediate SQS wake notification

Successful SQS enqueues now schedule matching on-demand workloads immediately.
The existing reconciliation interval remains a fallback for persisted, delayed
and in-flight messages. Queue matching includes account, region and name, and
FIFO deduplication does not publish a second notification. A notification does
not receive or delete the message, and a wake failure cannot fail SendMessage.
No configuration or AWS client changes are required.

## Kubernetes health-probe support

Existing `readinessProbe`, `startupProbe` and `livenessProbe` declarations can now
use exec, HTTP/HTTPS or TCP actions. Supply the rebuilt Linux network relay even
for strict pods that use network probes. Rebuild the controller and relay as a
pair; stop an older owned relay before restarting with the new controller.
Probe checks inspect actual guest listeners and do not wake sleeping pods or
extend idle time. An external health request through the wake proxy still does.

Startup success survives hibernation and recovery, but resets when the container
restarts. Unhealthy applications now terminate and restart according to the Pod
restart policy, including the configured termination grace period. Probe host
overrides, lifecycle hooks and per-container restart overrides remain
unsupported. gRPC support is described in the later operations update. See [health checks](../../docs/configuration/microsandbox-kubernetes.md#health-checks)
for the supported semantics and preview limitations.

## Run the integration checks

Start the helper with a dedicated disposable runtime home, then run:

```sh
FLOCI_TEST_MICROSANDBOX_URL=http://127.0.0.1:8186 \
FLOCI_TEST_MICROSANDBOX_TOKEN=your-local-runtime-token \
FLOCI_TEST_RUNTIME=microsandbox \
./mvnw test -Dtest=RdsPostgresRuntimeIntegrationTest,RdsMysqlRuntimeIntegrationTest,MicrosandboxRdsRuntimeTest,MicrosandboxSidecarClientTest
```

The real-runtime test uses AWS SDK management requests and SQL through Floci's
proxy. It checks repeated hibernation, preserved process memory and writes,
ordinary stop/start, password rotation, and PostgreSQL logical snapshot restore.
The [shared runtime matrix](../runtime-tests/README.md) runs the same supported
behavior against Docker and microsandbox, including pause after cold restart.
The MySQL suite checks both non-root and root masters, global master privileges,
and RAM-only MEMORY tables alongside durable InnoDB tables. Its default image is
`mysql:8.0.45`; set `FLOCI_TEST_MYSQL_IMAGE` to test another official MySQL image.
The suite passed with `mysql:8.0.45` and `mysql:8.4.8` on Apple Silicon with
512 MiB guest RAM, including repeated password changes for both master types.

## Updating from the PostgreSQL-only experiment

No configuration change is required. Existing PostgreSQL journals default to
the PostgreSQL engine when their older records have no engine field. New records
retain the engine and startup options, so changing image configuration cannot
silently change an existing MySQL disk's authentication settings on restart.
Master passwords remain owned by the existing RDS service metadata; the runtime
journal does not duplicate them. Rotation is tested through the public proxy,
including both accounts created by an official image for a root master.

## ECS health and task protection

ECS now applies task-definition health checks to Docker containers. Task health
uses only essential containers with declared checks: an unhealthy check wins,
then an unknown check, then all healthy checks. Image-only checks and optional
containers do not determine task health. Applications that previously depended
on an image's `HEALTHCHECK` must declare the check in their ECS task definition.

ECS task scale-in protection now defaults to 120 minutes, validates the AWS
1 through 2880 minute range, and expires in `GetTaskProtection`. Service scale-in
and deployment replacement preserve protected tasks until protection expires or
is cleared. Explicit `StopTask` and service deletion still stop those tasks.
An on-demand workload with a protected task therefore cannot become idle while
that task remains protected.

The behavior follows AWS's [container health rules](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/healthcheck.html)
and [task protection contract](https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_UpdateTaskProtection.html).

## Restored AWS compatibility from the on-demand branch

Docker and microsandbox share RDS cluster metadata and Aurora MySQL discovery handling.
The discovery shim recognizes only the complete AWS JDBC driver discovery statements;
other SQL is passed through unchanged. It reports the one backing writer, not independent
Aurora replicas. CloudFront viewer-host routing across accounts, ElastiCache
resource tags, user-group pagination and local Redis TLS are also restored.
`CreateDefaultSubnet` is parked with the other EC2 changes.
ElastiCache execution remains Docker-backed.

Replace calls to `/_aws/rds/tls/hosts` with the startup environment variable
`FLOCI_SERVICES_RDS_TLS_HOSTNAMES` (comma-separated DNS names). Replace calls to
`/_aws/rds/tls/ca` with a copy of `<persistent-path>/tls/rds-ca.crt` after the first RDS
proxy starts. Preserve that persistent directory across restarts. See
[local database TLS](../../docs/services/rds.md#local-database-tls-names) for details.

## Multiple regular Kubernetes containers

A native Pod can now contain multiple regular applications in one microVM.
Applications share localhost and declared Pod volumes, with separate restart
counts, probes, logs and exit/OOM status. A restart affects only the failed
container. Strict Pod readiness waits for every application; proxy availability
continues to advertise working wake routes while actual app readiness gates
forwarded traffic.

New VM allocations use the sum of regular-container CPU/memory declarations,
compared with the largest sequential init stage, plus the existing guest
headroom. An existing single-container Pod keeps its journaled allocation.
Journal version 4 introduced multi-container state; current writes use format 6, or 7 for PVC Pods.
Do not downgrade the controller until those Pods and their snapshots have been deleted. No manifest changes are
needed for existing single-container Pods. Ephemeral containers are still
rejected explicitly.

## Kubernetes restartable init sidecars

Init containers can now set `restartPolicy: Always` to stay running alongside
the applications. Startup probes gate subsequent init stages, readiness probes
gate actual Pod readiness, and sidecars restart independently even with Pod
`restartPolicy: Never`. Regular application completion shuts sidecars down in
reverse order so Jobs can finish. Deletion signals applications before sidecars.
The single Pod termination grace deadline persists across controller recovery.

Journal version 5 introduced sidecar state; current writes use format 6, or 7 for PVC Pods.
Sidecars are included in guest resource allocation. Older single-container,
sequential-init and multi-application journals remain readable. Delete sidecar Pods and their snapshots before
downgrading the controller. Existing manifests need no changes.

## Environment isolation and simulator routing

The optional `FLOCI_NETWORK_CONFIG_FILE` restores environment-wide Docker egress
isolation and HTTP/HTTPS simulator origin routing from the on-demand branch.
Native helpers and pod controllers use the same file, with explicit
`network.native.allowed-egress`, controlled `dns-nameservers` and an optional
public `ca-file`. Isolation stays disabled by default. See
[workload networking](../../docs/configuration/workload-network.md) for setup.

The public CA mount has moved from `/var/run/floci/certs/ca.crt` in the old branch
to `/etc/floci/certs/ca.crt`, which survives guests mounting a fresh `/run` tmpfs.
Applications should use `FLOCI_CA_BUNDLE` rather than hard-coding either path.
Native guests and checkpoints carry the exact network-file identity. Changing
the file requires deliberate guest recreation; an older active guest prevents
the helper from claiming that the new boundary is enforced. Native restore
keeps the environment boundary even if a workload asks to allow all egress.

## Kubernetes/Aurora scope and parked EC2 work

Native execution work targets Kubernetes pods and Aurora. Existing RDS
PostgreSQL/MySQL support and the generic shared microVM adapter remain available.
New Docker EC2 fixes, private metadata transport, on-demand EC2 wake rules,
IAM/metadata changes, `CreateDefaultSubnet` and their new fixtures are preserved
on `parked/microsandbox-ec2-20261017` at `fb9eab28a`. That parked tree has already
removed the native EC2 provider. The original native EC2 implementation is preserved
on `backup/microsandbox-before-ec2-removal-20261017` at `e0e81a91c`.
This branch retains the EC2 implementation, documentation and tests from baseline
`0a5809a55`; none of those new EC2 changes are enabled here.
ECS, ElastiCache and Lambda retain their existing execution backends.

Remove `floci.services.ec2.runtime` and every `floci.services.ec2.microsandbox-*`
setting from application configuration. Remove `FLOCI_SERVICES_EC2_RUNTIME` and
all `FLOCI_SERVICES_EC2_MICROSANDBOX_*` environment variables. Remove
`runtime.type: ec2` entries from on-demand workload files. The baseline EC2
configuration continues to work without these new settings.

`FLOCI_MICROSANDBOX_GUEST_AGENT_PATH` is retained for one-shot DNS routing on native
Kubernetes/database helpers and controllers. It no longer enables a Docker EC2
metadata transport in this branch. Kubernetes and database helper URLs, tokens,
runtime homes and wake rules are unchanged.

Before switching an installation that used native EC2, use
`backup/microsandbox-before-ec2-removal-20261017` to export needed instance data
and terminate only its owned EC2 VMs and checkpoints. The Docker-only parked
branch cannot perform that native cleanup. Native disks and
`ec2-microsandbox-machines.json` are not Docker state and cannot be adopted by
changing configuration. Keep ownership
journals until those resources are cleaned up. Recreate required instances using
the baseline EC2 provider. Other native workloads can keep their runtime home
and journals. The retained shared runtime test matrix covers databases;
Kubernetes qualification remains separate.

## Kubernetes terminal, forwarding and gRPC probes

Rebuild the controller and Linux network-relay binary together. The relay probe
contract is now 2 because it accepts gRPC health requests. Shut down an existing
owned relay through its private authenticated control socket before restarting
with the new binary; the controller rejects an incompatible running relay.

Standard Kubernetes exec now supports TTY and live resize. Port forwarding
connects to the Pod's own loopback network, wakes a cold or sleeping Pod on its
first connection, and prevents hibernation while connected. It requires
`--network-relay-binary`; the same binary is copied privately into the VM through
SDK stdin. gRPC readiness/startup/liveness probes accept numeric ports and the
optional service field. No manifest changes are required for existing probes.

## Native Pod VXLAN address reuse

The host controller now honors the explicit `--node-ip` for peer routing instead
of resolving a fixed Docker hostname. A private `network-host` record pins that
address while Pod journals remain, including direct controller CLI restarts.
On first upgrade, existing guest routes must prove the same host address through
their owned peer DNAT entries. Cold Pods with no guest network or checkpoints
need no route migration. Missing or conflicting route evidence blocks startup;
do not delete ownership records to force adoption. A different host address is
accepted only after the controller has no Pod journals. A peer challenge prefers loopback HTTP listeners;
when loopback is not reachable, only the same verified address on a local host
interface may receive frontend traffic. `proxyUrl` reflects that address.
Interface-bound UDP ingress is owned by the controller, checks the exact peer
source IP, Pod source port and VXLAN VNI, and forwards only to the existing
loopback SDK publication. No guest daemon or wildcard host bind is added. Native
Linux VM qualification remains outstanding; the socket and source-fencing paths
have a separate Linux Docker namespace regression.

Native Pod peer VXLAN links now use `20000 + slot` as their fixed source port,
derived from each Pod's persisted unique network slot. Peer DNAT and conntrack entries include that source port,
so two Pods keep distinct routes when the runtime reuses a sleeping VM's guest IP.
The guest and peer receive port remains 8472. Controller recovery validates each
owned peer link against its Pod journal before replacing a legacy link with the
same VNI, addresses, bridge and MTU. It removes only that Pod's exact legacy DNAT
rule and UDP conntrack tuple. The owned Linux peer must provide `conntrack` (the
pinned k3s image includes it). A changed or unowned peer link blocks migration.
New and recovered Pod journals use format 6, or 7 for PVC Pods, before changing
peer network state.
Older controllers reject that version, so do not downgrade a controller while
these journals and their native resources remain. Guest application data and RAM
checkpoints do not change during the peer-link migration.

## Attached command cancellation

Canceling a native attached command now kills that command and waits briefly for its
observed exit status. It does not stop the VM. If the runtime cannot confirm an exit,
the operation still reports an unknown outcome for journal reconciliation. The pinned
runtime reports `-1` for a killed command on Apple Silicon; callers must accept that
observed status rather than assuming every exit fits an unsigned byte.


## Isolated native DNS and RAM restore

Rebuild the native host helper, pod controller and static Linux guestnet binary
together. Set `FLOCI_MICROSANDBOX_GUEST_AGENT_PATH` for both native control
processes when environment isolation is enabled. DNS setup now installs kernel
routes to the shared resolvers and exits. It adds no resident DNS process.
Configure one to three local IPv4 resolvers on ports other than 53, 137, 853,
5353 or 5355, which the pinned runtime intercepts or blocks.

SDK 0.7.8 does not retain network policy or DNS configuration during full RAM
restore. The adapter now records effective policy against immutable guest and
checkpoint IDs, reapplies it before RAM runs, blocks the SDK default DNS path,
and refreshes guest DNS routes before admitting restored traffic. It also checks
the public CA bytes, so replacing a certificate at the same path requires guest
recreation.

Recreate previously isolated native guests and checkpoints: they lack the new
verified policy/DNS records and are deliberately rejected for execution or
restore. Stop and deletion remain available for cleanup. This change does not
require recreating nonisolated workloads.

## Kubernetes projected service-account identity

Native Pods now accept the default service-account automount. You can remove the
previous `automountServiceAccountToken: false` workaround when the application
needs Kubernetes API access. Combined projected volumes accept ConfigMap, Secret,
DownwardAPI metadata and Pod-bound service-account tokens. Resource-field,
ClusterTrustBundle and PodCertificate projections remain rejected.

Grant the controller `get` on `serviceaccounts` and `create` on
`serviceaccounts/token`, alongside its existing read permissions. It uses real
TokenRequest audiences, expiration and exact Pod/ServiceAccount identities. Tokens
rotate centrally at 80% TTL or 24 hours while awake; no guest identity daemon is
added. Tokens travel through stdin and are not stored in lifecycle journals.

Wake reissues identity before VM restore and refreshes projected directories
before forwarding requests. Running-controller recovery also requires successful
identity refresh before controller proxy routes reopen. API or projection failures block these
paths. Identity-bearing Pods now freeze their application processes before
capture and refresh projected tokens before thaw. Legacy identity checkpoints
without freeze proof require Pod replacement. Reopen
projected files instead of retaining file descriptors. Token `subPath` mounts stay
pinned and do not rotate, matching Kubernetes; use a directory mount for renewable
credentials. RAM checkpoints contain projected credentials and process memory,
so keep runtime files and backups private.

This adds Kubernetes service-account authentication and explicit token audiences.


## Kubernetes AWS credential admission

The existing EKS admission webhook now honors ServiceAccount IRSA annotations and
native Pods can reach the EKS Pod Identity link-local credentials relay. Enable
Floci TLS for admission, configure the cluster OIDC provider and role trust for
IRSA, or install the Pod Identity addon and associate the ServiceAccount. Recreate
existing Pods to apply admission changes; existing Pods are not mutated in place.
Rebuild the native controller and cold-create existing native Pods once to install
the dedicated credentials route. A previous RAM snapshot retains its old routes.
Explicit AWS environment variables are preserved and no static access keys are
injected. Projected token refresh uses application freezing for new checkpoints, as described below.

## EKS Pod Identity role trust

Pod Identity credential exchange now requires the associated role to trust the
`pods.eks` service for `sts:AssumeRole` and, unless the association disables session
tags, `sts:TagSession`. Update roles that previously used an empty trust policy.
Explicit Deny and mismatched namespace, ServiceAccount, cluster or account conditions
return AccessDenied and do not mint credentials. This is shared by Docker and native
Pods and uses the existing AWS credential response shape. Target-role chaining and
downstream session-tag ABAC remain separate limitations described in the EKS docs.

## Shared native disk admission

Rebuild the host helper and Pod controller together to use optional disk admission.
Set `FLOCI_MICROSANDBOX_DISK_LIMIT_MIB` and/or
`FLOCI_MICROSANDBOX_MIN_FREE_DISK_MIB` consistently on helpers sharing the same
native home. Zero leaves each limit disabled. The private capacity ledger persists
these limits across restarts. The authenticated capacity API exposes `diskMiB`,
`minFreeDiskMiB` and conservative byte accounting.

Preload images before enabling disk admission. Uncached images, unconverted
layers, unknown writable sizes and old checkpoints without recorded root geometry
are rejected before starting or pausing a workload. Recreate old checkpoints with
the rebuilt helpers before enabling limits. Kubernetes reserves a capture before
freezing tasks; Aurora reserves before pausing. A refused capture leaves the
running workload available, and a refused wake retains its saved checkpoint.

Accounting includes SDK image, snapshot, sandbox, volume and memory storage,
complete writable geometry and pending native operations. It does not credit
sparse or reflink savings. This is an admission budget, not a hard filesystem
quota. External CLI/SDK operations and unrelated host writes bypass it. No
automatic pruning is performed, and lowering a limit never deletes workloads or
revokes an already-prepared restore. Docker storage is unaffected. See the
[shared adapter accounting details](../microsandbox-runtime/README.md#disk-admission).

The shared adapter now resolves home-directory symlinks consistently for both
SDK and CLI operations, without creating missing directories. This prevents
duplicate snapshot identities caused by `/tmp` versus `/private/tmp` on macOS.
Existing dangling alias entries are not silently discarded: SDK storage inventory
continues to reject unknown sizes. Preserve the old home and reconcile those index
entries, or preload a fresh dedicated home before enabling disk admission.

## Shared microVM capacity admission

Rebuild both the Go host helper and every native pod controller. Helpers using one
runtime home now share RAM and concurrent-start accounting. Configure
`FLOCI_MICROSANDBOX_MEMORY_LIMIT_MIB` and `FLOCI_MICROSANDBOX_START_LIMIT` consistently
on those processes. Zero leaves that limit unlimited. Existing Java and pod-only
limits still apply in addition; Docker is unaffected. The host-helper capacity API
can change the persisted limits without stopping running guests.

Recreate RAM checkpoints after upgrading: new captures retain the memory geometry
needed to reserve a restore safely. Old checkpoints without that private record
are refused before their source VM is consumed. Known capacity refusal leaves
workloads cold and retryable; it is distinct from an unknown runtime outcome.
Reservations for unknown accepted operations survive helper restart and require
reconciliation, not automatic time-based expiry. Budget configured guest RAM
separately from JVM/runtime overhead and disk storage.

## Kubernetes filesystem and supplemental groups

Rebuild the native pod controller and the new static Linux `guestfs` utility
(`tools/microsandbox-runtime/cmd/guestfs`). Configure the controller with
`--guest-fs-binary` pointing to that artifact for the host CPU architecture.
The helper runs on demand and adds no resident process. Without it, Pods requiring
`fsGroup`, supplemental groups or `Strict` group policy fail admission explicitly.

Native Pods now apply `fsGroup` to disk/memory `emptyDir` and projected
ConfigMap/Secret/DownwardAPI/token volumes. A Secret with mode `0400` becomes
group-readable (`0440`) when `fsGroup` is set, as it does under kubelet. Refreshed
projections receive their group permissions before publication. These volume
types ignore `fsGroupChangePolicy`, matching Kubernetes. Local persistent volumes
apply the selected policy, including `OnRootMismatch`, before init and application
containers start.

`Merge` retains image-defined memberships; `Strict` excludes them. Initial
containers, sidecars, application startup, exec and probes use the same resolved
groups, including after RAM restore. Kubernetes status reports the initial Linux
identity. Existing Pods without the new group fields need no manifest changes.
Recreate an existing Pod to change its security context, following Kubernetes
immutability rules.

## Refresh identity before resuming application execution

New checkpoints of Pods with service-account token projections freeze application
containers and restartable init sidecars before saving RAM. The restored guest agent
refreshes token files before those processes are thawed. Kubernetes `subPath` token
mounts remain pinned and cannot receive rotation; use the projected directory mount.

Recreate native Pods with older token-bearing checkpoints once. Their RAM snapshots
do not prove application processes were frozen, so the controller refuses to restore
them. Ordinary running Pods can create new checkpoints with the new controller.
Interrupted freeze/thaw operations require their recorded guest completion proof;
missing or ambiguous completion leaves the workload unavailable instead of starting
an application with stale credentials. No new guest daemon or AWS endpoint is added.

## Kubernetes HTTP request activity

Rebuild Floci and the native pod controller together. Proxy availability now
defaults to `microsandbox.dev/proxy-protocol: http`, replacing the previous raw
TCP activity rule for both new and recovered unannotated proxy Pods. Strict
availability is unchanged. HTTP/1 and prior-knowledge cleartext HTTP/2 (h2c/gRPC)
requests through Services or Pod IPs wake the selected pod regardless of whether
they came through Floci's gateway. Activity lasts through the complete response
or upgraded stream, including WebSockets. An idle frontend HTTP keepalive
connection does not wake or pin the VM. It can stay open during hibernation, and
its next request wakes the pod. This uses the shared controller, without adding
a resident proxy sidecar to every pod. The application endpoint must support
the incoming HTTP protocol; HTTP/2-to-HTTP/1 translation is not provided.

Before upgrading a Deployment that serves raw TCP or encrypted TLS, explicitly
set `microsandbox.dev/proxy-protocol: tcp` on its Pod template. This retains the
previous behavior, where a connection wakes the pod and keeps it awake until it
closes. The mode applies to all declared proxy ports. For HTTPS request accounting,
terminate TLS before the HTTP listener and forward cleartext HTTP; TCP passthrough
cannot inspect encrypted requests. Port-forward remains connection-based.
Roll replacement Pods when changing proxy protocol, following the existing
runtime-annotation immutability rule.

Lifecycle responses add `activeRequests` separately from `activeConnections`.
External coordinators must consider both counters before hibernation. The first
covers direct HTTP requests and their response/upgrade streams; the second covers
TCP-mode proxy and port-forward sessions. `activitySequence` advances on activity
acquisition and release, letting Floci detect requests completed between polls
and reset idle time even when both current counts are zero. This requires no
extra workload configuration. Floci sends the observed sequence as
`expectedActivitySequence` on hibernate; newer activity causes a conflict before
mutation. External coordinators should use the same fence when that field is
available. Floci's gateway HTTP activity, SQS
visible/in-flight counts and RabbitMQ ready/unacknowledged counts are unchanged.
Kubernetes health probes do not reset the normal idle timer. Direct HTTP health
requests through the pod proxy are ordinary activity unless a separately configured
gateway handles them synthetically; no health path is guessed by the pod proxy.

Idle frontend HTTP connections have a ten-minute timeout and a shared limit of
1024, separate from the 1024 in-flight request limit; they do not keep guest RAM
allocated.

Native qualification passed on 2026-10-17 on macOS arm64 with microsandbox 0.7.8
and k3s v1.35.0-k3s1, using `qualify.py --http-activity-only`. It verified that a
bare connection leaves the VM cold, HTTP through a Service wakes it, an unfinished
response blocks both manual and automatic hibernation, and idle keepalive permits
automatic hibernation. The next request reused that same connection and retained
the RAM marker, kernel boot ID and application process identity. Docker and native
pod clients both reached the application through Service names and Pod IPs.
Owned resources were cleaned up and guest RAM returned to zero. This native run
used HTTP/1; HTTP/2, gRPC trailers, WebSockets and forwarding headers are covered
by Go race tests. Native Linux qualification was not repeated for this change.

This run also exposed and fixed shared restore admission reopening checkpoints
by an ambiguous raw ID. Revalidation now uses the original checkpoint selector
and checks its immutable ID, digest and memory size. Rebuild the Go host helper
as well as the pod controller to apply that fix to Aurora restores.

## Repeatable native cluster setup

`tools/microsandbox-pods/manage.py` now builds matching host/guest helpers and
prepares an owned k3s development cluster with private certificates and a scoped
controller identity. Existing manually configured controllers need no changes.
The helper deliberately creates its own cluster rather than adopting an existing
one. See the [setup commands](../microsandbox-pods/README.md#development-cluster-setup).

Retain both the cluster directory and native runtime home. The foreground `run`
command preserves sleeping and running guests when stopped; `prepare` renews
30-day certificates with the controller stopped. `status` reports RAM and journal
state. `destroy` refuses active controllers, remaining native Pods, workload
journals and uncertain operations, and never prunes images or checkpoints.
The generated admin kubeconfig is for deployments; the controller uses a separate
identity that can read application Secrets and mint service-account tokens but
cannot create workloads, change Secrets, administer RBAC or modify other nodes.

## Kubernetes Deployment activation granularity

Floci now defaults to waking one Deployment replica for HTTP or queue activity,
reusing a Ready or starting replica when possible. `runtime.wake-replicas` sets a
larger fixed target for parallel consumers, capped by the actual desired replica
count. Kubernetes replicas and Pod identities remain unchanged. Unselected cold
pods stay cold, and normal idle hibernation still checks every owned pod.

The default `runtime.backend-routing: pod` routes requests to the selected pod's
first declared TCP port instead of a Service that could choose another cold
replica. Remove `runtime.backend-url` for this mode and rebuild the pod
controller so it advertises `proxyProtocol`. HTTP proxy pods use their wake
listener for application requests and their direct endpoint for health checks;
strict pods use their direct HTTP endpoint. The controller's loopback listener
addresses are resolved on its configured host without changing their ports;
Floci must be able to reach those listeners.

For HTTPS, custom ingress, another application port or existing Service routing,
keep the exact `backend-url` and add `runtime.backend-routing: service` explicitly.
Service balancing can wake more replicas than `wake-replicas`; forwarded health
checks through a wake proxy can also renew activity in this compatibility mode.
An explicit
`backend-url` without that routing choice now fails configuration validation
rather than being silently redirected. This policy does not change direct
Kubernetes Service routing, infer queue-depth scaling, or change AWS API shapes.

## Pod root disk geometry

Pod root disks still default to 4 GiB. The opt-in annotation
`microsandbox.dev/root-disk-size: "8Gi"` selects at least 1 GiB, rounded up to
whole MiB. The controller journals the effective disk size before VM creation
and supplies it to shared disk admission. Changing this annotation requires Pod
replacement. Existing allocation journals migrate to their original 4 GiB root
disk; recovering a checkpoint never derives a different size from current
annotations. New allocation records missing disk geometry are rejected.
Kubernetes `ephemeral-storage` requests/limits and disk `emptyDir.sizeLimit`
remain unsupported.

## Container metrics and native storage maintenance

The kubelet TLS endpoint now supports container CPU and working-set memory
through `/metrics/resource` and `/stats/summary`. No configuration change is
required. Cold and sleeping containers remain absent from current samples;
metrics never wake them or renew activity. Whole-host node metrics are not
fabricated from guest allocations. See the [metrics contract](../microsandbox-pods/METRICS.md).

The authenticated Pod and host helpers add `/v1/storage` inventory and
`/v1/storage/prune` maintenance. Cleanup defaults to a preview and a 24-hour age
limit. It retains durable snapshots, guest disks and PVCs. Image removal is
separately opt-in, never forced, and blocked by outstanding native admissions.
The Pod helper always retains the base image needed by cold Pods. Operators using
the host helper should retain other preloaded images needed by never-started
workloads. See [storage maintenance](../microsandbox-runtime/STORAGE.md) for exact
accounting and cleanup limits. AWS endpoint shapes and Docker execution are unchanged.

## Managed background retention

New managed credential generations carry a private ownership marker. Successful
selection retains the active and previous complete generations and removes only
older complete generations with the same cluster owner. Existing unmarked
generations remain untouched. Controller logs rotate after 10 MiB with two
backups; manager logs rotate on startup. Copy-and-truncate log rotation can lose
diagnostic lines at its boundary and never alters workload recovery records.

## Lifecycle hooks and retained logs

Rebuild the native Pod controller, Linux network relay and Linux guestfs helper
together. New container attempts use bounded output capture with authentic
receipt timestamps; existing checkpoints retain readable raw output and require
a container restart before timestamp/time-filtered logs become available. Logs
can be read or followed while a Pod sleeps without waking it.

Strict sleeping singleton containers now retain logical `Running` status with
Ready=false, matching the existing multi-container behavior. Their VM remains
stopped and consumes no active VM RAM. Proxy-ready mode remains available.
Graceful deletion of a previously running sleeping Pod with `preStop` can restore
it within the remaining deletion grace and shared admission budget to run its
hook. Cold Pods and deletions with an observed zero grace period do not start execution.

Completed `postStart`/`preStop` hooks are persisted per container attempt and are
not replayed by RAM restore. An incomplete hook after a controller crash fails
instead of replaying unknown side effects. Hook-bearing Pods now use journal
version 8. Earlier controllers reject that
version before orphan cleanup or native reconciliation; keep the current
controller until these Pods and their checkpoints are removed. A pre-v8 journal
containing hooks is refused rather than guessing whether its side effects ran.

Downward API volume resource limits without explicit CPU/memory limits expose the
virtual node's advertised allocatable capacity, as Kubernetes does. This is not
the guest's RAM allocation or the shared VM budget's current free capacity. See
[Pod operations](../microsandbox-pods/OPERATIONS.md) for accepted handlers, log
bounds, and remaining explicit compatibility limitations.

## Shared-capacity HTTP waiting

Pod HTTP wake requests now wait within their existing request deadline when the
shared native budget is occupied by Aurora or another runtime client. A known
pre-dispatch admission refusal leaves the Pod cold or sleeping, releases local
start/RAM permits, and retries at most once per 250 ms per Pod. Concurrent HTTP
requests share that retry deadline. Unknown native outcomes and other failures
remain errors and are never replayed by this wait path.

## Native Pod environment isolation requires a protected peer

The Pod controller now refuses startup when `FLOCI_NETWORK_CONFIG_FILE` enables
environment isolation. Earlier versions checked only the SDK guest boundary's
fingerprint. That does not prove protection of inner traffic decapsulated by the
Kubernetes peer or DNS forwarded by that peer, so the previous guarantee was
incomplete. Kubernetes NetworkPolicy is a separate control and cannot close this
environment-wide gap by itself. Aurora isolation through the native host helper
continues to use its existing SDK boundary.

Stop previously running native Pods with the old controller before upgrading an
isolated deployment. The new startup refusal does not stop already running VMs.
Keep the old controller and runtime state available for owned cleanup; do not
disable isolation merely to make the controller start. Use the existing protected
Docker Kubernetes backend when this boundary is required until peer integration
is implemented and qualified. Ordinary native Pods without environment isolation
retain their existing runtime behavior.

## Ordinary Pod termination

Native running Pods now receive SIGTERM and their configured termination grace
even without lifecycle hooks, sidecars or PVCs. Existing applications can use
their normal shutdown handlers. The owned network return route remains available
while those handlers run. An observed zero grace period skips the graceful wait;
cold and sleeping Pods without preStop hooks remain stopped during deletion.
PVC writeback and ownership checks still gate destructive cleanup.

Immediate API force deletion can leave only the saved Pod specification for
cleanup. It does not guarantee that processes stop immediately or bypass a saved
nonzero grace period. Configure `terminationGracePeriodSeconds: 0` on the Pod
when zero grace must survive removal of the API object. This matches the
[Kubernetes force-deletion caveat](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_delete/).

The native provider keeps its existing explicit zero-grace hook skip. Kubernetes
kubelet can still run preStop because it applies a minimum effective grace, as
described in [the upstream discussion](https://github.com/kubernetes/kubernetes/issues/123408).
The shared qualification records that difference instead of claiming exact
zero-grace hook parity.

### Managed protected Kubernetes peers

For native Pods in an isolated Floci environment, prepare a new managed cluster
with `--environment-network`, `--isolation-helper` and `--environment-dns`.
These refer to existing environment resources. The manager records their immutable
identities and starts k3s only after its controlled resolver is installed. Reusing
a directory that previously created an unisolated peer is rejected.

Protected records use manager format 2 and always enable secure NetworkPolicy
mode. Every native application and init container must explicitly drop `NET_RAW`
or `ALL`. This is separate from the environment's destination allowlist and
simulator routing. Older managers reject the record; retain the matching tools
when restarting or renewing credentials. The manager never deletes the shared
Floci environment or isolation helper when destroying its owned empty cluster.

## Explicit environment egress and protected-peer metadata

`network.allowed-egress` now accepts explicit public or private IPv4 CIDRs, TCP/UDP and a
single port or inclusive port range. Unlisted traffic remains denied. Existing
`network.native.allowed-egress` keeps its native-local meaning. Configure local Route 53 A
records when an allowed external service needs a hostname: the shared address exception does
not turn on public DNS forwarding or follow changing hostname addresses. Explicit DNS-server
address/port rules are supported, with no default DNS exception.

Restart Floci with the same resource namespace and network configuration file to upgrade the
existing isolation helper. Successful policy installation publishes the exact-file fingerprint
and protected bridge metadata in the helper, and adds empty exact native transport tuple sets.
Subsequent policy refresh preserves registrations owned by the native controller. An
incompatible existing tuple-set schema fails installation instead of replacing its contents.
Native controller versions requiring this contract refuse older or mismatched helpers. No
AWS endpoint, response shape or application sidecar is added.

For protected peers, `--peer-udp-port` fixes the host UDP publication for the
cluster's shared VXLAN transport. Choose an unused port before starting Floci
and permit exactly that host UDP port in `network.native.allowed-egress` in the
shared immutable network file. Reuse the recorded port on later prepare runs.
This avoids granting a broad host UDP range. Plain peers retain ephemeral ports
unless this option is supplied explicitly.

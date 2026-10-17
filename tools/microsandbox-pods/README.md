# Native Pods preview

A shared Virtual Kubelet controller runs actual OCI apps inside native
microsandbox VMs and preserves their Pod UID across RAM sleep/resume. See the
[feature inventory and recovery rules](ARCHITECTURE.md) before using this preview.
The [completion plan](../microsandbox/ROADMAP.md) tracks the remaining Kubernetes,
Aurora and shared-runtime work.

Requirements: Go1.26+, CGO and a C compiler, native microsandbox **0.7.8** with matching runtime library,
Docker, Python3 and OpenSSL. Qualification creates an isolated privileged
k3s container and native VMs, then removes its uniquely owned fixture resources.
The controller uses the official Go SDK through the adjacent
`tools/microsandbox-runtime` module. Keep both directories in the checkout.
Image launch, inspection and host diagnostics still invoke the pinned CLI;
other lifecycle operations, snapshots and guest exec use the SDK. The Go SDK's
managed-job launch mode prevents full RAM capture, so image launch retains the
CLI's background-process mode. The network relay needs no CGO.
The controller requires glibc 2.28+ on Linux; it no longer supports a pure-Go
`CGO_ENABLED=0` controller build. Build Linux controllers with a Linux C toolchain.

From the repository root:

```sh
cd tools/microsandbox-pods
go test -race ./...
go build -o /tmp/microsandbox-pods .
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /tmp/network-relay-linux ./cmd/network-relay
python3 qualify.py \
  --controller /tmp/microsandbox-pods --msb /path/to/msb \
  --msb-home /private/runtime-home --report /tmp/native-pods.json \
  --network-relay-binary /tmp/network-relay-linux
```

Optional `--cold-pods 500` verifies 500 COLD declarations allocate no VM or RAM,
then boots one. The default is two. Other checks cover Deployment ownership,
environment references, mounted ConfigMaps/Secrets, disk and memory `emptyDir`,
logs/exec, TTY/resize, localhost port-forward, gRPC/HTTP/TCP/exec probes, startup gating, liveness restart, Pod/Service/DNS/source IP, two
RAM-preserving cycles, controller crash adoption, Always restart and cleanup.
Volume checks cover binary data, file modes, read-only projections, optional
sources, updates while awake and sleeping, and `emptyDir` retention through
container restart and RAM resume. The report records timings, allocated snapshot
bytes and controller RSS.
It also verifies image command defaults, common security contexts, seccomp,
variable guest allocations, CPU/memory limits, real OOM attribution and failure
recovery when an application cannot publish its exit status.
Init checks cover sequential completion, retry/failure, maximum stage resources,
running-task recovery, logs and completion retained through RAM resume and app
restart. SubPath checks cover pinned ConfigMap/Secret files and directories,
environment expansion, new emptyDir subdirectories and rejected escaping links.
A disposable `registry:2` fixture serves a pinned BusyBox image through an
authenticated TLS frontend and checks pull-secret rotation. Its temporary CA is
installed only in its test guest; production TLS settings are unchanged.
`--proxy-pods 500 --scheduling-cpu 500 --scheduling-memory 384Gi
--network-relay-binary /tmp/network-relay-linux` additionally checks 500 replicas
of one fixture app, each requesting one CPU and 768 MiB, in a lazy Deployment
becoming available without VMs or init execution, first Service activation,
PodIP/ClusterIP TCP routes, activity protection, auto-idle and RAM resume. Use the
peer architecture (`arm64` or `amd64`) for the Linux relay build.

Optional `--floci-checkout /path/to/floci --java-home /path/to/jdk25` runs the
Java gateway test against a fresh COLD Deployment and its client/runtime tests.
It first-boots through HTTP and performs two further sleep/wake cycles. Keep other
Maven processes out of that checkout during the run. Optional
`--maven-ready-file /tmp/native-maven-ready` waits before starting Maven until that
file exists, allowing other test runs to finish while the native fixture stays live.

The Linux relay binary is also required for HTTP/HTTPS/TCP/gRPC probes and port-forward in strict
mode. Build it together with the controller. See the
[health-check behavior and upgrade notes](../../docs/configuration/microsandbox-kubernetes.md#health-checks).

## Development cluster setup

The setup helper creates an owned, pinned k3s peer, private certificates and a
dedicated controller identity. From the repository root:

```sh
python3 tools/microsandbox-pods/manage.py build --binaries /private/native-tools
python3 tools/microsandbox-pods/manage.py prepare \
  --directory /private/native-cluster --binaries /private/native-tools \
  --msb /path/to/msb --msb-home /private/runtime-home
python3 tools/microsandbox-pods/manage.py run --directory /private/native-cluster
```

Use the generated `admin.kubeconfig` to deploy applications. The controller uses
its separate `controller.kubeconfig` and the generated `controller-rbac.json`.
Its role can read Pods, ConfigMaps, Secrets, PVCs and StorageClasses across the
cluster, mint bound service-account tokens, publish status and manage its named
node and lease. The shared local provisioner also creates and manages PVs.
It cannot create workloads, change Secrets, administer RBAC or change other nodes.
Use a dedicated development cluster: the controller still needs trusted access
to application credentials and to the local Docker socket.

`prepare --node-ip <host-IPv4>` selects the host address reachable from the
cluster explicitly. No special hostname is required with this setting. Otherwise
setup discovers Docker's host address, honoring `/etc/hosts` entries before DNS.
Repeated setup retains the recorded address so existing routing and certificates
keep the same identity. This is generic Docker networking, with no vendor-specific
runtime mode.

The directory must be private (0700); generated credential files are 0600.
Keep it and the runtime home across restarts. `run` supervises the controller in
the foreground and retains guests on exit. For optional background supervision:

```sh
python3 tools/microsandbox-pods/manage.py start --directory /private/native-cluster
python3 tools/microsandbox-pods/manage.py status --directory /private/native-cluster
python3 tools/microsandbox-pods/manage.py stop --directory /private/native-cluster
```

Both modes check the controller and kubelet certificates hourly and renew them
within seven days of their 30-day expiry. Complete replacement credentials are
prepared privately before the controller is stopped. An atomic generation
selection and graceful restart preserve Pod journals and native guests. Failed
certificate preparation leaves the running controller unchanged and retries in
one minute. `renew --directory ...` requests the same rotation immediately.
Logs are in `controller.log` and, for background startup, `manager.log`.
The active controller log rotates after 10 MiB and retains two bounded backups;
manager startup rotates its own log at the same threshold. Copy-and-truncate
rotation keeps the child descriptor valid but can lose diagnostic lines at the
rotation boundary. After a successful credential selection, cleanup retains the
active and previous generations. Unknown, incomplete and older unmarked credential
directories remain for inspection.

The supervisor holds the cluster setup lock and stops only its own child process.
Its private authenticated Unix socket handles status and stop requests; stored
PIDs are never used to adopt or signal another process. A child crash permits
three recovery starts in five minutes, each using the existing native ownership
and pending-operation fences. Repeated failure stops supervision for inspection.
If the supervisor itself crashes while its child survives, a new start refuses
the occupied controller endpoint. Inspect and stop that surviving controller
before restarting supervision; there is no automatic PID adoption or host
startup service. Short private directory paths are needed for Unix socket limits.

Repeated setup checks ownership and preserves the
cluster, workload state and control token. It refuses a missing cluster when
workload ownership state exists. Recreating that cluster requires deliberate
recovery instead of silently adopting a new identity.

`manage.py status --directory /private/native-cluster` reports controller health,
active RAM, journal states, uncertain native operations and runtime filesystem
free space. Its `sharedCapacity` reports guest RAM, concurrent starts, outstanding
reservations and limits across pods and Aurora using the same runtime home. When
disk admission is enabled, it also reports observed, reserved and committed disk
bytes. An unverifiable inventory is `unavailable`, never zero. This explicit
diagnostic uses the authenticated private controller API; ordinary readiness
checks do not scan disk usage.

`manage.py storage --directory ...` reports native storage categories and the
conservative remaining admission budget. `manage.py prune --directory ...`
previews eligible runtime memory cleanup older than 24 hours; add `--apply` to
perform it. Image cleanup additionally requires `--images`. Use
`--older-than-seconds N` and repeat `--retain-image registry/image:tag` to set the
age and preserve extra preloaded application images. The controller always keeps
its native guest base image. Owned guests, snapshots and pending reservations
remain protected by the shared runtime, and an unknown inventory is a refusal.
This maintenance does not impose a filesystem hard quota or prune Pod RAM
checkpoints. See [native storage maintenance](../microsandbox-runtime/STORAGE.md).

`manage.py destroy --directory /private/native-cluster` removes only
the owned empty k3s peer. First delete its native Pods, wait for controller cleanup
to drain, then stop the controller. Cleanup refuses remaining Pod or recovery
records and retains configuration for inspection. It never prunes runtime images
or checkpoints. `python3 -m unittest discover -s tools/microsandbox-pods -p
test_manage.py` checks these ownership and credential boundaries.

The controller writes a private `native-command.pending` intent before each native
mutation. Once a successful result or a confirmed admission refusal is checked,
it durably records that terminal outcome before removing the fence. If the
controller exits in that final window, startup can clear the fence using read-only
checks of the original controller identity, Pod UID and generation, immutable VM
IDs, and checkpoint IDs, digests and restored lineage. It never repeats the native
operation during this check. Command arguments, environment values and guest
configuration are not stored in this record.

An intent without a terminal receipt, a legacy plaintext fence, changed ownership
or an unprovable native state still blocks startup. A matching VM name, elapsed
time or a stopped controller process does not prove that a delegated operation
finished. Stop the controller and any surviving native commands, reconcile the
owned guest and checkpoint state, then remove only a confirmed stale fence. This
receipt recovery does not change the existing Pod lifecycle recovery rules; an
interrupted initial startup can still leave that Pod failed.

For an existing development cluster, configure the controller directly:

`run --pod-workers 8` controls concurrent Kubernetes reconciliation, including
cold-Pod cleanup. The controller accepts 1 through 64 workers and defaults to
eight. This does not increase the separate limit on simultaneous guest starts
or bypass shared RAM and disk admission.

Provide an owned Docker k3s worker labeled `io.floci.pod-probe=<owner>` with exactly
one published UDP8472 binding. Reserve an unused /22 inside the cluster Pod CIDR.
Keep cluster, peer, owner, node and persistent private state directory unchanged
on restart. The setup helper above owns a separate cluster; it does not adopt an
existing peer.

```sh
microsandbox-pods \
  --kubeconfig /private/cluster.yaml --node native-pods \
  --msb /path/to/msb --msb-home /private/runtime-home \
  --state /private/native-pod-state \
  --network-peer owned-k3s-worker --peer-owner development-owner \
  --pod-cidr 10.42.200.0/22 --cluster-cidr 10.42.0.0/16 \
  --proxy-backend-cidr 10.42.204.0/22 --network-relay-binary /tmp/network-relay-linux \
  --service-cidr 10.43.0.0/16 --cluster-dns 10.43.0.10 \
  --listen 0.0.0.0:10250 --node-ip HOST_IP_REACHABLE_FROM_K3S \
  --tls-cert /private/server.crt --tls-key /private/server.key \
  --client-ca /private/kubelet-client-ca.crt \
  --control-listen 127.0.0.1:18187 --control-token-file /private/token \
  --cluster-name development --max-pods 512 --max-active-memory-mib 4096 \
  --scheduling-cpu 500 --scheduling-memory 384Gi
```

The serving certificate must cover the node IP and be trusted by the API server.
The client CA authenticates its kubelet client. The kubeconfig needs Virtual
Kubelet Pod/status, node/lease, ConfigMap/Secret lookup and authorization permissions,
plus Deployment, ReplicaSet and NetworkPolicy discovery. Qualification uses only
its disposable cluster's administrator credentials.

Use this in a Deployment's Pod template:

```yaml
metadata:
  annotations:
    microsandbox.dev/availability: proxy
    microsandbox.dev/proxy-protocol: http
    microsandbox.dev/idle-owner: controller
    microsandbox.dev/idle-timeout-seconds: "120"
spec:
  nodeSelector:
    microsandbox.dev/runtime: native
  tolerations:
    - key: microsandbox.dev/runtime
      operator: Equal
      value: native
      effect: NoSchedule
  containers:
    - name: app
      image: busybox:1.37.0
      command: [sh, -ec, "mkdir -p /www; echo ready >/www/index.html; exec httpd -f -p 8080 -h /www"]
      ports:
        - containerPort: 8080
      readinessProbe:
        exec:
          command: [test, -f, /www/index.html]
```

Proxy availability delays first boot and makes a Deployment available through
actual shared wake routes. Omit it (or use `availability: strict`) to start normally
with guest readiness. Advanced `on-demand: "true"` without proxy availability
creates COLD, not-Ready Pods for an external gateway. Unsupported fields
fail explicitly, including unsupported CSI volumes and extended
resources. Scheduler requests survive sleeping independently of active VM
admission. New guests size themselves from CPU/memory requests and limits, with
one vCPU/768 MiB minimum and 256 MiB guest memory headroom. Declared limits use
actual CPU quotas and memory cgroups. See the
[capacity rules](../../docs/configuration/microsandbox-kubernetes.md#capacity-planning).

`--scheduling-cpu` and `--scheduling-memory` advertise virtual capacity so cold
pods can retain realistic requests. These quantities do not allocate guest RAM.
The independent active budget controls boot/restore admission, and a full budget
makes further wakes wait. Defaults remain four CPUs and the active memory budget
when the scheduling flags are omitted.

## Multiple application containers

Regular containers share the Pod VM, localhost network and declared Pod volumes.
Each container has its own image configuration, security/resource limits, probes,
exit status and restart backoff. A failed container restarts independently under
`Always` or `OnFailure`; completed ordinary init containers and healthy siblings
are not replayed. Strict readiness requires all applications to be ready. Proxy
readiness still means the owned wake routes are available, while traffic waits
for actual application readiness.

The VM allocation covers the peak of concurrently running applications and init
sidecars, including each ordinary init stage, plus guest headroom. Named `kubectl logs -c` and
`kubectl exec -c` select the corresponding container. Container logs preserve raw
bytes. The controller reads legacy single-container, sequential-init and
multi-container journals, then writes format 6, or 7 for local PVCs, to fence
older controllers from upgraded networking and persistent-volume cleanup. Do not
downgrade against upgraded state.
Ephemeral containers remain an explicit rejection pending debugging support.

The real Kubernetes qualification includes these scenarios. Use
`--applications-only --cold-pods 0` to run just this subset with the usual owned
cluster setup and cleanup. On Apple Silicon, the subset passed shared localhost
and memory-backed volumes, named logs/exec, independent restart, two full RAM
cycles, running/sleeping controller recovery and all-container completion.

## Restartable init sidecars

An init container with `restartPolicy: Always` starts in declaration order and
continues alongside later init stages and regular applications. Its startup
probe must pass before the next init stage starts; readiness affects actual Pod
readiness independently. Sidecars restart even when the Pod uses `Never`, while
completed init stages and running applications retain their state.

When regular applications finish, sidecars receive termination signals in
reverse declaration order and the Pod can complete. Pod deletion signals regular
applications first. One Pod grace deadline covers shutdown and survives
controller recovery. Legacy sidecar journals use version 5 and upgrade to format
6, or 7 when mounting local PVCs; delete their Pods and snapshots before
downgrading the controller. Use `--sidecars-only --cold-pods 0` for the focused
real Kubernetes qualification.

On Apple Silicon, that qualification passed startup/readiness gating,
independent restart under Pod `Never`, preserved process memory through a full
RAM cycle and controller recovery, reverse sidecar shutdown, and a real Job's
`Complete` condition. Owned fixture cleanup passed.

## Interactive operations

`kubectl exec -it` supports terminal allocation, live resize, stdin and guest exit
status. Pod deletion cancels attached exec. Non-TTY exec retains separate stdout
and stderr streams. `kubectl port-forward` connects inside the Pod VM, including
to applications bound only to localhost. It wakes a cold or sleeping Pod on the
first connection and pins the guest against hibernation until the stream closes.
Forwarding requires the Linux relay binary; the controller installs its exact
bytes privately into the guest through SDK stdin.

`--operations-only --cold-pods 0` selects the real Kubernetes operations fixture,
which builds a small Linux gRPC health application using the local Go toolchain.
The fixture passed on Apple Silicon with serving/not-serving transitions,
interactive resize and exit status 23, and localhost forwarding before and after
RAM hibernation. Attach to the
application's original stdio and metrics remain
explicit unsupported operations.

## Mounted configuration and temporary storage

Build the static Linux permission helper from `tools/microsandbox-runtime` with
`CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /tmp/guestfs-linux ./cmd/guestfs`
(use `amd64` on x86 hosts), and pass `--guest-fs-binary /tmp/guestfs-linux` to the
controller. It is installed lazily inside each started guest and runs only for
volume setup and OCI runtime operations. It adds no resident guest process.

This enables `fsGroup`, `supplementalGroups`, and `supplementalGroupsPolicy`.
`Merge` includes the image user's group memberships; `Strict` includes only the
primary group and explicitly declared supplemental and filesystem groups.
Initial containers, restartable sidecars, application containers, probes and exec
use the same initial identity, also exposed as `containerStatuses[].user.linux`.
Volume directories receive setgid and group access. Read-only projections gain
group read permission before atomic publication, including refreshed Secrets.
Temporary and projected volumes ignore `fsGroupChangePolicy`, matching kubelet.
Native local persistent volumes also honor `fsGroupChangePolicy`, including
`OnRootMismatch`; ownership is applied before init and application containers.

`qualify.py --groups-only --guest-fs-binary /tmp/guestfs-linux` runs the same
permission assertions against Docker k3s and native Pods, then checks native RAM
restore and controller recovery. Existing controller flags are also required.

Pods can mount [native local PVCs](STORAGE.md), ConfigMap, Secret and `emptyDir`
volumes. The local provisioner supports Filesystem RWO, node affinity and
Delete/Retain reclaim policies. A sleeping Pod keeps its single-Pod attachment;
ordinary CSI volumes remain unsupported. ConfigMap and Secret
mounts support `items`, nested relative paths, binary data, `optional`,
`defaultMode` and per-item modes. They are always read-only inside the application,
including when `volumeMounts[].readOnly` is omitted. `subPath` and `subPathExpr`
can select a file or directory. ConfigMap and Secret subPath mounts retain their
selected content until a new container start; ordinary directory mounts refresh.

```yaml
volumes:
  - name: settings
    configMap:
      name: app-settings
      items:
        - key: application.yaml
          path: application.yaml
  - name: scratch
    emptyDir: {}
  - name: memory-scratch
    emptyDir:
      medium: Memory
      sizeLimit: 64Mi
containers:
  - name: app
    # Include image, command and ports from your existing container definition.
    volumeMounts:
      - name: settings
        mountPath: /etc/app
      - name: scratch
        mountPath: /var/app/cache
      - name: memory-scratch
        mountPath: /var/app/temporary
```

Source objects are read on first activation, periodically while the VM is awake,
and after RAM restore before the wake proxy forwards traffic. Missing required
sources prevent first VM allocation. Strict pods wait Pending and retry; a cold
proxy pod can retry activation on a later request after the source is fixed.
Cold and sleeping pods do not poll source objects. Refresh swaps each volume's
projected directory atomically; applications must reopen files to consume updates. Existing open file
descriptors can keep the previous content. Environment references retain their
original values until Pod replacement.

Configuration refresh errors preserve the last good contents and are retried.
Pods without renewable identity can resume background processes before the
post-restore refresh and briefly observe previous configuration. Identity-bearing
Pods use the freeze/refresh/thaw sequence described below.

Both `emptyDir` variants survive container restart and RAM hibernation, and belong
only to their Pod UID. Pod deletion removes them with its VM. Disk-backed
`emptyDir` shares the VM's root disk, which defaults to 4 GiB; disk `sizeLimit` is
unsupported. The immutable annotation `microsandbox.dev/root-disk-size: "8Gi"`
chooses a root disk of at least 1 GiB, rounded up to whole MiB. Its size is
journaled before boot and used for shared disk admission; recovery and RAM
restore retain that geometry. Replace the Pod to change it. This does not add
support for `ephemeral-storage` resource requests or limits. Memory
`emptyDir` uses guest tmpfs and accepts a positive size limit up to the guest's
RAM allocation. Its
contents consume guest RAM while awake and checkpoint space while sleeping.

Combined `projected` volumes support ConfigMaps, Secrets, DownwardAPI Pod
metadata and Pod-bound service-account tokens. Default service-account token
automount works without a manifest override. Tokens come from the Kubernetes
TokenRequest API, with the requested audience and Pod UID, and rotate in the
shared controller at 80% of their lifetime or after 24 hours. The controller's
credentials need `get` on ServiceAccounts and `create` on `serviceaccounts/token`,
in addition to its existing Pod, ConfigMap and Secret permissions.

Identity projections use read-only guest tmpfs. The controller keeps tokens only
in memory and sends them through guest stdin, never command arguments or lifecycle
journals. It reissues tokens before restoring a sleeping VM, then requires the
refreshed projection before forwarding wake traffic. Running-controller recovery
also refreshes identity before reopening controller proxy routes. For Pods with mounted
service-account tokens, the controller freezes each running application and restartable
init sidecar through containerd before capturing RAM. On restore, the guest agent and
containerd run while those application cgroups remain frozen. Projection refresh must
succeed before the controller thaws the recorded process identities. This also protects
background processes from reading the previous token immediately after RAM restore.
Applications must reopen projected files.
Old token-bearing checkpoints without application freeze proof cannot be resumed by
this controller; recreate those Pods once. Unknown freeze/thaw completion remains
blocked rather than guessing that guest execution stopped.
Kubernetes `subPath` mounts remain pinned and do not receive token rotation or
wake refresh; mount the projected directory for renewable credentials.

This implements Kubernetes API identity and explicitly projected audiences.
Floci's EKS admission webhook also injects IRSA annotations and Pod Identity
associations; native Pods route `169.254.170.23/32` to the existing cluster relay.
Configure the cluster OIDC provider and role trust for IRSA, or install the
Pod Identity addon and create an association. The TLS webhook requirements and
annotation settings are described in [EKS documentation](../../docs/services/eks.md).
DownwardAPI resource fields,
ClusterTrustBundle and PodCertificate projections, ownership overrides, CSI,
host paths, mount propagation and recursive read-only options remain unsupported. Secret
projection data lives on guest tmpfs, but RAM checkpoints retain its contents and
any secrets in process memory. Protect the runtime home and checkpoint backups;
the controller does not encrypt checkpoints.

`--identity-only` qualifies real API TLS, default automount, audience enforcement,
RBAC allow/deny, downward metadata updates, HTTP-triggered RAM wake and identity
refresh across controller recovery. It never prints token contents.
On Apple Silicon/OrbStack with microsandbox 0.7.8 and k3s 1.35, the isolated
identity run passed all six checks and cleanup on 2026-10-17. Both sleeping and
running controller recovery retained the application process while reissuing
its token before the next proxy request.

`--aws-identity-only --floci-checkout /path/to/floci --java-home /path/to/jdk25`
runs one real AWS Go SDK probe on both Docker and native workers. It exercises
IRSA and EKS Pod Identity, exact-role SSM allow/deny policies, explicit credential
precedence, and rejected token audiences and subjects. Native checks preserve the
application process through controller restart and RAM resume while replacing its
projected token and cached AWS credentials. The fixture uses a standard Service
for AWS API access and one shared node relay for the Pod Identity endpoint.

Add `--identity-expiry-wait` to wait for the actual ten-minute projected tokens
to expire while the native guests sleep. The probe deliberately shortens the SDK
credential cache window; it does not wait for the issued one-hour AWS sessions to
expire. The fixture calls Floci's real admission handler and applies its patch to
both worker types, but does not qualify installation of the TLS webhook. The probe
reports hashes and role identities without token bodies or secret keys. As with the Java
gateway qualification, reserve the checkout's Maven test process for this run.

## Wake coordination

An awake gateway or durable-queue coordinator calls the authenticated [lifecycle
API](API.md). Set `idle-owner: external` for proxy Pods managed by that coordinator,
so queue activity and direct requests share one idle decision. Proxy mode defaults
to `microsandbox.dev/proxy-protocol: http`: HTTP/1 and prior-knowledge cleartext
HTTP/2 (h2c/gRPC) requests through a Service or PodIP wake the pod, and each
response or upgraded stream holds it awake until completion. Idle frontend keepalive connections do
not wake or pin the VM and can survive hibernation. The next request on the same
connection wakes it. The shared controller implements this without a per-pod
resident proxy. The backend must support the incoming HTTP protocol; the proxy
does not translate HTTP/2 to HTTP/1.

Set `microsandbox.dev/proxy-protocol: tcp` for existing raw TCP/TLS listeners to
retain connection-based wake and activity. This setting applies to every declared
proxy port in the pod. HTTPS must be terminated before an HTTP-mode listener or
use TCP mode; encrypted traffic cannot receive request-level idle accounting.
Port-forward remains connection-based. Strict mode requires an external wake route.
Scaling a Deployment to zero deletes RAM identity, so hibernation retains replicas.
Floci's coordinator defaults to `runtime.wake-replicas: 1` and reuses an already
awake replica before waking another. Its default `backend-routing: pod` sends
HTTP to that selected pod's first declared TCP port; omit `backend-url`.
Set a larger `wake-replicas` for parallel queue consumers without changing
Kubernetes desired replicas. Explicit `backend-routing: service` preserves a
custom `backend-url`, including TLS or another application port, but Service
balancing can wake additional replicas. See the [configuration and migration
rules](../../docs/configuration/microsandbox-kubernetes.md).

Both proxy modes translate client source IPs. Direct HTTP health requests wake apps
and count as requests; configure synthetic health handling at the Floci gateway
when they should not. Kubernetes probes and status checks do not wake sleeping
guests or extend their normal idle timer. SQS visible/in-flight and RabbitMQ
ready/unacknowledged message accounting is unchanged.
Reserve both /22 ranges without overlap
with existing nodes, and preserve those network settings across controller restart.

Use `qualify.py --activation-only --floci-checkout /path/to/floci` with the usual
controller, microsandbox, runtime-home and report arguments to test Java gateway
and queue activation against two real cold replicas with only one guest's RAM
budget. This fixture checks that the other replica stays cold and that the
selected replica retains its identity and memory over two hibernation cycles.

Application lifecycle hooks, projected resources, bounded retained logs and
Kubernetes Events are described in [OPERATIONS.md](OPERATIONS.md). Rebuild the
controller, network relay and guestfs together to enable timestamp capture for new
container attempts; existing raw-log checkpoints remain readable.

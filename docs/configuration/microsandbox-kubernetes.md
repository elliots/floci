# Experimental native Kubernetes pods

The native pod backend is opt-in. A shared host controller advertises a tainted
Kubernetes node; each selected Pod UID owns a microsandbox VM. The Kubernetes
control plane and the shared controller stay running while application VMs
hibernate. There is no kubelet in each application VM.

The host controller lives in this checkout under
[`tools/microsandbox-pods`](../../tools/microsandbox-pods/README.md). It is an experimental native tool, not a published
sidecar image. Its `README.md` describes installation, the owned network peer,
kubelet TLS credentials and the executable qualification; `ARCHITECTURE.md`
explains lifecycle and networking behavior. The controller
uses microsandbox 0.7.8 and a dedicated persistent runtime home and state directory.

Proxy availability also requires `--network-relay-binary` pointing to the Linux
relay executable built from that tool's `./cmd/network-relay` package. Its
architecture must match the Docker network peer, for example `GOOS=linux
GOARCH=arm64` on an ARM64 peer. Reserve `--proxy-backend-cidr` separately from
the public native Pod CIDR and other workers. Its default is `10.42.204.0/22`,
alongside the default public `10.42.200.0/22`. Both ranges belong inside the
cluster's Pod CIDR. See the controller setup instructions for the complete command.

The controller uses the explicit `--node-ip` for peer-to-host routing. A bounded
challenge selects loopback listeners when the peer can reach them. Otherwise,
the address must belong to a specific local host interface and pass the same
challenge. Frontend listeners never silently bind all host interfaces. On that
interface, bounded in-process UDP forwarding accepts only the owned peer's source
IP, each Pod's fixed VXLAN source port and its expected VNI, then forwards to the
SDK's loopback publication. SDK application backends and authenticated controller
endpoints remain on loopback. Ordinary socket behavior and source rejection are
tested in a Linux Docker namespace; native Linux VM networking still requires
qualification on a KVM host.

## Scheduling and waking

Opt in at the pod template using the node selector and matching toleration:

```yaml
spec:
  template:
    metadata:
      annotations:
        microsandbox.dev/availability: proxy
        microsandbox.dev/proxy-protocol: http
        microsandbox.dev/idle-owner: external
    spec:
      nodeSelector:
        microsandbox.dev/runtime: native
      tolerations:
        - key: microsandbox.dev/runtime
          operator: Equal
          value: native
          effect: NoSchedule
      enableServiceLinks: false
```

The current guest profile supports multiple regular containers in one VM and uses each image's command defaults unless the Pod overrides them. Check the feature inventory below before moving a
Deployment. Unsupported pod specifications are rejected before guest creation.
The example disables automatic Service environment variables so hundreds of
Services do not expand every application's environment. DNS-based Service
discovery still works. Keep Service links enabled if the application relies on
those variables; this example does not change Kubernetes defaults.
An existing pod retains its resolved environment across hibernation and controller
restarts. Change its Deployment template to pick up new ConfigMap, Secret or
Service environment values in replacement pods.

The example selects lazy deployment with Floci owning the idle policy. It needs
declared TCP container ports. The shared proxy registers the pod without creating
a VM, pulling the application image or allocating guest RAM. Once its listener
and routing are available, Kubernetes sees the pod as Ready, so a rollout or
Helm's `--wait` can finish while the application stays cold. Its first request
starts the application and waits for actual readiness before forwarding bytes.
Later idle cycles preserve RAM through hibernation.

This is an explicit local runtime extension: Ready describes proxy availability,
not proof that the application has started successfully. A bad image, command or
readiness probe can still fail on first use. The controller's lifecycle API keeps
reporting actual `COLD`, `SLEEPING`, `RUNNING` or `READY` application state separately.

Set `microsandbox.dev/availability: strict`, or omit it, to start immediately and
require application readiness before Kubernetes reports Ready. The advanced
combination `strict` with `microsandbox.dev/on-demand: "true"` defers first boot
while keeping cold pods Pending and not Ready; it needs an initial wake before
rollout readiness waits can complete.

## Choosing the idle owner

Use exactly one idle owner for a Deployment:

- With a Floci workload definition, including any SQS/RabbitMQ triggers, set
  `microsandbox.dev/idle-owner: external`. Floci accounts for gateway requests,
  direct proxy requests, active TCP/port-forward connections, queued and in-flight
  messages before hibernating.
- For a standalone HTTP/TCP Deployment without a Floci workload definition, leave
  the proxy mode's default `idle-owner: controller`. The native controller
  hibernates after its last HTTP request or stream finishes and the idle interval
  passes. With `proxy-protocol: tcp`, it waits for connections to close instead.
  Set `microsandbox.dev/idle-timeout-seconds: "120"` to change that interval,
  from 1 to 86400 seconds. This mode does not inspect message queues.

Floci rejects proxy pods that also enable controller-owned idle decisions. Queue
processing must not be interrupted by an independent HTTP idle timer. Direct
proxy wakes are discovered through one shared cached controller observation,
instead of inspecting each sleeping VM.

Enable the [on-demand coordinator](microsandbox-workloads.md) and supply the
controller's loopback lifecycle endpoint and bearer token:

```sh
export FLOCI_ON_DEMAND_POD_CONTROLLER_URL=http://127.0.0.1:18187
export FLOCI_ON_DEMAND_POD_CONTROLLER_TOKEN=replace-with-at-least-32-characters
```

The controller takes the matching credential from `--control-token-file`. This
listener is separate from the standard Kubernetes API and kubelet TLS listener.
Floci must be able to reach its loopback interface, so the initial arrangement
runs Floci and the controller on the same host.

```yaml
workloads:
  orders:
    host: orders.localhost
    runtime:
      type: kubernetes
      namespace: default
      deployment: orders
      wake-replicas: 1
      backend-routing: pod
    queues: [orders-incoming]
    idle-timeout-seconds: 120
    startup-timeout-seconds: 240
    readiness:
      path: /ready
```

The default `backend-routing: pod` discovers the selected pod's published first
TCP application port. Real gateway requests use its HTTP wake proxy when proxy
availability is enabled, or its direct HTTP endpoint in strict mode. Running
health checks use the selected direct endpoint so monitoring does not renew the
application idle timer. Floci and the controller must share reachable application
listeners; a loopback address advertised by the controller is resolved on the
configured controller host, with its published port unchanged. This does not
open or tunnel that port remotely.

Omit `backend-url` in pod routing mode. For HTTPS, a custom ingress endpoint or a
different application port, explicitly select `backend-routing: service` and set
`backend-url` to the same reachable HTTP(S) origin used before, bypassing the
Floci wake gateway. Service routing can select and wake additional cold pods.
If that endpoint itself traverses a wake proxy, forwarded health checks are
ordinary proxy requests and can extend its idle timer. Pod routing avoids that
by using the selected direct endpoint for running health checks.
Proxy pod routing requires `microsandbox.dev/proxy-protocol: http`; opaque
TCP/TLS listeners need the explicit Service routing choice. In proxy availability mode,
declared TCP ports accept traffic through both Pod IPs and Services while the VM
is cold or hibernated. In strict mode, direct calls to sleeping pods or ClusterIPs
do not wake them; route requests through the Floci gateway.
The SQS, RabbitMQ and IoT-to-SQS rules use the shared wake coordinator. Queue-only workloads can omit the hostname and
disable HTTP readiness. Proxy availability currently requires at least one
declared TCP port, so queue-only pods without a port use strict availability.

### HTTP requests and TCP compatibility

Proxy availability defaults to `microsandbox.dev/proxy-protocol: http`. Each HTTP
request through a Service or Pod IP wakes its selected pod and waits for actual
application readiness. This applies to requests from any reachable client,
including other pods; a request need not pass through the Floci gateway.
HTTP/1 and prior-knowledge cleartext HTTP/2 (h2c), including gRPC, use request
activity. The application endpoint must support the incoming HTTP protocol; the
proxy does not translate HTTP/2 requests to an HTTP/1 backend. Streaming
responses and upgraded connections such as WebSockets keep the pod awake until
that response or stream ends. An idle frontend HTTP keepalive connection neither
wakes the pod nor prevents hibernation. It can remain connected while the pod
sleeps; its next request wakes the pod again. The shared host controller handles
this behavior without a resident proxy sidecar in each pod.

For existing raw TCP or TLS listeners, explicitly set
`microsandbox.dev/proxy-protocol: tcp` on the Pod template. This retains the
previous connection-based behavior: connecting wakes the pod, and the entire
connection, including idle HTTP keepalive carried over it, keeps the pod awake.
The setting applies to all declared proxy ports in the pod. This compatibility
mode does not add support for further protocols or change port-forward behavior.
Strict availability is unchanged.
Encrypted HTTPS cannot use request accounting at the HTTP listener. Terminate
TLS before that listener and forward cleartext HTTP, or select `tcp` for TLS
passthrough. TLS passthrough cannot distinguish idle connections from requests.

Floci's gateway request and message accounting is unchanged. Configure synthetic
HTTP health responses at that gateway when health polling should stay green
without waking an application. Direct HTTP monitoring requests through the pod
proxy are ordinary requests and count as activity; the proxy does not infer
health URLs or synthesize healthy application responses. Kubernetes probes use
their existing internal path and do not renew the normal idle timer.

Floci verifies the actual Deployment and its ReplicaSet ownership chain before
changing a pod. Hibernation retains its replica count and Pod UIDs. Strict sleeping
pods become not Ready; proxy-mode pods remain available through the wake listener.
The Floci gateway and wake proxy wait for actual container readiness before
forwarding application traffic. Strict direct Pod IP connections are ordinary
network traffic and do not pass through that gate. A rollout, replacement pod or
changed Deployment generation during a transition causes an error instead of
applying an old operation to new pods.
The legacy `runtime.replicas` field does not scale this backend. Manage replicas
through ordinary Kubernetes manifests, and keep at least one desired replica.
Deployments already scaled to zero must be scaled up before this backend can
adopt their pods.

A Floci workload activation defaults to `runtime.wake-replicas: 1`. It reuses a
Ready or already-starting replica before restoring a sleeping replica or booting
a cold one, with Pod UID ordering to make equal choices stable. It waits for
actual readiness only on the selected replicas. Other cold replicas remain cold;
Kubernetes desired replicas, Pod UIDs and proxy readiness do not change.

Set `wake-replicas` to a larger positive number when a queue needs parallel
consumers. The target is capped at the Deployment's current desired replicas;
it never scales the Deployment. The same configured minimum applies to gateway
HTTP and SQS/RabbitMQ activation, including IoT rules routed into SQS. It is a
fixed wake target, not automatic scaling from queue depth or request concurrency.
Already-awake additional replicas are left running until the workload's normal
idle decision, and direct Service traffic can wake more replicas independently.
Choose a target that fits the shared active RAM budget. Whole-workload idle
hibernation still accounts for every owned replica's requests and streams.

If `runtime.cluster-name` is supplied, it must match the controller's advertised
`--cluster-name`. One configured controller endpoint currently serves the local
wake coordinator. AWS account and region fields do not select another Kubernetes
cluster. A namespace/Deployment pair can have only one Floci workload rule, so
two rules cannot independently put each other's pods to sleep.

Change runtime annotations on the Deployment's pod template to roll out new pod
identities. Changing them in place on an existing pod is rejected.

## Feature inventory

The initial profile is deliberately small enough to verify against real guests.
The VM boundary can accommodate the remaining features, but they require
container-runtime and Kubernetes integration work.

| Area | Initial implementation | Remaining compatibility work |
| --- | --- | --- |
| Execution | Multiple regular OCI containers per pod, sequential init containers, restartable init sidecars, image ENTRYPOINT/CMD defaults, command/args overrides and environment expansion | Ephemeral debugging containers |
| Lifecycle | Configurable lazy or strict availability, Pod UID ownership, Deployment discovery, RAM/disk hibernation, Always/OnFailure task restart, exec/HTTP postStart and exec/HTTP/sleep preStop hooks | Custom stop signals and broader rollout qualification |
| Environment | Literal values and resolved ConfigMap/Secret environment sources | In-place environment refresh requires Pod replacement |
| Networking | IPv4 Pod IPs, cluster DNS, Services through an owned bridge; shared HTTP request wake proxy for 1 to 16 declared ports, explicit TCP compatibility mode, opt-in node-wide TCP/UDP NetworkPolicy | Raw-socket policy workloads, IPv6 policies, UDP/SCTP wake and arbitrary CNI plugins |
| Health checks | Exec, HTTP/HTTPS, TCP and gRPC readiness, startup and liveness probes; named TCP ports and thresholds | Host overrides |
| Operations | Current/previous/follow/timestamp-filtered bounded logs, Events, exec with TTY/resize and localhost port-forward | Attach and ephemeral debugging |
| Storage | Configurable private VM disk, disk/memory emptyDir, ConfigMap and Secret mounts, projected volumes including resourceFieldRef, subPath and subPathExpr, opt-in local persistent volumes | CSI, disk quotas and certificate bundles |
| Identity | Namespaced registry pull secrets; real Pod-bound TokenRequest projections, default automount, IRSA injection and EKS Pod Identity routing | Target-role chaining and downstream session-tag ABAC |
| Resources | Request/limit-based guest sizes, CPU shares/quotas, memory limits, kernel OOM attribution, container CPU/working-set metrics, shared native RAM/disk/start admission | Eviction, Pod-level resources and in-place resizing |
| Security | VM isolation, authenticated controller transport, UID/GID, fsGroup and supplemental groups, non-root checks, read-only root filesystems, privilege escalation, capabilities and RuntimeDefault/Unconfined seccomp | Privileged containers, custom seccomp, SELinux and AppArmor profiles |

Most-feature support should build on the shared controller and the guest
container runtime. Adding a full kubelet to each application would make hundreds
of sleeping workloads substantially more expensive.

Proxy mode forwards requests or TCP streams through shared host listeners, so
applications see the proxy's connection source instead of the original client
Pod IP. Strict direct networking preserves the original Pod IP. The controller's owned Linux peer runs
one shared network relay for all proxy-mode pods, with a separate private backend
CIDR for actual guests; it does not start one relay process per pod.

Enable `--network-policy=true` on a dedicated native controller to enforce
IPv4 TCP/UDP policies, default deny, selectors, IP blocks and port rules. Every
native container on that node must explicitly drop `NET_RAW` or `ALL` and omit
administrative capabilities, including Pods not currently selected by a policy.
Live updates are watched; policy is installed before a sleeping application
thaws. In ordinary mode, selecting policies remain unsupported and are fenced
when added to running Pods. See the [scope, transport protections and migration
rules](../../tools/microsandbox-pods/NETWORK_POLICY.md).

## Image startup and security

Ordinary images can use their own ENTRYPOINT and CMD; an explicit Pod `command`
is no longer required. Kubernetes overrides apply: `command` replaces the image
entrypoint and suppresses its CMD; `args` alone retains the image entrypoint.
Without either override, both image defaults apply. The image's environment,
working directory and USER remain defaults, and Pod settings override them.
Application images must include a Linux variant matching the native host
architecture (`arm64` or `amd64`). This backend does not emulate another CPU architecture.
Metadata is read from the guest's cached image, so `Never` and cached
`IfNotPresent` starts do not need registry access.

Pod command/argument references such as `$(SETTING)` expand from the resolved Pod
environment. `$$(SETTING)` stays literal, as do unknown references. Image command
defaults are not expanded, and variables present only in the image environment
are not sources for Pod command expansion. This matches Kubernetes behavior.

Pod and container `runAsUser`, `runAsGroup` and `runAsNonRoot` are supported;
container values override Pod values. A non-root requirement rejects UID zero
and rejects a named image USER unless a numeric `runAsUser` makes it verifiable.
Container `readOnlyRootFilesystem`, `allowPrivilegeEscalation` and capability
`add`/`drop`, including `ALL`, configure the actual OCI process. Exec retains
that process's security settings. Pod and container seccomp profiles support
`RuntimeDefault` and `Unconfined`, with container settings taking precedence.
Omitting the profile leaves seccomp unconfined. For example:

```yaml
securityContext:
  runAsUser: 1000
  runAsGroup: 1000
  runAsNonRoot: true
  seccompProfile:
    type: RuntimeDefault
containers:
  - name: app
    image: your-application:version
    securityContext:
      readOnlyRootFilesystem: true
      allowPrivilegeEscalation: false
      capabilities:
        drop: [ALL]
```

Use writable volume mounts for application scratch space when the root filesystem
is read-only. `fsGroup`, supplemental groups and their `Merge`/`Strict` policies
are supported through the short-lived guest filesystem helper. SELinux, AppArmor, `Localhost` seccomp, Windows
security settings and privileged containers remain rejected. Omitting
`allowPrivilegeEscalation` uses Kubernetes's permissive default; set it to `false`
when the application should be unable to gain privileges through exec.

## Volumes

For persistent data, use the opt-in
[native local StorageClass](../../tools/microsandbox-pods/STORAGE.md). Its
Filesystem RWO volumes retain data across Pod replacement and remain attached
through RAM sleep. This local provisioner does not implement EBS or CSI. It
enforces one Pod attachment at a time, including sleeping Pods, and supports
`Delete` and `Retain` reclaim policies.

ConfigMap and Secret directory mounts support `items`, nested relative paths,
`optional`, `defaultMode`, per-item modes and binary data. These source volumes
are always read-only inside the application, even when the mount omits
`readOnly`. The controller needs API permission to read their source objects in
the Pod's namespace.

A cold pod reads its sources on first activation, so a configuration update after
deployment is included at first boot. A missing required object or key prevents
VM allocation. Strict pods remain Pending and retry; proxy pods retain their
cold availability and a later wake request can retry after the source is fixed.
An optional missing object or key produces an empty mount or omits that key.

Awake pods refresh mounted sources about every 30 seconds. Each successful
projection switches its `..data` symlink atomically, following Kubernetes volume
projection conventions. Reopen files to consume changes; existing file
descriptors can keep previous content. There is no transaction across different
volumes or across multiple separate file opens. An unavailable source or API
error retains the last successfully projected contents and is retried.

Cold and sleeping pods do not poll sources or wake to refresh them. After RAM
restore, the controller attempts a refresh before releasing traffic through the
wake proxy, with the same last-good-content behavior on errors. The native
runtime has already resumed application and background processes at that point;
this is not a guarantee that their first instruction sees updated configuration.
Environment-variable references remain unchanged until Pod replacement.

Both disk-backed `emptyDir` and `emptyDir.medium: Memory` survive container
restart and RAM hibernation. Their contents belong to a single Pod UID and are
removed with its VM when the Pod is deleted. Disk-backed volumes share the
guest's root disk (4 GiB by default). Memory volumes use guest tmpfs, optionally bounded by
`sizeLimit` up to the pod's guest allocation; their data consumes guest RAM and
RAM-checkpoint space. Disk-backed `sizeLimit` is not implemented.

Secret projections also use guest tmpfs. Hibernation writes their contents and
secrets in process memory into RAM checkpoints, so protect the runtime home and
checkpoint backups. The controller does not encrypt checkpoints.

`subPath` selects a file or directory inside a supported volume. `subPathExpr`
expands references from that container's resolved Pod environment. The two fields
are mutually exclusive. Absolute paths, parent traversal and links escaping the
volume are rejected, as are empty, `.` or unclean paths. Projected subpaths must
exist even when the source is optional; missing or empty expression variables fail
validation. ConfigMap and Secret subPath mounts stay pinned to the selected
content while ordinary directory mounts receive source updates. A new
container start resolves the subPath again; RAM resume retains its existing mount.

Combined `projected` volumes support ConfigMap, Secret, DownwardAPI metadata and
`serviceAccountToken` sources. The API server's default `kube-api-access` volume
works, including its CA certificate and namespace file. TokenRequest uses the
Pod's exact UID and ServiceAccount; an omitted audience uses the API server's
default. An explicit audience and expiration are preserved. Expiration defaults
to 3600 seconds and must be at least 600 seconds.

The shared controller rotates tokens at 80% of their lifetime or after 24 hours,
with its normal awake projection refresh. No per-Pod token daemon is added.
Controller credentials need `get` on `serviceaccounts` and `create` on
`serviceaccounts/token`. Tokens use read-only guest tmpfs and private stdin
transport. They are absent from controller journals and command arguments, but
RAM checkpoints contain mounted tokens and any copies in process memory.

Sleeping Pods do not poll the API. Wake requests new tokens before restoring the
VM and requires projection before forwarded traffic; running-controller recovery
also refreshes identity before controller proxy routes reopen. For token-bearing
Pods, capture freezes application processes and restartable init sidecars. Restore
refreshes their projections before thawing those exact processes, so background
applications read fresh projected files after resume. Tokens already cached by
an application still depend on its normal refresh behavior. Projected directories
update atomically; applications must reopen files.
As in Kubernetes, `subPath` pins the selected file and does not rotate tokens.

DownwardAPI supports Pod name, namespace, UID, labels and annotations, including
individual label and annotation keys. Resource fields, ClusterTrustBundle,
PodCertificate, CSI, host paths, mount propagation and recursive read-only
options are rejected. `fsGroup` and supplemental groups are supported. Native
local PVC limits are documented in the storage guide above.

## Image pull policy

`IfNotPresent` and `Never` inspect the guest containerd cache and only reuse
fully downloaded, unpacked images. `IfNotPresent` pulls a missing image;
`Never` fails when it is missing. `Always` resolves the image through the registry
on every container start. With an omitted policy, an untagged image or `:latest`
defaults to `Always`; other tags and digests default to `IfNotPresent`.

The cache belongs to the Pod VM and survives hibernation and container restarts.
A new Pod VM has an empty application-image cache, so `Never` cannot bootstrap
it without preloading. This preview does not share application images across
Pod VMs. Cached `IfNotPresent` restarts do not depend on registry DNS or availability.

Pods can reference `imagePullSecrets` in their own namespace, using
`kubernetes.io/dockerconfigjson` or legacy `kubernetes.io/dockercfg` Secrets.
Registry host patterns and repository path prefixes select matching credentials.
Secrets are fetched for each actual pull, so credential rotation applies to a
later `Always` start. Cached `IfNotPresent` and `Never` starts skip secret lookup.
The controller needs `get` permission for these Secrets.

Missing or malformed referenced Secrets fail the pull with a sanitized error.
External credential helpers and token-only Docker config entries are unsupported;
provide username/password credentials. Registry HTTPS verification stays enabled.
Credentials travel to the guest over stdin and are not stored in the controller
journal or command arguments. During a pull, containerd's `ctr --user` briefly
holds them in guest process arguments, outside application PID namespaces.

## Init containers

Ordinary init containers execute sequentially before the regular applications.
They share the Pod's volumes and network and support the same image defaults,
environment, resource limits and process security settings. Each must exit
successfully before the next starts. `restartPolicy: Never` makes an init failure
fail the Pod; `OnFailure` and `Always` retry it with backoff.

Restartable init containers declare `restartPolicy: Always` on the init
container itself. They stay running alongside later stages and applications.
The next init stage waits until the sidecar has started and its startup probe
has succeeded, if present. Sidecar readiness probes affect actual Pod readiness,
and liveness/startup failures restart only that sidecar. They restart even when
the Pod uses `Never`; completed init stages are not replayed after a restart.
After all regular applications finish, sidecars stop in reverse declaration
order so Jobs can complete. Pod deletion stops regular applications before
sidecars, using one shared termination grace deadline. Lifecycle hooks run before
readiness (`postStart`) or SIGTERM (`preStop`), and never on ordinary RAM sleep or
restore. Graceful deletion can resume a sleeping Pod to run its `preStop` hook
within the remaining grace period. Cold and zero-grace deletions never boot for
hooks. Ephemeral debugging containers remain unsupported.

Status exposes `initContainerStatuses` and the `Initialized` condition. Init logs
are available through `kubectl logs POD -c INIT_NAME`. Completed initialization
survives regular application restart, RAM hibernation and controller recovery.
An uncertain interrupted launch fails closed rather than repeating its effects.
Logs retain bounded current and previous attempts and support following,
timestamps, time filters, tails and byte limits. Reading sleeping logs never
wakes a VM. Older checkpoints without timestamp capture keep readable raw logs
until their container restarts with the updated helper. Named exec is available
while an init container is running. See
[application operations](../../tools/microsandbox-pods/OPERATIONS.md) for retention
bounds, hook recovery and projection semantics.
Initialization must complete before the Pod can hibernate.

Proxy availability can report a cold Pod Ready while `Initialized` is still
false. This is the selected local availability policy: init execution starts on
first wake, and traffic waits for initialization and actual application readiness.
Strict Pods stay Pending until initialization completes. Initialization does not
hold a concurrent-start slot for its entire lifetime, but its running VM counts
against the active RAM budget.

## Health checks

Readiness, startup and liveness probes support exec, HTTP/HTTPS, TCP and gRPC actions.
Their timings and thresholds follow the
[Kubernetes probe definitions](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/),
subject to the preview limitations below.
HTTP/TCP/gRPC probes require the Linux `--network-relay-binary`, including in strict
mode. The peer checks the actual guest Pod address, or the private backend
address in proxy mode, without using a wake listener. Numeric probe ports need
not be declared as application ports; named ports must identify a declared TCP
port. HTTP headers, paths, success/failure thresholds, delays and timeouts are
honored. HTTPS checks skip certificate verification, as Kubernetes does.

Startup probes gate readiness and liveness. A failed startup or liveness probe
terminates the task with SIGTERM, waits the probe-specific or Pod termination
grace period, then escalates to SIGKILL if needed. The existing Pod restart policy
and backoff determine replacement of the container; the Pod UID and VM remain.
`Never` pods terminate on probe failure. Container restart-policy overrides are
not supported. Readiness failure alone never restarts a task.

Cold and sleeping guests are not probed. Successful startup is journaled and
survives RAM resume and controller recovery; a fresh container restart runs its
startup probe again. Readiness must pass again after restore. These internal
health checks do not renew the idle timer. External traffic to a proxy, including
an external monitoring request, still wakes the app and counts as activity.

gRPC probes use the standard health Check RPC, an optional named service and a
numeric port. Only SERVING succeeds; timeouts, unknown services and connection
failures count as failed checks. Probe `host` overrides are rejected. To select an HTTP virtual host,
use an HTTP `Host` header. Same-host HTTP redirects are followed; redirects to
another host or a redirect loop return success with a warning, matching the
Kubernetes probe convention. Probes run serially per Pod, so a slow probe can
delay the next check; this preview does not provide kubelet's independent probe
workers or Kubernetes probe Events yet.

Rebuild the controller and Linux relay together. An older running relay with probe contract 1 must be shut down through its private authenticated control socket
before installing the new binary and restarting the controller. The controller
rejects an incompatible relay rather than silently accepting failed checks.

## Capacity planning

The controller's `--max-pods` defaults to 512 tracked pod identities, including
sleepers. Its `--max-active-memory-mib` defaults to 4096 MiB of active pod guest
RAM. Each pod defaults to a 4 GiB private root disk. Set the immutable Pod
annotation `microsandbox.dev/root-disk-size: "8Gi"` to choose another size, at
least 1 GiB. Sizes round up to whole MiB and participate in shared host disk
admission. The effective geometry is recorded before boot and retained across
recovery and RAM restore. Replace the Pod to resize its root disk; existing
journals retain their original 4 GiB size. This annotation does not implement
Kubernetes `ephemeral-storage` requests, limits or disk `emptyDir.sizeLimit`.
New guest CPU and RAM sizes cover the larger of
regular applications plus all restartable init sidecars, or each sequential init
stage plus the sidecars already started before it. Each container contributes
the greater of its request and limit:
CPU rounds up to a whole vCPU, and memory
adds 256 MiB for the guest kernel, agent and containerd, then rounds up to 2 MiB.
The minimum remains one vCPU and 768 MiB. For example, a 1 GiB application memory
limit creates a 1280 MiB guest. A pod that cannot fit the configured active budget
is rejected before creating a VM.

CPU requests set scheduler reservations and container CPU shares. Only declared
limits set CPU quotas and memory ceilings; requests are not converted into hard
limits. Memory-limit kills report `OOMKilled` from the kernel's `oom_kill`
counter. An ordinary SIGKILL does not acquire that reason. A read-only cgroup
mount lets applications inspect their own limits. Limits and RAM state
survive hibernation. The allocation is journaled once, so controller upgrades do
not resize or miscount existing guests. In-place Pod resource resizing is not
supported; replace the Pod to change its profile.

This budget is independent of Floci's Java runtime budget for RDS and
generic standalone applications. Allocate both budgets from the host's available RAM
after leaving space for the host, Floci, Docker, brokers and the Kubernetes
control plane. CPU/memory are supported; extended resources, hugepages,
ephemeral-storage reservations/limits, Pod-level resources and RuntimeClass
overhead remain rejected.

Kubernetes scheduling still counts each pod's declared resource requests while
it sleeps. Hibernation frees actual VM RAM; it does not remove scheduler
reservations or rewrite requests. Set `--scheduling-cpu` and
`--scheduling-memory` to advertise virtual capacity independently of the active
guest RAM budget. Both accept positive Kubernetes quantities. For example:

```sh
--max-pods 512 --scheduling-cpu 500 --scheduling-memory 384Gi --max-active-memory-mib 4096
```

This allows 500 cold pods requesting one CPU and 768 MiB each to schedule. Each
new guest receives 1024 MiB including headroom, so four can fit the 4 GiB active
budget. A wake waits
for admission when that budget is full; advertising virtual capacity does not
increase physical RAM or evict active workloads. Keep enough active capacity for
the dependency graph used by an actual request. Without the new flags, the node
continues advertising four CPUs and the configured active memory budget.

The shared controller uses a bounded Kubernetes API client, defaulting to
`--kube-api-qps 50 --kube-api-burst 100`. These limits allow a large recovered Pod
set to publish readiness promptly. Lower them for a constrained control plane;
registration and recovery then take longer.

`--pod-workers` bounds concurrent Kubernetes reconciliations, including deletion
of cold pods. It defaults to eight and accepts 1 through 64. Increase it only
when cleanup is waiting on acknowledgements rather than control-plane capacity.
The separate two-guest start limit and RAM/disk admission still govern VM
activation. A Kubernetes Pod disappearing does not prove its owned runtime
cleanup has finished; inspect remaining journal states before removing the
controller or cluster.

The default pod-count limit is a configuration ceiling, not a measured claim that
500 applications fit or perform well. Cold registration avoids the cost of
booting every application during deployment. Once an application has run,
checkpoint storage, image layers and VM disks also need capacity. Qualification
must measure the application's active working set, concurrent resume latency,
idle controller memory and disk use before treating this as a complete enterprise
development environment.

Clients need timeouts that allow for first boot and application startup. A
gateway startup timeout does not extend the caller's own timeout. Initial image
pulls take longer than restoring an existing RAM checkpoint.

## Measured qualification

On 2026-10-16, the expanded native qualification on Apple Silicon macOS with
OrbStack and microsandbox 0.7.8 completed 49 native fixture checks successfully.
That run deployed 500 replicas of one application, each requesting
one CPU and 768 MiB: 500 CPUs and 375 GiB of retained scheduler reservations.
The node advertised 500 CPUs and 384 GiB independently of its 4 GiB active guest
RAM budget.

| Measurement | Result |
| --- | --- |
| All 500 replicas proxy-Ready and COLD, with init deferred | 52.754 seconds |
| Application VMs created at that point | 0 |
| Active guest RAM | 0 MiB |
| Shared controller RSS | 125.125 MiB |
| Shared Linux relay RSS | 13.094 MiB |
| Combined controller and relay RSS | 138.219 MiB |
| First Service request booting and initializing one selected pod | 37.369 seconds |
| Controller recovery with 500 replicas | 64.245 seconds |
| Service request restoring a sleeping proxy pod | 2.066 seconds |
| Strict pod hibernate, two cycles | 2.475 / 2.295 seconds |
| Strict pod resume, two cycles | 3.715 / 4.685 seconds |
| Checkpoint allocation for a 768 MiB guest, two cycles | 590.320 / 591.508 MiB |

The other 499 replicas remained cold and uninitialized after the first request.
The selected pod's initializer ran once and remained completed across RAM resume.
Recovery included
those cold replicas, one sleeping replica and a separate running native client
pod. Its timing ends when the controller's lifecycle health endpoint responds;
it does not measure publication of every Kubernetes Ready condition.

The native checks also verified sequential init failure/retry and recovery,
subpath pinning and escape rejection, private registry pulls and credential
rotation, image defaults, security settings, CPU/memory limits, OOM attribution,
mounted configuration, probes, networking, logs/exec, listener loss,
active-connection protection and RAM retention. A separate post-rebase native
fixture verified Java gateway wake, deferred init execution and two further
hibernate/resume cycles. All 73 Java tests passed without failures, errors or
skips. The native and Java phases were verified in separate runs.
Both runs removed their owned VMs, checkpoints, containers and fixture volumes.
After the rebase, 192 Linux controller unit tests, the macOS Go race suite and
`go vet` passed. Linux skipped one test requiring an explicitly owned Docker
peer; the native fixture covers actual peer networking. Native Linux execution
has not been qualified.

RSS excludes Kubernetes, Docker, Floci, host overhead and filesystem caches.
Fixture timings include readiness and verification work. These results cover one
application shape, not capacity for 500 distinct applications or 500 retained
checkpoints. Previously activated applications also need private disks and
checkpoint storage. Image downloads, working sets and concurrent wakes affect
real workloads.

## Shared environment network boundary

Native Pods can use an existing isolated Floci environment through a protected
managed peer. The SDK filters direct guest traffic; the peer filters inner traffic
after Service destination translation, and the outer Docker firewall constrains
traffic leaving its bridge. The controller verifies actual rules and exact owned
transport allowances, not just the network-file fingerprint. Kubernetes
NetworkPolicy adds Pod-level restrictions independently.

Use a fresh managed cluster record, the same exact network YAML as Floci, a fixed
peer UDP port allowed by `network.native.allowed-egress`, and preloaded k3s system
images. Protected setup enables secure networking, requiring every native regular
and init container to drop `NET_RAW` or `ALL`. Shared CoreDNS and Floci DNS remain
infrastructure services; no per-Pod DNS sidecar is added. Global explicit IPv4
TCP/UDP allowances, HTTPS simulator routes and the configured registry CA are
described in [workload networking](workload-network.md#native-microvm-boundary).

See the [managed setup commands](../../tools/microsandbox-pods/README.md#development-cluster-setup)
and [migration instructions](../../tools/microsandbox/MIGRATION.md#native-pod-environment-isolation-requires-a-protected-peer).
Protected-environment native qualification is still being run. The separate
NetworkPolicy matrix has passed 21 checks, and its restricted installed smoke
passed seven; those results do not substitute for the environment-boundary run.

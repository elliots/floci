# Moving from the on-demand runtime to smolvm-pods

A note for trying your application on smolvm-pods after Floci's on-demand runtime. The branch
`feat/on-demand-runtime` is the newest state of that runtime, and
`backup/elliots-on-demand-runtime-20261018` is an older copy of it, without `rabbitmq-queues`.
Delete this note once you have moved.

- [What changes](#what-changes)
- [Configuration fields](#configuration-fields)
- [Try your application](#try-your-application)
- [What does not work yet](#what-does-not-work-yet)
- [What to watch and report back](#what-to-watch-and-report-back)
- [Roll back](#roll-back)

## What changes

| On-demand runtime | smolvm-pods |
| --- | --- |
| Floci scaled an idle Deployment or ECS service to zero, or stopped an EC2 instance, and the next request or message started it cold | Pods keep running. An idle pod sleeps: its VM's memory is saved to the node's disk and the VM process exits. A wake restores that memory in about 0.2 s, so the app goes on where it was, with no cold start |
| Kubernetes saw 0 replicas while a workload slept | Kubernetes sees the pod Running and Ready, with the same IP and no restarts |
| Calls went through Floci's HTTP gateway on 127.0.0.1:8080, routed by Host to a `backend-url` | No gateway. Clients call Services, NodePorts or pod IPs directly. Nothing is proxied, and the app sees the address the packet arrives with: the calling pod's IP, or the node's for a NodePort |
| HTTP, gRPC and WebSocket calls through the gateway woke a workload, and an open WebSocket held it awake | Any incoming TCP connection wakes the pod: its first SYN is held until the pod is awake. UDP does not wake it. A quiet WebSocket, SSE or gRPC stream no longer holds the pod awake, and the pod's pushes on it stop while it sleeps |
| A configured SQS or RabbitMQ queue woke its consumers | Not yet ([What does not work yet](#what-does-not-work-yet)) |
| Configured GET and HEAD health paths got a synthetic answer | kubelet's own probes are answered while the pod sleeps, with the results the app gave just before it slept. Any other health checker is a client and wakes the pod |
| Floci's YAML file listed the workloads | Each pod opts in with the RuntimeClass `smolvm-pods` and tunes itself with annotations |

A pod sleeps by itself when, for its idle timeout (5 minutes by default), no packet passed on a
connection opened to it other than kubelet's probes, its VM used no more than 0.5 vCPU on average
over 60 s, nothing holds it awake (an exec, an API lease), and every probe passes. TCP keepalives
and HTTP/2 or gRPC pings on a caller's connection to the pod are packets too, so they keep it
awake ([Known limits](docs/architecture.md#known-limits)). Its own outbound connections (a
database pool, a queue poll, DNS) neither keep it awake nor wake it.
[The idle rule](docs/architecture.md#the-idle-rule) and [health probes while a pod is not
awake](docs/architecture.md#health-probes-while-a-pod-is-not-awake) have the details.

A Deployment opts in on its pod template. Annotations go on the template's metadata, not on the
Deployment's own, and their values are quoted strings:

```yaml
spec:
  template:
    metadata:
      annotations:
        smolvm-pods.floci.io/idle-timeout: "2m"
    spec:
      runtimeClassName: smolvm-pods
```

| Annotation | Use |
| --- | --- |
| `smolvm-pods.floci.io/idle-timeout` | The pod's idle timeout, from `10s` to `24h`, such as `"2m"` |
| `smolvm-pods.floci.io/hibernate` | `"false"`: the pod never sleeps |
| `smolvm-pods.floci.io/cpu-threshold` | vCPUs above which the pod is busy and stays awake, such as `"1"`; `"off"` lets it sleep however busy it is |
| `smolvm-pods.floci.io/cpu-window` | The time over which CPU is averaged, from `5s` to `1h` |
| `smolvm-pods.floci.io/wake-on-terminate` | `"true"`: SIGTERM wakes a sleeping pod and reaches the app, for a graceful shutdown on a delete or a rollout. Without it, a sleeping pod reports exit 143 at once and its shutdown code never runs |
| `smolvm-pods.floci.io/rosetta` | `"false"` turns off Rosetta, which amd64 images need. It is on by default |

All of them are in [docs/configuration.md](docs/configuration.md#pod-annotations).

## Configuration fields

Fields of `on-demand.yaml` (`FLOCI_ON_DEMAND_CONFIG_FILE`), for each workload under `workloads`:

| Field | Now |
| --- | --- |
| `enabled` | Not needed. Every pod of the RuntimeClass may sleep, and `smolvm-pods.floci.io/hibernate: "false"` keeps one awake. To turn a workload off, scale it to 0: nothing scales it back |
| `account-id`, `region` | Come with Floci's message wakes, to find the queues |
| `host` | Not needed: there is no gateway. Callers use the Service that `backend-url` named |
| `idle-timeout-seconds` | The annotation `smolvm-pods.floci.io/idle-timeout`, from `10s` to `24h`, or `FLOCI_NODE_IDLE_TIMEOUT` in the node's `/etc/floci-node/env` for every pod ([Changing a node setting](#changing-a-node-setting)). The default is 5 minutes, not 120 s |
| `startup-timeout-seconds` | Not needed: a wake never starts the app again. A pod's first start is an ordinary pod start, with its own `startupProbe` |
| `queues` | Comes with Floci's message wakes. Until then, mark consumers `smolvm-pods.floci.io/hibernate: "false"` |
| `rabbitmq-queues` (`broker-id`, `virtual-host`, `queue`, `username-env`, `password-env`) | Comes with Floci's message wakes. Until then, mark the consumers `smolvm-pods.floci.io/hibernate: "false"`. The observer credential variables it names are not needed |
| `health.requests` | No replacement: a health checker other than kubelet is a client and wakes the pod. Drop such checks or point them elsewhere. kubelet's probes are answered while the pod sleeps |
| `health.sleeping-response` (`status`, `body`, `content-type`) | Not needed: a sleeping pod answers a kubelet probe with the status it last gave, and an empty body |
| `readiness` (`enabled`, `path`, `status`) | Not needed: the pod's own `readinessProbe`. A pod sleeps only when its probes pass |
| `runtime.type: kubernetes` | `runtimeClassName: smolvm-pods` on the pod template |
| `runtime.type: ecs`, `runtime.service` | No replacement: only Kubernetes pods sleep |
| `runtime.type: ec2`, `runtime.instance-id`, `runtime.backend-port` | No replacement: only Kubernetes pods sleep |
| `runtime.cluster-name` | Not needed today: the node you start by hand is the cluster. Comes back when Floci's EKS service creates the node |
| `runtime.namespace`, `runtime.deployment` | Not needed for sleep and wake, which go by pod. Come back with Floci's message wakes, to name a queue's consumer |
| `runtime.replicas` | Not needed: the Deployment's own `replicas`, which nothing changes |
| `runtime.backend-url` | Not needed: that Service is now what callers use |
| `runtime.backend-protocol` | Not needed: nothing is proxied, so HTTP/1.1, h2c, gRPC, WebSocket and any other TCP protocol pass unchanged |

Floci's environment:

| Variable | Now |
| --- | --- |
| `FLOCI_ON_DEMAND_ENABLED` | Not needed: Floci takes no part yet. Leave it off ([Try your application](#try-your-application), step 1), unless that Floci also serves your old cluster ([Roll back](#roll-back)) |
| `FLOCI_ON_DEMAND_CONFIG_FILE` | Comes back with Floci's message wakes, in a new format with its own guide |
| `FLOCI_ON_DEMAND_GATEWAY_HOST`, `FLOCI_ON_DEMAND_GATEWAY_PORT` | Not needed: there is no gateway |
| `FLOCI_ON_DEMAND_RECONCILE_INTERVAL_MILLIS` | Not needed: the node reads the traffic to its pods every 15 s. Queue checks come with Floci's message wakes |
| `FLOCI_ON_DEMAND_MAX_CONCURRENT_STARTS` | Not needed: a node runs at most 4 wakes and 2 sleeps at once. `FLOCI_NODE_MAX_AWAKE_PODS` and `FLOCI_NODE_MAX_AWAKE_MEMORY` in the node's `/etc/floci-node/env` cap awake pods, and are off by default ([Changing a node setting](#changing-a-node-setting)) |
| `FLOCI_ON_DEMAND_MAX_PENDING_REQUESTS`, `FLOCI_ON_DEMAND_MAX_PENDING_REQUESTS_PER_WORKLOAD` | Not needed: nothing queues requests. The node holds up to 1,024 frames or 4 MiB for a waking pod, and clients resend the rest |
| `FLOCI_SECURITY_ALLOW_UNSAFE_NETWORK_EXPOSURE`, for the gateway | Not needed: pods reach Floci on the Mac's loopback |

Fields of `network.yaml` (`FLOCI_NETWORK_CONFIG_FILE`):

| Field | Now |
| --- | --- |
| `network.isolation.enabled` | Comes with egress control. A coarse stopgap is in [What does not work yet](#what-does-not-work-yet) |
| `network.routes.NAME.origins`, exact and wildcard | Come with Floci's simulators and DNS. A stopgap for exact names is in [What does not work yet](#what-does-not-work-yet) |
| `network.routes.NAME.target.backend-url`, `target.preserve-host` | The same |
| `FLOCI_CA_BUNDLE` and the trust admission webhook | Come with egress control and trust, which puts Floci's CA in every container |
| Floci in Docker with `--dns 127.0.0.1`, `FLOCI_SERVICES_DOCKER_NETWORK`, `FLOCI_DOCKER_RESOURCE_NAMESPACE` for isolation | Not needed for the node |

The later work is listed in the roadmap's
[work left](../../docs/design/smolvm-runtime-roadmap.md#work-left-in-order).

### Changing a node setting

`floci-node` reads the node service's settings from the node's `/etc/floci-node/env` each time
it starts the node service: `FLOCI_NODE_IDLE_TIMEOUT`, `FLOCI_NODE_CPU_THRESHOLD` and
`FLOCI_NODE_CPU_WINDOW` for idle sleep, and `FLOCI_NODE_MAX_AWAKE_PODS`,
`FLOCI_NODE_MAX_AWAKE_MEMORY` (MiB, every pod VM counted as 1024) and `FLOCI_NODE_ADMISSION_WAIT`
for wake admission. The caps are empty, so off, by default. After a change, restart the node
service; `floci-node` starts it again 1 s later, and the pods keep running. The file survives
restarts of the node:

```sh
scripts/node-vm.sh exec -- sed -i 's/^FLOCI_NODE_IDLE_TIMEOUT=.*/FLOCI_NODE_IDLE_TIMEOUT=2m/' /etc/floci-node/env
scripts/node-vm.sh exec -- sed -i 's/^FLOCI_NODE_MAX_AWAKE_PODS=.*/FLOCI_NODE_MAX_AWAKE_PODS=4/' /etc/floci-node/env
scripts/node-vm.sh exec -- pkill -f "^smolvm-pods node-service"
```

A wake over a cap waits up to 10 s for room, then is refused. [The node's
settings](docs/configuration.md#the-nodes-settings) lists every setting, and [Wake
admission](node/README.md#wake-admission) says how to size the caps.

## Try your application

1. **Run Floci on the Mac** as usual, on port 4566, with `FLOCI_BASE_URL=http://100.96.0.1:4566`
   when apps follow the URLs that Floci returns (step 5). Any build works for the runtime, since
   Floci takes no part yet. Keep your on-demand branch's build, with on-demand off, if the app
   uses its other changes, such as the `/_aws/rds/tls` helpers. `FLOCI_ON_DEMAND_ENABLED` stays
   off because its coordinator would scale the deployments it is configured for, and a workload
   without `cluster-name` uses your current kubeconfig context, which may be the node.

2. **Start a node by hand**, as [docs/getting-started.md](docs/getting-started.md) describes
   from [What you need](docs/getting-started.md#what-you-need) to
   [Start a node](docs/getting-started.md#start-a-node). It needs an Apple silicon Mac of M3 or
   later, Docker with the containerd image store, and a smolvm checkout at the commit that
   `scripts/build.sh` pins, beside the floci repository (`../smolvm` from its root) or named by
   an exported `SMOLVM_SRC`, with smolvm-floci prepared in its `target/floci`
   ([The checkouts](docs/getting-started.md#the-checkouts)). Then, from `tools/smolvm-pods`:

   ```sh
   scripts/build.sh
   node/libkrun/build.sh
   node/kernel/build.sh
   ```

   Build the patched guest agent if its checksum is not `AGENT_SHA256`
   ([The guest agent](docs/getting-started.md#the-guest-agent)), pull the images that
   `node-vm.sh build` needs ([The node image](docs/getting-started.md#the-node-image)), and start
   the node:

   ```sh
   scripts/node-vm.sh build
   scripts/node-vm.sh create -- --port 17070:7070
   scripts/node-vm.sh start
   mkdir -p ~/.kube
   scripts/node-vm.sh kubeconfig >~/.kube/smolvm-pods-node.yaml
   export KUBECONFIG=~/.kube/smolvm-pods-node.yaml
   ```

   `create` gives the node 4 vCPUs, 6 GiB and a 40 GiB storage disk. An awake pod can take about
   1.1 GiB of the node's memory, and the default node has about 5.2 GiB free, room for about four
   awake pods that use all of their RAM. More awake at once start the node's OOM killer, which
   kills a pod's VM: the pod fails, and kubelet recreates it with its memory lost. For more pods,
   give the node more, such as `create --cpus 8 --mem 16384`,
   and cap the awake pods with `FLOCI_NODE_MAX_AWAKE_PODS`
   ([Changing a node setting](#changing-a-node-setting)). Every container keeps a full copy of
   its image on the storage disk, so for many or large images add `--storage GIB`. The node
   takes the Mac's port 6443 for its Kubernetes API, NodePorts 30080 to 30099 on the same Mac
   ports, and 17070 for the node service's API; `--api-port`, `--node-ports` and the `--port`
   mapping move them.

3. **Import your images.** The node pulls none:

   ```sh
   docker save orders:dev | scripts/node-vm.sh exec -i -- ctr -n k8s.io images import --platform linux/arm64 -
   ```

   An amd64 image runs under Rosetta: import a single-platform amd64 image,
   `docker save --platform linux/amd64 IMAGE`, with `--platform linux/amd64`. Set
   `imagePullPolicy: IfNotPresent` or `Never` on every container: `Always`, the default for a
   `:latest` tag or no tag, asks a registry and fails. The import names an image only by its tag,
   so a manifest that names one by digest, as Helm charts often do, also needs the digest name in
   the node, which `ctr -n k8s.io images tag` adds
   ([Import the image](docs/getting-started.md#import-the-image)).

4. **Change your manifests.**
   - Add `runtimeClassName: smolvm-pods` and the annotations to each pod template that should
     sleep. Pods without it run in runc on the same node and never sleep.
   - Where you had `idle-timeout-seconds: 120`, add `smolvm-pods.floci.io/idle-timeout: "2m"`.
   - Mark queue consumers, and pods that do work on their own timers, with
     `smolvm-pods.floci.io/hibernate: "false"`.
   - Remove what sent application names to Floci's gateway: Services, DNS entries or ingress
     rules for names such as `orders.dev.test`. Callers use the backend Services directly.
   - For calls from the Mac, use a NodePort Service with a `nodePort` from 30080 to 30099. The
     node has no ingress controller, no LoadBalancer, no storage class and no metrics (traefik,
     servicelb, local-storage and metrics-server are off), so an Ingress does nothing and a PVC
     stays Pending.
   - Point the AWS SDKs at Floci (step 5).

   For example:

   ```yaml
   apiVersion: apps/v1
   kind: Deployment
   metadata:
     name: orders
     namespace: integration-test
   spec:
     replicas: 1
     selector:
       matchLabels:
         app: orders
     template:
       metadata:
         labels:
           app: orders
         annotations:
           smolvm-pods.floci.io/idle-timeout: "2m"
       spec:
         runtimeClassName: smolvm-pods
         containers:
           - name: orders
             image: orders:dev
             imagePullPolicy: IfNotPresent
             env:
               - name: AWS_ENDPOINT_URL
                 value: http://100.96.0.1:4566
             ports:
               - containerPort: 8080
             readinessProbe:
               httpGet:
                 path: /ready
                 port: 8080
   ---
   apiVersion: v1
   kind: Service
   metadata:
     name: orders
     namespace: integration-test
   spec:
     type: NodePort
     selector:
       app: orders
     ports:
       - port: 8080
         nodePort: 30080
   ```

   A new node has only the default namespaces, so create yours first with
   `kubectl create namespace integration-test`. Then apply the manifests with `kubectl apply -f`,
   or run your tool with `KUBECONFIG` set to the node's.
   `curl --resolve orders.dev.test:8080:127.0.0.1 http://orders.dev.test:8080/orders` becomes
   `curl http://127.0.0.1:30080/orders`.

5. **Reach Floci from pods.** TCP to the node's default gateway, smolvm's, reaches the Mac's
   loopback, so pods reach Floci at `http://100.96.0.1:4566`. Check the address with
   `scripts/node-vm.sh exec -- ip -4 route show default`. Give it to the apps as their endpoint:
   `AWS_ENDPOINT_URL`, which current AWS SDKs read, or the app's own setting, with `AWS_REGION`
   and the credentials your tools use. Names under `localhost.floci.io` resolve to 127.0.0.1,
   which in a pod is the pod itself.

   For a name instead of the address, a Service without a selector should work, though the
   `network` end-to-end suite reaches the Mac by the address. Pods in its namespace then reach
   Floci at
   `http://floci:4566`, and their S3 clients need path-style addressing (`forcePathStyle`, or
   `addressing_style: path` in boto3). With a host name as the endpoint, the AWS SDKs call
   `BUCKET.floci`, which does not resolve; only an IP endpoint such as
   `http://100.96.0.1:4566` gets path-style addressing on its own.

   ```yaml
   apiVersion: v1
   kind: Service
   metadata:
     name: floci
     namespace: integration-test
   spec:
     ports:
       - port: 4566
   ---
   apiVersion: discovery.k8s.io/v1
   kind: EndpointSlice
   metadata:
     name: floci
     namespace: integration-test
     labels:
       kubernetes.io/service-name: floci
   addressType: IPv4
   ports:
     - port: 4566
   endpoints:
     - addresses: ["100.96.0.1"]
   ```

   URLs that Floci writes into its answers name `localhost:4566` by default: an SQS `QueueUrl`,
   for example, or a pre-signed URL that Floci returns, such as an SES export job's. For pods to
   follow them, start Floci with `FLOCI_BASE_URL=http://100.96.0.1:4566`. Clients on the Mac
   itself then get URLs they cannot reach, so give those clients an explicit endpoint.

6. **Check that pods sleep and wake.**

   ```sh
   scripts/node-vm.sh exec -- smolvm-pods ctl list | jq -r '.pods[] | "\(.namespace)/\(.name) \(.state)"'
   ```

   - Leave the app alone for its idle timeout and up to 15 s more: its state goes from `awake` to
     `asleep`, and `kubectl get pods` still shows it Running and Ready with no restarts.
   - Call it with `curl http://127.0.0.1:30080/orders`. The first request takes about 0.2 s
     longer than when the pod is awake, and the state is `awake` again.
   - `scripts/node-vm.sh exec -- smolvm-pods ctl events` prints each change with its reason. A
     sleep is `awake` to `pausing` to `asleep` with the reason `idle`. A wake is `asleep` to
     `waking` to `awake` with the reason `traffic`, and the caller and port in `reason_detail`.
   - An idle pod whose pause is refused, by the pre-sleep probe check or `hibernate: "false"`,
     says why: an event from `awake` to `awake` with the reason `idle` and the error
     `not sleeping: REASON`.
   - A pod that the activity monitor keeps busy gets no event. That covers recent traffic, such
     as TCP keepalives or HTTP/2 or gRPC pings on a caller's pooled connection, a recent wake,
     CPU above the threshold, CPU not measured yet, conntrack that could not be read, and no
     recorded IP. The node service logs these only at debug level, as
     `pod sandbox ID is busy: REASONS` (below), and an idle pod that a lease, a running exec or a
     container create keeps awake as `is idle, but busy with leases, execs or creates`.
   - `scripts/node-vm.sh exec -- smolvm-pods ctl get integration-test/POD` shows
     `"hibernate": false` for a pod marked `hibernate: "false"`.
   - Let pods sleep by themselves. `ctl hibernate` works, but a pod that never slept by itself
     has no recorded probe results, so kubelet's next probe wakes it.

   `ctl events` prints the events so far and exits. To follow them live:

   ```sh
   scripts/node-vm.sh exec -- curl -sN --unix-socket /run/smolvm-pods/api.sock 'http://smolvm-pods/v1/events?follow=true'
   ```

   To see why a busy pod stays awake, set `RUST_LOG=debug` in `/etc/floci-node/env` and restart
   the node, since `RUST_LOG` reaches the node service only at a node restart. Every pod starts
   again cold. Then search the node service's log for `is busy:`:

   ```sh
   scripts/node-vm.sh exec -- sed -i 's/^RUST_LOG=.*/RUST_LOG=debug/' /etc/floci-node/env
   scripts/node-vm.sh stop
   scripts/node-vm.sh start
   scripts/node-vm.sh exec -- grep 'is busy:' /var/log/smolvm-pods/node-service.log
   ```

## What does not work yet

- **Floci does not create the node.** Start it by hand as above. Floci's EKS clusters still run
  their pods in runc, without the RuntimeClass, so they refuse pods that name `smolvm-pods`. If
  your tool creates its cluster through Floci's EKS API, point its deploy step at the node's
  kubeconfig instead.
- **Message consumers do not wake for messages.** A message for a sleeping consumer, and the
  answer to its long poll, wait until something else wakes it. Mark consumers of SQS, SNS, IoT,
  RabbitMQ or MQTT with `smolvm-pods.floci.io/hibernate: "false"` for now. Do the same for pods
  that run work on their own timers, such as schedulers and poll loops, which pause while the pod
  sleeps. Floci's message wakes will wake consumers through the node service's API, from a
  configuration like your `queues` and `rabbitmq-queues`.
- **No simulators through host overrides.** Floci does not resolve vendor hostnames to
  simulators, issue their certificates, or put its CA in pods. Point the app at the simulator's
  Service through the app's own setting where it has one. Otherwise, `hostAliases`, untested on
  VM pods, maps an exact hostname to the simulator Service for one pod: give that Service a fixed
  `clusterIP` from the node's Service range, 10.43.0.0/16, and list it in the app's pod template:

  ```yaml
  spec:
    template:
      spec:
        hostAliases:
          - ip: 10.43.200.10
            hostnames: ["api.vendor.test"]
  ```

  HTTPS then needs a certificate that the app already trusts. Wildcard origins have no stopgap.
  A simulator in the RuntimeClass sleeps, and wakes on calls, like any pod.
- **No outbound blocking.** Pods reach the internet through the node. A coarse stopgap on a Mac
  is to create the node with `scripts/node-vm.sh create -- --allow-cidr 100.96.0.1/32`, which
  lets it send TCP and UDP only to smolvm's gateway, so pods still reach Floci and anything else
  on the Mac's loopback. DNS stays open, because the gateway answers every DNS query itself.
- **ECS services and EC2 instances** have no replacement: only Kubernetes pods sleep.
- **Limits that may hit your app** ([Known limits](docs/architecture.md#known-limits)):
  - Every pod VM has 2 vCPUs and 1 GiB of RAM, whatever the pod requests.
  - `kubectl exec -i` or `-t`, `kubectl attach`, `kubectl cp` into a pod and
    `kubectl port-forward` do not work on VM pods. `kubectl exec POD -- COMMAND` and
    `kubectl logs` do. Containers with `stdin: true` or `tty: true` are refused when they are
    created.
  - TCP keepalives and HTTP/2 or gRPC pings on connections to the pod, such as a caller's
    connection pool, keep it awake.
  - metrics-server is off, so a HorizontalPodAutoscaler and `kubectl top` get no metrics.
  - amd64 execs under Rosetta take about 2.4 s each, so an exec probe with a short timeout can
    fail in the pod's first minute.
  - `hostNetwork` pods are refused, and pods get IPv4 only.
  - A restart of the node VM loses every pod, awake or asleep; Deployments start them again cold.
  - Each container gets its own copy of its volumes when it is created: containers do not share
    an emptyDir, an emptyDir starts empty again when its container restarts, and ConfigMap,
    Secret and service account token volumes do not update. Every file of a copy is readable by
    all users, so a Secret's `defaultMode` is not kept, and programs that refuse a loosely
    permissioned key, such as PostgreSQL's `ssl_key_file` or OpenSSH, fail.
  - Floci's EKS pod identity (IRSA) is not available on the node.

## What to watch and report back

After a run, collect:

```sh
scripts/node-vm.sh exec -- smolvm-pods ctl list > pods.json
scripts/node-vm.sh exec -- smolvm-pods ctl events > events.jsonl
scripts/node-vm.sh exec -- cat /var/log/smolvm-pods/node-service.log > node-service.log
scripts/node-vm.sh exec -- cat /var/log/smolvm-pods/front-shim.log > front-shim.log
scripts/node-vm.sh exec -- cat /var/log/k3s.log > k3s.log
kubectl get pods -A -o wide > kubectl-pods.txt
kubectl get events -A > kubectl-events.txt
```

`floci-node`'s own log is the node VM's log, which the smolvm CLI that `node-vm.sh` runs prints.
From `tools/smolvm-pods`:

```sh
"${SMOLVM_SRC:-../../../smolvm}/target/floci/kernel/bin/smolvm-floci" machine logs --name smolvm-pods-node
```

Report back:

- pods that never sleep, with their `not sleeping:` reasons or their `is busy:` lines, and pods
  that sleep when you expect them awake;
- requests that failed, hung or were reset, with the time, to match them with the events;
- slow wakes (`duration_ms`, and `capture_to_release_ms` for a wake on traffic) and failed ones
  (an `error` in an event, or `state` `failed` with its `failure` in `ctl list`);
- container restarts or exits in `kubectl get pods`, with `kubectl describe pod` for each;
- how the app behaves after a wake: dropped database or broker connections, a wrong clock,
  stale configuration;
- `checkpoint_bytes` for each pod, and the node VM's memory on the Mac.

## Roll back

Nothing in Floci or in your old cluster changes, so going back undoes the steps above.

1. Stop the node with `scripts/node-vm.sh stop`, which keeps it: a later `start` brings it back,
   with every pod started again cold. `scripts/node-vm.sh delete` removes it, its disks and the
   images you imported, and smolvm's cached copies of its image when no other node uses them.
2. Remove `runtimeClassName: smolvm-pods` from your manifests. A cluster without the RuntimeClass
   refuses pods that name it, and ignores the annotations. Restore what sent application names
   to Floci's gateway. Restore the apps' old Floci endpoint, remove the `floci` Service and
   EndpointSlice, and unset `FLOCI_BASE_URL` if you set it to `http://100.96.0.1:4566`.
3. Run your on-demand branch's Floci as before, from a worktree of
   `backup/elliots-on-demand-runtime-20261018`, or of `feat/on-demand-runtime` when the app uses
   `rabbitmq-queues`, with JDK 25 as `JAVA_HOME`. Without a `network.yaml`, it runs on the Mac:

   ```sh
   git -C ~/repos/floci worktree add --detach ~/repos/floci-on-demand backup/elliots-on-demand-runtime-20261018
   cd ~/repos/floci-on-demand
   FLOCI_ON_DEMAND_ENABLED=true FLOCI_ON_DEMAND_CONFIG_FILE=/path/to/on-demand.yaml ./mvnw quarkus:dev
   ```

   With a `network.yaml` that has routes or isolation, that Floci refuses to start on the Mac
   (`Workload network routing requires Floci in Docker on a dedicated bridge network`), so run it
   in Docker as before: built from the worktree with
   `docker build -f docker/Dockerfile -t floci:on-demand .`, on a rootful Docker daemon, on the
   dedicated bridge network that `FLOCI_SERVICES_DOCKER_NETWORK` names, and with
   `--dns 127.0.0.1` for isolation. The worktree's `docs/configuration/workload-network.md`
   has the settings, under Docker deployment.

The two setups can run side by side with one Floci for both. Pods on the node reach Floci at
`100.96.0.1:4566`, which is the Mac's loopback port 4566, and two Flocis cannot both take it. Run
the on-demand Floci with `FLOCI_ON_DEMAND_ENABLED=true`, started without the node's
`KUBECONFIG`, since a workload without `cluster-name` uses the current kubeconfig context, and
with no node workloads in `on-demand.yaml`. Otherwise give the second Floci another Mac port,
such as `FLOCI_PORT=4567`; when that is the node's Floci, set the pods' `AWS_ENDPOINT_URL` to
`http://100.96.0.1:4567`, and its `FLOCI_BASE_URL` to the same. Floci takes the Mac's port 4566,
the node takes 6443, 30080 to 30099 and 17070, Floci's EKS clusters take 6500 to 6599, and the
gateway takes 8080.

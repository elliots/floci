# smolvm Runtime

smolvm-pods runs each Kubernetes pod in its own microVM and puts idle pods to sleep
(`tools/smolvm-pods`). Floci has one smolvm-pods node, which RDS, EKS and Lambda share
([The Floci node](#the-floci-node)): the runtime file's, or one that Floci creates. Floci calls the
node service of its nodes over their API. The runtime file tells Floci about the nodes it did not
create, the message consumers it wakes when messages arrive for them, and the nodes' network:
which hosts pods reach, which third-party hosts go to simulators, and how containers trust
Floci's CA.

| Variable | Key | Default |
|---|---|---|
| `FLOCI_RUNTIME_CONFIG_FILE` | `floci.runtime.config-file` | _(none)_: no runtime file, so no declared nodes and no workloads |
| `FLOCI_RUNTIME_DNS_LISTEN` | `floci.runtime.dns-listen` | _(empty)_: `127.0.0.1:15353`, or `0.0.0.0:15353` when Floci runs in a container ([Network](#network)) |

## The runtime file

```yaml
version: 1
instance-id: laptop
nodes:
  dev:
    url: http://127.0.0.1:17070
    token-file: /Users/me/.floci/dev-node-token
workloads:
  orders:
    queues: [orders-incoming]
    runtime:
      type: kubernetes
      node: dev
      namespace: shop
      deployment: orders
```

| Rule | Detail |
|---|---|
| Format | YAML, or JSON when the file name ends in `.json`. One document; YAML aliases (`*name`) are refused |
| `version` | Required, the number `1` |
| Top level | Only `version`, `instance-id`, `nodes`, `network` and `workloads` |
| Sections | `nodes`, `network` and `workloads`, each optional |
| Unknown fields and duplicate keys | Refused, at every level |
| An empty section | `nodes:` with nothing under it is the same as no `nodes` |
| Errors | Each names the path of the field at fault, such as `nodes.dev.url` or `workloads.orders.queues.0`, or the line and column of a syntax error |

`instance-id` is optional: 1 to 32 of `a-z`, `0-9` and `-`. It names this Floci in the leases
it takes on nodes, so that two Floci instances that call one node keep their leases apart.

### Startup

When `FLOCI_RUNTIME_CONFIG_FILE` is set, Floci refuses to start if the file is missing or
invalid, and names the first problem:

```
The runtime file /Users/me/.floci/runtime.yaml (FLOCI_RUNTIME_CONFIG_FILE) is invalid: nodes.dev.url: must be http:// or https:// with a host and a port, and nothing else
```

### Changes while Floci runs

Every 2 seconds Floci checks the file's modification time and size. When either changed, it
reads the whole file again:

- A valid file replaces every section at once.
- An invalid file is logged as an ERROR with its first problem, and the definitions in force
  stay until the file is fixed:

  ```
  ERROR [io.git.hec.flo.run.nod.RuntimeFileWatcher] The runtime file /Users/me/.floci/runtime.yaml is invalid, so the definitions in force stay: version: 2 is not supported; this Floci reads version 1
  ```

  While the file is invalid, Floci reads it again every 2 seconds, so a fix outside it, such as
  `chmod 600` on a token file it names, takes effect without a change to the file. The ERROR is
  logged again when the file or its problem changes.

- A deleted file is an invalid one: the definitions in force stay until it comes back.
- New `workloads` apply from Floci's next read of the queues, and the leases of a workload that
  the file no longer has, or that it disables, are released.

Write the file whole, or write a temporary file and rename it, so that Floci never reads half
of it.

## Nodes

`nodes` declares the smolvm-pods nodes that Floci did not create, such as a node made by hand
with `node-vm.sh`. Floci calls each one's node service over TCP, with its bearer token, as
`tools/smolvm-pods/docs/configuration.md` describes under "The API over TCP". When the file lists
nodes, one of them is [the Floci node](#the-floci-node) that RDS uses, and Floci creates none. Floci
knows the node it creates as `floci`, and finds it as `cluster:ACCOUNT/REGION/NAME` while the EKS
cluster `NAME` runs on it ([smolvm-pods nodes](../services/eks.md#smolvm-pods-nodes)).

| Field | Rule |
|---|---|
| `nodes.NAME` | 1 to 63 of `A-Z`, `a-z`, `0-9`, `_` and `-`. Floci knows the node as `node:NAME` |
| `url` | Required. `http://` or `https://`, a host and a port, and nothing else: no user, path, query or fragment. Where Floci reaches the node service, such as the Mac port that smolvm forwards to the node's port 7070 |
| `token-file` | A file that holds the node service's token. Neither its group nor others may read or write it (`chmod 600`). A relative path is relative to the runtime file's directory |
| `token-env` | An environment variable of Floci's process that holds the token |

A node has exactly one of `token-file` and `token-env`. The token is one word of at least 16
visible ASCII characters, the node service's own rule; spaces and line breaks around it are
ignored. Floci reads it when it reads the runtime file, which is refused when the token is
missing or breaks the rule, and again when the node service answers 401, so a node that gets a
new token needs no change to the runtime file. Floci never logs the token.

### Declaring a node made with node-vm.sh

From `tools/smolvm-pods`, create a node that publishes its node service on the Mac's port 17070,
and copy its token:

```sh
scripts/node-vm.sh create --service-port 17070
scripts/node-vm.sh start
mkdir -p ~/.floci
(umask 077 && scripts/node-vm.sh token > ~/.floci/dev-node-token)
```

Then write the runtime file, here `~/.floci/runtime.yaml`:

```yaml
version: 1
nodes:
  dev:
    url: http://127.0.0.1:17070
    token-file: dev-node-token
```

and start Floci with `FLOCI_RUNTIME_CONFIG_FILE=$HOME/.floci/runtime.yaml`.

### RDS databases on a node

With `FLOCI_SERVICES_RDS_SMOLVM_ENABLED=true`, RDS runs its database containers on the Floci node,
as hibernating microVMs, through the node service's Docker proxy and tunnels.
`FLOCI_SERVICES_RDS_SMOLVM_IDLE_TIMEOUT` sets how long a database is idle before it sleeps. See
[Databases on a smolvm-pods node](../services/rds.md#databases-on-a-smolvm-pods-node).

| Variable | Key | Default |
|---|---|---|
| `FLOCI_SERVICES_RDS_SMOLVM_ENABLED` | `floci.services.rds.smolvm.enabled` | `false` |
| `FLOCI_SERVICES_RDS_SMOLVM_IDLE_TIMEOUT` | `floci.services.rds.smolvm.idle-timeout` | _(empty)_: the node's `--idle-timeout` |

### Lambda functions on a node

With `FLOCI_SERVICES_LAMBDA_SMOLVM_ENABLED=true`, Lambda runs its environments on the Floci node,
each a microVM on dockerd's default bridge with the network of the node's pods, which sleeps
between invocations. `FLOCI_SERVICES_LAMBDA_SMOLVM_IDLE_TIMEOUT` sets how long an environment has
no invocation before it sleeps. See
[Functions on a smolvm-pods node](../services/lambda.md#functions-on-a-smolvm-pods-node).

| Variable | Key | Default |
|---|---|---|
| `FLOCI_SERVICES_LAMBDA_SMOLVM_ENABLED` | `floci.services.lambda.smolvm.enabled` | `false` |
| `FLOCI_SERVICES_LAMBDA_SMOLVM_IDLE_TIMEOUT` | `floci.services.lambda.smolvm.idle-timeout` | _(empty)_: the node's `--idle-timeout` |

### Errors

| Message | Fix |
|---|---|
| `nodes.dev: needs exactly one of token-file and token-env` | Give the node one of the two |
| `nodes.dev.token-file: the token file PATH is open to its group or others; ...` | `chmod 600 PATH` |
| `nodes.dev.token-file: the token file PATH must hold one token of at least 16 visible ASCII characters` | Copy the node's token again with `node-vm.sh token` |
| `nodes.dev.token-env: the environment variable NAME is not set` | Set it in Floci's environment, or use `token-file` |
| `nodes.dev.url: must be http:// or https:// with a host and a port, and nothing else` | Write the URL as `http://HOST:PORT` |
| `nodes.dev.port: unknown field; nodes.dev has url, token-file, token-env` | Remove the field |
| `nodes.dev: duplicate key` | Give each node its own name |

## The Floci node

Floci has one smolvm-pods node for everything that runs on one: RDS's databases with
`FLOCI_SERVICES_RDS_SMOLVM_ENABLED=true`, the one EKS cluster with
`FLOCI_SERVICES_EKS_PROVIDER=smolvm-pods` ([smolvm-pods nodes](../services/eks.md#smolvm-pods-nodes)),
and Lambda's environments with `FLOCI_SERVICES_LAMBDA_SMOLVM_ENABLED=true`
([Functions on a smolvm-pods node](../services/lambda.md#functions-on-a-smolvm-pods-node)).
Redis and Valkey caches join the same node with `FLOCI_SERVICES_ELASTICACHE_SMOLVM_ENABLED=true`.
Their auth proxies stay in Floci and use node tunnels, so connecting wakes a sleeping cache.
`FLOCI_SERVICES_ELASTICACHE_SMOLVM_IDLE_TIMEOUT` overrides the node's idle timeout for caches.
Memcached continues to use host Docker.
Set `FLOCI_SERVICES_ECR_SMOLVM_ENABLED=true` to keep the ECR backing registry and its image data
on the node ([ECR registry on the node](../services/ecr.md#registry-on-the-smolvm-pods-node)).

Images can be fetched or built on another machine and transferred into the node. A host Docker
daemon is an optional image source, not a requirement once the node has the images it needs.
An isolated node cannot pull from external registries itself. Kubernetes uses a separate
containerd image store and pulls through [Floci's ECR](../services/eks.md#pods-floci-and-images).

- **The runtime file's node.** When the runtime file lists nodes, the one that
  `FLOCI_RUNTIME_NODE_NAME` names, by its key, is the node, or the only one when the setting is
  empty. Floci neither creates nor deletes it. Several nodes and no name, or a name that the file
  does not list, is an error that names the nodes, which a create in RDS reports. An EKS cluster
  needs a node whose k3s Floci configures, so `CreateCluster` fails on the runtime file's node.
- **The node Floci creates.** Otherwise Floci creates a node with `node-vm.sh` on the first use,
  whichever comes first: an RDS create, of an instance or a cluster, an EKS `CreateCluster`, or a
  Lambda cold start. Uses that come at once create one node. An RDS or EKS use answers at once: an
  RDS instance or cluster is `creating` and an EKS cluster `CREATING` until the node is ready, and
  a Lambda cold start waits for it, which took 12 seconds on an
  M4 Max. A use that comes later takes the node as it is.

The node outlives its users: deleting the EKS cluster or the last database keeps it, since
creating a node is the slow part. An emulator reset, `POST /_floci/state/reset`, deletes it, and so
does Floci's stop with `memory` storage, since nothing of Floci's state then names it.

Once the node is ready, Floci logs its machine name, its Mac ports, its admin UI and the command
that points the Docker CLI at its dockerd. [Using the Floci node day to day](smolvm-node.md) covers
finding the node, kubectl, Docker, watching its pods and databases sleep and wake, its metrics and
logs, and troubleshooting.

### Creating the node

`FLOCI_RUNTIME_NODE_EXTRA_PORTS` adds comma-separated TCP mappings such as `443:30443,8080:30080`.
The host ports must be distinct from the node's API allocation ranges and NodePorts. The two API
ports, NodePorts and extra mappings together may use at most 64 mappings. The host ports must be
free when the node starts.
Binding ports below 1024 depends on the host's privileges and policy. On the tested macOS
machine, an unprivileged process was denied access to port 443; use an available higher host
port or arrange an appropriate host-level forward.

`FLOCI_RUNTIME_NODE_VOLUMES` adds comma-separated directory shares, for example
`/Users/me/project:/workspace:ro`. Both paths must be absolute; the host directory must exist.
Omit `:ro` for a writable share. A container on the node can then bind `/workspace`.

These settings apply when Floci creates a node. Changing them does not modify an existing node;
recreate the node to apply them. Existing workload data on that node must be backed up before
recreating it.

Floci creates the node only natively on an Apple silicon Mac of M3 or later, as a JVM or the
native binary, not in a container, with `node-vm.sh`, the smolvm checkout and the node image of
`node-vm.sh build` (`tools/smolvm-pods/docs/getting-started.md`). When RDS or EKS runs on the node
and the runtime file lists none, Floci refuses to start unless every setting below is valid, and
names each problem with its variable; a first use that Floci cannot serve fails with the same
problems.

```sh
export FLOCI_RUNTIME_NODE_NODE_VM_SCRIPT=$PWD/tools/smolvm-pods/scripts/node-vm.sh
export FLOCI_RUNTIME_NODE_SMOLVM_SRC=$(cd .. && pwd)/smolvm
export FLOCI_RUNTIME_NODE_IMAGE=$PWD/tools/smolvm-pods/target/node/node-image.tar
export FLOCI_RUNTIME_NODE_IMAGE_SHA256=$(shasum -a 256 "$FLOCI_RUNTIME_NODE_IMAGE" | cut -d' ' -f1)
export FLOCI_SERVICES_RDS_SMOLVM_ENABLED=true
export FLOCI_SERVICES_EKS_PROVIDER=smolvm-pods
```

| Variable | Default | Description |
|---|---|---|
| `FLOCI_RUNTIME_NODE_NAME` | _(empty)_ | The runtime file's node, by its key under `nodes`; empty takes the only one |
| `FLOCI_RUNTIME_NODE_NODE_VM_SCRIPT` | _(none)_ | Absolute path of `tools/smolvm-pods/scripts/node-vm.sh` |
| `FLOCI_RUNTIME_NODE_SMOLVM_SRC` | _(none)_ | Absolute path of the smolvm checkout that `node-vm.sh` runs (`SMOLVM_SRC`) |
| `FLOCI_RUNTIME_NODE_IMAGE` | _(none)_ | Absolute path of the node image tar that `node-vm.sh build` wrote |
| `FLOCI_RUNTIME_NODE_IMAGE_SHA256` | _(none)_ | The image tar's SHA-256 |
| `FLOCI_RUNTIME_NODE_DATA_PATH` | `./data/runtime-node` | Where Floci keeps the node's seed |
| `FLOCI_RUNTIME_NODE_CPUS` | `4` | vCPUs of the node |
| `FLOCI_RUNTIME_NODE_MEMORY_MIB` | `8192` | Memory of the node, in MiB: room for a few databases, each in a microVM that sleeps when idle, and an EKS cluster's pods |
| `FLOCI_RUNTIME_NODE_STORAGE_GIB` | `64` | Disk for the image, the node's volumes and the databases' data, in GiB |
| `FLOCI_RUNTIME_NODE_OVERLAY_GIB` | `8` | Disk for changes to the node's root file system, in GiB |
| `FLOCI_RUNTIME_NODE_ROSETTA` | `true` | Share Rosetta with the node, so that amd64 pods run |
| `FLOCI_RUNTIME_NODE_API_SERVER_BASE_PORT` | `6500` | First Mac port of the node's Kubernetes API |
| `FLOCI_RUNTIME_NODE_API_SERVER_MAX_PORT` | `6599` | Last Mac port of the node's Kubernetes API |
| `FLOCI_RUNTIME_NODE_NODE_SERVICE_BASE_PORT` | `6600` | First Mac port of the node service's API |
| `FLOCI_RUNTIME_NODE_NODE_SERVICE_MAX_PORT` | `6699` | Last Mac port of the node service's API |
| `FLOCI_RUNTIME_NODE_NODE_PORTS` | `32080-32099` | The node's NodePorts, published on the same Mac ports: `FIRST-LAST`, `PORT` or `none`. The node gets none when another process holds one of them |
| `FLOCI_RUNTIME_NODE_FLOCI_ENDPOINT` | _(empty)_ | `HOST:PORT` where k3s and containerd in the node reach Floci, for the token webhook, the admission webhook and the ECR mirror. Empty is `host.docker.internal:<FLOCI_PORT>`. The node maps `host.docker.internal`, and the host when it is a name, to its gateway, which reaches the Mac's loopback |
| `FLOCI_RUNTIME_NODE_KEEP_RUNNING_ON_SHUTDOWN` | `true` | Leave the node running when Floci stops with persistent storage |
| `FLOCI_RUNTIME_NODE_READY_TIMEOUT_SECONDS` | `600` | How long a node that Floci created, started or adopted may take to be ready before its users fail |
| `FLOCI_RUNTIME_NODE_IDLE_TIMEOUT` | _(empty)_ | The node's `FLOCI_NODE_IDLE_TIMEOUT`, a duration such as `5m` |
| `FLOCI_RUNTIME_NODE_CPU_THRESHOLD` | _(empty)_ | The node's `FLOCI_NODE_CPU_THRESHOLD`: vCPUs such as `0.5`, or `off` |
| `FLOCI_RUNTIME_NODE_CPU_WINDOW` | _(empty)_ | The node's `FLOCI_NODE_CPU_WINDOW`, a duration |
| `FLOCI_RUNTIME_NODE_MAX_AWAKE_PODS` | _(empty)_ | The node's `FLOCI_NODE_MAX_AWAKE_PODS`; 0 is no cap |
| `FLOCI_RUNTIME_NODE_MAX_AWAKE_MEMORY_MIB` | _(empty)_ | The node's `FLOCI_NODE_MAX_AWAKE_MEMORY`, in MiB; 0 is no cap |
| `FLOCI_RUNTIME_NODE_ADMISSION_WAIT` | _(empty)_ | The node's `FLOCI_NODE_ADMISSION_WAIT`, a duration |
| `FLOCI_RUNTIME_NODE_REPORTED_MEMORY_GIB` | _(empty)_ | The node's `FLOCI_NODE_MEMORY_GIB`, the memory that kubelet reports. Applies at the next node restart |
| `FLOCI_RUNTIME_NODE_REPORTED_CPUS` | _(empty)_ | The node's `FLOCI_NODE_CPUS`, the CPUs that kubelet reports. Applies at the next node restart |
| `FLOCI_RUNTIME_NODE_RUST_LOG` | _(empty)_ | The node's `RUST_LOG`. Applies at the next node restart |

A duration is a Go duration above 0, such as `30s`, `5m` or `1m30s`
(`tools/smolvm-pods/docs/configuration.md`, "The node's settings"). The keys are
`floci.runtime.node.*`, such as `floci.runtime.node.memory-mib`.

`node-vm.sh create` gets the size, three sets of Mac ports, all on the Mac's loopback, and a seed
directory, `<FLOCI_RUNTIME_NODE_DATA_PATH>/<node>/seed`, which the node mounts read-only and installs
at every boot (`tools/smolvm-pods/node/README.md`, "The seed"). The directory is readable only by
the user that runs Floci: it holds the node's Kubernetes admin token, its node service token and its
service account signing key. The node service settings above go into the seed's `env`; an empty one
keeps the node's own default. Floci writes the `env` each time it creates, starts or adopts the
node, and a running node applies a changed idle or admission setting within about 6 seconds, with
its pods kept.

The seed also sets up the node's k3s for the EKS cluster that may run on it, created now or later:
its name in Kubernetes, an EC2 private DNS name in the region of the first use, its Service and pod
ranges, `10.100.0.0/16` and `10.42.0.0/16` unless an EKS cluster is the first use and asks for
others, and, with the `smolvm-pods` provider, the token and admission webhooks, the service account
issuer and the ECR mirror. The mirror serves the ECR repositories of every account, whichever
account the first use was in. The node belongs to the partition of its first use's region: an EKS
cluster in another partition is refused ([smolvm-pods nodes](../services/eks.md#smolvm-pods-nodes)).

The node is ready once its Kubernetes API answers `GET /readyz`, the node is `Ready` since its VM
last started, RuntimeClass `smolvm-pods` exists, the node service answers its API, and the node
applied the network profile that Floci publishes to it ([Network](#network)). RDS waits for the
node's dockerd too. A node created while the runtime file's `network.isolation.enabled` is `true`
is isolated: Floci runs `node-vm.sh create --isolate`, with the CIDRs of `network.egress.allow` as
the node's allow list, and keeps that list in step with the file. The node keeps the isolation it
was created with.

### When Floci stops and starts

| Storage mode | `FLOCI_RUNTIME_NODE_KEEP_RUNNING_ON_SHUTDOWN` | When Floci stops |
|---|---|---|
| `memory` | Any | Floci deletes the node |
| `persistent`, `hybrid` or `wal` | `true`, the default | The node keeps running, and its pods and databases keep their memory |
| `persistent`, `hybrid` or `wal` | `false` | Floci stops the node; its disks stay, and its pods and databases start again cold |

Floci keeps the node's record with its state. When Floci starts with a record, it adopts the
running node, starts a stopped one, and creates a missing one again with the same ports; a node
created again has a new cluster CA and none of the old workloads or databases. The persisted RDS
databases keep their endpoint ports at once, and they and the EKS cluster come back once the node
is ready. Floci then deletes the `node-vm.sh` machines that it created, by its
`FLOCI_DOCKER_RESOURCE_NAMESPACE` and its `FLOCI_RUNTIME_NODE_DATA_PATH`, and that the record does
not name; without a record, as after a run with `memory` storage, it deletes all of them, since
nothing of its state reaches them. It never deletes another machine.

A node that stops or disappears while Floci runs is logged once and left as it is. Start it with
`node-vm.sh start --name NODE`, or restart Floci, which creates a missing node again. A reset or a
stop that deletes the node runs after the node's creation, which stops early, and waits for it up
to 15 seconds; the node goes once its creation ends. A node that `node-vm.sh` cannot delete is
logged with the command that deletes it. Floci stops the node after RDS and EKS stop their
containers on it.

## Workloads

A pod of the RuntimeClass `smolvm-pods` wakes on incoming connections. A message consumer that
polls a queue makes only outbound connections, so a message for it does not wake it. `workloads`
names those consumers: when messages arrive in a workload's queues, Floci holds a lease on each
of its pods, which keeps the pod awake while the consumer works, and wakes the pods through the
node service's API.

```yaml
workloads:
  orders:
    enabled: true
    account-id: '000000000000'
    region: us-east-1
    idle-timeout-seconds: 120
    startup-timeout-seconds: 30
    queues: [orders-incoming]
    rabbitmq-queues:
      - broker-id: b-0b1c2d3e-0000-4000-8000-000000000001
        virtual-host: /
        queue: background-jobs
    runtime:
      type: kubernetes
      cluster-name: integration-test
      namespace: integration-test
      deployment: orders
```

| Field | Rule |
|---|---|
| `workloads.NAME` | 1 to 64 of `A-Z`, `a-z`, `0-9`, `_`, `.` and `-` |
| `enabled` | `true` or `false`, default `true`. Floci holds no lease for a disabled workload |
| `account-id` | 12 digits in quotes. Default: Floci's default account, `FLOCI_DEFAULT_ACCOUNT_ID` |
| `region` | An AWS region. Default: Floci's default region, `FLOCI_DEFAULT_REGION` |
| `idle-timeout-seconds` | 0 to 86400, default 120: how long Floci keeps the pods' leases after the last event of the workload's queues, such as a send, a receive or a delete |
| `startup-timeout-seconds` | 1 to 120, default 30: the limit of one wake, which covers the node's wait for room and its queue of resumes |
| `queues` | SQS queue names in the workload's account and region, not URLs or ARNs: 1 to 80 of `A-Z`, `a-z`, `0-9`, `_` and `-`, ending in `.fifo` for a FIFO queue |
| `rabbitmq-queues` | Queues on Floci's Amazon MQ for RabbitMQ brokers in the workload's account and region. `broker-id` and `queue` are required, and `virtual-host` is `/` by default. Floci follows them as its own broker user ([RabbitMQ queues](#rabbitmq-queues)); `username-env` and `password-env`, both or neither, are accepted and not used |
| `runtime.type` | Required: `kubernetes` |
| `runtime.cluster-name` | A Floci EKS cluster in the workload's account and region, whose node Floci created |
| `runtime.node` | A node of the same file's `nodes` |
| `runtime.namespace` | The pods' namespace, default `default` |
| `runtime.deployment` | The pods of this Deployment |
| `runtime.statefulset` | The pods of this StatefulSet |
| `runtime.selector` | The pods that have every one of these labels, such as `{app: orders}`. Quote a value that YAML reads as a number or a boolean |

A workload has at least one queue in `queues` or `rabbitmq-queues`, exactly one of
`runtime.cluster-name` and `runtime.node`, and exactly one of `deployment`, `statefulset` and
`selector`. No two workloads select the same pods: the same node, namespace and Deployment,
StatefulSet or selector. A queue, broker or cluster that does not exist yet has no messages, so
the workload's pods are not woken until it does.

### Messages

Floci runs SQS, so it learns of every change to a queue's messages as it happens, and reads a
queue only at the moments below. A message that a consumer can receive wakes the workload's pods
at once, within the time of the wake. A delayed message wakes them when it becomes visible. A
consumer's long poll ends when its pod goes to sleep, which resets the poll's connection, so it
claims no message while the consumer sleeps: the consumer's next poll after the wake gets it.

Every producer inside Floci sends through SQS, so SNS subscriptions, EventBridge rules, IoT rules,
S3 notifications, EventBridge Scheduler, Pipes, Step Functions and redrives that deliver into a
listed SQS queue wake its consumers like any other message. A message that a receive moves to a
dead-letter queue wakes the workload that lists the dead-letter queue. An MQTT consumer is woken
by an IoT rule that sends its topic's messages to an SQS queue that the consumer reads. List a
dead-letter queue only in the workload that drains it: its messages keep that workload's pods
awake until they are gone.

Floci reads a workload's SQS queues only at these moments, in the workload's account and region:

| When | What the read decides |
|---|---|
| Floci starts, after an emulator reset, and when a change of the runtime file adds the workload or its queues | Messages that wait or are in flight make the workload busy; a delayed message sets a timer. A workload that is busy then decides at the end of its linger instead |
| A delayed message's time comes, while the workload is idle | The message, visible now, makes the workload busy |
| `idle-timeout-seconds` after the last event of the workload's queues | Queues with no visible and no in-flight message release the leases. Otherwise Floci keeps them until the next event |

A queue that does not exist has no messages. Floci logs it once, at a read, so check the queue's
name, account and region when a consumer is never woken:

```
INFO  [io.git.hec.flo.run.wak.SqsQueueWatch] Workload orders reads the SQS queue orders-incoming in account 000000000000 and region us-east-1, which does not exist; its messages wake the workload's pods once it does
```

When a queue cannot be read, Floci logs a warning, keeps the leases the workload holds, wakes no
other pod of it, and reads again after 1 second, then after 2, 4 and up to 30 seconds.

### RabbitMQ queues

Floci follows the queues of the Amazon MQ brokers that it runs in Docker through each broker's
[firehose](https://www.rabbitmq.com/docs/firehose). For each virtual host that a workload reads,
Floci turns tracing on (`rabbitmqctl trace_on`) unless it is on already, and consumes the
virtual host's trace exchange, `amq.rabbitmq.trace`, as its own broker user. A publish that the
broker routes to a listed queue, and a delivery from one to a consumer or a `basic.get`, are
events of the workloads that read the queue. A publish wakes the workload's pods at once, within
the time of the wake; a delivery wakes them when the workload is idle, such as a delivery to a
consumer whose connection stayed open while it slept. Every publish and delivery restarts the
linger, so a steady stream of messages keeps the consumer awake.

RabbitMQ publishes no event for an acknowledgement, nor for a message that leaves a queue with no
delivery: one that expires or overflows, a purge, or a deleted queue. So at the end of the linger
Floci reads the counts of the virtual host's queues once, `messages_ready` and
`messages_unacknowledged`, with `rabbitmqctl list_queues` in the broker's container. These are
the queues' own counts at that moment; the management API's statistics lag them by up to 10
seconds.

| The listed queues hold | Floci |
|---|---|
| No message | Releases the leases |
| Ready or unacknowledged messages | Keeps the leases, and reads again one linger later while the queues hold messages |

A message that a consumer has received but not acknowledged keeps the consumer's pods awake, so
acknowledge a message once its work is done. RabbitMQ's `consumer_timeout`, 30 minutes by
default, returns a delivery that a consumer holds longer to its queue.

Floci finds the broker with Amazon MQ's `DescribeBroker` in the workload's account and region,
and follows it while it is `RUNNING` in a container that this run of Floci started: from the
moment it runs, or from the moment the runtime file names it.

**Floci's broker user.** At the first watch of a broker, Floci adds the user
`floci-wakes-RANDOM` there with `rabbitmqctl`, with no tags and a random password. Both are new
at each start of Floci and kept only in memory. In each followed virtual host, the user may
configure `^amq\.gen-.*`, write `^(amq\.gen-.*|amq\.default)$` and read
`^(amq\.rabbitmq\.(trace|event)|amq\.gen-.*)$`: its own server-named queues, the default
exchange that their dead letters go through, and the trace and event exchanges. Floci never logs
the password.

**Each followed virtual host** has one AMQP connection named `floci message wakes`, with a
30-second heartbeat, and two server-named, exclusive queues. The trace queue holds at most 10,000
traces and is bound with `publish.#` and with `deliver.QUEUE` for each listed queue. When it is
full, its oldest trace goes to the second queue, which holds one, and Floci reads the virtual
host's counts once. A queue name with a dot-separated word `*` or `#`, or longer than 247 bytes,
cannot be a binding key of its own, so Floci binds `deliver.#`, every delivery of the virtual
host, and picks its queues by their exact names.

**What the broker does not trace.** Dead-lettered messages (an expired TTL, a rejection without
requeue, an overflow), requeues, and messages published in a transaction reach a queue with no
traced publish: a publish in a transaction is traced when it is sent, before the commit. The
broker delivers them to a consumer whose connection is open, and that delivery is an event. Once
the workload sleeps and the broker has closed its consumer's connection, such a message waits for
the workload's next event. For a retry queue whose messages dead-letter into the work queue, list
the retry queue in the same workload: its ready messages keep the workload awake until they reach
the consumer. A trace that the broker drops before it reaches Floci's queue goes unseen.

**A virtual host that does not exist yet.** Floci logs it once, enables the plugin
`rabbitmq_event_exchange` on that broker, logs that too, and follows the broker's `vhost.created`
events on `/`. When the virtual host is created, Floci follows it. Amazon MQ does not offer the
plugin, so Floci enables it only on a broker with a missing virtual host, and leaves it on.

When no workload reads a virtual host any more, Floci closes its connection and turns tracing off
there if Floci turned it on, so tracing that was on before stays on. When no workload reads a
broker, Floci deletes its user there.

| What Floci finds | The queue |
|---|---|
| No broker with that id in the workload's account and region, or one that is not `RUNNING` | Has no messages |
| A broker whose container this run of Floci did not start, such as a broker of an earlier run or one of `floci.services.amazonmq.mock: true` | Has no messages |
| No virtual host or no queue of that name | Has no messages until it exists |
| The broker closes Floci's connection or channel, such as at a restart, or cancels one of its consumers, as when a user deletes one of Floci's `amq.gen-*` queues, or a `rabbitmqctl` command fails | Floci keeps the leases, wakes nothing new from the broker, and follows it again after 1 second, then after 2, 4 and up to 30 seconds. A restart turns tracing off, so Floci turns it on again. Each new subscription reads the counts once |
| The broker's container is gone | Has no messages, until the broker runs again |
| The broker's container is stopped, such as with `docker stop` | Has no messages, so its workloads hold no lease for it. Floci tries to follow it again after 1 second, then after 2, 4 and up to 30 seconds, and follows it once the container runs |
| The broker refuses Floci's user | Has no messages, until the runtime file changes or a broker runs |
| A read of the counts that fails, or gets no answer within 10 seconds | Floci keeps the leases and reads again after 1 second, then after 2, 4 and up to 30 seconds |
| A queue with no counts in `list_queues`, such as a classic queue that crashed or a quorum queue with no leader | Floci logs a warning once, keeps the leases of the workloads that read the queue, and reads again after 1 second, then after 2, 4 and up to 30 seconds. The other queues of the virtual host are read as usual |

Floci logs each subscription, a missing queue once, and a failed read once as a warning, until a
read succeeds again:

```
INFO  [io.git.hec.flo.run.wak.RabbitMqQueueWatch] Floci follows the firehose of virtual host / of broker b-0b1c2d3e-0000-4000-8000-000000000001 for message wakes, as the broker user floci-wakes-5de3d21a
INFO  [io.git.hec.flo.run.wak.RabbitMqQueueWatch] Workload jobs reads the RabbitMQ queue background-jobs of broker b-0b1c2d3e-0000-4000-8000-000000000001 and virtual host /, which does not exist; its messages wake the workload's pods once it does
WARN  [io.git.hec.flo.run.wak.RabbitMqQueueWatch] Following the firehose of virtual host / of broker b-0b1c2d3e-0000-4000-8000-000000000001 failed, so Floci keeps the leases of the workloads that read its queues and tries again: java.io.IOException: the broker closed Floci's connection: CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'
```

**What user tooling sees** on a followed broker: the user `floci-wakes-RANDOM`, the connection
`floci message wakes` and its two `amq.gen-*` queues, tracing on in the followed virtual hosts
(`rabbitmqctl list_vhosts name tracing`), and the event exchange plugin on a broker that had a
missing virtual host.

**Cost.** With tracing on, the broker converts every publish and every delivery of the virtual
host, of every queue, and copies it with its body to the trace exchange; RabbitMQ says that
"performance will drop somewhat". Floci's queue receives a copy of each publish of the virtual
host and of each delivery of a listed queue. Each subscription runs a few `rabbitmqctl` commands
in the broker's container, and each read one, `list_queues`, which takes about 0.3 seconds.

### Pods and leases

Floci finds a workload's pods in the pod records of its node, by their labels and by the
Deployment or StatefulSet that controls them, so it needs no credentials for the Kubernetes API.
It follows the events of each node that an enabled workload names, from the node service's
`GET /v1/events?follow=true`, and lists a workload's namespace when the workload becomes busy,
when a change of the runtime file gives a busy workload another node, namespace or selection of
pods, when the node's events show a pod it has not listed, and when a follow of the events
starts. A follow that ends or fails starts again after 1 second, then after 2, 4 and up to 30
seconds.

| Pod state | Floci |
|---|---|
| `creating` | Holds the lease, and wakes the pod once its event shows that it runs |
| `awake`, `pausing`, `asleep`, `waking` | Holds the lease, then wakes the pod. A wake of an awake pod answers at once, and cancels a pause that started just before the lease |
| `failed`, `deleting` | Nothing |

Each lease lasts 60 seconds, and Floci renews it every 20 seconds while the workload is busy.
`idle-timeout-seconds` runs from the last event of the workload's queues: a send, a receive, a
delete, a visibility change, a purge or a move. When it passes with no event, Floci reads the
queues once. If no message is visible or in flight, it releases the leases, and the pod sleeps at
the node's next idle check, unless traffic on connections opened to it keeps it awake. If one is,
Floci keeps the leases until the next event, such as the message's delete, and the linger starts
again from there.

- A consumer that handles each message at once, with messages closer together than
  `idle-timeout-seconds`, stays awake for the whole stream, with no read of its queues, and
  sleeps `idle-timeout-seconds` after its last delete.
- A burst of messages 10 seconds apart costs one wake and one sleep, the sleep
  `idle-timeout-seconds` after the last message.
- A message in flight past `idle-timeout-seconds`, or one that nobody receives, keeps the leases.
- A delayed message neither wakes the consumer nor extends the linger until it is visible.
- Floci decides for a workload at most once a second, so with `idle-timeout-seconds: 0` the
  leases end about 1 second after the last event once the queues are empty.

The pod's own idle timeout for incoming traffic, the annotation
`smolvm-pods.floci.io/idle-timeout`, is a separate setting.

A lease is named `floci:WORKLOAD`, or `floci:INSTANCE:WORKLOAD` with `instance-id`, and its
holder is `floci wakes: WORKLOAD`. When Floci lists a namespace, it deletes the leases with its
own prefix that no workload wants there, such as those of a workload removed while Floci was
stopped. When Floci stops, its leases expire within 60 seconds. The node's events show Floci's
wakes with the source `floci` and the reason `aws`, whose `queue` names the queue whose messages
wait: the SQS queue's ARN, or `amqp://BROKER-ID/VHOST/QUEUE` with the virtual host and the queue
percent-encoded, such as `amqp://b-0b1c2d3e-0000-4000-8000-000000000001/%2F/background-jobs`.

A node whose admission caps (`FLOCI_NODE_MAX_AWAKE_PODS`, `FLOCI_NODE_MAX_AWAKE_MEMORY`) leave no
room refuses a wake, and resets every connection held at the pod, as it does for any refused
wake. Floci keeps the pod's lease, logs one warning, and sends no other wake to that node until
the node's events show one of its pods asleep, failed or deleted, or 5 minutes pass. It then
wakes the pods that waited, oldest first, one at a time. A node service that does not answer is
called again after 1 second, then after 2, 4 and up to 30 seconds. A node service that refuses
Floci's token is logged as an error every 30 seconds.

### Startup, restarts and resets

Floci starts to watch the workloads' queues at the end of its startup, once its storage is loaded,
and reads each of them once then. It watches them while the runtime file has enabled workloads.

| Event | What happens |
|---|---|
| Floci starts again with `persistent`, `hybrid` or `wal` storage | Messages that wait in the queues wake the pods at Floci's first read. SQS keeps no visibility times, so every stored message is visible, also one that was delayed or in flight. Floci holds the leases again under the same names, and deletes the last run's leases that no workload wants |
| Floci starts again with `memory` storage | The queues are gone, so no message wakes a pod. The last run's leases expire within 60 seconds, or Floci deletes them when it next lists their namespace |
| Floci starts again with an Amazon MQ broker | Floci stops a broker's container when it stops and does not start it again, so the broker's queues have no messages and wake no pod until the broker is created again |
| A broker restarts | Floci follows it again once it answers, and turns its tracing on again |
| Floci stops for longer than a lease | A pod may sleep in the middle of its work. When Floci starts again with its storage, the message, visible again, wakes the pod at the first read |
| The node service restarts | Floci follows its events again from the new head and lists the namespaces of busy workloads, and calls it again from 1 second up to 30 seconds later. The node keeps its leases |
| The node restarts | Its pods are new. Floci forgets the old ones, and leases and wakes the new ones while the workload is busy |
| An EKS cluster is deleted and created again | Its node is new, under the same name. Floci forgets the leases it held on the old node, follows the new one, and leases and wakes its pods while the workload is busy |
| An emulator reset, `POST /_floci/state/reset` | Floci releases every lease it holds, ends its RabbitMQ subscriptions, then reads each watched queue once and follows each broker that still runs. The reset leaves no SQS queues or Amazon MQ brokers, so a workload stays idle until a message arrives. It also deletes the smolvm-pods node of every EKS cluster, so a workload whose `runtime.cluster-name` names one has no node until the cluster is created again |

### Consumers

- An SQS queue's visibility timeout covers a wake as well as the work, and a queue with a
  dead-letter queue has a `maxReceiveCount` of 3 or more.
- An SQS consumer fails a request that gets no answer within its timeout, and polls again: a
  refused wake resets the answer to a poll that waits at the pod. In the AWS SDK for JavaScript
  v3, `requestTimeout` only logs a warning unless the client's `requestHandler` also has
  `throwOnRequestTimeout: true`.
- A consumer's connection to its broker is reset as its pod goes to sleep, so the broker requeues
  its unacknowledged messages at once. The consumer reconnects when it wakes. A queue that the consumer
  declared exclusive or auto-delete goes with the connection, so its messages are lost: give
  consumers that sleep durable queues, which keep the messages for the next connection.

### Workload errors

| Message | Fix |
|---|---|
| `workloads.orders.runtime.node: names no node in nodes` | Declare the node under `nodes`, or name a Floci EKS cluster with `cluster-name` |
| `workloads.orders.runtime: needs exactly one of cluster-name and node` | Keep one of the two |
| `workloads.orders.runtime: needs exactly one of deployment, statefulset and selector` | Keep one of the three |
| `workloads.orders.runtime.type: must be kubernetes` | Only Kubernetes pods sleep and wake |
| `workloads.orders: needs at least one queue in queues or rabbitmq-queues` | List the queues the consumer reads |
| `workloads.orders.account-id: must be 12 digits in quotes, such as '000000000000'` | Quote the account, which YAML otherwise reads as a number |
| `workloads.orders.queues.0: must be an SQS queue name: ...` | Write the queue's name, not its URL or ARN |
| `workloads.orders-eu.runtime: selects the same pods as workloads.orders` | Give one workload both workloads' queues |
| `workloads.orders.replicas: unknown field; workloads.orders has enabled, account-id, ...` | Remove the field |

## Network

The `network` section shapes what the pods of every smolvm-pods node reach, the nodes Floci's EKS
service creates and the nodes of `nodes` alike. Floci publishes it to each node as the node's
network profile, with `PUT /v1/network`, as soon as the node's service answers, again at once when
the file changes, and again when a check every 30 seconds finds that the node holds another one.

```yaml
version: 1
network:
  isolation:
    enabled: true
  floci:
    extra-ports: [9000]
  trust:
    mode: system
  egress:
    allow:
      - cidr: 203.0.113.10/32
        protocol: tcp
        ports: [443]
  routes:
    payments:
      origins:
        - https://api.vendor.test
        - https://*.vendor.test
      target:
        backend-url: http://payments-simulator.simulators.svc.cluster.local:8080
        protocol: http1
```

| Field | Rule |
|---|---|
| `isolation.enabled` | `true` blocks every connection from pods but to the cluster, Floci, routed hosts and `egress.allow`; default `false` |
| `floci.extra-ports` | More ports of the Floci host that pods reach, besides Floci's own (below) |
| `trust.mode` | `system` (default), `env` or `off`: how containers trust Floci's CA ([Trust](#trust)) |
| `egress.allow` | Outside networks that isolated pods reach: `cidr`, an IPv4 address or CIDR; `protocol`, `tcp` (default), `udp` or `any`; `ports`, empty for every port |
| `routes.NAME` | 1 to 63 of `a-z`, `0-9` and `-` |
| `routes.NAME.origins` | `https://HOST[:PORT]` or `http://HOST[:PORT]`, `HOST` a DNS name or `*.` and a domain for every name below it, at any depth. No path, query or credentials, no port 53 and no port of Floci's, no host under `localhost.floci.io`, `localhost.localstack.cloud`, `host.docker.internal`, `cluster.local`, `floci.hostname` or `floci.dns.extra-suffixes`. One port serves either HTTP or HTTPS origins, and an origin belongs to one route |
| `routes.NAME.target.backend-url` | `http://` with a Service name `NAME.NAMESPACE.svc.cluster.local`, an address in the cluster's Service range, or `host.floci.internal` for a server on the Floci host, and a port |
| `routes.NAME.target.protocol` | `http1` (default), `h2c` or `auto`: what the relay offers routed TLS clients with ALPN |

### What pods reach

Each node owns the address `169.254.53.1`, which its pods reach through their default route.
Containers of the node's dockerd on its default bridge, such as RDS's databases and Lambda's
environments, get the same network as pods, and count as part of the cluster:

| Port on `169.254.53.1` | Goes to |
|---|---|
| 53 | The node's DNS forwarder, which asks Floci |
| Floci's ports | Floci on the Floci host |
| A routed origin's port | The node's relay, which ends TLS and splices to the route's backend |
| Any other | The sinkhole, which logs the attempt and resets it |

Floci's ports are its AWS endpoint, `FLOCI_PORT`, and the listeners its services publish for
clients: Amazon MQ's AMQP and console ports, IoT's MQTT ports, the proxies of ElastiCache,
MemoryDB, RDS, Redshift, Neptune, OpenSearch and MWAA, MSK's Kafka ports, ECR's registries and
Timestream for InfluxDB's ports, each over its configured range, plus `floci.extra-ports`, and the
Lambda Runtime API's range while Lambda runs on the node.

Floci answers the nodes' DNS at `FLOCI_RUNTIME_DNS_LISTEN`:

| Name | Answer |
|---|---|
| `localhost.floci.io` and below, `localhost.localstack.cloud` and below, `host.docker.internal`, `floci.hostname` and `floci.dns.extra-suffixes` | `169.254.53.1` |
| A routed origin's host | `169.254.53.1` |
| A name that a Floci service owns, such as a Cloud Map namespace | Its records |
| Any other name | Floci's resolvers' answer, or NXDOMAIN while `isolation.enabled` is `true` |

Set `FLOCI_HOSTNAME=localhost.floci.io` and give pods `http://localhost.floci.io:4566` as
`AWS_ENDPOINT_URL`, so that the endpoints Floci returns, such as an SQS `QueueUrl` or a broker's
endpoint, resolve in pods.

With `isolation.enabled`, pods reach the cluster's pods and Services, Floci's ports, routed
hosts, the node's endpoints, such as `169.254.170.23:80` for EKS Pod Identity, and
`egress.allow`. Every other connection, datagram, DNS server, and all IPv6, is blocked, and a
blocked TCP connection is reset at once. On a Mac a node created isolated (`node-vm.sh create
--isolate`, or an EKS cluster created while `isolation.enabled` is `true`) is also limited by
smolvm at the VM's boundary; a node created open keeps that boundary open until it is created
again. Floci and the containers it runs in Docker are not limited.

### Routes

A pod that calls `https://api.vendor.test` resolves the name to `169.254.53.1`, and the node's relay
reads the TLS server name, ends TLS with a certificate for that name, and passes the connection
to the route's backend, which sees the original `Host`. The certificate comes from an intermediate
CA that Floci issues for the routed names only and sends in the profile: it can sign no other
name, and Floci issues a new one when the routed names change. An `http://` origin is routed by its
`Host` header. A sleeping simulator wakes on the relay's connection, and the wake's reason names
the calling pod and the route, such as `{"kind": "traffic", "from": "default/orders", "via":
"route payments", "port": 8080}`. A backend that does not accept a connection in 40 seconds gets
the client `503` with `Retry-After: 1`; a host that no route names gets `502`.

### Trust

Mode `system` writes Floci's CA into each container's copy of its image when the container is
created: the CA is appended to every PEM bundle where Go and OpenSSL look, a bundle is created
where the image has none, and each Java trust store is replaced with one that holds the JVM's
public roots and Floci's CA, in the store's own format. `/etc/floci-ca.pem` and
`/etc/floci-ca-bundle.pem` hold the CA and the bundle, and the container gets `NODE_EXTRA_CA_CERTS`,
`DENO_CERT`, `REQUESTS_CA_BUNDLE`, `AWS_CA_BUNDLE` and `FLOCI_CA_BUNDLE` unless it sets them. Mode
`env` writes only those two files and the variables, with `SSL_CERT_FILE` and `CURL_CA_BUNDLE` too.
Mode `off` writes nothing; then a route's HTTPS is trusted only by clients that trust Floci's CA
themselves. A container created while the node has routes or isolation but no trust material
fails, with the reason. Keep Floci's data directory: when Floci's CA changes, each container gets
the new CA in its files at the pod's next wake, and the wake's event carries a `trust stale`
warning, but a process that already loaded the old CA, such as a JVM, trusts it until it
restarts.

### Network events

Each node counts what its sinkhole blocked and what its relay passed, per pod, destination, port
and protocol over a minute, and serves them at `GET /v1/network/events`, with the TLS server name
or HTTP `Host` that each blocked connection sent. `GET /v1/network` reports the profile's
generation, whether the node is isolated, and the counts since the node service started.
Floci follows the network events of every node it publishes to, from the moment it starts
following, and logs each blocked attempt at INFO, once per event, with the pod, the destination,
port and protocol, the count and the names it sent, such as `node:dev blocked shop/orders: tcp to
93.184.215.14:443, count 3, names api.example.com`. Floci logs each name its DNS refuses under
isolation, once a minute.


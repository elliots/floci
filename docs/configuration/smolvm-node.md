# Using the Floci node day to day

The Floci node is the smolvm-pods node VM that runs RDS's databases and the EKS cluster
([The Floci node](smolvm-runtime.md#the-floci-node)). This page is about using it from the Mac:
finding it, kubectl, the Docker CLI, watching pods and databases sleep and wake, metrics, logs, and
what to do when something goes wrong. Commands run from `tools/smolvm-pods` in the floci
repository.

## Finding the node

Each time the node becomes ready, after Floci creates, starts or adopts it, Floci logs where it is:

```
INFO  [io.git.hec.flo.run.flo.FlociNode] smolvm-pods node floci-node-0123456789abcdef is ready: Kubernetes API https://127.0.0.1:6500, node API http://127.0.0.1:6600, admin UI http://127.0.0.1:6600/ui, seed and token in /src/floci/data/runtime-node/floci-node-0123456789abcdef/seed. Docker CLI: eval "$(SMOLVM_SRC=/src/smolvm /src/floci/tools/smolvm-pods/scripts/node-vm.sh docker-env --name floci-node-0123456789abcdef)"
```

A node that the runtime file declares is one you made, so Floci logs nothing of it.

- **The machine.** Floci's node is a `node-vm.sh` machine named `floci-node-` and 16 hex digits.
  `node-vm.sh` works on it as on a node you made, with the smolvm checkout that Floci uses:
  set `SMOLVM_SRC` to `FLOCI_RUNTIME_NODE_SMOLVM_SRC`, and `SMOLVM` too if Floci's environment
  sets it. Name the node with `--name` or `SMOLVM_PODS_NODE`:

  ```sh
  export SMOLVM_SRC=$FLOCI_RUNTIME_NODE_SMOLVM_SRC
  scripts/node-vm.sh status                  # every node, with its state and Mac ports
  export SMOLVM_PODS_NODE=floci-node-0123456789abcdef
  scripts/node-vm.sh exec -- smolvm-pods ctl list
  ```

- **The Mac ports.** All on `127.0.0.1`: the Kubernetes API from 6500 to 6599, the node service's
  API and admin UI from 6600 to 6699, and the NodePorts 32080 to 32099 on the same ports
  ([Creating the node](smolvm-runtime.md#creating-the-node) has the settings).
- **The seed and the token.** Floci keeps the node's seed in
  `<FLOCI_RUNTIME_NODE_DATA_PATH>/<node>/seed`, `./data/runtime-node/<node>/seed` from Floci's
  working directory by default, readable only by the user that runs Floci. `api-token` there is the
  node service's bearer token, the same as `node-vm.sh token`. The seed also holds the Kubernetes
  admin token and the service account signing key.
- **The disks.** smolvm keeps the node's disks under `~/Library/Caches/smolvm/vms`. Images, Docker
  volumes and the databases' data are on the node's storage disk, so a deleted node takes them
  along.

Leave the node's life to Floci: a node that you stop or delete while Floci runs is logged and left
so ([When Floci stops and starts](smolvm-runtime.md#when-floci-stops-and-starts)).

## kubectl, k9s and Lens

**Through EKS.** With the `smolvm-pods` EKS provider, the cluster is the node's k3s.
`aws eks update-kubeconfig` writes a kubeconfig whose user runs `aws eks get-token`, which Floci
checks against IAM. Floci refuses tokens signed with the default `test`/`test` keys, so give the
kubeconfig a profile with an IAM user's keys:

```sh
export AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws iam create-user --user-name dev
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws iam create-access-key --user-name dev \
    --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text
aws configure set aws_access_key_id ACCESS_KEY_ID --profile floci-dev
aws configure set aws_secret_access_key SECRET_ACCESS_KEY --profile floci-dev
aws configure set region us-east-1 --profile floci-dev
aws configure set endpoint_url http://localhost:4566 --profile floci-dev
aws eks update-kubeconfig --name dev --profile floci-dev
kubectl get nodes
```

`update-kubeconfig --profile` records the profile in the kubeconfig, so kubectl, k9s and Lens sign
in with that user's keys, at Floci's endpoint.

**The admin kubeconfig.** `node-vm.sh kubeconfig` prints k3s's own admin kubeconfig for the node's
Kubernetes API port. It needs no AWS credentials, works while Floci is down, and works on a node
that runs only databases:

```sh
scripts/node-vm.sh kubeconfig --name floci-node-0123456789abcdef >~/.kube/floci-node.yaml
KUBECONFIG=~/.kube/floci-node.yaml k9s
```

**Lens.** Lens started from the Dock does not get the shell's `PATH`, so it does not find `aws`
for an `update-kubeconfig` kubeconfig and fails with `exec: "aws": executable file not found in
$PATH`. Add the admin kubeconfig to Lens instead, or write the absolute path, such as
`/opt/homebrew/bin/aws`, as the user's `exec.command` in the kubeconfig.

**`kubectl top`.** The node runs k3s's metrics-server, so `kubectl top nodes` and
`kubectl top pods` work about a minute after the node starts. A sleeping pod is missing from
`kubectl top pods`: the node reports it with no memory, which metrics-server leaves out. The node
answers those stats from its last sample, so metrics-server neither wakes a sleeping pod nor keeps
an awake one from sleeping. The `CPU(%)` and `MEMORY(%)` of `kubectl top nodes` are of the larger
capacity that kubelet reports, so read the absolute values.

`kubectl port-forward` does not reach VM pods: reach them through a NodePort Service.

## Docker

The node's dockerd runs the databases and any container you start there. The node service's API
serves dockerd's Engine API at the path `/docker`, behind its token. `node-vm.sh docker-env`
points the Docker CLI at it:

```sh
eval "$(scripts/node-vm.sh docker-env --name floci-node-0123456789abcdef)"
docker ps
```

It writes `~/.config/smolvm-pods/docker/<node>/config.json` (directory mode 0700, file mode 0600),
whose `HttpHeaders` send the token, and prints the exports of `DOCKER_HOST`,
`tcp://127.0.0.1:<port>/docker`, and of `DOCKER_CONFIG`, that directory. The token goes only to this
node, not to every daemon as it would from `~/.docker/config.json`. `unset DOCKER_HOST
DOCKER_CONFIG` goes back to your own Docker. With that `DOCKER_CONFIG` the CLI does not see the
plugins in `~/.docker/cli-plugins`, such as `docker compose`.

RDS's databases are containers labelled `io.floci.service=rds` and `io.floci.resource-id=<the DB
identifier>`, with names that start with `floci-aws-rds-`, each in a microVM
(`HostConfig.Runtime` `io.containerd.smolvm-pods.v2`):

```sh
docker ps --filter label=io.floci.service=rds
docker logs -f CONTAINER                   # works while the database sleeps, and does not wake it
docker exec CONTAINER pg_isready           # wakes a sleeping database
docker inspect CONTAINER
```

`docker ps` shows a sleeping database `Up`. Change databases through the RDS API, not with
`docker stop` or `docker rm`: Floci owns these containers.

## Watching sleep and wake

- **The admin UI** at `http://127.0.0.1:<node API port>/ui` shows every pod and database by state,
  who woke whom, and each one's timeline. It asks once for the token, `api-token` in the seed.
- **The events.** Every state change, live, one JSON object per line:

  ```sh
  token=$(cat ./data/runtime-node/floci-node-0123456789abcdef/seed/api-token)
  curl -sN -H "Authorization: Bearer $token" "http://127.0.0.1:6600/v1/events?follow=true&cursor=0" |
      jq -c '{time, pod, from, to, reason, reason_detail, error}'
  ```

- **Kubernetes Events.** For pods, the node sends `Slept` and `Woke` Events:

  ```text
  $ kubectl get events --field-selector involvedObject.name=web
  3m7s   Normal   Slept   pod/web   slept in 377 ms (idle), a checkpoint of 168 MiB
  35s    Normal   Woke    pod/web   woke in 104 ms (traffic from 100.96.0.1 to port 8080)
  ```

  Databases are Docker containers, not pods, so they have no Kubernetes Events: watch them in the
  UI or the events.

- **Now.** `scripts/node-vm.sh exec -- smolvm-pods ctl list` prints every pod and database with its
  state.

## Metrics

`GET /metrics` on the node API is in the Prometheus text format: pods by state, wakes, sleeps,
their durations, checkpoints and admission (`tools/smolvm-pods/docs/configuration.md`, "Metrics").
A Prometheus on the Mac, run as the user that runs Floci so that it can read the token file,
scrapes it with:

```yaml
scrape_configs:
  - job_name: floci-node
    authorization:
      type: Bearer
      credentials_file: /src/floci/data/runtime-node/floci-node-0123456789abcdef/seed/api-token
    static_configs:
      - targets: ["127.0.0.1:6600"]
```

## Logs

| Where | What | How |
| --- | --- | --- |
| `/var/log/smolvm-pods/node-service.log` in the node | Pod and database VMs, sleeps, wakes, the idle loop | `scripts/node-vm.sh exec --stream -- tail -f /var/log/smolvm-pods/node-service.log` |
| `/var/log/smolvm-pods/front-shim.log` | The shim that containerd starts | `exec -- tail -n 100 FILE` |
| `/var/log/k3s.log` | k3s and kubelet | `exec -- tail -n 100 FILE` |
| `/var/log/dockerd.log` | dockerd | `exec -- tail -n 100 FILE` |
| A pod's output | kubelet's pod logs | `kubectl logs -f POD`, or `scripts/node-vm.sh kubectl --stream -- logs -f POD` |
| A database's output | dockerd's log of the container | `docker logs -f CONTAINER`; Floci also copies it into its own log, on lines with `[rds:ARN]` |
| floci-node | The node VM's own log: its setup, the capacity, why it stopped | `"$SMOLVM_SRC"/target/floci/kernel/bin/smolvm-floci machine logs --name NODE` |

`node-vm.sh exec` and `node-vm.sh kubectl` print a command's output when it ends unless they get
`--stream`, so `tail -f` and `logs -f` need it.

## What does not work

- **A terminal or stdin in a microVM.** `docker exec -it` and `docker exec -i` into a database, and
  `kubectl exec -it`, `kubectl attach` and `kubectl cp` into a VM pod, are refused:
  `smolvm-pods does not give VM pod processes a terminal`. Connect `psql` or `mysql` to the
  database's RDS endpoint, `localhost` and its port from `describe-db-instances`, or run one
  command without `-i` and `-t`: `docker exec CONTAINER psql -U USER -d DB -c 'select 1'`.
  Containers in runc, without the runtime, take `-it`.
- **`kubectl port-forward`** to a VM pod: use a NodePort Service.
- **`docker pause`** of a microVM: the node puts it to sleep instead.
- **Volumes, load balancers and ingress for pods.** The node has no default StorageClass, ServiceLB
  or Ingress controller.

## Troubleshooting

| Symptom | What to do |
| --- | --- |
| Floci logs `smolvm-pods node NODE is stopped, and Floci leaves it so`, and databases and the cluster do not answer | `scripts/node-vm.sh start --name NODE`, or restart Floci, which starts it |
| kubectl fails with `x509: certificate signed by unknown authority` after the node was created again | A node created again has a new cluster CA: run `aws eks update-kubeconfig` or `node-vm.sh kubeconfig` again |
| `docker` answers `API returned a 401 (Unauthorized)`, or Prometheus gets 401 | The node has another token, as a node created again does: run `node-vm.sh docker-env` again, and point Prometheus at the current `api-token`. The admin UI asks again in a new tab |
| `docker-env` says `machine NODE publishes no node service API` | The node was made without `--service-port`, which only a node you made can lack: make it again with `node-vm.sh create --service-port PORT` |
| A database never sleeps | The events, or the UI's timeline, say why: an `awake` to `awake` event with `not sleeping: REASON`. A client that keeps sending on its connection, such as a pool's validation query or a monitoring check, keeps it awake, and so does a running `docker exec`. `FLOCI_SERVICES_RDS_SMOLVM_IDLE_TIMEOUT` sets how long it must be idle |
| `kubectl top` says `Metrics API not available` | metrics-server starts within about a minute of the node. A node from an image built before metrics-server was turned on has none: build the image again and let Floci create the node again |

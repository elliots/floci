# floci on smolvm-pods: remaining gaps

## Context

floci runs natively on a Mac. With these settings it runs one smolvm-pods node VM, which runs the EKS cluster's pods, the RDS databases and the Lambda environments, each in its own pod VM:

```sh
FLOCI_SERVICES_EKS_PROVIDER=smolvm-pods
FLOCI_SERVICES_RDS_SMOLVM_ENABLED=true
FLOCI_SERVICES_LAMBDA_SMOLVM_ENABLED=true
```

The node is isolated (`--isolate`):

- It reaches only the gateway to the Mac's loopback.
- Routed names resolve only through floci's node DNS at the node address, `169.254.53.1`.

The goal is that a user of floci needs no Docker daemon on the Mac (Docker Desktop or OrbStack), apart from building the node image once. These are the remaining gaps, most important first.

## 1. Image supply still needs the Mac's Docker

This is the main gap. Items 2 and 3 depend on it.

- `LambdaSmolvmBackend.ensureImage` and `RdsSmolvmBackend` (around `:225-253`) pull an image into the Mac's Docker. They then `saveImageCmd` it and `loadImageCmd` it into the node's dockerd.
- Their fallback, "the node's dockerd pulls it", cannot work: an isolated node has no egress and no DNS.

**Wanted:**

- floci pulls images without a Docker daemon: a registry client writes a tar, with ECR and Docker Hub credentials.
- floci loads the tar into the node's dockerd.
- Where k3s needs the image, floci also loads it into containerd's `k8s.io` namespace. The node dockerd's image store is separate from k3s's.

## 2. The ECR "prefer local images" check looks at the Mac's Docker

- `EcrRegistryManager.rewriteImageUri` (around `:123-152`) asks the Mac's daemon whether an image is present (`isPresentOnDaemon`).
- If the image is not there, it starts a `registry:2` container on the Mac's Docker.

**Wanted:** when Lambda or RDS run on the node, this check looks at the node's dockerd. The registry runs on the node, or not at all.

## 3. Lambda's image CMD comes from the Mac's Docker

- For an Image-package function, `ContainerLauncher.handlerFor` and `imageCmd` set `_HANDLER` from `ImageConfig.Command`, or else from the image's CMD. They read the CMD from the Mac's daemon.
- `KubernetesPodLauncher` (around `:194-196`) sets `_HANDLER` only from the function's Handler, never from the image's CMD.

**Wanted:**

- For environments on the node, read the CMD from the node's dockerd, or from the image config that item 1 pulls.
- Give `KubernetesPodLauncher` the same CMD fallback.

## 4. ElastiCache has no node backend

- `ElastiCacheContainerManager` runs Redis containers on the Mac's Docker.
- Pods on the node cannot resolve or reach them, and get `getaddrinfo ENOTFOUND` for the cache endpoint.

**Wanted:** an ElastiCache backend on the node's dockerd, like the RDS one:

- pod-VM containers;
- reached through the node tunnel;
- endpoint names answered by the node DNS.

## 5. Extra port mappings for the node

- floci creates the node with fixed Mac port mappings: the API port, the service port and `--node-ports`.
- smolvm allows 64 mappings per machine, fixed when the machine is created.
- `node-vm.sh create` passes extra smolvm options after `--`, but floci never passes any.

**Wanted:**

- A node setting for extra mappings, for example `443:30443`, so that a NodePort in the cluster can serve Mac port 443. Each mapping counts against the limit of 64.
- Check that smolvm's port forwarder can bind a port below 1024 without root. macOS allows a process that is not root to bind such a port on all interfaces.

## 6. Something rewrites the node's `/etc/resolv.conf`

- At boot, `floci-node` does three things:
  - sets `/etc/resolv.conf` to the node address;
  - keeps the original file in `upstream-resolv.conf`;
  - gives pods `pod-resolv.conf`.
- On a running node, `/etc/resolv.conf` was later found back at smolvm's resolver (`100.96.0.1`). This was 27 minutes after boot, with no reboot.
- dockerd now runs with `--dns <node address>`, so containers no longer depend on that file. Processes on the node itself still use it, and smolvm's resolver answers no names on an isolated node.

These are already ruled out as the writer:

- `node-vm.sh`;
- floci's node code;
- the node service;
- the guest agent's resolv.conf writer, which runs only at boot.

**Wanted:** find what rewrites the file and stop it, or make `floci-node` restore the file.

## 7. Keep only named connections through a pause (optional)

- A pause runs smolvm's `PodFreeze`, which resets every TCP connection that leaves the pod VM. The guest agent does this in `pod_freeze.rs` (around `:348-372`, `SOCK_DESTROY`).
- The current fix is a pod-level annotation, `smolvm-pods.floci.io/reset-on-pause: "false"`, which skips the reset entirely. floci sets it on every Lambda environment, because the runtime's `GET /runtime/invocation/next` must survive the freeze.

**Wanted:**

- An option in the freeze request that names the connections to keep, by destination IP and port or by a port range.
- With it, a Lambda environment keeps only its Runtime API connection and still drops all the others.

This needs:

- a change to the smolvm protocol and guest agent;
- a new pinned smolvm commit;
- a new `AGENT_SHA256`.

## 8. Flaky node test

- `node::tests::a_freed_block_of_a_few_mib_leaves_no_resident_memory` failed with "the resident set grew by 1024 KiB", against a limit just under 1024 KiB.
- It passed with the recent smolvm-pods changes set aside: the network table change and the `reset-on-pause` change.
- It measures allocator memory and uses none of the changed code. The threshold probably depends on how the binary is laid out, but this is not proven.

**Wanted:** make the test robust, or find the cause.

## 9. Smaller gaps on the node's dockerd

- **amd64:** only pod VMs get Rosetta, and nothing registers binfmt on the node itself.
  - amd64 runc containers probably fail.
  - So do `RUN` steps in `docker build --platform linux/amd64`.

  This is not checked yet.
- **Mac paths:** floci creates the node with no `--volume` shares, so `docker run -v <mac path>:…` on the node's dockerd cannot bind Mac paths. `node-vm.sh create --volume` exists, and floci could offer a setting for it.
- **stdin:** `docker exec -i` into a pod-VM container is refused (`refuse_input`). Support stdin, or document a workaround: put the input in an environment variable and pipe it through `sh` in the container.

## Already fixed, for reference

- dockerd runs with `--dns <node address>`.
- Isolated pods can reach the API server and kubelet ports at the node's own addresses. metrics-server needs this.
- Each pod VM is sized from its memory limit.
- The relay waits up to 30 s for a client's first bytes.
- Lambda environments keep their connections through a pause.
- An event whose environment dies before taking it goes once to a new environment.
- Runtime polls whose connections have closed are discarded before dispatch.

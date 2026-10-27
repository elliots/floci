# floci on smolvm-pods: remaining gaps

## Context

floci runs natively on a Mac. With these settings it runs one smolvm-pods node VM, which runs the EKS cluster's pods, the RDS databases and the Lambda environments, each in its own pod VM:

```sh
FLOCI_SERVICES_EKS_PROVIDER=smolvm-pods
FLOCI_SERVICES_RDS_SMOLVM_ENABLED=true
FLOCI_SERVICES_LAMBDA_SMOLVM_ENABLED=true
FLOCI_SERVICES_ELASTICACHE_SMOLVM_ENABLED=true
FLOCI_SERVICES_ECR_SMOLVM_ENABLED=true
```

The node is isolated (`--isolate`):

- It reaches only the gateway to the Mac's loopback.
- Routed names resolve only through floci's node DNS at the node address, `169.254.53.1`.

Users may fetch or build images on the Mac or another machine and transfer them into the node. Floci itself should not require a host Docker daemon once the node has its images. A daemon-free client for fetching external images is not required. These are the remaining gaps, most important first.

## 1. Image supply and the node registry

- `LambdaSmolvmBackend.ensureImage` and `RdsSmolvmBackend` (around `:225-253`) pull an image into the Mac's Docker. They then `saveImageCmd` it and `loadImageCmd` it into the node's dockerd.
- Their fallback, "the node's dockerd pulls it", cannot reach external registries on an isolated node. The node has Floci DNS, but no external registry egress.

**Wanted:**

- With `FLOCI_SERVICES_ECR_SMOLVM_ENABLED=true`, the emulated ECR registry and its data run on the node. The node image includes the registry image; Floci does not need a host daemon to start it.
- Users push images prepared elsewhere through Floci's existing ECR endpoint. The node dockerd and k3s's separate containerd store can both pull them. A real-node test verifies both pulls while Floci's host Docker socket is unavailable.
- Host Docker pulls and archive transfer remain an optional convenience. Switching an existing host-backed ECR to the node selects a separate image store; images must be pushed again.

## 2. The ECR "prefer local images" check includes the selected node

- Fixed for Lambda: `EcrRegistryManager.rewriteImageUri` checks the selected node's dockerd first, then the Mac's daemon, under the resolved image reference.
- If neither has the image, Floci can use the emulated registry on the node when `FLOCI_SERVICES_ECR_SMOLVM_ENABLED=true`.

The host-backed registry remains the default to preserve existing stores.

## 3. Lambda image CMD selection

- Fixed for the Docker launcher: prepare the image on the selected daemon before reading its CMD. Node launches inspect the node's image, honoring the requested platform and any `ImageConfig.Command` override.
- For a Kubernetes Lambda whose image is in node-backed ECR, Floci reads the image CMD and pins the pod to its manifest digest. `ImageConfig.Command` and an explicit handler take precedence.

**Wanted:**

- Other Kubernetes registry references still need an explicit handler or `ImageConfig.Command`; their image CMD is not yet inspected.

## 4. ElastiCache node backend

- Redis and Valkey can run on the shared node with `FLOCI_SERVICES_ELASTICACHE_SMOLVM_ENABLED=true`. Their proxy, readiness checks and cluster formation connect through the node tunnel. Cache hostnames from the `floci:tls-hostnames` tag resolve through node DNS.
- The real-node test verifies a cache's data survives a sleep and a tunnel connection wakes it. An AWS SDK test exercises provisioning and deletion against a fake node.
- Memcached remains on host Docker.

## 5. Extra port mappings for the node

- `FLOCI_RUNTIME_NODE_EXTRA_PORTS` supplies extra TCP mappings when Floci creates a node, in addition to the API port, service port and `--node-ports`.
- smolvm allows 64 mappings per machine. TCP mappings can be updated live; changes to UDP mappings require stopping the machine.
- Floci validates port ranges, overlaps and the total limit before passing the options to `node-vm.sh create`. Existing nodes must be recreated to change the mappings.

Port 443 and 1023 both returned permission denied to the unprivileged test process on the tested Mac. An available higher port works without that privilege; a low port needs an appropriate host-level arrangement.

## 6. Node resolver overwrite fixed

- At boot, `floci-node` does three things:
  - sets `/etc/resolv.conf` to the node address;
  - keeps the original file in `upstream-resolv.conf`;
  - gives pods `pod-resolv.conf`.
- On a running node, `/etc/resolv.conf` was later found back at smolvm's resolver (`100.96.0.1`). This was 27 minutes after boot, with no reboot.
- dockerd now runs with `--dns <node address>`, so containers no longer depend on that file. Processes on the node itself still use it, and smolvm's resolver answers no names on an isolated node.

**Cause and fix:** interactive exec prepares the persistent overlay again. The guest agent refreshes a regular `/etc/resolv.conf` during that preparation, even on an already running node. It preserves a valid image-owned symlink, so `floci-node` now links `/etc/resolv.conf` to `/etc/floci-node/pod-resolv.conf`. The saved upstream resolver remains separate. A real-node regression repeats interactive exec and checks both files.

## 7. Keep only named connections through a pause

- The freeze request can preserve named destination IPs and port ranges while resetting other external TCP connections.
- Lambda preserves its Runtime API connection. Older nodes retain the compatibility annotation and preserve all connections.
- A real-node test verifies that one named connection survives a pause while an unrelated connection is reset.

## 8. Allocator test fixed

- `node::tests::a_freed_block_of_a_few_mib_leaves_no_resident_memory` failed with "the resident set grew by 1024 KiB", against a limit just under 1024 KiB.
- It passed with the recent smolvm-pods changes set aside: the network table change and the `reset-on-pause` change.
- It measured the approximate RSS counter in `/proc/self/statm`. This can differ from the precise resident mapping total around the 1024 KiB boundary.

**Fixed:** measure `/proc/self/smaps_rollup`, warm the measurement before allocating, and run a separate untuned allocator control. The tuned process must release the allocation; the untuned control must retain it. The threshold was not relaxed.

## 9. Smaller gaps on the node's dockerd

- **amd64:** pod VMs use Rosetta automatically. An ordinary Docker container can use Rosetta
  with a read-only bind mount of `/mnt/rosetta`; a real-node check ran an amd64 binary this way.
  The mount is not automatic, and amd64 Docker build steps still need a solution.
- **Mac paths:** `FLOCI_RUNTIME_NODE_VOLUMES` passes directory shares when creating the node. Containers bind the resulting node path, not the original Mac path.
- **stdin:** `docker exec -i` into a pod-VM container is refused (`refuse_input`). The node guide documents passing small input as an environment variable and piping it through `sh` in the container.

## Already fixed, for reference

- dockerd runs with `--dns <node address>`.
- Isolated pods can reach the API server and kubelet ports at the node's own addresses. metrics-server needs this.
- Each pod VM is sized from its memory limit.
- Container memory statistics now report that VM's actual memory limit, including after node recovery, instead of always reporting 1 GiB. Older node samples retain the 1 GiB fallback.
- The relay waits up to 30 s for a client's first bytes.
- Lambda environments keep their connections through a pause.
- An event whose environment dies before taking it goes once to a new environment.
- Runtime polls whose connections have closed are discarded before dispatch.

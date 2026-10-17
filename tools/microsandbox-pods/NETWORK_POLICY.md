# Native Kubernetes NetworkPolicy

The native controller implements IPv4 TCP and UDP NetworkPolicy in an explicit
node-wide mode. Enable it when creating a dedicated native node:

```sh
python3 tools/microsandbox-pods/manage.py run \
  --directory /private/native-cluster --network-policy
```

The controller flag is `--network-policy=true`. Every regular and init container
on that native node must explicitly drop `NET_RAW` or `ALL`, even if no policy
currently selects its Pod. For example:

```yaml
securityContext:
  capabilities:
    drop: [ALL]
```

Adding `NET_RAW`, `NET_ADMIN`, `SYS_ADMIN`, `SYS_MODULE`, `BPF`, `SYS_RAWIO`,
`SYS_PTRACE`, `PERFMON` or `ALL` is rejected. Existing restrictions on privileged
containers and host namespaces also apply. The application cgroup hierarchy is
mounted read-only. These restrictions are explicit admission requirements; the
controller does not silently remove requested capabilities.

Without this mode, ordinary native workloads retain their usual capabilities.
A selecting NetworkPolicy is unsupported: creation and wake are refused, and a
policy added while the Pod is running causes its traffic to be fenced and its
applications frozen or VM paused. This does not turn an ordinary node into an
enforcing node automatically.

## Supported rules

- Directional ingress and egress isolation, empty default-deny rules and the
  additive union of all selecting policies.
- Pod selectors, namespace selectors, their intersection within one peer, and
  the union of separate peers. Label and Pod-IP changes are watched.
- IPv4 `ipBlock` and `except`, numeric ports, TCP/UDP protocols and port ranges.
- Named ingress ports on the target Pod, and named egress ports when the
  destination is identified by Pod or namespace selectors.
- Policy changes while running and the latest policy before thawing a sleeping
  application, including controller recovery.

IPv6 policy peers, SCTP and named egress ports for arbitrary IP blocks are
rejected explicitly. IPv4 egress isolation also denies unmarked application
IPv6 egress, so the guest's node-facing IPv6 interface cannot bypass the policy.
Loopback and replies to admitted traffic remain available. Node operations,
including image pulls, use their separate trusted socket identity; application
DNS uses the Pod's ClusterFirst resolver and needs an explicit DNS allow rule
when egress is isolated.

## Enforcement and source addresses

The shared owned Linux peer enforces cluster traffic after Service destination
translation and before accepting wake-proxy traffic. It checks the original
client Pod address, so denying a request does not wake a cold destination.
The application behind a proxy still sees the proxy's source address. Strict
direct Pod networking preserves the original source address.

Guest output rules also cover direct node-facing egress that does not traverse
the peer. Small cgroup BPF programs distinguish trusted node sockets from
application sockets and reject changes to the reserved socket mark. Application
UDP sockets cannot send to port 8472 or the peer's published VXLAN port, including
through IPv4-mapped IPv6 sockets. These ports are reserved for the transport.
Native Pod addresses must arrive through the owned bridge and expected VXLAN
interface. Ordinary CNI workload traffic is denied access to those outer
transport ports before host or Service translation, and cannot claim a native
Pod or trusted node source address.

The peer, its administrative processes, Kubernetes API server and host are
trusted. Kubernetes exec and port-forward are administrative access, not a
NetworkPolicy isolation boundary. Identities of ordinary Docker-worker Pods
continue to depend on that worker's CNI: this implementation does not claim to
authenticate a hostile Docker Pod that can forge another Docker Pod's address.
External CNI plugins, host-network workloads and arbitrary external workers are
not covered by this owned-peer qualification.

Policy enforcement uses shared controller watches, peer firewall rules and
one-shot guest helpers. It adds no resident daemon per Pod. Full compatibility
with raw-socket applications would require separating application and node
network namespaces; it is not provided by relaxing the capability requirement.

## Updates, failures and recovery

Three shared watches track Pods, namespaces and policies. A fresh inventory must
be installed before a reconnect becomes healthy. The controller coalesces bursts
and installs a deny fence on the peer before changing a running guest's rules.
There is no atomic transaction across all VMs: direct guest egress converges when
the Pod's current lifecycle operation releases its lock. Failed installations
are not recorded as applied.

An unavailable policy inventory withdraws readiness and prevents new activation.
In enforcing mode it denies application traffic until the inventory recovers.
In ordinary mode it preserves unselected running applications while withholding
readiness and new activation; a watch reconnect alone does not permanently fail
those Pods. A known selecting unsupported policy still triggers fencing.

If guest enforcement fails, the controller tries to freeze application cgroups
and independently pause and verify the exact VM. It does not force-stop a VM
with possibly dirty persistent volumes. If neither operation can be confirmed,
the Pod is unavailable and reports `NetworkPolicyUncertain`; peer traffic stays
blocked, but direct guest egress cannot be claimed stopped. Operator recovery
must resolve that VM before treating the node as isolated.

Policy-capable Pod journals use format 9. Earlier controllers reject them.
Changing the node-wide mode while Pod journals remain is refused. To migrate,
use the original controller mode to stop and delete those Pods, retain their
PVCs, then recreate them on the new node mode. RAM checkpoints are not silently
adopted without the socket and policy fences installed at original creation.

This capability is separate from the environment-wide simulator-routing and
egress boundary. The SDK boundary alone cannot protect traffic tunneled through
the Docker peer. Protected managed peers now combine verified peer filtering,
the outer Docker bridge firewall and the SDK boundary, with shared controlled
DNS and exact host transport allowances. Setup requires a fresh protected record
and enables the node-wide capability rules above. See the
[environment setup](../../docs/configuration/workload-network.md#native-microvm-boundary).
Its full native qualification is still being run; the independent NetworkPolicy
matrix passed 21 checks and the restricted installed smoke passed seven.

The rule model follows the [Kubernetes NetworkPolicy contract](https://kubernetes.io/docs/concepts/services-networking/network-policies/).

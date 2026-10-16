# Workload network isolation and simulated services

Floci can resolve third-party hostnames locally and send their HTTP or HTTPS requests to your
simulators. Optional network isolation blocks connections outside the environment's Docker
networks and the native runtime's explicit local destinations. Isolation is disabled by
default and is independent of AWS security-group rules.

Set `FLOCI_NETWORK_CONFIG_FILE` to the network YAML or JSON file. Floci reads and validates
it at startup; restart after changes.

## Configure origins

For example, `network.yaml`:

```yaml
network:
  isolation:
    enabled: true
  routes:
    payments:
      origins:
        - https://api.vendor.test
        - https://uploads.vendor.test:443
        - http://api.vendor.test:80
      target:
        backend-url: http://payments-simulator:8080
```

Each route needs a nonempty `origins` list and a `target.backend-url`. Origins are exact HTTP(S)
origins using DNS hostnames, without paths, credentials, queries, or fragments.
Wildcard origins such as `https://*.example.test` cover subdomains at any depth, but
not `example.test` itself. Exact origins take priority; otherwise the most specific
wildcard wins. The gateway issues certificates for matching HTTPS names on demand.
Hostnames are case insensitive; omitted ports mean 80 for HTTP and 443 for HTTPS. Duplicate
origins, unknown fields, and obvious backend loops fail startup. HTTP and HTTPS cannot share
the same origin port. Port 9443 and `network.floci.internal` are reserved for pod admission.

The gateway forwards the method, path, query, and body to the backend URL. By default it uses
the backend URL's authority for `Host` and writes the original authority to `X-Forwarded-Host`,
replacing any client-supplied value. For simulators that select responses by the original Host,
set `target.preserve-host: true`. Streaming and WebSocket forwarding are supported.

Backend addresses must resolve to containers on the environment's Docker networks. Container
names, network aliases, container IPv4 addresses, and Floci-owned local DNS records are supported.
Floci checks local DNS answers against Docker network membership and never uses public DNS for
backend resolution. For Kubernetes simulators, use a reachable node address and NodePort.
Cluster-only Service DNS names and pod addresses are not gateway backend addresses. HTTPS
backend targets validate against Floci's generated CA.

Successful backend lookups are cached for one second, with at most 1,024 hostnames retained.
After expiry, Floci checks network membership again; failed lookups never reuse expired answers.
A replaced container's old address can remain cached for up to one second. Requests are not
automatically replayed if that address stops accepting connections.

## Forward to an on-demand simulator

Networking can call an [on-demand gateway](microsandbox-workloads.md) using a URL. For example, add this route
under `network.routes` in `network.yaml`:

```yaml
notifications:
  origins:
    - https://notifications.vendor.test
    - https://status.vendor.test
  target:
    backend-url: http://notifications.localhost.floci.io:8080
```

In `on-demand.yaml`, configure the matching gateway hostname and actual backend:

```yaml
workloads:
  notifications-simulator:
    host: notifications.localhost.floci.io
    idle-timeout-seconds: 120
    runtime:
      type: microsandbox
      image: your-simulator:1.0.0
      backend-port: 8080
      cpus: 1
      memory-mib: 512
      root-disk-mib: 4096
    readiness:
      path: /ready
```

Set `FLOCI_ON_DEMAND_ENABLED=true`, `FLOCI_ON_DEMAND_CONFIG_FILE=/etc/floci/on-demand.yaml`,
`FLOCI_ON_DEMAND_GATEWAY_HOST=0.0.0.0`, and `FLOCI_ON_DEMAND_GATEWAY_PORT=8080`. The deployment's
network-exposure consent also covers this listener. The built-in `*.localhost.floci.io` DNS
suffix points at Floci. The network gateway sends `Host: notifications.localhost.floci.io:8080`,
and the on-demand gateway handles activation, health exclusions, and request leases normally.
The real backend URL must bypass both gateways. Leave `preserve-host` disabled for this route.

## Docker deployment

Run Floci in a container with a rootful Linux Docker daemon and dedicated user-defined bridge
networks. Docker Desktop and OrbStack use a Linux daemon even when the host is macOS. Running
Floci itself as a host process, rootless Docker, host networking, and non-bridge drivers are
rejected when this facility is configured. The native helper and pod controller run on the host
and enforce their own boundary, described below.

Start Floci with `--dns 127.0.0.1` when isolation is enabled. This makes the Floci container's
own Docker resolver forward unknown names to Floci, preventing indirect public DNS lookups.
Floci validates this setting at startup. Spawned containers receive Floci's resolver automatically.

For example, add these settings to your existing Compose deployment:

```yaml
services:
  floci:
    # Keep your existing image, socket mount, ports, and persistent data volume.
    dns: [127.0.0.1]
    environment:
      QUARKUS_HTTP_HOST: 0.0.0.0
      FLOCI_SECURITY_ALLOW_UNSAFE_NETWORK_EXPOSURE: 'true'
      FLOCI_NETWORK_CONFIG_FILE: /etc/floci/network.yaml
      FLOCI_SERVICES_DOCKER_NETWORK: floci-dev
      FLOCI_DOCKER_RESOURCE_NAMESPACE: isolated-dev
    volumes:
      - ./network.yaml:/etc/floci/network.yaml:ro
    networks: [floci-dev]

  payments-simulator:
    image: your-payments-simulator:1.0.0
    networks: [floci-dev]

networks:
  floci-dev:
    name: floci-dev
```

The dedicated origin gateway has its own container address, so origin ports do not compete with
Floci's AWS HTTPS listener or host-published ports. There is no need to publish the gateway's
ports on the host. Main AWS API TLS can remain disabled while simulated HTTPS origins are enabled.

The network helper uses the `floci.network.security-group-enforcement.helper-image`
setting. Its default, `floci/network-helper:local`, is built from the repository's
network-helper recipe when absent. The helper needs host network access and `NET_ADMIN` to
install a separate nftables table. It does not replace Docker's firewall tables.

## HTTPS trust

Floci uses its persistent generated local CA and issues a certificate covering the configured
origins. Keep Floci's persistent data directory across restarts to retain that CA.
You do not need to provide server certificates or private keys.

Managed application containers receive a read-only public certificate at:

```text
FLOCI_CA_BUNDLE=/etc/floci/certs/ca.crt
```

The file contains the public CA certificate only. Signing keys and gateway private keys remain
inside Floci. The variable points at the mounted file but does not, by itself, configure an
application's TLS client. Configure your client or trust store to add this CA when the variable
is present. For example:

```sh
curl --cacert "$FLOCI_CA_BUNDLE" https://api.vendor.test/v1/customers
```

Java clients may require a JVM trust store. Clients with their own trust configuration or
certificate pinning need explicit application support. HTTP needs no certificate configuration.

Floci-managed EKS clusters receive a Kubernetes mutating admission webhook which mounts the
node's public CA file into regular and init containers and supplies `FLOCI_CA_BUNDLE`. The
webhook uses `failurePolicy: Fail`: new pod admission fails when trust injection is unavailable.
The CA volume name and environment variable are reserved. The node's kubelet receives a
non-loopback resolver file so CoreDNS forwards external names to Floci instead of substituting
public fallback resolvers. Cluster Service DNS continues to use CoreDNS.

Recreate existing workload containers and EKS clusters when first enabling this feature.
Existing pods are not retroactively mutated. New clusters wait for the `floci-network-trust`
admission configuration before becoming active. External
Kubernetes clusters and the external Kubernetes Lambda executor are unsupported under isolation;
Floci cannot enforce their node boundary through this configuration.

## Enforcement and logs

The boundary covers the dedicated Docker bridges, including EC2 containers, ECS containers,
Lambda Docker runtimes, simulators, and the outer containers of Floci-managed k3s nodes. It is
installed before managed application startup and applies to both IPv4 and IPv6. Published-port
containers attach to their protected network before they start. Security groups remain a
separate policy and cannot reopen outbound access through this boundary.

Connections within protected networks and replies to incoming connections remain possible.
New connections to other networks, the Docker host, or the internet are dropped. Direct IP
connections, alternative DNS servers, and changes to a node's own firewall cannot bypass the
outer bridge policy. Dedicated networks are required because every container on those bridges,
including independently started simulators, is affected. Do not attach those containers to
additional unprotected networks or expose Docker administration to application workloads.

With isolation enabled, Floci logs DNS decisions at DEBUG and the policy monitor logs
connection attempts observed at the boundary. Routed requests log their origin and route;
unmapped origins are rejected. Public DNS forwarding and Route 53 Resolver forwarding rules
are disabled in this mode. Existing local DNS records still resolve. Logs omit HTTP headers
and bodies. Source addresses identify connections; shared resolvers, caches, and Kubernetes
NAT can prevent attribution to an individual application or pod.

The monitor continues writing Docker logs and the kernel policy remains installed when Floci
stops, so surviving workloads do not regain external access. To disable isolation,
set `network.isolation.enabled: false` in the configured file and restart the same Floci
environment. Keep its resource namespace unchanged so Floci can identify its policy helper.

Image pulls by the host Docker daemon are provisioning traffic outside this boundary. For a
fully offline environment, preload the helper, application, and node images. k3s also needs its
pause, CoreDNS, and application images imported into its own containerd store or supplied by a
local registry. Package downloads from inside isolated workloads are blocked and need local
repositories or prebuilt images. This facility isolates trusted development workloads; it does
not sandbox hostile privileged containers or an administrator controlling the Docker daemon.

## Native microVM boundary

Set `FLOCI_NETWORK_CONFIG_FILE` for Floci, the native host helper and the pod controller.
They must read identical file bytes. Add native destinations to that same file:

```yaml
network:
  isolation:
    enabled: true
  native:
    dns-nameservers: ["127.0.0.1:5300"]
    allowed-egress:
      - destination: "172.24.0.0/16"
        protocol: tcp
        ports: "80-443"
      - destination: host
        protocol: udp
        ports: "31000"
    ca-file: /absolute/host/path/floci-ca.crt
  routes:
    payments:
      origins: ["https://api.vendor.test"]
      target:
        backend-url: http://payments-simulator:8080
```

These addresses and ports are examples, not automatic discovery. Publish Floci's controlled
DNS resolver on the chosen host port and use a locally reachable protected bridge subnet or
explicit simulator port. The host UDP example permits a configured pod relay. Include the
local AWS endpoint, credentials endpoint and required peer relay ports for the workloads you
run. Allow only the required ports: an allowed local forward proxy could itself provide
internet access. Native isolation does not change the host's firewall.

The native adapter intersects each workload's requested policy with this local allowlist.
It cannot grant public destinations, and a workload security group cannot reopen them.
IPv6 egress is denied. TCP and UDP exceptions require private IPv4 CIDRs or `host`, plus an
explicit port or range. DNS goes only to the configured local IP:port resolvers; those
resolvers must also have public forwarding disabled. No native DNS configuration means
startup fails while isolation is enabled.

`ca-file` is optional and names a public PEM certificate bundle on the native host, not a
private signing key. It is copied into guests before boot at `/etc/floci/certs/ca.crt`, with
`FLOCI_CA_BUNDLE` pointing to it. Clients still need to opt into trusting the CA as described
above. Mount the same network file into Floci; its host-side certificate path is interpreted
by the native helper, not resolved inside the Floci container.

The helper checks existing active guests before advertising readiness. Guests and full RAM
checkpoints are tied to a SHA-256 fingerprint of the entire configuration file. A helper
with a different fingerprint rejects new requests from Floci. An older guest or checkpoint
cannot be started, resumed or executed under a changed boundary; recreate it deliberately.
Stop and delete remain available for cleanup. Changing even a comment changes this identity.
Use a dedicated runtime home and keep it together with Floci's journals across restarts.

Native isolation also requires `FLOCI_MICROSANDBOX_GUEST_AGENT_PATH` on the native
helper and pod controller. Build the static Linux `cmd/guestnet` binary from
`tools/microsandbox-runtime` for the host architecture, as described in the
[guest transport guide](../../tools/microsandbox-runtime/guestnet/README.md). The initial installer needs a shell and standard
file utilities in the guest. It does not install packages or run a DNS daemon.

One-time guest setup routes DNS directly to the configured shared resolvers using
kernel nftables rules. There is no resident DNS process per VM. Configure one to
three local IPv4 resolver endpoints. Use a direct resolver port such as 5300. Ports 53, 137, 853, 5353 and 5355 are
intercepted or blocked by the pinned SDK and cannot be used for these endpoints.
The guest setup supports UDP and TCP; the shared resolver must provide whichever
transports its clients need.

SDK 0.7.8 drops network policy and DNS configuration on full RAM restore. The adapter
therefore persists each guest's effective rules alongside its immutable identity,
copies that record to the checkpoint, and reapplies the rules before RAM resumes.
Restore blocks the SDK's default DNS gateway. Guest routing is refreshed before the
operation succeeds, including stale DNS connections if the host gateway changes.
Keep the helper's `floci-network-boundaries` and `floci-network-snapshots` directories
with its runtime home. Old isolated guests/checkpoints without these records must
be recreated. Changed CA bytes also require guest recreation even if the network
file's path and text are unchanged.

Linux/KVM qualification remains separate from Apple Silicon validation.
Host image downloads are provisioning traffic. Image pulls inside an isolated guest need
preloaded content or a registry reachable through the local allowlist.

The native helper validates `network.native.ca-file` when it loads the configuration and retains those public certificate bytes. Before launch it creates a private, content-addressed copy under its runtime home and passes that copy to the CLI. Replacing or deleting the configured CA file after startup cannot inject different contents into a later guest; restart the helper to load a changed certificate. A modified or non-private cached copy causes launch to fail.

When a native public CA is configured, `FLOCI_CA_BUNDLE` is reserved. Supplying it through `--env` or `-e` is rejected before launch, including attempts to repeat the expected value. Application environment variables remain unchanged when no native CA is configured.

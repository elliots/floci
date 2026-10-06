# Workload network isolation implementation plan

## Objective

Provide optional network isolation and named routes containing multiple HTTP or HTTPS origins.
Managed applications call their normal third-party hostnames while Floci sends those requests
to local simulators. Isolation is disabled by default.

## Configuration and behavior

- `FLOCI_NETWORK_CONFIG_FILE` selects the network YAML/JSON file containing
  `network.isolation.enabled` and `network.routes`.
- `FLOCI_ON_DEMAND_CONFIG_FILE` selects the on-demand YAML/JSON file containing `workloads`.
  Each parser validates its file's schema at startup. Configuration changes require a restart.
- Each network route has an `origins` list and a `target.backend-url`. Reject duplicate origins,
  unknown fields, and routing loops at startup.
- Use the backend URL's authority for `Host` by default and carry the original authority in
  `X-Forwarded-Host`. Support `target.preserve-host` for simulators that need the original Host.
- When isolation is enabled, always log DNS lookups and connection routing or denial, without
  logging credentials or request bodies. Unknown destinations must never fall back to the internet.
- Use the persistent Floci CA by default. Issue certificates for configured HTTPS names.
  Distribute only the public CA to application containers and expose its path as `FLOCI_CA_BUNDLE`.
  Applications choose how to add that certificate to their TLS trust configuration.
- Preserve request methods, paths, query strings, bodies, streaming, and WebSocket behavior.
- Enforce isolation before application execution, independently of emulated security groups.
  Reject unsupported runtime or networking arrangements instead of starting unprotected workloads.
- Protect both IPv4 and IPv6 and prevent public DNS fallback. Keep Floci infrastructure and
  approved simulator destinations reachable. A gateway failure must not restore external access.

## Architecture

`runtime.network` owns network configuration, origin routing, DNS policy, isolation enforcement,
and public CA delivery. Routes address their backends by URL. Backend lookup uses Docker network
inspection or Floci-owned local DNS records, and checks addresses against protected network
membership before forwarding.

`core.common.http.HttpReverseProxy` provides HTTP and WebSocket forwarding. The network gateway
and on-demand gateway both use it. `runtime.ondemand` owns activation, health exclusions, and
request leases. A network route can reach an on-demand simulator by using its gateway URL as
the backend. The on-demand runtime integration validates external Kubernetes execution against
the active isolation policy.

The Docker lifecycle installs a dedicated host nftables table before managed containers start.
The table covers the environment's dedicated bridges, both address families, and the outer
network of managed k3s nodes. Local peers and replies remain reachable; other destinations are
denied. Kernel rules and a persistent, rotating-log monitor survive Floci shutdown. An explicit
disabled configuration removes the owned policy on the next start.

A dedicated relay address serves configured origin ports. AWS listeners retain their own
addresses and certificates. DNS never forwards unknown names under isolation. Certificates come
from the persistent Floci CA; only its public certificate is mounted into application containers.
EKS admission supplies the same read-only file and environment variable to regular and init
containers. Managed nodes supply CoreDNS with Floci's resolver, and cluster readiness waits for
the admission configuration.

Supported environments are Floci in rootful Linux Docker on dedicated user-defined bridges,
including Docker Desktop/OrbStack and Floci-managed EKS. Native host processes, external
Kubernetes execution, host/default networking, and rootless daemons fail closed when configured.
Deployment prerequisites and provisioning-image limitations are documented in
[Workload network isolation](../configuration/workload-network.md).

## Implementation steps

- [x] Define strict network configuration parsing and immutable configuration models.
- [x] Implement URL-based origin routing and HTTPS certificate issuance with the Floci CA.
- [x] Provide HTTP and WebSocket forwarding for the network and on-demand gateways.
- [x] Integrate local DNS answers and mandatory lookup and routing logs with no public fallback.
- [x] Install network enforcement and deliver the public CA before managed workloads execute.
      Cover Kubernetes application containers and refuse unsupported launch paths explicitly.
- [x] Cover configuration, HTTPS, routing, lifecycle, and isolation with focused tests,
      including multiple origins, unknown destinations, direct IP attempts, and disabled behavior.
- [x] Document configuration and deployment examples. Run relevant tests, Checkstyle,
      partition-check, docs-check, and diff validation.

## Validation

Use local HTTP and HTTPS servers for deterministic gateway tests. Verify that HTTPS succeeds with
Floci's public CA and fails with an unrelated trust root or incorrect hostname. Exercise on-demand
activation through a gateway backend URL. Docker-gated tests must prove that an application reaches
its simulator but cannot reach an unapproved destination, including when DNS or the gateway fails.

Validation results:

- 553 tests passed across 36 suites with no failures or skips, including configuration, generated
  HTTPS trust, DNS, admission mutation, Docker isolation, EC2/ECS/EKS on-demand runtimes, queue
  activation, HTTP/2, WebSockets, streaming, and request leases.
- The Docker integration test verifies IPv4 and IPv6 local connectivity, blocked direct IP and
  host-published destinations, drop counters for IPv6, resistance to clearing the node's own
  firewall, and restored connectivity after policy removal.
- A live Java 25 Floci container loaded network and on-demand files. SDK-launched ECS callers
  verified generated-CA HTTPS, multiple origins, backend authority rewriting, original authority
  forwarding, and DNS denial. A network route reached the on-demand HTTP gateway through a local
  DNS-backed URL and changed the simulator ECS service from zero to one desired task.
- Live ECS and EC2 checks verified public CA delivery and a read-only CA mount. A live SDK-created
  EKS cluster admitted a pod with the CA and variable automatically; the pod verified HTTPS to
  both origin aliases through cluster DNS. Direct IP attempts from ECS and Kubernetes to an
  unapproved network timed out.
- Live shutdown/restart checks verified that denial persists while Floci is stopped, and an
  explicit disabled configuration removes the owned helper and restores connectivity.
- Java 25 packaging, Checkstyle, partition-check, documentation regeneration, and diff validation
  passed. The full repository test suite and native-image build were not run.

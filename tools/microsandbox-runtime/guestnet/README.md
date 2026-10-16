# Guest network transports

The `guestnet` package contains a cancellable TCP stdin/stdout connector used by
Kubernetes port-forward and one-shot isolated DNS routing. It contains no AWS
handlers or credential transport. Service-specific protocol and identity
semantics stay outside this package.

Build the static Linux guest executable for the guest architecture:

```sh
cd tools/microsandbox-runtime
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -o /path/to/guestnet-arm64 ./cmd/guestnet
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o /path/to/guestnet-amd64 ./cmd/guestnet
```

Set `FLOCI_MICROSANDBOX_GUEST_AGENT_PATH` to the matching binary as seen by
the native host helper and pod controller when using environment isolation.
The adapter streams it through private guest stdin for the one-shot DNS setup,
then removes the temporary binary and configuration. Kubernetes port-forward
uses the same connector through its existing relay binary. Neither path starts
a resident DNS daemon.

`--forward 127.0.0.1:8080` connects stdin/stdout to a TCP listener, preserves
half-close and drains the final reply. It accepts explicit unicast IPv4
destinations only. No guest configuration file is needed in this mode.

`--configure-dns --config /private/dns.json` installs DNS routing once and exits.
It requires root with Linux network administration privileges, but no external
`nft`, `iptables`, DNS server process or guest package installation:

```json
{"dnsRouting":{"nameservers":["host.microsandbox.internal:5300","10.10.0.2:5300"]}}
```

One to three explicit private IPv4 resolver endpoints are accepted. The special
host name is read directly from `/etc/hosts` on every invocation, never through
DNS. Public, loopback, implicit-port and arbitrary hostname endpoints are rejected.
Ports 53, 853, 5353, 5355 and 137 are reserved by the runtime's DNS interceptor
and cannot be direct upstream ports. Publish the controlled resolver on a
different port, such as 5300, for both UDP and TCP.
The installer writes reserved DNS addresses `198.18.0.53` through `198.18.0.55`
to `/etc/resolv.conf`, retaining its search domains and resolver options. An
owned IPv4 `floci_dns` nftables NAT output table translates TCP and UDP port 53
on those addresses to the configured endpoints. Other tables and destinations
are preserved. Existing nameservers are removed; there is no fallback resolver.

Run this command after both boot and RAM restore, before exposing the workload:
guest kernel rules survive RAM snapshots but the host gateway can change. Each
invocation replaces the owned rules atomically and expires only conntrack flows
to the reserved DNS endpoints, using their original tuple, zone and flow ID.
Long-lived UDP sockets then use the current target. Existing TCP DNS connections
may need to reconnect. Rule installation, conntrack expiry or resolver-file
failures cause a nonzero exit; the caller must fail workload admission. The host
network policy still must allow only the configured upstreams and deny the
runtime's default DNS path during restore. Guest NAT is routing, not an egress
security boundary.

Run `go test -race ./...` and `go vet ./...` from the parent module.
`TestDNSRoutingKernel` is opt-in with `FLOCI_GUESTNET_KERNEL_TEST=1` and must run
only in a disposable Linux container or guest with `NET_ADMIN`: it updates that
environment's resolver file and DNS table. It covers TCP/UDP translation,
multiple nameservers, preservation of unrelated tables and reconfiguration of
an already-used UDP socket.

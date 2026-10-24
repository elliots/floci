//! The node's nftables table, `inet floci`, written whole with `nft -f`
//! whenever the profile changes.
//!
//! Pods reach the node address on `cni0`. There, DNS goes to the forwarder,
//! Floci's ports to Floci on the Floci host (the gateway, then masquerade),
//! routed origins' ports to the relay, and anything else to the sinkhole.
//! Under isolation, pods reach only the cluster, Floci, the endpoints and the
//! allow list, and every other connection goes to the sinkhole.

use std::fmt::Write as _;
use std::net::Ipv4Addr;
use std::process::{Command, Stdio};

use super::profile::{AllowProtocol, Checked, Cidr, Ports};
use super::{DNS_PORT, RELAY_PORT, SINK_PORT};

/// Where Floci listens, from the node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlociHost {
    /// The node's default gateway (a smolvm node on a Mac).
    Gateway(Ipv4Addr),
    /// The node itself (a Linux host).
    Loopback,
}

/// What the table needs besides the profile.
#[derive(Debug, Clone)]
pub struct Node {
    pub address: Ipv4Addr,
    pub floci: FlociHost,
    /// The interface of the default route.
    pub iface: String,
    pub cluster: Vec<Cidr>,
    /// The node's ports that pods may reach under isolation besides DNS, the
    /// relay and the sinkhole: the API server and kubelet.
    pub node_ports: Vec<u16>,
}

fn set(out: &mut String, name: &str, kind: &str, elements: &[String]) {
    let _ = write!(out, "  set {name} {{ type {kind}; flags interval;");
    if !elements.is_empty() {
        let _ = write!(out, " elements = {{ {} }};", elements.join(", "));
    }
    out.push_str(" }\n");
}

fn ports(list: &[Ports]) -> Vec<String> {
    let mut sorted = list.to_vec();
    sorted.sort_by_key(|p| p.0);
    let mut merged: Vec<Ports> = Vec::new();
    for p in sorted {
        match merged.last_mut() {
            Some(last) if p.0 <= last.1.saturating_add(1) => last.1 = last.1.max(p.1),
            _ => merged.push(p),
        }
    }
    merged.iter().map(Ports::to_string).collect()
}

/// The match of an allow entry, such as `ip daddr 203.0.113.10/32 tcp dport { 443 }`.
fn allow_match(cidr: &Cidr, protocol: AllowProtocol, list: &[u16]) -> String {
    let ports = list
        .iter()
        .map(u16::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    match (protocol, list.is_empty()) {
        (AllowProtocol::Tcp, true) => format!("ip daddr {cidr} meta l4proto tcp"),
        (AllowProtocol::Udp, true) => format!("ip daddr {cidr} meta l4proto udp"),
        (AllowProtocol::Any, true) => format!("ip daddr {cidr}"),
        (AllowProtocol::Tcp, false) => format!("ip daddr {cidr} tcp dport {{ {ports} }}"),
        (AllowProtocol::Udp, false) => format!("ip daddr {cidr} udp dport {{ {ports} }}"),
        (AllowProtocol::Any, false) => {
            format!("ip daddr {cidr} meta l4proto {{ tcp, udp }} th dport {{ {ports} }}")
        }
    }
}

/// The table for `profile`, with isolation on or off.
pub fn render(node: &Node, profile: &Checked, isolation: bool) -> String {
    let addr = node.address;
    let route_ports: Vec<Ports> = profile
        .routes
        .iter()
        .flat_map(|r| r.origins.iter().map(|o| Ports(o.port, o.port)))
        .collect();
    let mut targets = profile.floci_ports.clone();
    targets.extend(
        profile
            .profile
            .endpoints
            .iter()
            .map(|e| Ports(e.target_port, e.target_port)),
    );
    let mut host = targets.clone();
    let dns = profile.profile.floci.dns_port;
    host.push(Ports(dns, dns));
    // The relay (a node process) dials a `host.floci.internal` backend on the gateway, so with isolation it is let through
    // the node's output chain at the backend's port, whatever it is (a TLS-terminating front on the host listens on a port
    // of its own, not one of Floci's).
    host.extend(
        profile
            .routes
            .iter()
            .filter(|r| r.backend_host == "host.floci.internal")
            .map(|r| Ports(r.backend_port, r.backend_port)),
    );
    let allows: Vec<String> = profile
        .allow
        .iter()
        .map(|a| allow_match(&a.cidr, a.protocol, &a.ports))
        .collect();
    let (to_floci, gateway) = match node.floci {
        FlociHost::Gateway(gw) => (gw.to_string(), Some(gw)),
        FlociHost::Loopback => ("127.0.0.1".to_string(), None),
    };
    let cluster: Vec<String> = node.cluster.iter().map(Cidr::to_string).collect();

    let mut t = String::new();
    t.push_str("add table inet floci\ndelete table inet floci\ntable inet floci {\n");
    set(&mut t, "cluster4", "ipv4_addr", &cluster);
    set(
        &mut t,
        "floci_ports",
        "inet_service",
        &ports(&profile.floci_ports),
    );
    set(&mut t, "route_ports", "inet_service", &ports(&route_ports));
    set(&mut t, "floci_targets", "inet_service", &ports(&targets));
    set(&mut t, "host_ports", "inet_service", &ports(&host));

    t.push_str("  chain pods_nat {\n    type nat hook prerouting priority -90;\n");
    t.push_str("    iifname != \"cni0\" return\n");
    for e in &profile.profile.endpoints {
        let _ = writeln!(
            t,
            "    ip daddr {} tcp dport {} dnat ip to {to_floci}:{}",
            e.address, e.port, e.target_port
        );
    }
    let _ = writeln!(
        t,
        "    ip daddr {addr} meta l4proto {{ tcp, udp }} th dport 53 dnat ip to {addr}:{DNS_PORT}"
    );
    let _ = writeln!(
        t,
        "    ip daddr {addr} tcp dport @floci_ports dnat ip to {to_floci}"
    );
    let _ = writeln!(
        t,
        "    ip daddr {addr} tcp dport @route_ports dnat ip to {addr}:{RELAY_PORT}"
    );
    let _ = writeln!(
        t,
        "    ip daddr {addr} meta l4proto {{ tcp, udp }} dnat ip to {addr}:{SINK_PORT}"
    );
    if isolation {
        t.push_str("    ip daddr @cluster4 return\n");
        if let Some(gw) = gateway {
            let _ = writeln!(t, "    ip daddr {gw} tcp dport @floci_ports return");
        }
        for m in &allows {
            let _ = writeln!(t, "    {m} return");
        }
        let _ = writeln!(
            t,
            "    meta l4proto {{ tcp, udp }} dnat ip to {addr}:{SINK_PORT}"
        );
    }
    t.push_str("  }\n");

    t.push_str("  chain node_nat {\n    type nat hook output priority -90;\n");
    let _ = writeln!(
        t,
        "    ip daddr {addr} meta l4proto {{ tcp, udp }} th dport 53 dnat ip to {addr}:{DNS_PORT}"
    );
    t.push_str("  }\n");

    if let Some(gw) = gateway {
        t.push_str("  chain pods_snat {\n    type nat hook postrouting priority 110;\n");
        let _ = writeln!(
            t,
            "    ct status dnat ip daddr {gw} oifname \"{}\" masquerade",
            node.iface
        );
        // The relay dials a `host.floci.internal` backend from the node address, which the gateway does not know:
        // the connection leaves as the node's own.
        let _ = writeln!(
            t,
            "    ip saddr {addr} ip daddr {gw} oifname \"{}\" masquerade",
            node.iface
        );
        t.push_str("  }\n");
    }

    if isolation {
        t.push_str("  chain pods_forward {\n    type filter hook forward priority -10;\n");
        t.push_str("    iifname != \"cni0\" return\n");
        t.push_str("    ct state established,related accept\n");
        t.push_str("    ip daddr @cluster4 accept\n");
        if let Some(gw) = gateway {
            let _ = writeln!(t, "    ip daddr {gw} tcp dport @floci_targets accept");
        }
        for m in &allows {
            let _ = writeln!(t, "    {m} accept");
        }
        t.push_str("    meta nfproto ipv6 counter drop\n    counter drop\n  }\n");
    }

    t.push_str("  chain pods_input {\n    type filter hook input priority -10;\n");
    t.push_str("    iifname != \"cni0\" return\n");
    t.push_str("    ct state established,related accept\n");
    let _ = writeln!(
        t,
        "    ct status dnat ip daddr {addr} meta l4proto {{ tcp, udp }} th dport {{ {DNS_PORT}, {RELAY_PORT}, {SINK_PORT} }} accept"
    );
    if gateway.is_none() {
        t.push_str("    ct status dnat ip daddr 127.0.0.1 tcp dport @floci_targets accept\n");
    }
    if !node.node_ports.is_empty() {
        let list = node
            .node_ports
            .iter()
            .map(u16::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        let _ = writeln!(t, "    tcp dport {{ {list} }} accept");
    }
    t.push_str("    ip daddr 127.0.0.0/8 counter drop\n");
    if isolation {
        t.push_str("    counter drop\n");
    }
    t.push_str("  }\n");

    if let (true, Some(gw)) = (isolation, gateway) {
        t.push_str("  chain node_output {\n    type filter hook output priority -10;\n");
        t.push_str("    oifname \"lo\" accept\n");
        t.push_str("    ct state established,related accept\n");
        t.push_str("    ip daddr @cluster4 accept\n");
        let _ = writeln!(t, "    ip daddr {gw} tcp dport @host_ports accept");
        for m in &allows {
            let _ = writeln!(t, "    {m} accept");
        }
        t.push_str("    meta nfproto ipv6 counter drop\n    counter drop\n  }\n");
    }
    t.push_str("}\n");
    t
}

/// The table of an isolated node before its first profile: pods reach only
/// the cluster and the node's DNS, which refuses every name. Node processes
/// are left to the node's boundary (smolvm's egress policy on a Mac), so that
/// k3s reaches Floci's webhooks at the gateway.
pub fn boot(node: &Node) -> String {
    let cluster: Vec<String> = node.cluster.iter().map(Cidr::to_string).collect();
    let mut t = String::new();
    t.push_str("add table inet floci\ndelete table inet floci\ntable inet floci {\n");
    set(&mut t, "cluster4", "ipv4_addr", &cluster);
    t.push_str("  chain pods_forward {\n    type filter hook forward priority -10;\n");
    t.push_str("    iifname != \"cni0\" return\n");
    t.push_str("    ct state established,related accept\n");
    t.push_str("    ip daddr @cluster4 accept\n");
    t.push_str("    counter drop\n  }\n");
    let addr = node.address;
    t.push_str("  chain pods_nat {\n    type nat hook prerouting priority -90;\n");
    t.push_str("    iifname != \"cni0\" return\n");
    let _ = writeln!(
        t,
        "    ip daddr {addr} meta l4proto {{ tcp, udp }} th dport 53 dnat ip to {addr}:{DNS_PORT}"
    );
    t.push_str("  }\n");
    t.push_str("  chain node_nat {\n    type nat hook output priority -90;\n");
    let _ = writeln!(
        t,
        "    ip daddr {addr} meta l4proto {{ tcp, udp }} th dport 53 dnat ip to {addr}:{DNS_PORT}"
    );
    t.push_str("  }\n");
    t.push_str("  chain pods_input {\n    type filter hook input priority -10;\n");
    t.push_str("    iifname != \"cni0\" return\n");
    t.push_str("    ct state established,related accept\n");
    let _ = writeln!(
        t,
        "    ct status dnat ip daddr {addr} meta l4proto {{ tcp, udp }} th dport {DNS_PORT} accept"
    );
    if !node.node_ports.is_empty() {
        let list = node
            .node_ports
            .iter()
            .map(u16::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        let _ = writeln!(t, "    tcp dport {{ {list} }} accept");
    }
    t.push_str("    counter drop\n  }\n}\n");
    t
}

/// The table of an open node before its first profile: only DNS at the node
/// address, and the loopback guard.
pub fn dns_only(node: &Node) -> String {
    let addr = node.address;
    let mut t = String::new();
    t.push_str("add table inet floci\ndelete table inet floci\ntable inet floci {\n");
    t.push_str("  chain pods_nat {\n    type nat hook prerouting priority -90;\n");
    t.push_str("    iifname != \"cni0\" return\n");
    let _ = writeln!(
        t,
        "    ip daddr {addr} meta l4proto {{ tcp, udp }} th dport 53 dnat ip to {addr}:{DNS_PORT}"
    );
    t.push_str("  }\n");
    t.push_str("  chain node_nat {\n    type nat hook output priority -90;\n");
    let _ = writeln!(
        t,
        "    ip daddr {addr} meta l4proto {{ tcp, udp }} th dport 53 dnat ip to {addr}:{DNS_PORT}"
    );
    t.push_str("  }\n");
    t.push_str("  chain pods_input {\n    type filter hook input priority -10;\n");
    t.push_str("    iifname != \"cni0\" return\n");
    t.push_str("    ip daddr 127.0.0.0/8 counter drop\n  }\n}\n");
    t
}

/// Loads `table` in one transaction.
pub fn load(table: &str) -> Result<(), String> {
    use std::io::Write;
    let mut child = Command::new("nft")
        .args(["-f", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("run nft: {e}"))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(table.as_bytes())
            .map_err(|e| format!("write to nft: {e}"))?;
    }
    let out = child
        .wait_with_output()
        .map_err(|e| format!("wait for nft: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "nft refused the table: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::profile::Profile;

    fn checked(isolation: bool) -> Checked {
        let text = include_str!("../../tests/fixtures/network-profile.json");
        let mut profile: Profile = serde_json::from_str(text).unwrap();
        profile.isolation = isolation;
        profile.check().unwrap()
    }

    fn mac() -> Node {
        Node {
            address: "169.254.53.1".parse().unwrap(),
            floci: FlociHost::Gateway("100.96.0.1".parse().unwrap()),
            iface: "eth0".into(),
            cluster: vec![
                Cidr::parse("10.42.0.0/16").unwrap(),
                Cidr::parse("10.100.0.0/16").unwrap(),
            ],
            node_ports: vec![6443, 10250],
        }
    }

    #[test]
    fn an_open_node_sends_the_node_address_by_port_and_keeps_the_guard() {
        let t = render(&mac(), &checked(false), false);
        assert!(
            t.contains("elements = { 10.42.0.0/16, 10.100.0.0/16 }"),
            "{t}"
        );
        assert!(
            t.contains("elements = { 1883, 4566, 5672-5699, 6379-6399, 8883 }"),
            "{t}"
        );
        assert!(t.contains("ip daddr 169.254.170.23 tcp dport 80 dnat ip to 100.96.0.1:4566"));
        assert!(t.contains("th dport 53 dnat ip to 169.254.53.1:7053"));
        assert!(t.contains("tcp dport @floci_ports dnat ip to 100.96.0.1\n"));
        assert!(t.contains("tcp dport @route_ports dnat ip to 169.254.53.1:7071"));
        assert!(t.contains("ct status dnat ip daddr 100.96.0.1 oifname \"eth0\" masquerade"));
        assert!(t.contains("ip saddr 169.254.53.1 ip daddr 100.96.0.1 oifname \"eth0\" masquerade"));
        assert!(t.contains("ip daddr 127.0.0.0/8 counter drop"));
        assert!(!t.contains("pods_forward"), "{t}");
        assert!(!t.contains("node_output"), "{t}");
        assert!(!t.contains("ip daddr @cluster4 return"), "{t}");
    }

    #[test]
    fn isolation_sends_everything_else_to_the_sinkhole() {
        let t = render(&mac(), &checked(true), true);
        assert!(
            t.contains("ip daddr 203.0.113.10/32 tcp dport { 443 } return"),
            "{t}"
        );
        assert!(
            t.contains("ip daddr 203.0.113.10/32 tcp dport { 443 } accept"),
            "{t}"
        );
        assert!(t.contains("    meta l4proto { tcp, udp } dnat ip to 169.254.53.1:7072\n"));
        assert!(t.contains("chain pods_forward"));
        assert!(t.contains("ip daddr 100.96.0.1 tcp dport @floci_targets accept"));
        assert!(t.contains("chain node_output"));
        assert!(t.contains("ip daddr 100.96.0.1 tcp dport @host_ports accept"));
        // floci_targets has the endpoint's target, host_ports the DNS port too.
        assert!(t.contains("set host_ports { type inet_service; flags interval; elements = { 1883, 4566, 5672-5699, 6379-6399, 8883, 15353 }; }"), "{t}");
        assert!(t.contains("tcp dport { 6443, 10250 } accept"));
        let input = &t[t.find("chain pods_input").unwrap()..];
        assert!(
            input[..input.find("}\n").unwrap()].ends_with("counter drop\n  "),
            "{input}"
        );
    }

    #[test]
    fn isolation_lets_the_relay_dial_the_port_of_a_route_back_to_floci() {
        let text = include_str!("../../tests/fixtures/network-profile.json");
        let mut value: serde_json::Value = serde_json::from_str(text).unwrap();
        value["routes"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "name": "front",
                "origins": ["http://*.front.example.test:80"],
                "backend": "host.floci.internal:8081",
                "protocol": "http1"
            }));
        let t = render(
            &mac(),
            &serde_json::from_value::<Profile>(value)
                .unwrap()
                .check()
                .unwrap(),
            true,
        );
        assert!(t.contains("set host_ports { type inet_service; flags interval; elements = { 1883, 4566, 5672-5699, 6379-6399, 8081, 8883, 15353 }; }"), "{t}");
        // only the relay is let through to it: a pod's own traffic still goes by floci_targets
        assert!(t.contains("set floci_targets { type inet_service; flags interval; elements = { 1883, 4566, 5672-5699, 6379-6399, 8883 }; }"), "{t}");
    }

    #[test]
    fn a_linux_host_sends_floci_ports_to_loopback() {
        let mut node = mac();
        node.floci = FlociHost::Loopback;
        let t = render(&node, &checked(true), true);
        assert!(t.contains("tcp dport @floci_ports dnat ip to 127.0.0.1\n"));
        assert!(t.contains("ct status dnat ip daddr 127.0.0.1 tcp dport @floci_targets accept"));
        assert!(!t.contains("masquerade"));
        assert!(!t.contains("node_output"));
    }

    #[test]
    fn an_empty_set_has_no_elements() {
        let mut profile = checked(false);
        profile.routes.clear();
        let t = render(&mac(), &profile, false);
        assert!(
            t.contains("set route_ports { type inet_service; flags interval; }"),
            "{t}"
        );
    }

    #[test]
    fn the_boot_table_holds_pods_to_the_cluster_and_dns() {
        let t = boot(&mac());
        assert!(!t.contains("node_output"), "{t}");
        assert!(t.contains("chain pods_forward"));
        assert!(t.contains("th dport 53 dnat ip to 169.254.53.1:7053"));
        assert!(!t.contains("@floci_ports"));
        let open = dns_only(&mac());
        assert!(open.contains("th dport 53 dnat ip to 169.254.53.1:7053"));
        assert!(!open.contains("pods_forward") && !open.contains("node_output"));
    }

    /// Writes every kind of table to `NFT_CHECK_DIR` (`target/nft-check`), for `nft -c -f` on a
    /// node.
    #[test]
    #[ignore]
    fn write_tables_for_nft_check() {
        let dir = std::path::PathBuf::from(
            std::env::var("NFT_CHECK_DIR").unwrap_or_else(|_| "target/nft-check".into()),
        );
        std::fs::create_dir_all(&dir).unwrap();
        let mut linux = mac();
        linux.floci = FlociHost::Loopback;
        for (name, table) in [
            ("open.nft", render(&mac(), &checked(false), false)),
            ("isolated.nft", render(&mac(), &checked(true), true)),
            ("linux.nft", render(&linux, &checked(true), true)),
            ("boot.nft", boot(&mac())),
            ("dns-only.nft", dns_only(&mac())),
        ] {
            std::fs::write(dir.join(name), table).unwrap();
        }
    }

    #[test]
    fn allow_entries_cover_each_protocol() {
        let cidr = Cidr::parse("192.0.2.0/24").unwrap();
        assert_eq!(
            allow_match(&cidr, AllowProtocol::Any, &[]),
            "ip daddr 192.0.2.0/24"
        );
        assert_eq!(
            allow_match(&cidr, AllowProtocol::Udp, &[53, 123]),
            "ip daddr 192.0.2.0/24 udp dport { 53, 123 }"
        );
        assert_eq!(
            allow_match(&cidr, AllowProtocol::Any, &[8443]),
            "ip daddr 192.0.2.0/24 meta l4proto { tcp, udp } th dport { 8443 }"
        );
    }
}

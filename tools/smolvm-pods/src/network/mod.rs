//! The node's network: the node address, which pods reach for DNS, Floci,
//! routed origins and nothing else, and the profile that Floci publishes with
//! `PUT /v1/network`.
//!
//! The node service owns the node address (`169.254.53.1` on a dummy
//! interface `floci0`), its nftables table ([`nft`]), and the services behind
//! it: the DNS forwarder ([`dns`]), the relay of routed origins ([`relay`])
//! and the sinkhole of blocked connections ([`sinkhole`]). Containers trust
//! Floci's CA through [`crate::trust`]. A node is isolated when it was
//! created with `FLOCI_NODE_ISOLATION=1` or when its profile says so.

pub mod dns;
pub mod events;
pub mod nft;
pub mod profile;
pub mod relay;
pub mod sinkhole;
pub mod sniff;

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, SystemTime};

use log::{info, warn};
use serde_json::{json, Value};

use crate::sync::{read, write};
use profile::{Checked, Cidr, Profile, Trust};

/// The node address's default.
pub const NODE_ADDRESS: Ipv4Addr = Ipv4Addr::new(169, 254, 53, 1);
/// The ports behind the node address.
pub const DNS_PORT: u16 = 7053;
pub const RELAY_PORT: u16 = 7071;
pub const SINK_PORT: u16 = 7072;

const IFACE: &str = "floci0";

/// dockerd's default bridge, which `floci-node` gives dockerd with `--bip`. Its
/// containers get the network that pods get, and the table counts it as part
/// of the cluster.
const DOCKER_CIDR: &str = "172.17.0.0/16";

/// The node's own ports that pods reach: the API server and kubelet.
const NODE_PORTS: [u16; 2] = [6443, 10250];

/// The applied profile, beside the pod records.
const PROFILE_FILE: &str = "network.json";
const EVENTS_FILE: &str = "network-events.jsonl";

/// The node's network settings, from its environment.
#[derive(Debug, Clone)]
pub struct NodeEnv {
    /// `FLOCI_NODE_ISOLATION=1`, which only `node-vm.sh create --isolate`
    /// sets, in the machine's create-time environment.
    pub isolation: bool,
    /// `FLOCI_NODE_CLUSTER_CIDRS`: k3s's cluster and Service CIDRs.
    pub cluster: Vec<Cidr>,
    /// `FLOCI_NODE_CLUSTER_DNS`: the cluster DNS Service's address.
    pub cluster_dns: Ipv4Addr,
    /// `FLOCI_NODE_NETWORK_ADDRESS`.
    pub address: Ipv4Addr,
}

impl NodeEnv {
    pub fn from_env() -> NodeEnv {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
        let cluster = var("FLOCI_NODE_CLUSTER_CIDRS")
            .map(|v| {
                v.split(',')
                    .filter_map(|c| match Cidr::parse(c.trim()) {
                        Ok(cidr) => Some(cidr),
                        Err(e) => {
                            warn!("FLOCI_NODE_CLUSTER_CIDRS: {e}");
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .filter(|c| !c.is_empty())
            .unwrap_or_else(|| {
                vec![
                    Cidr::parse("10.42.0.0/16").expect("a CIDR"),
                    Cidr::parse("10.43.0.0/16").expect("a CIDR"),
                ]
            });
        NodeEnv {
            isolation: var("FLOCI_NODE_ISOLATION").as_deref() == Some("1"),
            cluster,
            cluster_dns: var("FLOCI_NODE_CLUSTER_DNS")
                .and_then(|v| v.parse().ok())
                .unwrap_or(Ipv4Addr::new(10, 43, 0, 10)),
            address: var("FLOCI_NODE_NETWORK_ADDRESS")
                .and_then(|v| v.parse().ok())
                .unwrap_or(NODE_ADDRESS),
        }
    }
}

/// The interface and gateway of the default route, from `/proc/net/route`.
pub fn default_route(text: &str) -> Option<(String, Ipv4Addr)> {
    text.lines().skip(1).find_map(|line| {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() < 3 || f[1] != "00000000" {
            return None;
        }
        let gw = u32::from_str_radix(f[2], 16).ok()?;
        Some((f[0].to_string(), Ipv4Addr::from(gw.swap_bytes())))
    })
}

/// The profile in force and what the node made of it.
pub struct Applied {
    pub checked: Checked,
}

/// The `NAMESPACE/NAME` of the pod with an address, for events.
pub type PodNames = Arc<dyn Fn(Ipv4Addr) -> Option<String> + Send + Sync>;

pub struct Shared {
    pub node: nft::Node,
    pub env: NodeEnv,
    pub events: events::Log,
    pub dns: dns::FlociDns,
    pub names: PodNames,
    pub certs: relay::Certs,
    pub calls: relay::Calls,
    state_dir: PathBuf,
    applied: RwLock<Option<Arc<Applied>>>,
    error: RwLock<Option<String>>,
}

static NETWORK: OnceLock<Arc<Shared>> = OnceLock::new();

/// The node's network, once the node service started it.
pub fn get() -> Option<Arc<Shared>> {
    NETWORK.get().cloned()
}

/// The trust material that containers get, when a profile carries it.
pub fn trust() -> Option<Trust> {
    let shared = get()?;
    let applied = shared.applied()?;
    applied.checked.profile.trust.clone()
}

/// The relay's call that a connection from `from` belongs to, when `from` is
/// the node address and the relay dialled from that port.
pub fn relayed(from: SocketAddrV4) -> Option<relay::Call> {
    let shared = get()?;
    if *from.ip() != shared.node.address {
        return None;
    }
    shared.calls.get(from.port())
}

/// Whether a container must get trust material: Floci's profile routes hosts
/// or isolates pods, and so should carry it. An isolated node without a
/// profile has no Floci to trust yet.
pub fn needs_trust() -> bool {
    get()
        .and_then(|shared| shared.applied())
        .is_some_and(|a| a.checked.profile.isolation || !a.checked.routes.is_empty())
}

impl Shared {
    pub fn applied(&self) -> Option<Arc<Applied>> {
        read(&self.applied).clone()
    }

    pub fn effective_isolation(&self) -> bool {
        self.env.isolation || self.applied().is_some_and(|a| a.checked.profile.isolation)
    }

    /// Where Floci's node DNS listens, once a profile names it.
    pub fn floci_dns(&self) -> Option<SocketAddr> {
        let applied = self.applied()?;
        let port = applied.checked.profile.floci.dns_port;
        let host = match self.node.floci {
            nft::FlociHost::Gateway(gw) => gw,
            nft::FlociHost::Loopback => Ipv4Addr::LOCALHOST,
        };
        Some(SocketAddr::from((host, port)))
    }

    /// The Floci host from the node, for `host.floci.internal` backends.
    pub fn floci_host(&self) -> Ipv4Addr {
        match self.node.floci {
            nft::FlociHost::Gateway(gw) => gw,
            nft::FlociHost::Loopback => Ipv4Addr::LOCALHOST,
        }
    }

    pub fn pod_name(&self, ip: Ipv4Addr) -> String {
        (self.names)(ip).unwrap_or_else(|| ip.to_string())
    }

    /// Checks, stores and applies `profile`.
    pub fn put(&self, profile: Profile) -> Result<Value, String> {
        let checked = profile.check()?;
        let isolation = self.env.isolation || checked.profile.isolation;
        nft::load(&nft::render(&self.node, &checked, isolation))?;
        if let Err(e) = store(&self.state_dir.join(PROFILE_FILE), &checked.profile) {
            warn!("keep the network profile: {e}");
        }
        let generation = checked.profile.generation.clone();
        let routes = checked.routes.len();
        *write(&self.applied) = Some(Arc::new(Applied { checked }));
        *write(&self.error) = None;
        self.certs.clear();
        info!("applied network profile {generation}: isolation {isolation}, {routes} routes");
        Ok(self.status())
    }

    /// `GET /v1/network`.
    pub fn status(&self) -> Value {
        let applied = self.applied();
        let counters: serde_json::Map<String, Value> = self
            .events
            .counters()
            .into_iter()
            .map(|(kind, n)| {
                (
                    serde_json::to_value(kind)
                        .ok()
                        .and_then(|v| v.as_str().map(str::to_string))
                        .unwrap_or_default(),
                    json!(n),
                )
            })
            .collect();
        json!({
            "generation": applied.as_ref().map(|a| a.checked.profile.generation.clone()),
            "effective_isolation": self.effective_isolation(),
            "node_isolation": self.env.isolation,
            "address": self.node.address.to_string(),
            "cluster_cidrs": self.env.cluster.iter().map(Cidr::to_string).collect::<Vec<_>>(),
            "routes": applied.as_ref().map_or(0, |a| a.checked.routes.len()),
            "trust": applied.as_ref().and_then(|a| a.checked.profile.trust.as_ref()).map(|t| json!({
                "mode": t.mode,
                "ca_sha256": t.ca_sha256,
            })),
            "error": read(&self.error).clone(),
            "counters": counters,
        })
    }
}

fn store(path: &Path, profile: &Profile) -> Result<(), String> {
    let tmp = path.with_extension("json.tmp");
    let text = serde_json::to_vec_pretty(profile).map_err(|e| e.to_string())?;
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(&tmp)
            .map_err(|e| format!("open {}: {e}", tmp.display()))?;
        file.write_all(&text)
            .map_err(|e| format!("write {}: {e}", tmp.display()))?;
        file.sync_all()
            .map_err(|e| format!("sync {}: {e}", tmp.display()))?;
    }
    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| format!("chmod {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("rename to {}: {e}", path.display()))
}

/// The node, as nftables sees it.
pub fn node_of(env: &NodeEnv) -> Result<nft::Node, String> {
    let routes = std::fs::read_to_string("/proc/net/route")
        .map_err(|e| format!("read /proc/net/route: {e}"))?;
    let (iface, gateway) = default_route(&routes).ok_or("the node has no default route")?;
    let floci = match std::env::var("FLOCI_NODE_FLOCI_HOST").as_deref() {
        Ok("loopback") => nft::FlociHost::Loopback,
        _ => nft::FlociHost::Gateway(gateway),
    };
    let mut cluster = env.cluster.clone();
    cluster.push(Cidr::parse(DOCKER_CIDR)?);
    let own = iface_addresses(&iface);
    Ok(nft::Node {
        address: env.address,
        floci,
        iface,
        cluster,
        node_ports: NODE_PORTS.to_vec(),
        own,
    })
}

/// The IPv4 addresses on `iface`, or none when `ip` cannot say: then pods do
/// not reach node_ports at the node's own address, and nothing else changes.
fn iface_addresses(iface: &str) -> Vec<Ipv4Addr> {
    let out = crate::net::real_tool("ip").and_then(|tool| {
        std::process::Command::new(tool)
            .args(["-4", "-o", "addr", "show", "dev", iface])
            .output()
            .map_err(|e| e.to_string())
    });
    match out {
        Ok(out) if out.status.success() => parse_addresses(&String::from_utf8_lossy(&out.stdout)),
        _ => Vec::new(),
    }
}

/// The addresses of `ip -4 -o addr show` lines, such as
/// `2: eth0    inet 100.96.0.2/30 brd 100.96.0.3 scope global eth0`.
fn parse_addresses(text: &str) -> Vec<Ipv4Addr> {
    text.lines()
        .filter_map(|line| {
            let mut words = line.split_whitespace();
            words.find(|w| *w == "inet")?;
            words.next()?.split('/').next()?.parse().ok()
        })
        .collect()
}

/// Puts the node address on `floci0`, once.
pub fn add_address(address: Ipv4Addr) -> Result<(), String> {
    let tool = crate::net::real_tool("ip")?;
    let ip = |args: &[&str]| -> Result<String, String> {
        let out = std::process::Command::new(&tool)
            .args(args)
            .output()
            .map_err(|e| format!("run ip {}: {e}", args.join(" ")))?;
        if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).into_owned())
        } else {
            Err(format!(
                "ip {}: {}",
                args.join(" "),
                String::from_utf8_lossy(&out.stderr).trim()
            ))
        }
    };
    if ip(&["link", "show", IFACE]).is_err() {
        ip(&["link", "add", IFACE, "type", "dummy"])?;
    }
    let cidr = format!("{address}/32");
    if !ip(&["-4", "addr", "show", "dev", IFACE])?.contains(&format!("inet {cidr}")) {
        ip(&["addr", "add", &cidr, "dev", IFACE])?;
    }
    ip(&["link", "set", IFACE, "up"])?;
    Ok(())
}

/// The table that the node loads before k3s starts and before the node
/// service has a profile: on an isolated node the boot table, on any other
/// only the node address's DNS.
pub fn boot_table(node: &nft::Node, isolation: bool) -> String {
    if isolation {
        nft::boot(node)
    } else {
        nft::dns_only(node)
    }
}

/// `smolvm-pods _network-boot`, which `floci-node` runs before k3s.
pub fn boot() -> i32 {
    let env = NodeEnv::from_env();
    let node = match node_of(&env) {
        Ok(node) => node,
        Err(e) => {
            eprintln!("network boot: {e}");
            return 1;
        }
    };
    if let Err(e) = add_address(env.address) {
        eprintln!("network boot: {e}");
        return 1;
    }
    if let Err(e) = nft::load(&boot_table(&node, env.isolation)) {
        eprintln!("network boot: {e}");
        return 1;
    }
    0
}

/// Starts the node's network: the node address, the stored profile or the
/// boot table, and the forwarder, relay and sinkhole.
pub fn start(state_dir: &Path, names: PodNames) -> Result<Arc<Shared>, String> {
    let env = NodeEnv::from_env();
    let node = node_of(&env)?;
    add_address(env.address)?;
    let shared = Arc::new(Shared {
        node,
        events: events::Log::open(&state_dir.join(EVENTS_FILE)),
        dns: dns::FlociDns::default(),
        names,
        certs: relay::Certs::default(),
        calls: relay::Calls::default(),
        state_dir: state_dir.to_path_buf(),
        applied: RwLock::new(None),
        error: RwLock::new(None),
        env,
    });
    let stored = std::fs::read(state_dir.join(PROFILE_FILE))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Profile>(&bytes).ok());
    let restored = match stored {
        Some(profile) => match shared.put(profile) {
            Ok(_) => true,
            Err(e) => {
                warn!("the stored network profile is not applied: {e}");
                *write(&shared.error) = Some(e);
                false
            }
        },
        None => false,
    };
    if !restored {
        nft::load(&boot_table(&shared.node, shared.env.isolation))?;
    }
    let _ = NETWORK.set(shared.clone());
    tokio::spawn(dns::serve(shared.clone()));
    tokio::spawn(relay::serve(shared.clone()));
    tokio::spawn(sinkhole::serve(shared.clone()));
    let flusher = shared.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            flusher.events.flush(SystemTime::now());
        }
    });
    info!(
        "node network on {}: isolation {}, cluster {}",
        shared.node.address,
        shared.effective_isolation(),
        shared
            .env
            .cluster
            .iter()
            .map(Cidr::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    );
    Ok(shared)
}

/// The original destination of a connection or datagram that nftables
/// DNATed to a listener of the node address.
pub fn original_dst(fd: std::os::fd::BorrowedFd<'_>) -> Option<SocketAddrV4> {
    use nix::sys::socket::{getsockopt, sockopt::OriginalDst};
    let addr = getsockopt(&fd, OriginalDst).ok()?;
    Some(SocketAddrV4::new(
        Ipv4Addr::from(u32::from_be(addr.sin_addr.s_addr)),
        u16::from_be(addr.sin_port),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_route_gives_the_interface_and_gateway() {
        let text = "Iface\tDestination\tGateway\tFlags\n\
                    eth0\t00006064\t00000000\t0001\n\
                    eth0\t00000000\t01006064\t0003\n";
        assert_eq!(
            default_route(text),
            Some(("eth0".to_string(), Ipv4Addr::new(100, 96, 0, 1)))
        );
        assert_eq!(default_route("Iface\tDestination\tGateway\n"), None);
    }

    #[test]
    fn the_interface_addresses_come_from_ip_addr_show() {
        let text = "2: eth0    inet 100.96.0.2/30 brd 100.96.0.3 scope global eth0\\       valid_lft forever preferred_lft forever\n\
                    2: eth0    inet 100.96.0.6/30 scope global secondary eth0\n";
        assert_eq!(
            parse_addresses(text),
            vec![Ipv4Addr::new(100, 96, 0, 2), Ipv4Addr::new(100, 96, 0, 6)]
        );
        assert!(parse_addresses("").is_empty());
    }
}

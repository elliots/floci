//! The pod network path on nodes whose kernel has no traffic control.
//!
//! containerd runs CNI in the pod's network namespace before the pod's VM
//! starts, which leaves `eth0` there with the pod IP. smolvm's boot
//! attaches the VM to that namespace (`smolvm::agent::pod_net`): it reads
//! eth0 with `ip`, opens `tap0` there for the VM's frames, and joins the two
//! with tc `matchall`/`mirred` redirects. A smolvm node VM's kernel has no tc,
//! so the VM process runs this binary as smolvm's `ip` and `tc`, and a bridge
//! joins eth0 and tap0 instead. See docs/architecture.md, "Networking".
//!
//! The bridge is a two-port wire: no port learns addresses. The guest takes
//! eth0's MAC, which a bridge port cannot share, so eth0 gets the guest's MAC
//! with the locally administered bit flipped, and the `ip` stand-in flips it
//! back when smolvm reads eth0 again. eth0 keeps the pod IP, and the namespace
//! answers no ARP for it, so only the guest does. Deleting the sandbox removes
//! the bridge again ([`detach`]).

use std::ffi::{OsStr, OsString};
use std::io::Write;
use std::net::Ipv4Addr;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The bridge that joins eth0 and tap0 in the pod's network namespace.
pub const BRIDGE: &str = "podbr0";

// The names and filter priority that smolvm's attach uses (CNI_IFACE and
// TAP_IFACE in src/agent/pod_net.rs, REDIRECT_PREF in
// crates/smolvm-network/src/netns_tap.rs).
/// The pod's CNI interface, which [`BRIDGE`] joins to [`TAP_IFACE`].
pub const CNI_IFACE: &str = "eth0";
/// The tap of the pod's VM.
pub const TAP_IFACE: &str = "tap0";
const REDIRECT_PREF: &str = "49152";

/// smolvm's read of eth0's address and MAC (`read_pod_interface`).
const DISCOVERY: [&str; 4] = ["-j", "addr", "show", CNI_IFACE];

/// In the VM's data directory: the `ip` and `tc` links to this binary, and the
/// pod network namespace of the VM's first boot.
const TOOLS_DIR: &str = "smolvm-pods-bin";
const NETNS_RECORD: &str = "smolvm-pods-netns";

/// Settings of the pod network namespace, written before the bridge exists.
/// An optional key may be missing from the kernel.
const SYSCTLS: [(&str, &str, bool); 8] = [
    // eth0 keeps the pod IP, but only the guest answers ARP for it.
    ("net/ipv4/conf/all/arp_ignore", "8", false),
    // No gratuitous ARP with eth0's new MAC.
    ("net/ipv4/conf/all/arp_notify", "0", false),
    ("net/ipv4/conf/eth0/arp_notify", "0", false),
    ("net/ipv6/conf/all/disable_ipv6", "1", true),
    ("net/ipv6/conf/default/disable_ipv6", "1", true),
    // The namespace has no firewall rules for bridged frames to pass through.
    ("net/bridge/bridge-nf-call-iptables", "0", true),
    ("net/bridge/bridge-nf-call-ip6tables", "0", true),
    ("net/bridge/bridge-nf-call-arptables", "0", true),
];

pub type Mac = [u8; 6];

// ================================ VM boot ===================================

/// Prepare the VM process of a pod VM before smolvm boots it: put this
/// binary first on `PATH` as `ip` and `tc`, and enter the pod's network
/// namespace. The VM's threads, started later, are in it too, so smolvm sees
/// no IPv6 route there and gives the guest no IPv6 address of its own.
///
/// smolvm passes the namespace only to the first boot of a VM, which records
/// it. A later boot, such as a resume, gets the recorded one in its boot
/// config. A VM without a pod network namespace is left alone. A failure is
/// also written to the VM's startup error log.
pub fn prepare_boot(config: &Path) -> Result<(), String> {
    let raw = std::fs::read(config).map_err(|e| format!("read {}: {e}", config.display()))?;
    let mut boot: serde_json::Value =
        serde_json::from_slice(&raw).map_err(|e| format!("parse {}: {e}", config.display()))?;
    let result = boot_netns(config, &mut boot).and_then(|netns| match netns {
        Some(netns) => attach_process(config, &netns),
        None => Ok(()),
    });
    result.map_err(|e| {
        let message = format!("pod network: {e}");
        if let Some(log) = boot.get("startup_error_log").and_then(|v| v.as_str()) {
            let _ = std::fs::write(log, format!("error: {message}\n"));
        }
        message
    })
}

/// The pod network namespace this boot attaches to, if any. It is recorded
/// beside the boot config, or taken from the record into the boot config.
fn boot_netns(config: &Path, boot: &mut serde_json::Value) -> Result<Option<PathBuf>, String> {
    let record = vm_dir(config)?.join(NETNS_RECORD);
    if let Some(netns) = boot.get("pod_netns").and_then(|v| v.as_str()) {
        let netns = absolute(netns)?;
        std::fs::write(&record, netns.as_os_str().as_bytes())
            .map_err(|e| format!("write {}: {e}", record.display()))?;
        return Ok(Some(netns));
    }
    let saved = match std::fs::read(&record) {
        Ok(saved) => saved,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("read {}: {e}", record.display())),
    };
    let netns = absolute(&String::from_utf8_lossy(&saved))?;
    boot["pod_netns"] = serde_json::Value::String(netns.display().to_string());
    let updated = serde_json::to_vec(boot).map_err(|e| format!("serialize boot config: {e}"))?;
    std::fs::write(config, updated).map_err(|e| format!("write {}: {e}", config.display()))?;
    Ok(Some(netns))
}

fn vm_dir(config: &Path) -> Result<&Path, String> {
    config
        .parent()
        .ok_or_else(|| format!("{} has no directory", config.display()))
}

fn absolute(path: &str) -> Result<PathBuf, String> {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        Ok(path)
    } else {
        Err(format!(
            "the pod network namespace {} is not an absolute path",
            path.display()
        ))
    }
}

/// Make smolvm run the stand-ins, then move this process into the pod network
/// namespace. It must still have one thread: setns moves only the calling
/// thread.
fn attach_process(config: &Path, netns: &Path) -> Result<(), String> {
    let ns = std::fs::File::open(netns).map_err(|e| format!("open {}: {e}", netns.display()))?;
    let exe = std::env::current_exe().map_err(|e| format!("find this binary: {e}"))?;
    let tools = vm_dir(config)?.join(TOOLS_DIR);
    install_tools(&tools, &exe)?;
    let path = std::env::var_os("PATH").unwrap_or_default();
    let dirs = std::iter::once(tools).chain(std::env::split_paths(&path));
    let path = std::env::join_paths(dirs).map_err(|e| format!("set PATH: {e}"))?;
    std::env::set_var("PATH", path);
    nix::sched::setns(ns, nix::sched::CloneFlags::CLONE_NEWNET)
        .map_err(|e| format!("enter {}: {e}", netns.display()))
}

/// Make `dir` hold `ip` and `tc` as symbolic links to `exe`.
fn install_tools(dir: &Path, exe: &Path) -> Result<(), String> {
    if !exe.is_file() {
        return Err(format!("this binary, {}, is gone", exe.display()));
    }
    std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    for tool in [Tool::Ip, Tool::Tc] {
        let link = dir.join(tool.name());
        if std::fs::read_link(&link).is_ok_and(|target| target == exe) {
            continue;
        }
        let tmp = dir.join(format!(".{}.{}", tool.name(), std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        std::os::unix::fs::symlink(exe, &tmp)
            .and_then(|()| std::fs::rename(&tmp, &link))
            .map_err(|e| format!("link {} to {}: {e}", link.display(), exe.display()))?;
    }
    Ok(())
}

// =============================== stand-ins ==================================

/// The commands this binary stands in for when it runs under their name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tool {
    Ip,
    Tc,
}

impl Tool {
    /// The stand-in that `argv0` names, if any.
    pub fn from_argv0(argv0: &OsStr) -> Option<Tool> {
        match Path::new(argv0).file_name()?.to_str()? {
            "ip" => Some(Tool::Ip),
            "tc" => Some(Tool::Tc),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Tool::Ip => "ip",
            Tool::Tc => "tc",
        }
    }

    /// Run the stand-in with `args` and return its exit status.
    pub fn run(self, args: &[OsString]) -> i32 {
        let result = match self {
            Tool::Ip => ip(args),
            Tool::Tc => tc(args),
        };
        match result {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("{} (smolvm-pods): {e}", self.name());
                1
            }
        }
    }
}

/// The calls smolvm's `setup_tc_redirect` makes.
#[derive(Debug, PartialEq, Eq)]
enum TcCall {
    /// `qdisc replace dev eth0|tap0 ingress`
    IngressQdisc,
    /// `filter show|del dev eth0 ingress pref 49152`, for a stale redirect
    StaleRedirect,
    /// `filter add dev eth0 ... redirect dev tap0`
    RedirectToVm,
    /// `filter add dev tap0 ... redirect dev eth0`, the last call
    RedirectFromVm,
}

/// The middle of smolvm's redirect filter, before the target device.
const REDIRECT: [&str; 8] = [
    "protocol", "all", "matchall", "action", "mirred", "egress", "redirect", "dev",
];

fn parse_tc(args: &[&str]) -> Option<TcCall> {
    match args {
        ["qdisc", "replace", "dev", CNI_IFACE | TAP_IFACE, "ingress"] => Some(TcCall::IngressQdisc),
        ["filter", "show" | "del", "dev", CNI_IFACE, "ingress", "pref", REDIRECT_PREF] => {
            Some(TcCall::StaleRedirect)
        }
        ["filter", "add", "dev", from, "ingress", "pref", REDIRECT_PREF, rest @ ..] => {
            match (*from, rest.split_last()?) {
                (CNI_IFACE, (&TAP_IFACE, redirect)) if redirect == REDIRECT => {
                    Some(TcCall::RedirectToVm)
                }
                (TAP_IFACE, (&CNI_IFACE, redirect)) if redirect == REDIRECT => {
                    Some(TcCall::RedirectFromVm)
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// smolvm's `tc`, run in the pod network namespace. The redirects become the
/// bridge, made when smolvm asks for the second one, and their stale-filter
/// check finds nothing. Any other call fails.
fn tc(args: &[OsString]) -> Result<(), String> {
    let args: Vec<&str> = args
        .iter()
        .map(|a| a.to_str().ok_or("arguments are not UTF-8"))
        .collect::<Result<_, _>>()?;
    match parse_tc(&args) {
        Some(TcCall::RedirectFromVm) => bridge(),
        Some(_) => Ok(()),
        None => Err(format!(
            "this node's kernel has no traffic control, and only smolvm's pod redirect \
             is provided, not `tc {}`",
            args.join(" ")
        )),
    }
}

/// smolvm's `ip`, run in the pod network namespace. Once eth0 is a port of
/// the bridge its MAC is not the guest's, so smolvm's read of eth0 gets the
/// guest's MAC back. Everything else runs the node's `ip`.
fn ip(args: &[OsString]) -> Result<(), String> {
    let real = real_tool("ip")?;
    if args
        .iter()
        .map(OsString::as_os_str)
        .ne(DISCOVERY.map(OsStr::new))
    {
        let e = Command::new(&real).args(args).exec();
        return Err(format!("run {}: {e}", real.display()));
    }
    let addr = run(&real, &DISCOVERY)?;
    let links = parse_links(&run(&real, &["-j", "link", "show", "dev", CNI_IFACE])?)?;
    let bridged = links
        .iter()
        .any(|l| l.name == CNI_IFACE && l.master.as_deref() == Some(BRIDGE));
    let mut out = std::io::stdout().lock();
    out.write_all(&guest_view(&addr, bridged)?)
        .and_then(|()| out.flush())
        .map_err(|e| format!("write the output: {e}"))
}

/// `ip -j addr show eth0` as smolvm must see it: with the guest's MAC once
/// eth0 is a port of the bridge.
fn guest_view(addr: &[u8], bridged: bool) -> Result<Vec<u8>, String> {
    if !bridged {
        return Ok(addr.to_vec());
    }
    let mut value: serde_json::Value =
        serde_json::from_slice(addr).map_err(|e| format!("parse `ip -j addr`: {e}"))?;
    let address = value
        .get_mut(0)
        .and_then(|link| link.get_mut("address"))
        .ok_or("`ip -j addr` shows no address for eth0")?;
    let mac = address
        .as_str()
        .and_then(|mac| parse_mac(mac).ok())
        .ok_or("`ip -j addr` shows no MAC for eth0")?;
    *address = serde_json::Value::String(format_mac(flip_local_bit(mac)));
    let mut out = serde_json::to_vec(&value).map_err(|e| format!("serialize: {e}"))?;
    out.push(b'\n');
    Ok(out)
}

/// The first `name` on `PATH` that is not this binary.
fn real_tool(name: &str) -> Result<PathBuf, String> {
    let exe = std::env::current_exe()
        .and_then(std::fs::canonicalize)
        .map_err(|e| format!("find this binary: {e}"))?;
    find_tool(name, &std::env::var_os("PATH").unwrap_or_default(), &exe)
        .ok_or_else(|| format!("no {name} on PATH other than this binary"))
}

fn find_tool(name: &str, path: &OsStr, exe: &Path) -> Option<PathBuf> {
    std::env::split_paths(path)
        .map(|dir| dir.join(name))
        .filter(|p| {
            std::fs::metadata(p).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        })
        .find(|p| std::fs::canonicalize(p).is_ok_and(|p| p != exe))
}

/// Run `prog` and return its output, or an error with its error output.
fn run<S: AsRef<OsStr>>(prog: &Path, args: &[S]) -> Result<Vec<u8>, String> {
    let shown = || {
        let args: Vec<_> = args.iter().map(|a| a.as_ref().to_string_lossy()).collect();
        format!("{} {}", prog.display(), args.join(" "))
    };
    let out = Command::new(prog)
        .args(args)
        .output()
        .map_err(|e| format!("{}: {e}", shown()))?;
    if !out.status.success() {
        return Err(format!(
            "{}: {}",
            shown(),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(out.stdout)
}

/// Run the node's `ip` with `args` in the network namespace `netns`, and
/// return its output.
fn ip_in(netns: &str, args: &[&str]) -> Result<Vec<u8>, String> {
    let net = format!("--net={netns}");
    let args: Vec<&str> = [net.as_str(), "ip"].iter().chain(args).copied().collect();
    run(Path::new("nsenter"), &args)
}

// ================================ bridge ====================================

/// A link in `ip -j link show` output.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Link {
    name: String,
    mac: Option<Mac>,
    mtu: Option<u32>,
    master: Option<String>,
}

fn parse_links(json: &[u8]) -> Result<Vec<Link>, String> {
    let value: serde_json::Value =
        serde_json::from_slice(json).map_err(|e| format!("parse `ip -j link`: {e}"))?;
    let links = value.as_array().ok_or("`ip -j link` is not a list")?;
    Ok(links
        .iter()
        .filter_map(|l| {
            Some(Link {
                name: l.get("ifname")?.as_str()?.to_string(),
                mac: l
                    .get("address")
                    .and_then(|v| v.as_str())
                    .and_then(|mac| parse_mac(mac).ok()),
                mtu: l
                    .get("mtu")
                    .and_then(|v| v.as_u64())
                    .and_then(|m| u32::try_from(m).ok()),
                master: l.get("master").and_then(|v| v.as_str()).map(str::to_string),
            })
        })
        .collect())
}

/// One step of the bridge setup.
#[derive(Debug, PartialEq, Eq)]
enum Step {
    /// Write `value` to /proc/sys/`key`, which may be missing if `optional`.
    Sysctl {
        key: &'static str,
        value: &'static str,
        optional: bool,
    },
    /// Run `ip` with these arguments.
    Ip(Vec<String>),
}

fn ip_step(args: &[&str]) -> Step {
    Step::Ip(args.iter().map(|a| a.to_string()).collect())
}

/// The steps that join eth0 and tap0 with the bridge, given the namespace's
/// links. They can run again: after the first boot only tap0 is new.
fn bridge_steps(links: &[Link]) -> Result<Vec<Step>, String> {
    let link = |name: &str| links.iter().find(|l| l.name == name);
    let eth0 = link(CNI_IFACE).ok_or("the pod network namespace has no eth0")?;
    let tap0 = link(TAP_IFACE).ok_or("the pod network namespace has no tap0")?;
    let mtu = eth0.mtu.ok_or("eth0 has no MTU")?.to_string();
    if let Some(other) = tap0.master.as_deref().filter(|m| *m != BRIDGE) {
        return Err(format!("tap0 is already a port of {other}"));
    }

    let mut steps: Vec<Step> = SYSCTLS
        .iter()
        .map(|&(key, value, optional)| Step::Sysctl {
            key,
            value,
            optional,
        })
        .collect();
    match eth0.master.as_deref() {
        Some(BRIDGE) => {}
        Some(other) => return Err(format!("eth0 is already a port of {other}")),
        None => {
            let guest_mac = eth0.mac.ok_or("eth0 has no MAC")?;
            let port_mac = format_mac(flip_local_bit(guest_mac));
            // No forward delay: with one, the bridge arms a timer when eth0
            // joins, which moves a port that the node service holds in
            // `listening` on to `learning` and `forwarding`.
            if link(BRIDGE).is_none() {
                steps.push(ip_step(&[
                    "link",
                    "add",
                    "name",
                    BRIDGE,
                    "address",
                    &port_mac,
                    "type",
                    "bridge",
                    "mcast_snooping",
                    "0",
                    "forward_delay",
                    "0",
                ]));
            }
            steps.push(ip_step(&[
                "link", "set", "dev", CNI_IFACE, "address", &port_mac, "master", BRIDGE,
            ]));
        }
    }
    // No learning: cni0's hairpin port sends the guest's broadcasts back in
    // through eth0, and learning from them would place the guest behind eth0,
    // where the bridge drops frames for it.
    let learning_off = |dev| {
        ip_step(&[
            "link",
            "set",
            "dev",
            dev,
            "type",
            "bridge_slave",
            "learning",
            "off",
        ])
    };
    steps.push(learning_off(CNI_IFACE));
    steps.push(ip_step(&[
        "link", "set", "dev", TAP_IFACE, "mtu", &mtu, "master", BRIDGE,
    ]));
    steps.push(learning_off(TAP_IFACE));
    steps.push(ip_step(&["link", "set", "dev", BRIDGE, "up"]));
    Ok(steps)
}

/// Join eth0 and tap0 in the current network namespace.
fn bridge() -> Result<(), String> {
    let ip = real_tool("ip")?;
    let links = parse_links(&run(&ip, &["-j", "link", "show"])?)?;
    for step in bridge_steps(&links)? {
        match step {
            Step::Sysctl {
                key,
                value,
                optional,
            } => {
                let path = Path::new("/proc/sys").join(key);
                match std::fs::write(&path, value) {
                    Ok(()) => {}
                    Err(e) if optional && e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(format!("write {}: {e}", path.display())),
                }
            }
            Step::Ip(args) => {
                run(&ip, &args)?;
            }
        }
    }
    Ok(())
}

/// A MAC address written as six hex pairs separated by colons.
pub fn parse_mac(text: &str) -> Result<Mac, String> {
    let invalid = || format!("{text:?} is not a MAC address");
    let mut mac = [0u8; 6];
    let mut parts = text.trim().split(':');
    for byte in &mut mac {
        let part = parts.next().filter(|p| p.len() == 2).ok_or_else(invalid)?;
        *byte = u8::from_str_radix(part, 16).map_err(|_| invalid())?;
    }
    match parts.next() {
        Some(_) => Err(invalid()),
        None => Ok(mac),
    }
}

pub fn format_mac(mac: Mac) -> String {
    mac.map(|b| format!("{b:02x}")).join(":")
}

/// eth0's MAC as a bridge port: the guest's with the locally administered bit
/// flipped. Flipping it again gives the guest's MAC back.
fn flip_local_bit(mut mac: Mac) -> Mac {
    mac[0] ^= 0x02;
    mac
}

// =============================== deletion ===================================

/// Remove the pod VM's bridge and tap0 from the pod network namespace
/// `netns`. A sandbox's delete does this once its VM process has exited, and
/// containerd then deletes eth0 with CNI and removes the namespace. A
/// namespace that is gone has nothing to remove.
pub fn detach(netns: &str) -> Result<(), String> {
    if !Path::new(netns).exists() {
        return Ok(());
    }
    let links = parse_links(&ip_in(netns, &["-j", "link", "show"])?)?;
    for link in detach_links(&links) {
        ip_in(netns, &["link", "del", "dev", link])?;
    }
    Ok(())
}

/// The links of the namespace that [`detach`] deletes: tap0, which goes with
/// the VM process unless something else holds it, and the bridge.
fn detach_links(links: &[Link]) -> Vec<&'static str> {
    [TAP_IFACE, BRIDGE]
        .into_iter()
        .filter(|name| links.iter().any(|l| l.name == *name))
        .collect()
}

// ================================= guest ====================================

/// `ip -j addr show dev eth0` in the pod network namespace `netns`.
fn eth0_addr(netns: &str) -> Result<Vec<u8>, String> {
    ip_in(netns, &["-j", "addr", "show", "dev", CNI_IFACE])
}

/// The pod's address and MTU, as `ip -j addr show dev eth0` gives them.
#[derive(Debug, PartialEq, Eq)]
struct PodAddress {
    ip: Ipv4Addr,
    prefix: u8,
    mtu: u32,
}

fn parse_pod_address(json: &[u8]) -> Result<PodAddress, String> {
    let value: serde_json::Value =
        serde_json::from_slice(json).map_err(|e| format!("parse `ip -j addr`: {e}"))?;
    let link = value.get(0).ok_or("`ip -j addr` shows no eth0")?;
    let mtu = link
        .get("mtu")
        .and_then(|v| v.as_u64())
        .and_then(|m| u32::try_from(m).ok())
        .ok_or("eth0 has no MTU")?;
    let inet = link
        .get("addr_info")
        .and_then(|v| v.as_array())
        .and_then(|a| {
            a.iter()
                .find(|i| i.get("family").and_then(|f| f.as_str()) == Some("inet"))
        })
        .ok_or("eth0 has no IPv4 address")?;
    let ip = inet
        .get("local")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok())
        .ok_or("eth0 has no IPv4 address")?;
    let prefix = inet
        .get("prefixlen")
        .and_then(|v| v.as_u64())
        .and_then(|p| u8::try_from(p).ok())
        .ok_or("eth0 has no prefix length")?;
    Ok(PodAddress { ip, prefix, mtu })
}

/// The pod's IP and the guest's MAC, from eth0 in the pod network namespace
/// `netns`, for the node service's record. Once eth0 is a port of the
/// bridge, it has the guest's MAC with the locally administered bit flipped.
pub fn pod_network(netns: &str) -> Result<(Ipv4Addr, String), String> {
    parse_pod_network(&eth0_addr(netns)?)
}

fn parse_pod_network(json: &[u8]) -> Result<(Ipv4Addr, String), String> {
    let address = parse_pod_address(json)?;
    let link = parse_links(json)?
        .into_iter()
        .find(|l| l.name == CNI_IFACE)
        .ok_or("`ip -j addr` shows no eth0")?;
    let mac = link.mac.ok_or("eth0 has no MAC")?;
    let guest = match link.master.as_deref() {
        Some(BRIDGE) => flip_local_bit(mac),
        _ => mac,
    };
    Ok((address.ip, format_mac(guest)))
}

/// Shell script, run in the guest after the first boot, that gives eth0 the
/// pod network's MTU and checks that eth0 has the pod address. smolvm reads
/// eth0's MTU, but passes the guest only its address, gateway and MAC, and its
/// agent sets the MTU to 1500. Both are guest memory, so they survive
/// hibernation.
pub fn guest_setup_script(netns: &str) -> Result<String, String> {
    Ok(guest_script(&parse_pod_address(&eth0_addr(netns)?)?))
}

fn guest_script(pod: &PodAddress) -> String {
    let PodAddress { ip, prefix, mtu } = pod;
    format!(
        "set -e; ip link set dev eth0 mtu {mtu}; \
         ip addr show dev eth0 | grep -qF 'inet {ip}/{prefix} ' || \
         {{ echo 'eth0 does not have the pod address {ip}/{prefix}' >&2; exit 1; }}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// smolvm's tc calls, in the order `setup_tc_redirect` makes them.
    fn smolvm_tc_calls() -> Vec<Vec<&'static str>> {
        let redirect = |from, to| {
            vec![
                "filter", "add", "dev", from, "ingress", "pref", "49152", "protocol", "all",
                "matchall", "action", "mirred", "egress", "redirect", "dev", to,
            ]
        };
        vec![
            vec!["qdisc", "replace", "dev", "eth0", "ingress"],
            vec!["qdisc", "replace", "dev", "tap0", "ingress"],
            vec!["filter", "show", "dev", "eth0", "ingress", "pref", "49152"],
            vec!["filter", "del", "dev", "eth0", "ingress", "pref", "49152"],
            redirect("eth0", "tap0"),
            redirect("tap0", "eth0"),
        ]
    }

    #[test]
    fn smolvm_tc_calls_are_understood_and_the_last_one_bridges() {
        let parsed: Vec<_> = smolvm_tc_calls()
            .iter()
            .map(|c| parse_tc(c).unwrap())
            .collect();
        assert_eq!(
            parsed,
            [
                TcCall::IngressQdisc,
                TcCall::IngressQdisc,
                TcCall::StaleRedirect,
                TcCall::StaleRedirect,
                TcCall::RedirectToVm,
                TcCall::RedirectFromVm,
            ]
        );
    }

    #[test]
    fn other_tc_calls_are_refused() {
        let mut eth1 = smolvm_tc_calls()[5].clone();
        eth1[15] = "eth1";
        let mut pref = smolvm_tc_calls()[4].clone();
        pref[6] = "1";
        for call in [
            vec![],
            vec!["qdisc", "add", "dev", "eth0", "clsact"],
            vec!["qdisc", "replace", "dev", "eth1", "ingress"],
            vec!["filter", "show", "dev", "tap0", "ingress", "pref", "49152"],
            vec!["-s", "filter", "show", "dev", "eth0", "ingress"],
            eth1,
            pref,
        ] {
            assert_eq!(parse_tc(&call), None, "{call:?}");
        }
        let err = tc(&["qdisc".into(), "show".into()]).unwrap_err();
        assert!(err.contains("no traffic control"), "{err}");
    }

    #[test]
    fn only_ip_and_tc_are_stand_ins() {
        assert_eq!(Tool::from_argv0(OsStr::new("tc")), Some(Tool::Tc));
        assert_eq!(Tool::from_argv0(OsStr::new("/x/bin/ip")), Some(Tool::Ip));
        for other in [
            "containerd-shim-smolvm-pods-v2",
            "/proc/self/exe",
            "ipx",
            "",
        ] {
            assert_eq!(Tool::from_argv0(OsStr::new(other)), None, "{other}");
        }
    }

    #[test]
    fn flipping_the_local_bit_twice_gives_the_mac_back() {
        for mac in [
            "c2:3c:2a:92:39:90",
            "02:00:00:00:00:01",
            "00:16:3e:00:00:01",
        ] {
            let mac = parse_mac(mac).unwrap();
            let port = flip_local_bit(mac);
            assert_ne!(port, mac);
            assert_eq!(port[0] & 0x01, 0, "the port MAC stays unicast");
            assert_eq!(flip_local_bit(port), mac);
        }
        assert_eq!(
            format_mac(flip_local_bit(parse_mac("c2:3c:2a:92:39:90").unwrap())),
            "c0:3c:2a:92:39:90"
        );
    }

    #[test]
    fn macs_parse_only_from_six_hex_pairs() {
        assert_eq!(
            parse_mac("0a:58:0A:58:00:6c"),
            Ok([0x0a, 0x58, 0x0a, 0x58, 0x00, 0x6c])
        );
        assert_eq!(parse_mac("00:01:0a:ff:FF:10"), Ok([0, 1, 10, 255, 255, 16]));
        for bad in [
            "",
            "0a:58:0a:58:00",
            "0a:58:0a:58:00:6c:01",
            "a:58:0a:58:00:6c",
            "00:01:0a:ff:ff",
            "00:01:0a:ff:ff:10:11",
            "0:01:0a:ff:ff:10",
            "zz:01:0a:ff:ff:10",
        ] {
            assert!(parse_mac(bad).is_err(), "{bad:?}");
        }
    }

    /// `ip -j link show` in a pod namespace, as iproute2 6.1 prints it.
    fn links_json(eth0_mac: &str, eth0_master: Option<&str>, bridge: bool) -> Vec<u8> {
        let mut eth0 = serde_json::json!({
            "ifindex": 2, "link_index": 25, "ifname": "eth0",
            "flags": ["BROADCAST", "MULTICAST", "UP", "LOWER_UP"], "mtu": 1450,
            "qdisc": "noqueue", "operstate": "UP", "linkmode": "DEFAULT", "group": "default",
            "link_type": "ether", "address": eth0_mac, "broadcast": "ff:ff:ff:ff:ff:ff",
            "link_netnsid": 0
        });
        if let Some(master) = eth0_master {
            eth0["master"] = master.into();
        }
        let mut links = vec![
            serde_json::json!({"ifindex": 1, "ifname": "lo", "mtu": 65536,
                "link_type": "loopback", "address": "00:00:00:00:00:00"}),
            eth0,
            serde_json::json!({"ifindex": 3, "ifname": "tap0", "mtu": 1500,
                "link_type": "ether", "address": "8e:37:e0:fa:77:1a"}),
        ];
        if bridge {
            links.push(
                serde_json::json!({"ifindex": 4, "ifname": "podbr0", "mtu": 1450,
                "link_type": "ether", "address": "c0:3c:2a:92:39:90"}),
            );
        }
        serde_json::to_vec(&links).unwrap()
    }

    fn ips(steps: &[Step]) -> Vec<String> {
        steps
            .iter()
            .filter_map(|s| match s {
                Step::Ip(args) => Some(args.join(" ")),
                Step::Sysctl { .. } => None,
            })
            .collect()
    }

    #[test]
    fn the_first_boot_makes_the_bridge_and_gives_eth0_the_port_mac() {
        let links = parse_links(&links_json("c2:3c:2a:92:39:90", None, false)).unwrap();
        let steps = bridge_steps(&links).unwrap();
        assert_eq!(
            ips(&steps),
            [
                "link add name podbr0 address c0:3c:2a:92:39:90 type bridge mcast_snooping 0 \
                 forward_delay 0",
                "link set dev eth0 address c0:3c:2a:92:39:90 master podbr0",
                "link set dev eth0 type bridge_slave learning off",
                "link set dev tap0 mtu 1450 master podbr0",
                "link set dev tap0 type bridge_slave learning off",
                "link set dev podbr0 up",
            ]
        );
        // The namespace stops answering ARP before eth0's MAC changes.
        let first_ip = steps.iter().position(|s| matches!(s, Step::Ip(_))).unwrap();
        assert!(steps[..first_ip].contains(&Step::Sysctl {
            key: "net/ipv4/conf/all/arp_ignore",
            value: "8",
            optional: false,
        }));
        assert!(steps[..first_ip].contains(&Step::Sysctl {
            key: "net/ipv4/conf/eth0/arp_notify",
            value: "0",
            optional: false,
        }));
    }

    #[test]
    fn a_later_boot_only_adds_the_new_tap() {
        let links = parse_links(&links_json("c0:3c:2a:92:39:90", Some("podbr0"), true)).unwrap();
        assert_eq!(
            ips(&bridge_steps(&links).unwrap()),
            [
                "link set dev eth0 type bridge_slave learning off",
                "link set dev tap0 mtu 1450 master podbr0",
                "link set dev tap0 type bridge_slave learning off",
                "link set dev podbr0 up",
            ]
        );
    }

    #[test]
    fn an_interrupted_first_boot_reuses_the_bridge() {
        let links = parse_links(&links_json("c2:3c:2a:92:39:90", None, true)).unwrap();
        let steps = ips(&bridge_steps(&links).unwrap());
        assert_eq!(
            steps[0],
            "link set dev eth0 address c0:3c:2a:92:39:90 master podbr0"
        );
        assert!(!steps.iter().any(|s| s.starts_with("link add")));
    }

    #[test]
    fn a_delete_removes_tap0_and_the_bridge_and_leaves_eth0() {
        let mut links =
            parse_links(&links_json("c0:3c:2a:92:39:90", Some("podbr0"), true)).unwrap();
        assert_eq!(detach_links(&links), ["tap0", "podbr0"]);
        // The VM process has exited, which closed tap0.
        links.retain(|l| l.name != "tap0");
        assert_eq!(detach_links(&links), ["podbr0"]);
        links.retain(|l| l.name != "podbr0");
        assert!(detach_links(&links).is_empty());
    }

    #[test]
    fn a_namespace_that_is_gone_has_nothing_to_detach() {
        detach("/var/run/netns/floci-test-gone").unwrap();
    }

    #[test]
    fn the_bridge_needs_eth0_and_tap0_and_no_other_master() {
        let links = parse_links(&links_json("c2:3c:2a:92:39:90", Some("cni0"), false)).unwrap();
        let err = bridge_steps(&links).unwrap_err();
        assert!(err.contains("port of cni0"), "{err}");

        let mut links = parse_links(&links_json("c2:3c:2a:92:39:90", None, false)).unwrap();
        links.retain(|l| l.name != "tap0");
        assert!(bridge_steps(&links).unwrap_err().contains("no tap0"));
        links.retain(|l| l.name != "eth0");
        assert!(bridge_steps(&links).unwrap_err().contains("no eth0"));
    }

    /// `ip -j addr show eth0` in a pod namespace, as iproute2 6.1 prints it.
    fn addr_json(mac: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!([{
            "ifindex": 2, "link_index": 25, "ifname": "eth0",
            "flags": ["BROADCAST", "MULTICAST", "UP", "LOWER_UP"], "mtu": 1450,
            "qdisc": "noqueue", "master": "podbr0", "operstate": "UP", "group": "default",
            "txqlen": 1000, "link_type": "ether", "address": mac,
            "broadcast": "ff:ff:ff:ff:ff:ff", "link_netnsid": 0,
            "addr_info": [{"family": "inet", "local": "10.42.0.17", "prefixlen": 24,
                "broadcast": "10.42.0.255", "scope": "global", "label": "eth0",
                "valid_life_time": 4294967295u64, "preferred_life_time": 4294967295u64}]
        }]))
        .unwrap()
    }

    #[test]
    fn smolvm_reads_the_guest_mac_from_a_bridged_eth0() {
        let bridged = addr_json("c0:3c:2a:92:39:90");
        let seen: serde_json::Value =
            serde_json::from_slice(&guest_view(&bridged, true).unwrap()).unwrap();
        let expected: serde_json::Value =
            serde_json::from_slice(&addr_json("c2:3c:2a:92:39:90")).unwrap();
        assert_eq!(seen, expected, "only the MAC changes");

        let fresh = addr_json("c2:3c:2a:92:39:90");
        assert_eq!(guest_view(&fresh, false).unwrap(), fresh);
        assert!(guest_view(b"[]", true).is_err());
    }

    #[test]
    fn the_real_tool_is_the_first_one_that_is_not_this_binary() {
        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("shim");
        let ours = dir.path().join("ours");
        let theirs = dir.path().join("theirs");
        for d in [&ours, &theirs] {
            std::fs::create_dir(d).unwrap();
        }
        std::fs::write(&exe, b"").unwrap();
        std::fs::write(theirs.join("ip"), b"").unwrap();
        for f in [&exe, &theirs.join("ip")] {
            std::fs::set_permissions(f, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        std::os::unix::fs::symlink(&exe, ours.join("ip")).unwrap();
        std::fs::write(ours.join("tc"), b"not executable").unwrap();

        let exe = std::fs::canonicalize(&exe).unwrap();
        let path = std::env::join_paths([&ours, &theirs]).unwrap();
        assert_eq!(find_tool("ip", &path, &exe), Some(theirs.join("ip")));
        assert_eq!(find_tool("tc", &path, &exe), None);
    }

    #[test]
    fn the_tools_link_to_this_binary_and_stale_links_are_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("shim");
        std::fs::write(&exe, b"").unwrap();
        let tools = dir.path().join("vm").join(TOOLS_DIR);
        install_tools(&tools, &exe).unwrap();
        install_tools(&tools, &exe).unwrap();
        std::fs::remove_file(tools.join("tc")).unwrap();
        std::os::unix::fs::symlink("/old/shim", tools.join("tc")).unwrap();
        install_tools(&tools, &exe).unwrap();
        for name in ["ip", "tc"] {
            assert_eq!(std::fs::read_link(tools.join(name)).unwrap(), exe);
        }
        assert_eq!(std::fs::read_dir(&tools).unwrap().count(), 2);
        assert!(install_tools(&tools, &dir.path().join("gone")).is_err());
    }

    fn write_config(dir: &Path, pod_netns: Option<&str>) -> PathBuf {
        let config = dir.join("boot-config.json");
        let value = serde_json::json!({
            "rootfs_path": "/r",
            "startup_error_log": dir.join("startup-error.log"),
            "pod_netns": pod_netns,
            "resources": {"cpus": 2, "memory_mib": 1024, "network": true},
        });
        std::fs::write(&config, value.to_string()).unwrap();
        config
    }

    fn read_config(config: &Path) -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(config).unwrap()).unwrap()
    }

    #[test]
    fn the_first_boot_records_the_netns_and_a_resume_gets_it_back() {
        let dir = tempfile::tempdir().unwrap();
        let ns = "/var/run/netns/cni-1234";
        let config = write_config(dir.path(), Some(ns));
        let before = std::fs::read(&config).unwrap();
        let mut boot = read_config(&config);
        assert_eq!(
            boot_netns(&config, &mut boot).unwrap(),
            Some(PathBuf::from(ns))
        );
        assert_eq!(std::fs::read(&config).unwrap(), before);

        // smolvm's resume writes a boot config without the namespace.
        let config = write_config(dir.path(), None);
        let mut boot = read_config(&config);
        assert_eq!(
            boot_netns(&config, &mut boot).unwrap(),
            Some(PathBuf::from(ns))
        );
        let resumed = read_config(&config);
        assert_eq!(resumed["pod_netns"], ns);
        assert_eq!(resumed["resources"]["memory_mib"], 1024);
        assert_eq!(resumed["rootfs_path"], "/r");
    }

    #[test]
    fn a_vm_without_a_pod_netns_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let config = write_config(dir.path(), None);
        let before = std::fs::read(&config).unwrap();
        prepare_boot(&config).unwrap();
        assert_eq!(std::fs::read(&config).unwrap(), before);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn a_bad_netns_fails_the_boot_and_says_why() {
        let dir = tempfile::tempdir().unwrap();
        let config = write_config(dir.path(), Some("cni-1234"));
        let err = prepare_boot(&config).unwrap_err();
        assert!(err.contains("not an absolute path"), "{err}");
        let log = std::fs::read_to_string(dir.path().join("startup-error.log")).unwrap();
        assert!(log.starts_with("error: pod network: "), "{log}");

        let config = write_config(dir.path(), Some("/var/run/netns/floci-test-missing"));
        let err = prepare_boot(&config).unwrap_err();
        assert!(err.contains("floci-test-missing"), "{err}");
    }

    #[test]
    fn the_pod_address_is_read_from_ip() {
        assert_eq!(
            parse_pod_address(&addr_json("c0:3c:2a:92:39:90")).unwrap(),
            PodAddress {
                ip: Ipv4Addr::new(10, 42, 0, 17),
                prefix: 24,
                mtu: 1450,
            }
        );
        let no_ipv4 = br#"[{"ifname":"eth0","mtu":1500,"addr_info":[{"family":"inet6","local":"fe80::1","prefixlen":64}]}]"#;
        assert!(parse_pod_address(no_ipv4).is_err());
    }

    #[test]
    fn the_guest_mac_is_eth0s_with_the_local_bit_flipped_back() {
        assert_eq!(
            parse_pod_network(&addr_json("c2:3c:2a:92:39:90")).unwrap(),
            (
                Ipv4Addr::new(10, 42, 0, 17),
                "c0:3c:2a:92:39:90".to_string()
            )
        );
        let unbridged = String::from_utf8(addr_json("c0:3c:2a:92:39:90"))
            .unwrap()
            .replace(r#""master":"podbr0","#, "");
        assert_eq!(
            parse_pod_network(unbridged.as_bytes()).unwrap().1,
            "c0:3c:2a:92:39:90"
        );
    }

    #[test]
    fn the_guest_script_sets_the_mtu_and_checks_the_address() {
        let script = guest_script(&PodAddress {
            ip: Ipv4Addr::new(10, 42, 0, 17),
            prefix: 24,
            mtu: 1450,
        });
        assert!(
            script.contains("ip link set dev eth0 mtu 1450;"),
            "{script}"
        );
        assert!(
            script.contains("grep -qF 'inet 10.42.0.17/24 '"),
            "{script}"
        );
        let syntax = Command::new("sh")
            .args(["-n", "-c", &script])
            .status()
            .unwrap();
        assert!(syntax.success(), "the guest script is not valid sh");
    }
}

/// smolvm's own pod network attach, run through the stand-ins in real network
/// namespaces: a node with cni0 in hairpin mode, as flannel makes it, a pod
/// with eth0, and a third namespace whose tap plays the guest, joined to tap0
/// by a frame copy as the VM process does. It needs root, /dev/net/tun, `ip`,
/// `nsenter`, and SMOLVM_PODS_BIN set to the built binary, so it only runs
/// when asked for:
///
/// ```text
/// SMOLVM_PODS_BIN=<binary> <test binary> --ignored --exact net::netns_tests::smolvm_attaches_a_pod_through_the_bridge
/// ```
#[cfg(test)]
mod netns_tests {
    use super::*;

    use std::io::Read;
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, OnceLock};
    use std::thread::JoinHandle;
    use std::time::{Duration, Instant};

    use crate::test_netns::{open_tap, Netns};

    const POD_IP: Ipv4Addr = Ipv4Addr::new(10, 42, 0, 17);
    const GATEWAY: Ipv4Addr = Ipv4Addr::new(10, 42, 0, 1);
    const PORT: u16 = 8080;

    /// The node's `ip`, found before the stand-ins go first on PATH.
    static REAL_IP: OnceLock<PathBuf> = OnceLock::new();

    /// A namespace that the node's `ip` manages.
    fn netns(role: &str) -> Netns {
        Netns::with_ip(REAL_IP.get().unwrap(), &format!("test-{role}"))
    }

    impl Netns {
        fn link(&self, name: &str) -> Option<Link> {
            let all = parse_links(self.ip(&["-j", "link", "show"]).as_bytes()).unwrap();
            all.into_iter().find(|l| l.name == name)
        }

        fn learns(&self, name: &str) -> bool {
            let details: serde_json::Value =
                serde_json::from_str(&self.ip(&["-d", "-j", "link", "show", "dev", name])).unwrap();
            details[0]["linkinfo"]["info_slave_data"]["learning"]
                .as_bool()
                .unwrap()
        }

        /// Connect to `addr` from this namespace and read what it sends.
        fn fetch(&self, addr: SocketAddr) -> std::io::Result<String> {
            self.run(move || {
                let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(1500))?;
                stream.set_read_timeout(Some(Duration::from_secs(2)))?;
                let mut reply = String::new();
                stream.read_to_string(&mut reply)?;
                Ok(reply)
            })
        }
    }

    /// A TCP server in a namespace that sends `reply` to every client.
    struct Server {
        stop: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
    }

    impl Server {
        fn start(netns: &Netns, addr: SocketAddr, reply: &'static str) -> Server {
            let stop = Arc::new(AtomicBool::new(false));
            let (ready_tx, ready) = std::sync::mpsc::channel();
            let stopping = stop.clone();
            let thread = netns.spawn(move || {
                let listener = TcpListener::bind(addr).unwrap();
                listener.set_nonblocking(true).unwrap();
                ready_tx.send(()).unwrap();
                while !stopping.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            stream.set_nonblocking(false).unwrap();
                            let _ = stream.write_all(reply.as_bytes());
                        }
                        Err(_) => std::thread::sleep(Duration::from_millis(5)),
                    }
                }
            });
            ready.recv().unwrap();
            Server {
                stop,
                thread: Some(thread),
            }
        }
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            if let Some(t) = self.thread.take() {
                let _ = t.join();
            }
        }
    }

    fn dup(fd: RawFd) -> OwnedFd {
        // SAFETY: dup of an open fd returns a new fd that we own.
        let new = unsafe { libc::dup(fd) };
        assert!(new >= 0, "dup: {}", std::io::Error::last_os_error());
        // SAFETY: `new` is a fresh fd.
        unsafe { OwnedFd::from_raw_fd(new) }
    }

    /// Copies frames between two taps, as the VM process copies them between
    /// tap0 and the VM's network card.
    struct TapLink {
        stop: Arc<AtomicBool>,
        threads: Vec<JoinHandle<()>>,
    }

    impl TapLink {
        fn start(a: OwnedFd, b: OwnedFd) -> TapLink {
            let stop = Arc::new(AtomicBool::new(false));
            let (a2, b2) = (dup(a.as_raw_fd()), dup(b.as_raw_fd()));
            let threads = [(a, b2), (b, a2)]
                .into_iter()
                .map(|(from, to)| {
                    let stop = stop.clone();
                    std::thread::spawn(move || copy_frames(&from, &to, &stop))
                })
                .collect();
            TapLink { stop, threads }
        }
    }

    impl Drop for TapLink {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            for t in self.threads.drain(..) {
                let _ = t.join();
            }
        }
    }

    fn copy_frames(from: &OwnedFd, to: &OwnedFd, stop: &AtomicBool) {
        let mut buf = vec![0u8; 65536];
        while !stop.load(Ordering::SeqCst) {
            let mut poll = libc::pollfd {
                fd: from.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: one valid pollfd.
            if unsafe { libc::poll(&mut poll, 1, 50) } <= 0 {
                continue;
            }
            // SAFETY: reads into a buffer of the given length.
            let n = unsafe { libc::read(from.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
            if n <= 0 {
                break;
            }
            // SAFETY: writes the bytes just read.
            unsafe { libc::write(to.as_raw_fd(), buf.as_ptr().cast(), n as usize) };
        }
    }

    /// What a packet socket on a sleeping pod's eth0 did: whether it answered
    /// an ARP request for the pod, and whether it then saw a SYN to the pod.
    #[derive(Debug, Default)]
    struct Watch {
        answered_arp: bool,
        saw_syn: bool,
    }

    /// Watch `iface` in the calling thread's namespace with a packet socket,
    /// as the pod's catcher ([`crate::sleepnet`]) does for a sleeping pod:
    /// answer ARP requests for the pod with the guest's MAC `guest`, until a
    /// TCP SYN to the pod's port arrives or `wait` passes. Sends on `ready`
    /// once it listens.
    fn watch_sleeping_pod(
        iface: &str,
        guest: Mac,
        wait: Duration,
        ready: std::sync::mpsc::Sender<()>,
    ) -> Watch {
        let proto = (libc::ETH_P_ALL as u16).to_be();
        // SAFETY: plain socket call.
        let fd = unsafe { libc::socket(libc::AF_PACKET, libc::SOCK_RAW, proto.into()) };
        assert!(
            fd >= 0,
            "packet socket: {}",
            std::io::Error::last_os_error()
        );
        // SAFETY: `fd` is a fresh fd.
        let sock = unsafe { OwnedFd::from_raw_fd(fd) };
        let name = std::ffi::CString::new(iface).unwrap();
        // SAFETY: a NUL-terminated name.
        let index = unsafe { libc::if_nametoindex(name.as_ptr()) };
        assert_ne!(index, 0, "no {iface}");
        // SAFETY: zeroed sockaddr_ll is valid, then filled in.
        let mut addr: libc::sockaddr_ll = unsafe { std::mem::zeroed() };
        addr.sll_family = libc::AF_PACKET as u16;
        addr.sll_protocol = proto;
        addr.sll_ifindex = index as i32;
        // SAFETY: binds to a valid sockaddr_ll of the given size.
        let rc = unsafe {
            libc::bind(
                sock.as_raw_fd(),
                std::ptr::addr_of!(addr).cast(),
                std::mem::size_of::<libc::sockaddr_ll>() as u32,
            )
        };
        assert_eq!(rc, 0, "bind: {}", std::io::Error::last_os_error());
        ready.send(()).unwrap();

        let deadline = Instant::now() + wait;
        let mut buf = vec![0u8; 65536];
        let mut watch = Watch::default();
        while Instant::now() < deadline {
            let mut poll = libc::pollfd {
                fd: sock.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: one valid pollfd.
            if unsafe { libc::poll(&mut poll, 1, 50) } <= 0 {
                continue;
            }
            // SAFETY: reads into a buffer of the given length.
            let n = unsafe { libc::read(sock.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
            if n <= 0 {
                continue;
            }
            let frame = &buf[..n as usize];
            if let Some(reply) = arp_reply_for_pod(frame, guest) {
                // SAFETY: writes a whole frame from a buffer of its length.
                let sent =
                    unsafe { libc::write(sock.as_raw_fd(), reply.as_ptr().cast(), reply.len()) };
                assert_eq!(
                    sent,
                    reply.len() as isize,
                    "send the ARP reply: {}",
                    std::io::Error::last_os_error()
                );
                watch.answered_arp = true;
            } else if is_syn_to_pod(frame) {
                watch.saw_syn = true;
                break;
            }
        }
        watch
    }

    /// If `frame` is an ARP request for the pod's IP, the reply that gives
    /// the pod's IP the MAC `guest`.
    fn arp_reply_for_pod(frame: &[u8], guest: Mac) -> Option<Vec<u8>> {
        let arp = frame.get(14..42)?;
        let request = frame[12..14] == [0x08, 0x06]
            && arp[..8] == [0, 1, 0x08, 0x00, 6, 4, 0, 1]
            && arp[24..28] == POD_IP.octets();
        if !request {
            return None;
        }
        let (sender_mac, sender_ip) = (&arp[8..14], &arp[14..18]);
        let mut reply = Vec::with_capacity(42);
        reply.extend_from_slice(sender_mac);
        reply.extend_from_slice(&guest);
        reply.extend_from_slice(&[0x08, 0x06, 0, 1, 0x08, 0x00, 6, 4, 0, 2]);
        reply.extend_from_slice(&guest);
        reply.extend_from_slice(&POD_IP.octets());
        reply.extend_from_slice(sender_mac);
        reply.extend_from_slice(sender_ip);
        Some(reply)
    }

    /// Whether `frame` is an IPv4 TCP SYN, without ACK, to the pod's port.
    fn is_syn_to_pod(frame: &[u8]) -> bool {
        if frame.len() < 34 || frame[12..14] != [0x08, 0x00] {
            return false;
        }
        let ip = &frame[14..];
        let header = usize::from(ip[0] & 0x0f) * 4;
        let Some(tcp) = ip.get(header..header + 14).filter(|_| header >= 20) else {
            return false;
        };
        ip[9] == 6
            && ip[16..20] == POD_IP.octets()
            && tcp[2..4] == PORT.to_be_bytes()
            && tcp[13] & 0x12 == 0x02
    }

    #[test]
    #[ignore = "needs root, network namespaces and SMOLVM_PODS_BIN"]
    fn smolvm_attaches_a_pod_through_the_bridge() {
        let bin = PathBuf::from(
            std::env::var_os("SMOLVM_PODS_BIN").expect("SMOLVM_PODS_BIN is the built binary"),
        );
        let path = std::env::var_os("PATH").unwrap_or_default();
        let exe = std::fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
        REAL_IP
            .set(find_tool("ip", &path, &exe).expect("ip on PATH"))
            .unwrap();
        // The VM process's PATH: the stand-ins first.
        let tools = tempfile::tempdir().unwrap();
        install_tools(tools.path(), &bin).unwrap();
        let dirs = std::iter::once(tools.path().to_path_buf()).chain(std::env::split_paths(&path));
        std::env::set_var("PATH", std::env::join_paths(dirs).unwrap());

        // The node and the pod as flannel's bridge CNI leaves them.
        let node = netns("node");
        let pod = netns("pod");
        let vm = netns("vm");
        node.ip(&["link", "add", "cni0", "type", "bridge"]);
        node.ip(&["addr", "add", "10.42.0.1/24", "dev", "cni0"]);
        node.ip(&["link", "set", "cni0", "up"]);
        node.ip(&[
            "link",
            "add",
            "vethpod",
            "mtu",
            "1450",
            "type",
            "veth",
            "peer",
            "name",
            "eth0",
            "mtu",
            "1450",
            "netns",
            pod.name(),
        ]);
        node.ip(&["link", "set", "vethpod", "master", "cni0", "up"]);
        node.ip(&[
            "link",
            "set",
            "vethpod",
            "type",
            "bridge_slave",
            "hairpin",
            "on",
        ]);
        pod.ip(&["addr", "add", "10.42.0.17/24", "dev", "eth0"]);
        pod.ip(&["link", "set", "eth0", "up"]);
        pod.ip(&["route", "add", "default", "via", "10.42.0.1"]);
        let pod_mac = pod.link("eth0").unwrap().mac.unwrap();

        // Boot: the VM process attaches from inside the pod namespace.
        let attach = |pod: &Netns| {
            let netns = pod.path().display().to_string();
            pod.run(move || smolvm::agent::pod_net::attach_pod_netns(&netns).unwrap())
        };
        let first = attach(&pod);
        let launch = first.launch();
        assert_eq!(launch.ip, POD_IP);
        assert_eq!(launch.prefix, 24);
        assert_eq!(launch.gateway, Some(GATEWAY));
        assert_eq!(launch.mac, pod_mac, "the guest takes the pod's MAC");
        assert_eq!(launch.mtu, 1450);

        let eth0 = pod.link("eth0").unwrap();
        let tap0 = pod.link("tap0").unwrap();
        assert_eq!(eth0.master.as_deref(), Some(BRIDGE));
        assert_eq!(eth0.mac, Some(flip_local_bit(pod_mac)));
        assert_eq!(tap0.master.as_deref(), Some(BRIDGE));
        assert_eq!(tap0.mtu, Some(1450));
        assert!(!pod.learns("eth0") && !pod.learns("tap0"));
        let arp_ignore =
            pod.run(|| std::fs::read_to_string("/proc/sys/net/ipv4/conf/all/arp_ignore"));
        assert_eq!(arp_ignore.unwrap().trim(), "8");

        // The guest: a tap with the pod's address and the MAC smolvm gave it,
        // joined to tap0. It outlives the VM process, as guest memory does.
        let guest = vm.run(|| open_tap("eth0", false));
        let mac = format_mac(launch.mac);
        vm.ip(&["link", "set", "eth0", "address", &mac, "mtu", "1450", "up"]);
        vm.ip(&["addr", "add", "10.42.0.17/24", "dev", "eth0"]);
        vm.ip(&["route", "add", "default", "via", "10.42.0.1"]);
        let link = TapLink::start(dup(launch.tap_fd), dup(guest.as_raw_fd()));
        let guest_addr = SocketAddr::from((POD_IP, PORT));
        let node_addr = SocketAddr::from((GATEWAY, PORT));
        let _guest_server = Server::start(&vm, guest_addr, "guest");
        let _node_server = Server::start(&node, node_addr, "node");

        // Every connection starts with empty neighbor tables. The node's ARP
        // requests for the pod reach the pod namespace too, and the guest's
        // come back to eth0 through cni0's hairpin port.
        let flush = || {
            vm.ip(&["neigh", "flush", "all"]);
            node.ip(&["neigh", "flush", "all"]);
        };
        let traffic = |rounds| {
            for round in 0..rounds {
                flush();
                assert_eq!(node.fetch(guest_addr).unwrap(), "guest", "round {round}");
                flush();
                assert_eq!(vm.fetch(node_addr).unwrap(), "node", "round {round}");
            }
        };
        traffic(5);
        let neigh: serde_json::Value =
            serde_json::from_str(&node.ip(&["-j", "neigh", "show", "10.42.0.17"])).unwrap();
        assert_eq!(neigh[0]["lladdr"], mac, "the node finds the guest");

        // Sleep: the VM process exits, which closes and removes tap0.
        drop(link);
        drop(first);
        assert!(pod.link("tap0").is_none());
        // Nothing in the namespace answers ARP for the pod, so a client with
        // no neighbor entry for it would get no route to the pod. A packet
        // socket on eth0 answers ARP with the guest's MAC, as the pod's
        // catcher (crate::sleepnet) does, and then sees the client's SYN,
        // which waits instead of being refused.
        flush();
        let (ready_tx, ready) = std::sync::mpsc::channel();
        let sniffer = pod
            .spawn(move || watch_sleeping_pod("eth0", pod_mac, Duration::from_secs(3), ready_tx));
        ready.recv().unwrap();
        let asleep = node.fetch(guest_addr).unwrap_err();
        assert_eq!(asleep.kind(), std::io::ErrorKind::TimedOut, "{asleep}");
        let watch = sniffer.join().unwrap();
        assert!(watch.answered_arp, "no ARP request for the pod on eth0");
        assert!(watch.saw_syn, "no SYN seen on eth0");

        // Resume: the next attach gives the guest the same MAC and a new tap0.
        let second = attach(&pod);
        assert_eq!(second.launch().mac, pod_mac);
        assert_eq!(pod.link("eth0").unwrap().mac, Some(flip_local_bit(pod_mac)));
        let link = TapLink::start(dup(second.launch().tap_fd), dup(guest.as_raw_fd()));
        traffic(3);

        // Delete: once the VM process has exited, the bridge goes, and eth0
        // stays for CNI to delete. A tap0 that something still holds goes too.
        drop(link);
        let held_tap = dup(second.launch().tap_fd);
        drop(second);
        assert!(pod.link("tap0").is_some(), "tap0 is held");
        detach(&pod.path().display().to_string()).unwrap();
        drop(held_tap);
        assert!(pod.link(BRIDGE).is_none(), "the bridge is left");
        assert!(pod.link("tap0").is_none(), "tap0 is left");
        assert_eq!(pod.link("eth0").map(|l| l.master), Some(None));
    }
}

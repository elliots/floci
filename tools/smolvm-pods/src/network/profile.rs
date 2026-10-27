//! The network profile that Floci publishes with `PUT /v1/network`, and its
//! checks. `tests/fixtures/network-profile.json` is its contract with Floci.

use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    /// Floci's digest of the profile's content.
    pub generation: String,
    #[serde(default)]
    pub isolation: bool,
    pub floci: FlociPorts,
    #[serde(default)]
    pub endpoints: Vec<Endpoint>,
    #[serde(default)]
    pub routes: Vec<Route>,
    #[serde(default)]
    pub allow: Vec<Allow>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issuer: Option<Issuer>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trust: Option<Trust>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlociPorts {
    /// The port of Floci's node DNS on the Floci host.
    pub dns_port: u16,
    /// Floci's ports, each `PORT` or `FIRST-LAST`.
    pub ports: Vec<String>,
}

/// A link-local address and port that pods reach, sent to a Floci port.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub address: Ipv4Addr,
    pub port: u16,
    pub target_port: u16,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub name: String,
    /// `SCHEME://HOST:PORT`, the host exact or `*.` and a domain.
    pub origins: Vec<String>,
    /// `HOST:PORT` of the simulator.
    pub backend: String,
    #[serde(default)]
    pub protocol: Protocol,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    #[default]
    Http1,
    H2c,
    Auto,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Allow {
    pub cidr: String,
    #[serde(default)]
    pub protocol: AllowProtocol,
    /// No ports for every port.
    #[serde(default)]
    pub ports: Vec<u16>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AllowProtocol {
    #[default]
    Tcp,
    Udp,
    Any,
}

/// The intermediate CA from which the relay mints a leaf for each routed name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Issuer {
    pub certificate: String,
    /// Its PKCS#8 key in PEM.
    pub key: String,
    /// Its subject's common name, its only attribute.
    pub subject: String,
    /// Its subject key identifier, in hex.
    pub key_id: String,
    pub not_after: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Trust {
    #[serde(default)]
    pub mode: TrustMode,
    /// Floci's CA in PEM.
    pub ca: String,
    pub ca_sha256: String,
    /// Public roots and Floci's CA in PEM, for a root file system without a
    /// bundle.
    pub bundle: String,
    /// Java trust stores holding the same roots, in base64: PKCS12 without a
    /// password, and JKS with `changeit`.
    pub pkcs12: String,
    pub jks: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TrustMode {
    #[default]
    System,
    Env,
    Off,
}

/// The host part of an origin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Host {
    Exact(String),
    /// `*.BASE`: any name below `BASE`, at any depth.
    Below(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Origin {
    pub tls: bool,
    pub host: Host,
    pub port: u16,
}

impl Origin {
    pub fn parse(text: &str) -> Result<Origin, String> {
        let (scheme, rest) = text
            .split_once("://")
            .ok_or_else(|| format!("origin {text:?} has no scheme"))?;
        let tls = match scheme {
            "https" => true,
            "http" => false,
            _ => return Err(format!("origin {text:?}: the scheme must be http or https")),
        };
        let (host, port) = match rest.rsplit_once(':') {
            Some((host, port)) => (
                host,
                port.parse::<u16>()
                    .ok()
                    .filter(|p| *p != 0)
                    .ok_or_else(|| format!("origin {text:?} has a bad port"))?,
            ),
            None => (rest, if tls { 443 } else { 80 }),
        };
        let host = host.to_ascii_lowercase();
        let (wildcard, name) = match host.strip_prefix("*.") {
            Some(base) => (true, base.to_string()),
            None => (false, host),
        };
        if !is_dns_name(&name) {
            return Err(format!("origin {text:?} has no DNS host name"));
        }
        let host = if wildcard {
            Host::Below(name)
        } else {
            Host::Exact(name)
        };
        Ok(Origin { tls, host, port })
    }

    /// Whether `name`, in lower case, is this origin's host.
    fn matches(&self, name: &str) -> Option<usize> {
        match &self.host {
            Host::Exact(host) => (host == name).then_some(usize::MAX),
            Host::Below(base) => name
                .strip_suffix(base.as_str())
                .and_then(|head| head.strip_suffix('.'))
                .filter(|head| !head.is_empty())
                .map(|_| base.len()),
        }
    }
}

fn is_dns_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 253
        && name.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                && !label.starts_with('-')
                && !label.ends_with('-')
        })
        && name.parse::<Ipv4Addr>().is_err()
}

/// An IPv4 network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cidr {
    pub addr: Ipv4Addr,
    pub len: u8,
}

impl Cidr {
    pub fn parse(text: &str) -> Result<Cidr, String> {
        let (addr, len) = match text.split_once('/') {
            Some((addr, len)) => (addr, len.parse::<u8>().ok().filter(|l| *l <= 32)),
            None => (text, Some(32)),
        };
        let addr: Ipv4Addr = addr
            .parse()
            .map_err(|_| format!("{text:?} is not an IPv4 CIDR"))?;
        let len = len.ok_or_else(|| format!("{text:?} has a bad prefix length"))?;
        Ok(Cidr {
            addr: Ipv4Addr::from(u32::from(addr) & mask(len)),
            len,
        })
    }

    pub fn contains(&self, ip: Ipv4Addr) -> bool {
        u32::from(ip) & mask(self.len) == u32::from(self.addr)
    }
}

impl std::fmt::Display for Cidr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.addr, self.len)
    }
}

fn mask(len: u8) -> u32 {
    if len == 0 {
        0
    } else {
        u32::MAX << (32 - u32::from(len))
    }
}

/// A port or a range of ports, inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ports(pub u16, pub u16);

impl Ports {
    pub fn parse(text: &str) -> Result<Ports, String> {
        let port = |p: &str| {
            p.trim()
                .parse::<u16>()
                .ok()
                .filter(|p| *p != 0)
                .ok_or_else(|| format!("{text:?} is not a port or a range of ports"))
        };
        let ports = match text.split_once('-') {
            Some((first, last)) => Ports(port(first)?, port(last)?),
            None => {
                let p = port(text)?;
                Ports(p, p)
            }
        };
        if ports.0 > ports.1 {
            return Err(format!("{text:?} is an empty range"));
        }
        Ok(ports)
    }

    pub fn contains(&self, port: u16) -> bool {
        self.0 <= port && port <= self.1
    }
}

impl std::fmt::Display for Ports {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 == self.1 {
            write!(f, "{}", self.0)
        } else {
            write!(f, "{}-{}", self.0, self.1)
        }
    }
}

/// A route's checked origins and backend.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckedRoute {
    pub name: String,
    pub origins: Vec<Origin>,
    pub backend_host: String,
    pub backend_port: u16,
    pub protocol: Protocol,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CheckedAllow {
    pub cidr: Cidr,
    pub protocol: AllowProtocol,
    pub ports: Vec<u16>,
}

/// A profile whose every field was checked.
#[derive(Debug, Clone, PartialEq)]
pub struct Checked {
    pub profile: Profile,
    pub floci_ports: Vec<Ports>,
    pub routes: Vec<CheckedRoute>,
    pub allow: Vec<CheckedAllow>,
}

/// The route for a connection to `port`, with TLS or not, for host `name`:
/// an exact origin first, then the wildcard with the longest base.
pub fn find_route<'a>(
    routes: &'a [CheckedRoute],
    tls: bool,
    port: u16,
    name: &str,
) -> Option<&'a CheckedRoute> {
    let name = name.trim_end_matches('.').to_ascii_lowercase();
    routes
        .iter()
        .flat_map(|route| route.origins.iter().map(move |origin| (route, origin)))
        .filter(|(_, origin)| origin.tls == tls && origin.port == port)
        .filter_map(|(route, origin)| origin.matches(&name).map(|rank| (rank, route)))
        .max_by_key(|(rank, _)| *rank)
        .map(|(_, route)| route)
}

impl Profile {
    pub fn check(self) -> Result<Checked, String> {
        if self.generation.is_empty() {
            return Err("generation is empty".into());
        }
        if self.floci.dns_port == 0 {
            return Err("floci.dns_port is 0".into());
        }
        let floci_ports = self
            .floci
            .ports
            .iter()
            .map(|p| Ports::parse(p).map_err(|e| format!("floci.ports: {e}")))
            .collect::<Result<Vec<_>, _>>()?;
        let mut routes = Vec::new();
        let mut seen: Vec<Origin> = Vec::new();
        for route in &self.routes {
            let what = format!("routes.{}", route.name);
            if route.origins.is_empty() {
                return Err(format!("{what} has no origins"));
            }
            let mut origins = Vec::new();
            for text in &route.origins {
                let origin = Origin::parse(text).map_err(|e| format!("{what}: {e}"))?;
                if origin.port == 53 || floci_ports.iter().any(|p| p.contains(origin.port)) {
                    return Err(format!(
                        "{what}: origin {text:?} uses port {}, which is not routable",
                        origin.port
                    ));
                }
                if seen.contains(&origin) {
                    return Err(format!("{what}: origin {text:?} is routed twice"));
                }
                if seen
                    .iter()
                    .any(|o| o.port == origin.port && o.tls != origin.tls)
                {
                    return Err(format!(
                        "{what}: origin {text:?} puts HTTP and HTTPS on one port"
                    ));
                }
                seen.push(origin.clone());
                origins.push(origin);
            }
            let (host, port) = route
                .backend
                .rsplit_once(':')
                .and_then(|(h, p)| Some((h, p.parse::<u16>().ok().filter(|p| *p != 0)?)))
                .ok_or_else(|| format!("{what}: backend {:?} is not HOST:PORT", route.backend))?;
            if host.parse::<Ipv4Addr>().is_err() && !is_dns_name(host) {
                return Err(format!(
                    "{what}: backend host {host:?} is not a name or an IPv4 address"
                ));
            }
            routes.push(CheckedRoute {
                name: route.name.clone(),
                origins,
                backend_host: host.to_ascii_lowercase(),
                backend_port: port,
                protocol: route.protocol,
            });
        }
        let allow = self
            .allow
            .iter()
            .map(|a| {
                Ok(CheckedAllow {
                    cidr: Cidr::parse(&a.cidr).map_err(|e| format!("allow: {e}"))?,
                    protocol: a.protocol,
                    ports: a.ports.clone(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        if allow.iter().any(|a| a.ports.contains(&0)) {
            return Err("allow: port 0".into());
        }
        if !routes.is_empty() && self.issuer.is_none() {
            return Err("routes need an issuer".into());
        }
        Ok(Checked {
            profile: self,
            floci_ports,
            routes,
            allow,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> Profile {
        let text = include_str!("../../tests/fixtures/network-profile.json");
        serde_json::from_str(text).unwrap()
    }

    #[test]
    fn the_contract_fixture_parses_and_checks() {
        let checked = fixture().check().unwrap();
        assert!(checked.profile.isolation);
        assert_eq!(checked.floci_ports[0], Ports(4566, 4566));
        assert_eq!(checked.floci_ports[1], Ports(5672, 5699));
        assert_eq!(checked.routes[0].backend_port, 8080);
        assert_eq!(checked.allow[0].cidr.to_string(), "203.0.113.10/32");
        assert_eq!(checked.profile.endpoints[0].target_port, 4566);
    }

    #[test]
    fn an_unknown_field_is_refused() {
        let mut value = serde_json::to_value(fixture()).unwrap();
        value["preserve_host"] = serde_json::json!(true);
        assert!(serde_json::from_value::<Profile>(value).is_err());
    }

    #[test]
    fn exact_origins_win_then_the_longest_wildcard() {
        let mut profile = fixture();
        profile.routes = vec![
            Route {
                name: "wide".into(),
                origins: vec!["https://*.vendor.test".into()],
                backend: "wide.sim.svc.cluster.local:80".into(),
                protocol: Protocol::Http1,
            },
            Route {
                name: "narrow".into(),
                origins: vec!["https://*.eu.vendor.test".into()],
                backend: "narrow.sim.svc.cluster.local:80".into(),
                protocol: Protocol::Http1,
            },
            Route {
                name: "exact".into(),
                origins: vec!["https://api.eu.vendor.test:443".into()],
                backend: "exact.sim.svc.cluster.local:80".into(),
                protocol: Protocol::H2c,
            },
        ];
        let checked = profile.check().unwrap();
        let name =
            |host: &str| find_route(&checked.routes, true, 443, host).map(|r| r.name.as_str());
        assert_eq!(name("api.eu.vendor.test"), Some("exact"));
        assert_eq!(name("API.EU.vendor.test."), Some("exact"));
        assert_eq!(name("x.y.eu.vendor.test"), Some("narrow"));
        assert_eq!(name("pay.vendor.test"), Some("wide"));
        assert_eq!(
            name("vendor.test"),
            None,
            "a wildcard does not match its base"
        );
        assert_eq!(name("vendor.test.evil"), None);
        assert_eq!(
            find_route(&checked.routes, false, 443, "pay.vendor.test"),
            None
        );
        assert_eq!(
            find_route(&checked.routes, true, 8443, "pay.vendor.test"),
            None
        );
    }

    #[test]
    fn bad_profiles_are_refused_with_the_field() {
        type Change = Box<dyn Fn(&mut Profile)>;
        let cases: Vec<(Change, &str)> = vec![
            (
                Box::new(|p| p.routes[0].origins = vec!["ftp://a.test".into()]),
                "scheme",
            ),
            (
                Box::new(|p| p.routes[0].origins = vec!["https://a.test:53".into()]),
                "port 53",
            ),
            (
                Box::new(|p| p.routes[0].origins = vec!["https://a.test:4566".into()]),
                "port 4566",
            ),
            (
                Box::new(|p| p.routes[0].origins = vec!["https://10.0.0.1".into()]),
                "no DNS host",
            ),
            (
                Box::new(|p| p.routes[0].backend = "sim".into()),
                "HOST:PORT",
            ),
            (Box::new(|p| p.routes[0].origins = vec![]), "no origins"),
            (
                Box::new(|p| {
                    p.routes[0].origins =
                        vec!["https://a.test:8000".into(), "http://b.test:8000".into()]
                }),
                "HTTP and HTTPS",
            ),
            (
                Box::new(|p| p.floci.ports = vec!["9-1".into()]),
                "empty range",
            ),
            (
                Box::new(|p| p.allow[0].cidr = "203.0.113.0/40".into()),
                "prefix",
            ),
            (Box::new(|p| p.issuer = None), "issuer"),
        ];
        for (change, want) in cases {
            let mut profile = fixture();
            change(&mut profile);
            let e = profile.check().unwrap_err();
            assert!(e.contains(want), "{want}: {e}");
        }
    }

    #[test]
    fn cidrs_and_ports_parse() {
        let cidr = Cidr::parse("10.42.7.9/16").unwrap();
        assert_eq!(cidr.to_string(), "10.42.0.0/16");
        assert!(cidr.contains("10.42.200.1".parse().unwrap()));
        assert!(!cidr.contains("10.43.0.1".parse().unwrap()));
        assert_eq!(Cidr::parse("1.2.3.4").unwrap().len, 32);
        assert_eq!(Ports::parse("5672-5699").unwrap().to_string(), "5672-5699");
        assert_eq!(Ports::parse("4566").unwrap().to_string(), "4566");
        assert!(Ports::parse("0").is_err());
    }
}

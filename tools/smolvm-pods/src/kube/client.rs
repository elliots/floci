//! A small client of the Kubernetes API server on hyper and rustls, with the
//! credentials of a kubeconfig such as the kubelet's. It reads the server, the
//! cluster CA, and a client certificate or a token, and makes one HTTP/1.1
//! connection per request: a list, or a watch that streams JSON lines.

use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Limited};
use hyper::body::Incoming;
use hyper::client::conn::http1::SendRequest;
use hyper::header::{ACCEPT, AUTHORIZATION, HOST, USER_AGENT};
use hyper::{Request, Response, Uri};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use tokio_rustls::TlsConnector;

use crate::http1::{connect, ConnectError};

/// How long a request may take to connect and answer, and a list to arrive.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// The largest list the client reads.
const BODY_MAX: usize = 64 << 20;
/// The largest watch event the client reads.
const LINE_MAX: usize = 16 << 20;
/// The prefix of a kubelet's user name; the rest is its node's name.
const NODE_USER_PREFIX: &str = "system:node:";

/// An error of a request, with the HTTP status if the API server answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    pub status: Option<u16>,
    pub message: String,
}

impl ApiError {
    pub fn new(message: impl Into<String>) -> ApiError {
        ApiError {
            status: None,
            message: message.into(),
        }
    }

    /// The resourceVersion is too old: list again (HTTP 410 Gone).
    pub fn is_expired(&self) -> bool {
        self.status == Some(410)
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.status {
            Some(status) => write!(f, "HTTP {status}: {}", self.message),
            None => f.write_str(&self.message),
        }
    }
}

/// A kubeconfig, with only the fields the client reads.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Kubeconfig {
    #[serde(default)]
    clusters: Vec<NamedCluster>,
    #[serde(default)]
    users: Vec<NamedUser>,
    #[serde(default)]
    contexts: Vec<NamedContext>,
    #[serde(default)]
    current_context: String,
}

#[derive(Debug, Deserialize)]
struct NamedCluster {
    name: String,
    cluster: Cluster,
}

#[derive(Debug, Deserialize)]
struct NamedUser {
    name: String,
    #[serde(default)]
    user: User,
}

#[derive(Debug, Deserialize)]
struct NamedContext {
    name: String,
    context: Context,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Cluster {
    server: String,
    #[serde(default)]
    certificate_authority: Option<String>,
    #[serde(default)]
    certificate_authority_data: Option<String>,
    #[serde(default)]
    tls_server_name: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct User {
    #[serde(default)]
    client_certificate: Option<String>,
    #[serde(default)]
    client_certificate_data: Option<String>,
    #[serde(default)]
    client_key: Option<String>,
    #[serde(default)]
    client_key_data: Option<String>,
    #[serde(default)]
    token: Option<String>,
    #[serde(default, rename = "tokenFile")]
    token_file: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Context {
    cluster: String,
    #[serde(default)]
    user: String,
}

/// The body of an API server error.
#[derive(Deserialize)]
struct Status {
    #[serde(default)]
    message: String,
}

/// A client of one API server.
pub struct Client {
    /// The server URL, for messages.
    server: String,
    host: String,
    port: u16,
    /// The `Host` header.
    authority: String,
    tls: Option<(TlsConnector, ServerName<'static>)>,
    bearer: Option<String>,
    /// The common name of the client certificate.
    user: Option<String>,
}

impl Client {
    /// A client with the current context of the kubeconfig at `path`.
    /// Relative file paths in it are relative to its directory.
    pub fn from_kubeconfig(path: &Path) -> Result<Client, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("read the kubeconfig {}: {e}", path.display()))?;
        let dir = path.parent().unwrap_or(Path::new("/"));
        Client::parse(&text, dir).map_err(|e| format!("kubeconfig {}: {e}", path.display()))
    }

    fn parse(text: &str, dir: &Path) -> Result<Client, String> {
        let config: Kubeconfig = serde_yaml::from_str(text).map_err(|e| e.to_string())?;
        let context = match config.contexts.as_slice() {
            [only] if config.current_context.is_empty() => only,
            contexts => contexts
                .iter()
                .find(|c| c.name == config.current_context)
                .ok_or_else(|| format!("no context {:?}", config.current_context))?,
        };
        let cluster = config
            .clusters
            .iter()
            .find(|c| c.name == context.context.cluster)
            .map(|c| &c.cluster)
            .ok_or_else(|| format!("no cluster {:?}", context.context.cluster))?;
        let none = User::default();
        let user = match config.users.iter().find(|u| u.name == context.context.user) {
            Some(named) => &named.user,
            None if context.context.user.is_empty() => &none,
            None => return Err(format!("no user {:?}", context.context.user)),
        };

        let server: Uri = cluster
            .server
            .parse()
            .map_err(|e| format!("server {:?}: {e}", cluster.server))?;
        let https = match server.scheme_str() {
            Some("https") => true,
            Some("http") => false,
            _ => return Err(format!("server {:?} is not an HTTP URL", cluster.server)),
        };
        let authority = server
            .authority()
            .ok_or_else(|| format!("server {:?} has no host", cluster.server))?;
        let host = authority
            .host()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        let port = authority.port_u16().unwrap_or(if https { 443 } else { 80 });

        let ca = source(
            &cluster.certificate_authority_data,
            &cluster.certificate_authority,
            dir,
        )?;
        let cert = source(&user.client_certificate_data, &user.client_certificate, dir)?;
        let key = source(&user.client_key_data, &user.client_key, dir)?;
        let client_auth = match (cert, key) {
            (Some(cert), Some(key)) => Some((certs(&cert)?, private_key(&key)?)),
            (None, None) => None,
            _ => return Err("a client certificate needs both a certificate and a key".into()),
        };
        let user_name = client_auth
            .as_ref()
            .and_then(|(certs, _)| certs.first())
            .and_then(|cert| subject_common_name(cert));
        let bearer = match (&user.token, &user.token_file) {
            (Some(token), _) => Some(token.trim().to_string()),
            (None, Some(file)) => {
                let file = dir.join(file);
                let token = std::fs::read_to_string(&file)
                    .map_err(|e| format!("read the token {}: {e}", file.display()))?;
                Some(token.trim().to_string())
            }
            (None, None) => None,
        };

        let tls = if https {
            let ca = ca.ok_or("an HTTPS server needs a certificate authority")?;
            let name = cluster.tls_server_name.clone().unwrap_or(host.clone());
            let name = ServerName::try_from(name.clone())
                .map_err(|e| format!("server name {name:?}: {e}"))?;
            let config = tls_config(certs(&ca)?, client_auth)?;
            Some((TlsConnector::from(Arc::new(config)), name))
        } else {
            None
        };
        Ok(Client {
            server: cluster.server.clone(),
            host,
            port,
            authority: authority.as_str().to_string(),
            tls,
            bearer,
            user: user_name,
        })
    }

    pub fn server(&self) -> &str {
        &self.server
    }

    /// The node of a kubelet's credentials: `NAME` of the client
    /// certificate's common name `system:node:NAME`.
    pub fn node_name(&self) -> Option<&str> {
        self.user.as_deref()?.strip_prefix(NODE_USER_PREFIX)
    }

    /// GETs `path` (with its query) and parses the JSON answer.
    pub async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T, ApiError> {
        let read = async {
            let (_sender, response) = self.send(path).await?;
            let body = Limited::new(response.into_body(), BODY_MAX)
                .collect()
                .await
                .map_err(|e| ApiError::new(format!("read GET {path}: {e}")))?
                .to_bytes();
            serde_json::from_slice(&body)
                .map_err(|e| ApiError::new(format!("parse GET {path}: {e}")))
        };
        tokio::time::timeout(REQUEST_TIMEOUT, read)
            .await
            .map_err(|_| ApiError::new(format!("GET {path} timed out")))?
    }

    /// GETs `path` and returns its body as lines, for a watch.
    pub async fn stream(&self, path: &str) -> Result<Lines, ApiError> {
        let (sender, response) = tokio::time::timeout(REQUEST_TIMEOUT, self.send(path))
            .await
            .map_err(|_| ApiError::new(format!("GET {path} timed out")))??;
        Ok(Lines {
            _sender: sender,
            body: response.into_body(),
            buf: Vec::new(),
        })
    }

    async fn send(
        &self,
        path: &str,
    ) -> Result<(SendRequest<Empty<Bytes>>, Response<Incoming>), ApiError> {
        let tls = self
            .tls
            .as_ref()
            .map(|(connector, name)| (connector, name.clone()));
        let (mut sender, connection) = connect((self.host.as_str(), self.port), tls)
            .await
            .map_err(|e| {
                ApiError::new(match e {
                    ConnectError::Tcp(e) | ConnectError::Tls(e) => {
                        format!("connect to {}: {e}", self.server)
                    }
                    ConnectError::Http(e) => format!("HTTP to {}: {e}", self.server),
                })
            })?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::debug!("API server connection: {e}");
            }
        });

        let mut request = Request::get(path)
            .header(HOST, &self.authority)
            .header(ACCEPT, "application/json")
            .header(USER_AGENT, "smolvm-pods");
        if let Some(token) = &self.bearer {
            request = request.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        let request = request
            .body(Empty::new())
            .map_err(|e| ApiError::new(format!("GET {path}: {e}")))?;
        let response = sender
            .send_request(request)
            .await
            .map_err(|e| ApiError::new(format!("GET {path}: {e}")))?;
        let status = response.status();
        if status.is_success() {
            return Ok((sender, response));
        }
        let body = Limited::new(response.into_body(), 64 << 10)
            .collect()
            .await
            .map(|b| b.to_bytes())
            .unwrap_or_default();
        let message = serde_json::from_slice::<Status>(&body)
            .map(|s| s.message)
            .unwrap_or_else(|_| String::from_utf8_lossy(&body).trim().to_string());
        Err(ApiError {
            status: Some(status.as_u16()),
            message: format!("GET {path}: {message}"),
        })
    }
}

/// The body of a watch, one JSON document a line.
pub struct Lines {
    /// Keeps the connection while the body streams.
    _sender: SendRequest<Empty<Bytes>>,
    body: Incoming,
    buf: Vec<u8>,
}

impl Lines {
    /// The next line that is not blank, or none at the end of the body.
    pub async fn next(&mut self) -> Result<Option<Vec<u8>>, String> {
        loop {
            if let Some(end) = self.buf.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = self.buf.drain(..=end).collect();
                if line.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                return Ok(Some(line));
            }
            match self.body.frame().await {
                None if self.buf.iter().all(u8::is_ascii_whitespace) => return Ok(None),
                None => return Ok(Some(std::mem::take(&mut self.buf))),
                Some(Err(e)) => return Err(format!("read the watch: {e}")),
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        if self.buf.len() + data.len() > LINE_MAX {
                            return Err(format!("a watch event is over {LINE_MAX} bytes"));
                        }
                        self.buf.extend_from_slice(&data);
                    }
                }
            }
        }
    }
}

/// The bytes of a kubeconfig field: inline base64 data, or a file.
fn source(
    data: &Option<String>,
    file: &Option<String>,
    dir: &Path,
) -> Result<Option<Vec<u8>>, String> {
    if let Some(data) = data {
        let data: String = data.split_whitespace().collect();
        return base64::engine::general_purpose::STANDARD
            .decode(data)
            .map(Some)
            .map_err(|e| format!("base64 data: {e}"));
    }
    match file {
        Some(file) => {
            let file = dir.join(file);
            std::fs::read(&file)
                .map(Some)
                .map_err(|e| format!("read {}: {e}", file.display()))
        }
        None => Ok(None),
    }
}

fn certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, String> {
    let certs = rustls_pemfile::certs(&mut &pem[..])
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("read certificates: {e}"))?;
    if certs.is_empty() {
        return Err("no PEM certificate".into());
    }
    Ok(certs)
}

fn private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, String> {
    rustls_pemfile::private_key(&mut &pem[..])
        .map_err(|e| format!("read the private key: {e}"))?
        .ok_or_else(|| "no PEM private key".to_string())
}

fn tls_config(
    ca: Vec<CertificateDer<'static>>,
    client_auth: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
) -> Result<ClientConfig, String> {
    let mut roots = RootCertStore::empty();
    for cert in ca {
        roots
            .add(cert)
            .map_err(|e| format!("certificate authority: {e}"))?;
    }
    let builder =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|e| e.to_string())?
            .with_root_certificates(roots);
    let mut config = match client_auth {
        Some((certs, key)) => builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| format!("client certificate: {e}"))?,
        None => builder.with_no_client_auth(),
    };
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

/// The common name in a certificate's subject, by a small DER walk.
fn subject_common_name(der: &[u8]) -> Option<String> {
    const SEQUENCE: u8 = 0x30;
    const OID: u8 = 0x06;
    const COMMON_NAME: [u8; 3] = [0x55, 0x04, 0x03];
    let (_, certificate, _) = tlv(der).filter(|(tag, ..)| *tag == SEQUENCE)?;
    let (_, mut tbs, _) = tlv(certificate).filter(|(tag, ..)| *tag == SEQUENCE)?;
    // The explicit [0] version, then the serial, signature, issuer and
    // validity before the subject.
    if tbs.first() == Some(&0xa0) {
        tbs = tlv(tbs)?.2;
    }
    for _ in 0..4 {
        tbs = tlv(tbs)?.2;
    }
    let (_, mut names, _) = tlv(tbs).filter(|(tag, ..)| *tag == SEQUENCE)?;
    while !names.is_empty() {
        let (_, mut set, rest) = tlv(names)?;
        names = rest;
        while !set.is_empty() {
            let (_, pair, rest) = tlv(set)?;
            set = rest;
            let (tag, oid, value) = tlv(pair)?;
            if tag == OID && oid == COMMON_NAME {
                return String::from_utf8(tlv(value)?.1.to_vec()).ok();
            }
        }
    }
    None
}

/// One DER element: its tag, its contents and the bytes after it.
fn tlv(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = input.split_first()?;
    let (&first, mut rest) = rest.split_first()?;
    let len = if first < 0x80 {
        usize::from(first)
    } else {
        let count = usize::from(first & 0x7f);
        if count == 0 || count > 4 || rest.len() < count {
            return None;
        }
        let len = rest[..count]
            .iter()
            .fold(0usize, |len, &b| (len << 8) | usize::from(b));
        rest = &rest[count..];
        len
    };
    (rest.len() >= len).then(|| (tag, &rest[..len], &rest[len..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};

    fn cert_with_cn(cn: &str) -> (String, String) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.distinguished_name = DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::OrganizationName, "system:nodes");
        params.distinguished_name.push(DnType::CommonName, cn);
        let cert = params.self_signed(&key).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    fn b64(text: &str) -> String {
        base64::engine::general_purpose::STANDARD.encode(text)
    }

    #[test]
    fn the_node_name_is_the_client_certificates_common_name() {
        let (cert, _) = cert_with_cn("system:node:floci-node");
        let der = certs(cert.as_bytes()).unwrap().remove(0);
        assert_eq!(
            subject_common_name(&der).as_deref(),
            Some("system:node:floci-node")
        );
        assert_eq!(subject_common_name(&der[..der.len() / 2]), None);
        assert_eq!(subject_common_name(b""), None);
    }

    /// The layout of k3s's `agent/kubelet.kubeconfig`: files for the CA, the
    /// certificate and the key.
    #[test]
    fn a_kubelet_kubeconfig_with_files_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let (ca, _) = cert_with_cn("k3s-server-ca");
        let (cert, key) = cert_with_cn("system:node:node-1");
        std::fs::write(dir.path().join("server-ca.crt"), ca).unwrap();
        std::fs::write(dir.path().join("client-kubelet.crt"), cert).unwrap();
        std::fs::write(dir.path().join("client-kubelet.key"), key).unwrap();
        let config = format!(
            "apiVersion: v1
clusters:
- cluster:
    certificate-authority: {dir}/server-ca.crt
    server: https://127.0.0.1:6443
  name: local
contexts:
- context:
    cluster: local
    namespace: default
    user: user
  name: Default
current-context: Default
kind: Config
preferences: {{}}
users:
- name: user
  user:
    client-certificate: {dir}/client-kubelet.crt
    client-key: client-kubelet.key
",
            dir = dir.path().display()
        );
        let path = dir.path().join("kubelet.kubeconfig");
        std::fs::write(&path, config).unwrap();
        let client = Client::from_kubeconfig(&path).unwrap();
        assert_eq!(client.node_name(), Some("node-1"));
        assert_eq!((client.host.as_str(), client.port), ("127.0.0.1", 6443));
        assert_eq!(client.authority, "127.0.0.1:6443");
        assert!(client.tls.is_some() && client.bearer.is_none());
    }

    #[test]
    fn inline_data_and_a_token_are_read() {
        let (ca, _) = cert_with_cn("ca");
        let config = format!(
            "clusters:
- name: c
  cluster:
    server: https://[::1]
    certificate-authority-data: {}
    tls-server-name: kubernetes
contexts:
- name: x
  context: {{cluster: c, user: u}}
users:
- name: u
  user: {{token: \" secret\\n\"}}
",
            b64(&ca)
        );
        let client = Client::parse(&config, Path::new("/")).unwrap();
        assert_eq!((client.host.as_str(), client.port), ("::1", 443));
        assert_eq!(client.bearer.as_deref(), Some("secret"));
        assert_eq!(client.node_name(), None);
        let (_, name) = client.tls.as_ref().unwrap();
        assert_eq!(name, &ServerName::try_from("kubernetes").unwrap());
    }

    #[test]
    fn a_kubeconfig_that_does_not_add_up_is_refused() {
        let no_context = "clusters: []\ncontexts: []\ncurrent-context: gone\n";
        assert_eq!(
            Client::parse(no_context, Path::new("/")).err().unwrap(),
            "no context \"gone\""
        );
        let no_ca = "clusters: [{name: c, cluster: {server: \"https://10.0.0.1:6443\"}}]
contexts: [{name: x, context: {cluster: c}}]
";
        assert_eq!(
            Client::parse(no_ca, Path::new("/")).err().unwrap(),
            "an HTTPS server needs a certificate authority"
        );
        let (cert, _) = cert_with_cn("system:node:n");
        let half = format!(
            "clusters: [{{name: c, cluster: {{server: \"http://10.0.0.1:8080\"}}}}]
contexts: [{{name: x, context: {{cluster: c, user: u}}}}]
users: [{{name: u, user: {{client-certificate-data: {}}}}}]
",
            b64(&cert)
        );
        assert_eq!(
            Client::parse(&half, Path::new("/")).err().unwrap(),
            "a client certificate needs both a certificate and a key"
        );
    }
}

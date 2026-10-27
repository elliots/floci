//! HTTPS probe answers over rustls with a self-signed node certificate.
//!
//! Kubelet does not verify the certificate, and offers `h2` and `http/1.1` with ALPN. The
//! responder offers only `http/1.1`, so that HTTPS probes are answered as HTTP ones.

use std::io::{ErrorKind, Read, Write};
use std::sync::Arc;

use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::{ServerConfig, ServerConnection};

/// The name in the node's certificate.
const CERT_NAME: &str = "smolvm-pods-probe";

/// The node's TLS settings for HTTPS probes, with a new self-signed certificate. The crypto
/// provider is named, because the build has two.
pub fn server_config() -> Result<Arc<ServerConfig>, String> {
    let certified = rcgen::generate_simple_self_signed(vec![CERT_NAME.to_string()])
        .map_err(|e| format!("make the probe certificate: {e}"))?;
    let key = PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der());
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .and_then(|b| {
            b.with_no_client_auth().with_single_cert(
                vec![certified.cert.der().clone()],
                PrivateKeyDer::Pkcs8(key),
            )
        })
        .map_err(|e| format!("probe TLS settings: {e}"))?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// One TLS connection, without IO: records from the client in, plaintext out, and back.
pub struct TlsSession {
    conn: ServerConnection,
}

impl TlsSession {
    pub fn new(config: &Arc<ServerConfig>) -> Result<TlsSession, String> {
        ServerConnection::new(config.clone())
            .map(|conn| TlsSession { conn })
            .map_err(|e| format!("TLS: {e}"))
    }

    /// Takes records from the client and appends the plaintext they carry to `plain`.
    /// Returns true once the client closed its side.
    pub fn receive(&mut self, mut records: &[u8], plain: &mut Vec<u8>) -> Result<bool, String> {
        let mut closed = false;
        while !records.is_empty() {
            if self
                .conn
                .read_tls(&mut records)
                .map_err(|e| format!("TLS: {e}"))?
                == 0
            {
                break;
            }
            let state = self
                .conn
                .process_new_packets()
                .map_err(|e| format!("TLS: {e}"))?;
            closed |= state.peer_has_closed();
            let mut buf = [0u8; 1024];
            loop {
                match self.conn.reader().read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => plain.extend_from_slice(&buf[..n]),
                    Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(e) => return Err(format!("TLS: {e}")),
                }
            }
        }
        Ok(closed)
    }

    /// Sends `plain`, then closes the TLS session.
    pub fn send_and_close(&mut self, plain: &[u8]) -> Result<(), String> {
        self.conn
            .writer()
            .write_all(plain)
            .map_err(|e| format!("TLS: {e}"))?;
        self.conn.send_close_notify();
        Ok(())
    }

    /// Appends the records waiting for the client to `out`.
    pub fn output(&mut self, out: &mut Vec<u8>) {
        while self.conn.wants_write() {
            match self.conn.write_tls(out) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use rustls::pki_types::ServerName;
    use rustls::ClientConnection;

    use super::*;
    use crate::probe::check::insecure_client_config;
    use crate::probe::http::tests::KUBELET_REQUEST;

    /// A TLS client as kubelet's HTTPS probe makes it: no verification, ALPN `h2` and
    /// `http/1.1`.
    pub fn kubelet_client() -> ClientConnection {
        client(&[b"h2", b"http/1.1"])
    }

    fn client(alpn: &[&[u8]]) -> ClientConnection {
        let name = ServerName::try_from("10.42.0.9").unwrap();
        ClientConnection::new(Arc::new(insecure_client_config(alpn)), name).unwrap()
    }

    /// Moves records from the client to the server and back until neither has any.
    fn exchange(client: &mut ClientConnection, server: &mut TlsSession, plain: &mut Vec<u8>) {
        for _ in 0..10 {
            let mut records = Vec::new();
            while client.wants_write() {
                client.write_tls(&mut records).unwrap();
            }
            server.receive(&records, plain).unwrap();
            let mut back = Vec::new();
            server.output(&mut back);
            if records.is_empty() && back.is_empty() {
                return;
            }
            client.read_tls(&mut &back[..]).unwrap();
            client.process_new_packets().unwrap();
        }
        panic!("the TLS exchange does not end");
    }

    #[test]
    fn an_https_probe_gets_http_1_1_over_the_node_certificate() {
        let config = server_config().unwrap();
        let mut server = TlsSession::new(&config).unwrap();
        let mut client = kubelet_client();
        client.writer().write_all(KUBELET_REQUEST).unwrap();
        let mut plain = Vec::new();
        exchange(&mut client, &mut server, &mut plain);
        assert_eq!(client.alpn_protocol(), Some(&b"http/1.1"[..]));
        assert_eq!(plain, KUBELET_REQUEST);

        server.send_and_close(b"HTTP/1.1 200 OK\r\n\r\n").unwrap();
        exchange(&mut client, &mut server, &mut plain);
        let mut answer = Vec::new();
        client.reader().read_to_end(&mut answer).unwrap();
        assert_eq!(answer, b"HTTP/1.1 200 OK\r\n\r\n");
    }

    #[test]
    fn a_client_without_http_1_1_or_tls_is_refused() {
        let config = server_config().unwrap();
        let mut h2_only = client(&[b"h2"]);
        let mut hello = Vec::new();
        h2_only.write_tls(&mut hello).unwrap();
        let mut server = TlsSession::new(&config).unwrap();
        let refused = server.receive(&hello, &mut Vec::new()).unwrap_err();
        assert!(refused.contains("any known protocol"), "{refused}");

        let mut server = TlsSession::new(&config).unwrap();
        let mut plain = Vec::new();
        assert!(server
            .receive(b"GET / HTTP/1.1\r\n\r\n", &mut plain)
            .is_err());
        assert!(plain.is_empty());
    }
}

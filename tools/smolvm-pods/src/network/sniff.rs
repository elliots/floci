//! The name a client asks for in the first bytes of a connection: the server
//! name of a TLS ClientHello, or the `Host` of an HTTP/1.x request.

/// What the first bytes say.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hello {
    /// A TLS ClientHello, with its server name if it sent one.
    Tls(Option<String>),
    /// An HTTP/1.x request, with its `Host` without the port.
    Http(Option<String>),
    /// The HTTP/2 connection preface (h2c with prior knowledge).
    H2c,
    /// Not enough bytes yet.
    More,
    /// Something else.
    Other,
}

const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

pub fn sniff(data: &[u8]) -> Hello {
    if data.is_empty() {
        return Hello::More;
    }
    if data[0] == 0x16 {
        return tls(data);
    }
    if H2_PREFACE.starts_with(data) {
        return Hello::More;
    }
    if data.starts_with(H2_PREFACE) {
        return Hello::H2c;
    }
    http(data)
}

fn tls(data: &[u8]) -> Hello {
    // A TLS record: type, version, length; then a handshake: type 1, length.
    if data.len() < 5 {
        return Hello::More;
    }
    let record_len = usize::from(u16::from_be_bytes([data[3], data[4]]));
    if data.len() < 5 + record_len {
        return Hello::More;
    }
    let hs = &data[5..5 + record_len];
    if hs.len() < 4 || hs[0] != 1 {
        return Hello::Other;
    }
    let mut r = Reader(&hs[4..]);
    let name = (|| {
        r.skip(2 + 32)?; // version, random
        let sid = r.u8()?;
        r.skip(usize::from(sid))?;
        let suites = r.u16()?;
        r.skip(usize::from(suites))?;
        let methods = r.u8()?;
        r.skip(usize::from(methods))?;
        let ext_len = r.u16()?;
        let mut ext = Reader(r.take(usize::from(ext_len))?);
        while !ext.0.is_empty() {
            let kind = ext.u16()?;
            let len = ext.u16()?;
            let body = ext.take(usize::from(len))?;
            if kind == 0 {
                let mut list = Reader(body);
                let _list_len = list.u16()?;
                while !list.0.is_empty() {
                    let name_type = list.u8()?;
                    let len = list.u16()?;
                    let name = list.take(usize::from(len))?;
                    if name_type == 0 {
                        return Some(String::from_utf8_lossy(name).to_ascii_lowercase());
                    }
                }
            }
        }
        None
    })();
    Hello::Tls(name)
}

fn http(data: &[u8]) -> Hello {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut request = httparse::Request::new(&mut headers);
    match request.parse(data) {
        Ok(status) if status.is_complete() => {}
        Ok(_) if data.len() < 16 * 1024 => return Hello::More,
        _ => return Hello::Other,
    }
    let host = request
        .headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case("host"))
        .map(|h| {
            let value = String::from_utf8_lossy(h.value);
            let value = value.trim();
            let host = match value.rsplit_once(':') {
                Some((h, p)) if p.bytes().all(|b| b.is_ascii_digit()) => h,
                _ => value,
            };
            host.to_ascii_lowercase()
        });
    Hello::Http(host)
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.0.len() < n {
            return None;
        }
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Some(head)
    }

    fn skip(&mut self, n: usize) -> Option<()> {
        self.take(n).map(drop)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }

    fn u16(&mut self) -> Option<u16> {
        self.take(2).map(|b| u16::from_be_bytes([b[0], b[1]]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A ClientHello from rustls for `name`.
    fn client_hello(name: &str) -> Vec<u8> {
        use std::sync::Arc;
        let roots = rustls::RootCertStore::empty();
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        let server = rustls::pki_types::ServerName::try_from(name.to_string()).unwrap();
        let mut conn = rustls::ClientConnection::new(Arc::new(config), server).unwrap();
        let mut out = Vec::new();
        conn.write_tls(&mut out).unwrap();
        out
    }

    #[test]
    fn a_client_hello_gives_its_server_name() {
        let hello = client_hello("API.Vendor.test");
        assert_eq!(sniff(&hello), Hello::Tls(Some("api.vendor.test".into())));
        assert_eq!(sniff(&hello[..20]), Hello::More);
    }

    #[test]
    fn an_http_request_gives_its_host() {
        let req = b"GET /pay HTTP/1.1\r\nUser-Agent: x\r\nhost: Pay.Vendor.test:8080\r\n\r\n";
        assert_eq!(sniff(req), Hello::Http(Some("pay.vendor.test".into())));
        assert_eq!(sniff(b"GET /pay HTT"), Hello::More);
        assert_eq!(sniff(b"GET / HTTP/1.0\r\n\r\n"), Hello::Http(None));
        assert_eq!(sniff(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\0\0"), Hello::H2c);
        assert_eq!(sniff(b"PRI * HT"), Hello::More);
        assert_eq!(sniff(b"\x00\x01binary"), Hello::Other);
    }
}

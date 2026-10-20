//! An HTTP/1.1 client connection over TCP or TLS.

use bytes::Bytes;
use http_body_util::Empty;
use hyper::client::conn::http1::{self, SendRequest};
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_rustls::TlsConnector;

/// The stream under an HTTP connection: TCP, or TLS over TCP.
pub trait Io: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// An HTTP/1.1 connection, which a task drives while its requests are sent.
pub type Connection = http1::Connection<TokioIo<Box<dyn Io>>, Empty<Bytes>>;

/// The step of [`connect`] that failed.
#[derive(Debug)]
pub enum ConnectError {
    Tcp(std::io::Error),
    Tls(std::io::Error),
    Http(hyper::Error),
}

/// Opens an HTTP/1.1 connection for GET requests to `addr`, over TLS with the
/// connector and server name of `tls` if given.
pub async fn connect(
    addr: impl ToSocketAddrs,
    tls: Option<(&TlsConnector, ServerName<'static>)>,
) -> Result<(SendRequest<Empty<Bytes>>, Connection), ConnectError> {
    let tcp = TcpStream::connect(addr).await.map_err(ConnectError::Tcp)?;
    let _ = tcp.set_nodelay(true);
    let io: Box<dyn Io> = match tls {
        Some((connector, name)) => Box::new(
            connector
                .connect(name, tcp)
                .await
                .map_err(ConnectError::Tls)?,
        ),
        None => Box::new(tcp),
    };
    http1::handshake(TokioIo::new(io))
        .await
        .map_err(ConnectError::Http)
}

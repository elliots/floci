//! An HTTP/1.1 client connection over TCP or TLS.

use hyper::body::Body;
use hyper::client::conn::http1::{self, SendRequest};
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_rustls::TlsConnector;

/// The stream under an HTTP connection: TCP, or TLS over TCP.
pub trait Io: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// An HTTP/1.1 connection with request bodies of type `B`, which a task
/// drives while its requests are sent.
pub type Connection<B> = http1::Connection<TokioIo<Box<dyn Io>>, B>;

/// The step of [`connect`] that failed.
#[derive(Debug)]
pub enum ConnectError {
    Tcp(std::io::Error),
    Tls(std::io::Error),
    Http(hyper::Error),
}

/// Opens an HTTP/1.1 connection to `addr`, over TLS with the connector and
/// server name of `tls` if given; `prepare` sets the TCP socket up first.
pub async fn connect<B>(
    addr: impl ToSocketAddrs,
    tls: Option<(&TlsConnector, ServerName<'static>)>,
    prepare: impl FnOnce(&TcpStream),
) -> Result<(SendRequest<B>, Connection<B>), ConnectError>
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let tcp = TcpStream::connect(addr).await.map_err(ConnectError::Tcp)?;
    let _ = tcp.set_nodelay(true);
    prepare(&tcp);
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

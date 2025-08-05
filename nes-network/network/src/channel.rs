use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

pub(crate) struct Channel<R: AsyncRead, W: AsyncWrite> {
    pub(crate) reader: R,
    pub(crate) writer: W,
}

trait Listener {
    type Reader: AsyncRead;
    type Writer: AsyncWrite;
    async fn bind(&mut self);
    async fn listen(&mut self) -> Channel<Self::Reader, Self::Writer>;
}

struct TcpListener {
    listener_port: Option<tokio::net::TcpListener>,
}
impl Listener for TcpListener {
    type Reader = OwnedReadHalf;
    type Writer = OwnedWriteHalf;
    async fn bind(&mut self) {
        self.listener_port = Some(tokio::net::TcpListener::bind("0.0.0.0:9999").await.expect("yo"));
    }
    async fn listen(&mut self) -> Channel<OwnedReadHalf, OwnedWriteHalf> {
        let (stream, _) = self.listener_port.as_ref().expect("yooo").accept().await.expect("yoo");
        let (rx, tx) = stream.into_split();
        Channel {
            reader: rx,
            writer: tx,
        }
    }
}

struct 
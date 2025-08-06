use crate::protocol::ConnectionIdentifier;
use once_cell::sync;
use std::collections::HashMap;
use tokio::io::{ReadHalf, SimplexStream, WriteHalf};

#[derive(Debug)]
pub struct Channel {
    pub read: ReadHalf<SimplexStream>,
    pub write: WriteHalf<SimplexStream>,
}
struct MemCom {
    listening:
        tokio::sync::Mutex<HashMap<ConnectionIdentifier, tokio::sync::oneshot::Sender<Channel>>>,
}

static INSTANCE: sync::Lazy<MemCom> = sync::Lazy::new(|| MemCom {
    listening: tokio::sync::Mutex::new(HashMap::new()),
});

pub async fn memcom_listen(connection_identifier: ConnectionIdentifier) -> Channel {
    INSTANCE.listen(connection_identifier).await
}

pub async fn memcom_connect(connection_identifier: &ConnectionIdentifier) -> Channel {
    INSTANCE.connect(connection_identifier).await
}

impl MemCom {
    async fn listen(&self, connection: ConnectionIdentifier) -> Channel {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.listening.lock().await.insert(connection, tx);
        rx.await.unwrap()
    }

    async fn connect(&self, connection: &ConnectionIdentifier) -> Channel {
        let (client_read, server_write) = tokio::io::simplex(1024 * 1024);
        let (server_read, client_write) = tokio::io::simplex(1024 * 1024);

        let server_channel = Channel {
            read: server_read,
            write: server_write,
        };

        let client_channel = Channel {
            read: client_read,
            write: client_write,
        };

        let mut locked = self.listening.lock().await;
        let Some(channel) = locked.remove(connection) else {
            unimplemented!();
        };
        let _ = channel.send(server_channel);
        client_channel
    }
}

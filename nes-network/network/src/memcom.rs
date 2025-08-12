use crate::protocol::ConnectionIdentifier;
use once_cell::sync;
use std::collections::HashMap;
use tokio::io::{ReadHalf, SimplexStream, WriteHalf};
use tracing::{error, info};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
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

pub async fn memcom_listen(connection_identifier: ConnectionIdentifier) -> Result<Channel> {
    INSTANCE.listen(connection_identifier).await
}

pub async fn memcom_connect(connection_identifier: &ConnectionIdentifier) -> Result<Channel> {
    INSTANCE.connect(connection_identifier).await
}

impl MemCom {
    async fn listen(&self, connection: ConnectionIdentifier) -> Result<Channel> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut locked = self.listening.lock().await;
            match locked.entry(connection.clone()) {
                std::collections::hash_map::Entry::Occupied(_) => return Err("cannot bind".into()),
                std::collections::hash_map::Entry::Vacant(v) => v.insert(tx),
            };
        }
        Ok(rx.await.unwrap())
    }

    async fn connect(&self, connection: &ConnectionIdentifier) -> Result<Channel> {
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
            return Err("unreachable".into());
        };
        let _ = channel.send(server_channel);
        Ok(client_channel)
    }
}

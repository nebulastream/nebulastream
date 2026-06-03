/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

use super::control::*;
use crate::channel::Communication;
use crate::protocol::*;
use crate::receiver::channel::DataQueueItem;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tracing::{Instrument, error, info_span, warn};

pub struct ReceiverChannel {
    queue: async_channel::Receiver<DataQueueItem>,
}

pub enum ReceiverChannelResult {
    Ok(TupleBuffer),
    Closed,
    Error(Error),
}
impl ReceiverChannel {
    pub fn close(&self) {
        self.queue.close();
    }
    pub async fn receive(&self) -> ReceiverChannelResult {
        let data_queue_item = self.queue.recv().await.expect("Should outlive");
        match data_queue_item {
            DataQueueItem::Data(buffer) => ReceiverChannelResult::Ok(buffer),
            DataQueueItem::Error(error) => ReceiverChannelResult::Error(Error::from(error)),
            DataQueueItem::Close => ReceiverChannelResult::Closed,
        }
    }
}

pub struct NetworkService<C: Communication> {
    sender: NetworkingServiceController,
    listener: PhantomData<C>,
}

pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;

impl<C: Communication + 'static> NetworkService<C> {
    pub fn start(
        runtime: &Handle,
        connection_addr: ThisConnectionIdentifier,
        communication: C,
    ) -> Arc<NetworkService<C>> {
        let (tx, rx) = async_channel::bounded(10);
        let service = Arc::new(NetworkService {
            sender: tx.clone(),
            listener: Default::default(),
        });

        runtime.spawn(
            {
                let listener = rx;
                let controller = tx;
                let connection_id = connection_addr.clone();
                async move {
                    let control_socket_result = create_control_socket_handler(
                        listener,
                        controller,
                        connection_id,
                        communication,
                    )
                    .await;
                    match control_socket_result {
                        Ok(_) => warn!("Control stopped"),
                        Err(e) => error!("Control stopped with error: {:?}", e),
                    }
                }
            }
            .instrument(info_span!("receiver", this = %connection_addr)),
        );

        service
    }

    pub async fn register_channel(
        self: &Arc<NetworkService<C>>,
        channel: ChannelIdentifier,
        data_queue_size: usize,
    ) -> Result<ReceiverChannel> {
        let (data_queue_sender, data_queue_receiver) = async_channel::bounded(data_queue_size);
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(NetworkServiceControlCommand::RegisterChannel(
                channel,
                data_queue_sender,
                tx,
            ))
            .await
            .map_err(|_| "Networking Service was stopped")?;
        rx.await.map_err(|_| "Networking Service was stopped")?;
        Ok(ReceiverChannel {
            queue: data_queue_receiver,
        })
    }

    pub fn shutdown(self: Arc<NetworkService<C>>) -> Result<()> {
        self.sender.close();
        Ok(())
    }
}

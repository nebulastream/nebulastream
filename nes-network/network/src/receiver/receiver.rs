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

use crate::channel::Communication;
use crate::protocol::*;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tracing::{Instrument, error, info_span, warn};

use super::control::*;
pub struct ReceiverChannel {
    queue: async_channel::Receiver<TupleBuffer>,
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
    pub fn receive(&self) -> ReceiverChannelResult {
        let Ok(buffer) = self.queue.recv_blocking() else {
            return ReceiverChannelResult::Closed;
        };
        ReceiverChannelResult::Ok(buffer)
    }
}

pub struct NetworkService<C: Communication> {
    sender: NetworkingServiceController,
    runtime: Mutex<Option<Runtime>>,
    listener: PhantomData<C>,
}

pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;

impl<C: Communication + 'static> NetworkService<C> {
    pub fn start(
        runtime: Runtime,
        connection_addr: ThisConnectionIdentifier,
        communication: C,
    ) -> Arc<NetworkService<C>> {
        let (tx, rx) = async_channel::bounded(10);
        let service = Arc::new(NetworkService {
            sender: tx.clone(),
            runtime: Mutex::new(Some(runtime)),
            listener: Default::default(),
        });

        service
            .runtime
            .lock()
            .expect("BUG: No one should panic while holding this lock")
            .as_ref()
            .expect("BUG: The service was just started")
            .spawn(
                {
                    let listener = rx;
                    let controller = tx;
                    let connection_id = connection_addr.clone();
                    let communication = communication;
                    async move {
                        let control_socket_result = create_control_socket_handler(
                            listener,
                            controller,
                            connection_id,
                            communication,
                        )
                        .await;
                        match control_socket_result {
                            Ok(_) => {
                                warn!("Control stopped")
                            }
                            Err(e) => {
                                error!("Control stopped with error: {:?}", e);
                            }
                        }
                    }
                }
                .instrument(info_span!("receiver", this = %connection_addr)),
            );

        service
    }

    pub fn register_channel(
        self: &Arc<NetworkService<C>>,
        channel: ChannelIdentifier,
    ) -> Result<ReceiverChannel> {
        let (data_queue_sender, data_queue_receiver) = async_channel::bounded(10);
        let (tx, rx) = oneshot::channel();
        let Ok(_) = self
            .sender
            .send_blocking(NetworkServiceControlCommand::RegisterChannel(
                channel,
                data_queue_sender,
                tx,
            ))
        else {
            return Err("Networking Service was stopped".into());
        };
        rx.blocking_recv()
            .map_err(|_| "Networking Service was stopped")?;
        Ok(ReceiverChannel {
            queue: data_queue_receiver,
        })
    }

    pub fn shutdown(self: Arc<NetworkService<C>>) -> Result<()> {
        self.sender.close();
        let runtime = self
            .runtime
            .lock()
            .expect("BUG: No one should panic while holding this lock")
            .take()
            .ok_or("Networking Service was stopped")?;
        runtime.shutdown_timeout(Duration::from_secs(1));
        Ok(())
    }
}

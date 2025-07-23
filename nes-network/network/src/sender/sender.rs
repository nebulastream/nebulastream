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
use crate::sender::channel::{ChannelControlMessage, ChannelControlQueue};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tracing::{Instrument, debug, info_span};

pub struct SenderChannel {
    queue: ChannelControlQueue,
}

pub enum TrySendDataResult {
    Ok,
    Full(TupleBuffer),
    Closed(TupleBuffer),
}
impl SenderChannel {
    pub fn try_send_data(&self, buffer: TupleBuffer) -> TrySendDataResult {
        let result = self.queue.try_send(ChannelControlMessage::Data(buffer));
        match result {
            Ok(()) => TrySendDataResult::Ok,
            Err(async_channel::TrySendError::Full(ChannelControlMessage::Data(buffer))) => {
                TrySendDataResult::Full(buffer)
            }
            Err(async_channel::TrySendError::Closed(ChannelControlMessage::Data(buffer))) => {
                TrySendDataResult::Closed(buffer)
            }
            _ => unreachable!(),
        }
    }
    pub fn flush(&self) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.queue
            .send_blocking(ChannelControlMessage::Flush(tx))
            .map_err(|_| "Network Service Closed")?;
        rx.blocking_recv()
            .map_err(|_| "Network Service Closed".into())
    }

    pub fn close(self) -> bool {
        self.queue.close()
    }
}

pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub struct NetworkService<L: Communication> {
    runtime: Mutex<Option<Runtime>>,
    controller: NetworkingServiceController,
    communication: PhantomData<L>,
}
impl<L: Communication + 'static> NetworkService<L> {
    pub fn start(
        runtime: Runtime,
        this_connection: ThisConnectionIdentifier,
        communication: L,
    ) -> Arc<NetworkService<L>> {
        let (controller, listener) = async_channel::bounded(5);
        runtime.spawn(
            {
                let this_connection = this_connection.clone();
                let communication = communication.clone();
                async move {
                    debug!("Starting sender network service");
                    debug!(
                        "sender network service stopped: {:?}",
                        network_sender_dispatcher(this_connection, listener, communication).await
                    );
                }
            }
            .instrument(info_span!("sender", this = %this_connection)),
        );

        Arc::new(NetworkService {
            runtime: Mutex::new(Some(runtime)),
            controller,
            communication: Default::default(),
        })
    }

    pub fn register_channel(
        self: &Arc<NetworkService<L>>,
        connection: ConnectionIdentifier,
        channel: ChannelIdentifier,
    ) -> Result<SenderChannel> {
        let (tx, rx) = oneshot::channel();
        let Ok(_) =
            self.controller
                .send_blocking(NetworkingServiceControlMessage::RegisterChannel(
                    connection, channel, tx,
                ))
        else {
            return Err("Network Service Closed".into());
        };

        let internal = rx.blocking_recv().map_err(|_| "Network Service Closed")?;
        Ok(SenderChannel { queue: internal })
    }

    pub fn shutdown(self: Arc<NetworkService<L>>) -> Result<()> {
        let runtime = self
            .runtime
            .lock()
            .expect("BUG: Nothing should panic while holding the lock")
            .take()
            .ok_or("Networking Service was stopped")?;
        runtime.shutdown_timeout(Duration::from_secs(1));
        Ok(())
    }
}

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

use crate::channel::{Channel, Communication};
use crate::protocol::*;
use futures::SinkExt;
use std::collections::{HashMap, VecDeque};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::select;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, debug, info, info_span, trace, warn};

use super::control::*;
pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
enum ChannelHandlerStatus {
    ClosedByOtherSide,
    ClosedBySoftware,
    /// Like ClosedBySoftware, but the ChannelHandler failed to propagate to the other side.
    ClosedBySoftwareButFailedToPropagate(Error),
    Cancelled,
}

async fn create_channel<L: Communication>(
    this_connection: ThisConnectionIdentifier,
    target_identifier: ConnectionIdentifier,
    channel_identifier: ChannelIdentifier,
    communication: L,
) -> Result<(
    DataChannelSenderReader<L::Reader>,
    DataChannelSenderWriter<L::Writer>,
)> {
    let stream = connect(target_identifier, communication).await?;
    let (mut read, mut write) = identification_sender(stream);
    write
        .send(IdentificationRequest::IAmChannel(
            this_connection,
            channel_identifier,
        ))
        .await?;

    let response = read
        .next()
        .await
        .ok_or("Connection closed during identification")??;

    let stream = Channel {
        writer: write.into_inner().into_inner(),
        reader: read.into_inner().into_inner(),
    };

    match response {
        IdentificationResponse::Ok => Ok(data_channel_sender(stream)),
    }
}

const MAX_PENDING_ACKS: usize = 64;

pub(super) enum ChannelControlMessage {
    Data(TupleBuffer),
    Flush(oneshot::Sender<bool>),
}
pub(super) type ChannelControlQueue = async_channel::Sender<ChannelControlMessage>;
pub(super) type ChannelControlQueueListener = async_channel::Receiver<ChannelControlMessage>;
pub(super) struct ChannelHandler<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> {
    cancellation_token: CancellationToken,
    pending_writes: VecDeque<TupleBuffer>,
    wait_for_ack: HashMap<OriginSequenceNumber, TupleBuffer>,
    writer: DataChannelSenderWriter<W>,
    reader: DataChannelSenderReader<R>,
    queue: ChannelControlQueueListener,
}

enum ErrorOrStatus {
    Error(Error),
    Status(ChannelHandlerStatus),
}
type InternalResult<T> = std::result::Result<T, ErrorOrStatus>;

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> ChannelHandler<R, W> {
    pub fn new(
        cancellation_token: CancellationToken,
        queue: ChannelControlQueueListener,
        reader: DataChannelSenderReader<R>,
        writer: DataChannelSenderWriter<W>,
    ) -> Self {
        Self {
            cancellation_token,
            pending_writes: Default::default(),
            wait_for_ack: Default::default(),
            reader,
            writer,
            queue,
        }
    }
    async fn handle_request(
        &mut self,
        channel_control_message: ChannelControlMessage,
    ) -> InternalResult<()> {
        match channel_control_message {
            ChannelControlMessage::Data(data) => self.pending_writes.push_back(data),
            ChannelControlMessage::Flush(done) => {
                Self::cancellable(&self.cancellation_token, self.writer.flush())
                    .await?
                    .map_err(|e| ErrorOrStatus::Error(e.into()))?;
                let _ = done.send(self.pending_writes.is_empty() && self.wait_for_ack.is_empty());
            }
        }
        Ok(())
    }
    fn handle_response(&mut self, response: DataChannelResponse) -> InternalResult<()> {
        match response {
            DataChannelResponse::Close => {
                info!("Channel Closed by other receiver");
                return Err(ErrorOrStatus::Status(
                    ChannelHandlerStatus::ClosedByOtherSide,
                ));
            }
            DataChannelResponse::NAckData(seq) => {
                if let Some(write) = self.wait_for_ack.remove(&seq) {
                    warn!("NAck for {seq:?}");
                    self.pending_writes.push_back(write);
                } else {
                    return Err(ErrorOrStatus::Error(
                        format!("Protocol Error. Unknown Seq {seq:?}").into(),
                    ));
                }
            }
            DataChannelResponse::AckData(seq) => {
                if self.wait_for_ack.remove(&seq).is_none() {
                    return Err(ErrorOrStatus::Error(
                        format!("Protocol Error. Unknown Seq {seq:?}").into(),
                    ));
                };
                trace!("Ack for {seq:?}");
            }
        }

        Ok(())
    }

    /// Attempts to send the next pending TupleBuffer and adds its sequence number to the wait_for_ack set
    /// If sending fails the buffer is remains in the pending_writes queue
    async fn send_pending(
        cancel_token: &CancellationToken,
        writer: &mut DataChannelSenderWriter<W>,
        pending_writes: &mut VecDeque<TupleBuffer>,
        wait_for_ack: &mut HashMap<OriginSequenceNumber, TupleBuffer>,
    ) -> InternalResult<()> {
        if pending_writes.is_empty() {
            return Ok(());
        }

        let sequence_number = pending_writes
            .front()
            .expect("BUG: check value earlier")
            .sequence();
        trace!("Sending {:?}", sequence_number);
        assert!(
            wait_for_ack
                .insert(
                    sequence_number,
                    pending_writes
                        .pop_front()
                        .expect("BUG: checked value earlier"),
                )
                .is_none(),
            "Logic Error: Sequence Number was already in the wait_for_ack map. This indicates that the same sequence number was send via a single channel."
        );
        let next_buffer = wait_for_ack
            .get(&sequence_number)
            .expect("BUG: value was inserted earlier");

        let Some(result) = cancel_token
            .run_until_cancelled(writer.feed(DataChannelRequest::Data(next_buffer.clone())))
            .await
        else {
            pending_writes.push_front(
                wait_for_ack
                    .remove(&sequence_number)
                    .expect("BUG: value was inserted earlier"),
            );
            return Err(ErrorOrStatus::Status(ChannelHandlerStatus::Cancelled));
        };

        if result.is_err() {
            warn!("Sending {:?} failed", sequence_number);
            pending_writes.push_front(
                wait_for_ack
                    .remove(&sequence_number)
                    .expect("BUG: value was inserted earlier"),
            );
        }

        Ok(())
    }

    async fn cancellable<F: Future>(
        token: &CancellationToken,
        fut: F,
    ) -> InternalResult<F::Output> {
        let Some(r) = token.run_until_cancelled(fut).await else {
            return Err(ErrorOrStatus::Status(ChannelHandlerStatus::Cancelled));
        };
        Ok(r)
    }
    async fn read_from_software(
        cancellation_token: &CancellationToken,
        s: &mut ChannelControlQueueListener,
    ) -> InternalResult<ChannelControlMessage> {
        let Ok(r) = Self::cancellable(cancellation_token, s.recv()).await? else {
            // we don't send the stop here because we don't have access to the writer.
            // the `run` method will eventually fix up this error by sending the `Close`
            return Err(ErrorOrStatus::Status(
                ChannelHandlerStatus::ClosedBySoftwareButFailedToPropagate(
                    "Did not try to propagate".into(),
                ),
            ));
        };
        Ok(r)
    }

    async fn read_from_other_side(
        cancellation_token: &CancellationToken,
        s: &mut DataChannelSenderReader<R>,
    ) -> InternalResult<DataChannelResponse> {
        let Some(r) = Self::cancellable(cancellation_token, s.next()).await? else {
            return Err(ErrorOrStatus::Error("Connection Lost".into()));
        };

        match r {
            Err(e) => Err(ErrorOrStatus::Error(e.into())),
            Ok(r) => Ok(r),
        }
    }

    async fn run_internal(&mut self) -> core::result::Result<(), ErrorOrStatus> {
        loop {
            if self.pending_writes.is_empty() || self.wait_for_ack.len() >= MAX_PENDING_ACKS {
                select! {
                    response = Self::read_from_other_side(&self.cancellation_token, &mut self.reader) => self.handle_response(response?)?,
                    request = Self::read_from_software(&self.cancellation_token, &mut self.queue) => self.handle_request(request?).await?,
                }
            } else {
                select! {
                    response = Self::read_from_other_side(&self.cancellation_token, &mut self.reader) => self.handle_response(response?)?,
                    request = Self::read_from_software(&self.cancellation_token, &mut self.queue) => self.handle_request(request?).await?,
                    send_result = Self::send_pending(&self.cancellation_token, &mut self.writer, &mut self.pending_writes, &mut self.wait_for_ack) => send_result?,
                }
            }
        }
    }
    async fn run(&mut self) -> Result<ChannelHandlerStatus> {
        let status = match self
            .run_internal()
            .await
            .expect_err("Handler should have not terminated by its own.")
        {
            ErrorOrStatus::Error(e) => return Err(e),
            ErrorOrStatus::Status(status) => status,
        };

        // Fixup the ClosedBySoftwareButFailedToPropagate created in read_from_software
        if let ChannelHandlerStatus::ClosedBySoftwareButFailedToPropagate(_) = status {
            let Some(result) = self
                .cancellation_token
                .run_until_cancelled(self.writer.send(DataChannelRequest::Close))
                .await
            else {
                return Ok(ChannelHandlerStatus::Cancelled);
            };
            if let Err(e) = result {
                return Ok(ChannelHandlerStatus::ClosedBySoftwareButFailedToPropagate(
                    e.into(),
                ));
            };
            return Ok(ChannelHandlerStatus::ClosedBySoftware);
        }

        Ok(status)
    }
}

async fn channel_handler(
    cancellation_token: CancellationToken,
    this_connection: ThisConnectionIdentifier,
    target_connection: ConnectionIdentifier,
    channel_identifier: ChannelIdentifier,
    queue: ChannelControlQueueListener,
    communication: impl Communication,
) -> Result<ChannelHandlerStatus> {
    debug!("Channel negotiated. Connecting to {target_connection} on channel {channel_identifier}");
    let (reader, writer) = match create_channel(
        this_connection,
        target_connection.clone(),
        channel_identifier,
        communication,
    )
    .await
    {
        Ok((reader, writer)) => (reader, writer),
        Err(e) => {
            return Err(format!("Could not create channel to {target_connection}: {e:?}").into());
        }
    };

    let mut handler = ChannelHandler::new(cancellation_token, queue, reader, writer);
    handler.run().await
}

pub(super) fn create_channel_handler(
    this_connection: ThisConnectionIdentifier,
    target_connection: ConnectionIdentifier,
    channel: ChannelIdentifier,
    channel_cancellation_token: CancellationToken,
    queue: ChannelControlQueueListener,
    communication: impl Communication + 'static,
    controller: NetworkingConnectionController,
) {
    tokio::spawn(
        {
            let channel = channel.clone();
            let this_connection = this_connection.clone();
            let target_connection = target_connection.clone();
            let channel_cancellation_token = channel_cancellation_token.clone();
            async move {
                let channel_handler_result = channel_handler(
                    channel_cancellation_token.clone(),
                    this_connection,
                    target_connection,
                    channel.clone(),
                    queue.clone(),
                    communication,
                )
                .await;

                let Ok(status) = channel_handler_result else {
                    // If the channel has terminated due to an Error, the channel will be restarted.
                    // It does not really matter if this succeeds or not. If the channel or
                    // controller was terminated, then this channel wouldn't be restarted anyway.
                    let _ = channel_cancellation_token
                        .run_until_cancelled(controller.send(
                            NetworkingConnectionControlMessage::RetryChannel(
                                channel,
                                channel_cancellation_token.clone(),
                                queue,
                            ),
                        ))
                        .await;
                    return;
                };

                match status {
                    ChannelHandlerStatus::ClosedByOtherSide => {
                        info!("Channel Closed by other side.");
                        return;
                    }
                    ChannelHandlerStatus::ClosedBySoftware => {
                        info!("Channel Closed by software.");
                    }
                    ChannelHandlerStatus::ClosedBySoftwareButFailedToPropagate(e) => {
                        info!("Channel Closed by software.");
                        warn!("Failed to propagate ChannelClose to other side due to: {e}");
                    }
                    ChannelHandlerStatus::Cancelled => {
                        info!("Channel Closed by cancellation.");
                        return;
                    }
                }
            }
        }
        .instrument(info_span!(parent: Span::current(),"channel", channel = %channel)),
    );
}

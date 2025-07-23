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
use crate::protocol::*;
use futures::SinkExt;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::select;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, error, info, info_span, trace, warn};
pub(super) type Result<T> = std::result::Result<T, Error>;
pub(super) type Error = Box<dyn std::error::Error + Send + Sync>;

enum ChannelHandlerStatus {
    /// The channel handler has received a ChannelClose message from the other side.
    ClosedByOtherSide,
    /// The channel handler has noticed that the DataQueue has been closed, which indicates that
    /// the software side wants to terminate the connection. The ChannelHandler has propagated this
    /// to the other side.
    ClosedBySoftware,
    /// Like ClosedBySoftware, but the ChannelHandler failed to propagate to the other side.
    ClosedBySoftwareButFailedToPropagate(Error),
    /// The channel handler has been canceled via the CancellationToken, most likely due to
    /// NetworkService shutdown or the control connection stopped.
    Cancelled,
}
pub(super) type DataQueue = async_channel::Sender<TupleBuffer>;

async fn channel_handler<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    cancellation_token: CancellationToken,
    buffer_queue: &mut DataQueue,
    mut connection_reader: DataChannelReceiverReader<R>,
    mut connection_writer: DataChannelReceiverWriter<W>,
) -> Result<ChannelHandlerStatus> {
    let mut pending_buffer: Option<TupleBuffer> = None;
    loop {
        // First: Push received data to the registered channel. The channel handler will not receive
        // further data from the network if the registered channel cannot accept it. This implements
        // backpressure.
        // The `queue` is capable of buffering a limited amount of data, which should reduce the
        // amount of accidental backpressure
        if let Some(pending_buffer) = pending_buffer.take() {
            let sequence = pending_buffer.sequence();
            select! {
                _ = cancellation_token.cancelled() => return Ok(ChannelHandlerStatus::Cancelled),
                write_queue_result = buffer_queue.send(pending_buffer) => {
                    match write_queue_result {
                        Ok(_) => {
                            trace!("accepted data for sequence number {sequence:?}.");
                            // The registered channel has accepted the data, acknowledge the sequence
                            // number.
                            let Some(result) = cancellation_token.run_until_cancelled(connection_writer.send(DataChannelResponse::AckData(sequence))).await else {
                                return Ok(ChannelHandlerStatus::Cancelled);
                            };
                            // TODO: What should we do with the information that the sequence number
                            //       should have been acknowledged?
                            result?
                        },
                        Err(_) => {
                            // The registered channel has closed the `queue`. This implicitly closes
                            // the Channel.
                            let Some(result) = cancellation_token.run_until_cancelled(connection_writer.send(DataChannelResponse::Close)).await else {
                                return Ok(ChannelHandlerStatus::Cancelled);
                            };

                            if let Err(e) = result {
                                // TODO: What should we do if the Close has not been received on the other side.
                                //       If that is the case the other side will attempt to reconnect.
                                //       The Control socket will reject the channel registration. It
                                //       could be worthwhile to investigate if a Tombstone could inform
                                //       the other side that this channel has been permanently closed.
                                return Ok(ChannelHandlerStatus::ClosedBySoftwareButFailedToPropagate(e.into()))
                            }

                            return Ok(ChannelHandlerStatus::ClosedBySoftware);
                        }
                    }
                },
            }
        }

        // If all data has been pushed to the registered channel, the DataChannel waits for new data
        // from the other side.
        select! {
            _ = cancellation_token.cancelled() => return Ok(ChannelHandlerStatus::Cancelled),
            _ = tokio::time::sleep(Duration::from_secs(10)) => {
                warn!("No data received from sender for 10 seconds");
            },
            request = connection_reader.next() => pending_buffer = {
                // Reader next could fail if the connection aborts, in which case the channel fails,
                // but will be retried after a delay. See @create_channel_handler
                match request.ok_or("Connection Lost")?.map_err(|e| e)? {
                    // Received data will be pushed to the registered channel on the next iteration
                    DataChannelRequest::Data(buffer) => {
                        trace!("received data for sequence number {:?}.", buffer.sequence());
                        Some(buffer)
                    },
                    // The other side has closed the channel. This is propagated to the registered
                    // channel by closing the queue, which will interrupt any blocking reads.
                    // Returning `ClosedByOtherSide` will not cause any retries.
                    DataChannelRequest::Close => {
                        return Ok(ChannelHandlerStatus::ClosedByOtherSide);
                    },
                }
            }
        }
    }
}

pub(super) fn create_channel_handler<
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
>(
    channel_id: ChannelIdentifier,
    mut buffer_queue: DataQueue,
    channel_cancellation_token: CancellationToken,
    control: NetworkingServiceController,
) -> oneshot::Sender<(DataChannelReceiverReader<R>, DataChannelReceiverWriter<W>)> {
    let (tx, rx) = oneshot::channel();
    tokio::spawn({
        let channel = channel_id.clone();
        async move {
            // Channel is waiting for connection.
            let channel_opened = channel_cancellation_token.run_until_cancelled(rx).await;

            let Some(channel_opened) = channel_opened else {
                // Channel got canceled
                return;
            };

            let Ok((connection_reader, connection_writer)) = channel_opened else {
                // Channel was closed by the software-side
                return;
            };

            let channel_handler_result = channel_handler(
                channel_cancellation_token.clone(),
                &mut buffer_queue,
                connection_reader,
                connection_writer,
            )
            .await;

            let status = match channel_handler_result {
                Ok(status) => status,
                Err(e) => {
                    error!("Channel Failed: {e}. Retrying");
                    control
                        .send(NetworkServiceControlCommand::RetryChannel(
                            channel,
                            buffer_queue,
                            channel_cancellation_token,
                        ))
                        .await
                        .expect("ReceiverServer should not have closed while a channel is active");
                    return;
                }
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
        .instrument(info_span!(parent: Span::current(), "channel", channel_id = %channel_id))
    });
    tx
}

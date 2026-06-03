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
use std::collections::VecDeque;
use std::convert::Infallible;
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
    /// The channel handler has received a ChannelError message from the other side.
    ClosedByOtherSideWithError(String),
    /// The channel handler has noticed that the DataQueue has been closed, which indicates that
    /// the software side wants to terminate the connection. The ChannelHandler has propagated this
    /// to the other side.
    ClosedBySoftware,
    /// Like ClosedBySoftware, but the ChannelHandler failed to propagate to the other side.
    ClosedBySoftwareButFailedToPropagate(Error),
    /// The channel handler has been canceled via the CancellationToken, most likely due to
    /// NetworkService shutdown or the control connection stopped.
    Cancelled,
    /// The channel handler has received a network error. Not to be confused with the
    /// ClosedByOtherSideWithError, which is an error transmitted via the network but originated on
    /// the other side, instead of the network failing.
    NetworkError(String),
}
pub(super) type DataQueue = async_channel::Sender<DataQueueItem>;
pub(super) enum DataQueueItem {
    Data(TupleBuffer),
    Error(String),
    Close,
}

async fn send_to_other_side<W: AsyncWrite + Unpin>(
    cancellation_token: &CancellationToken,
    connection_writer: &mut DataChannelReceiverWriter<W>,
    response: DataChannelResponse,
) -> core::result::Result<(), ChannelHandlerStatus> {
    let result = cancellation_token
        .run_until_cancelled(connection_writer.send(response))
        .await;

    match result {
        None => Err(ChannelHandlerStatus::Cancelled),
        Some(Ok(_)) => Ok(()),
        Some(Err(e)) => {
            error!("Failed to send data to other side: {e}");
            Err(ChannelHandlerStatus::NetworkError(e.to_string()))
        }
    }
}

async fn read_from_other_side<R: AsyncRead + Unpin>(
    cancellation_token: &CancellationToken,
    connection_reader: &mut DataChannelReceiverReader<R>,
) -> core::result::Result<DataChannelRequest, ChannelHandlerStatus> {
    let result = cancellation_token
        .run_until_cancelled(connection_reader.next())
        .await;

    match result {
        None => Err(ChannelHandlerStatus::Cancelled),
        Some(None) => Err(ChannelHandlerStatus::NetworkError("Connection Lost".into())),
        Some(Some(Err(e))) => Err(ChannelHandlerStatus::NetworkError(
            format!("Connection Error: {e}").into(),
        )),
        Some(Some(Ok(r))) => Ok(r),
    }
}

async fn channel_handler<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    cancellation_token: CancellationToken,
    buffer_queue: &mut DataQueue,
    mut connection_reader: DataChannelReceiverReader<R>,
    mut connection_writer: DataChannelReceiverWriter<W>,
) -> core::result::Result<Infallible, ChannelHandlerStatus> {
    loop {
        let request = read_from_other_side(&cancellation_token, &mut connection_reader).await?;
        match request {
            // Received data will be pushed to the registered channel on the next iteration
            DataChannelRequest::Data(buffer) => {
                info!("received data for sequence number {:?}.", buffer.sequence());
                let sequence = buffer.sequence();
                match buffer_queue.send(DataQueueItem::Data(buffer)).await {
                    Ok(_) => {
                        info!("acked data for sequence number {:?}.", sequence);
                        send_to_other_side(
                            &cancellation_token,
                            &mut connection_writer,
                            DataChannelResponse::AckData(sequence),
                        )
                        .await?;
                    }
                    Err(_) => {
                        send_to_other_side(
                            &cancellation_token,
                            &mut connection_writer,
                            DataChannelResponse::Close,
                        )
                        .await?;
                        return Err(ChannelHandlerStatus::ClosedBySoftware);
                    }
                }
            }
            // The other side has closed the channel. This is propagated to the registered
            // channel by closing the queue, which will interrupt any blocking reads.
            // Returning `ClosedByOtherSide` will not cause any retries.
            DataChannelRequest::Close => {
                buffer_queue.send(DataQueueItem::Close).await.ok();
                return Err(ChannelHandlerStatus::ClosedByOtherSide);
            }
            DataChannelRequest::Error(message) => {
                buffer_queue
                    .send(DataQueueItem::Error(message.clone()))
                    .await
                    .ok();
                return Err(ChannelHandlerStatus::ClosedByOtherSideWithError(message));
            }
        };
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

            match channel_handler_result.unwrap_err() {
                ChannelHandlerStatus::NetworkError(e) => {
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
                ChannelHandlerStatus::ClosedByOtherSide => {
                    info!("Channel Closed by other side.");
                    return;
                }
                ChannelHandlerStatus::ClosedBySoftware => {
                    info!("Channel Closed by software.");
                }
                ChannelHandlerStatus::ClosedByOtherSideWithError(message) => {
                    info!("Channel Closed by other side with error: {message}.");
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

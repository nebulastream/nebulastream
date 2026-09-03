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
use crate::receiver::ReceiverChannelFTOptions;
use crate::receiver::backup::{recover_log, spawn_writer};
use futures::SinkExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::select;
use tokio::sync::oneshot;
use tokio_serde::formats::Cbor;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, error, info, info_span, trace, warn};
use crate::failpoint;

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

struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

async fn channel_handler<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    cancellation_token: CancellationToken,
    buffer_queue: &mut DataQueue,
    ft_options: ReceiverChannelFTOptions,
    closed: &mut bool,
    mut connection_reader: DataChannelReceiverReader<R>,
    mut connection_writer: DataChannelReceiverWriter<W>,
    channel_id: ChannelIdentifier,
) -> Result<ChannelHandlerStatus> {
    let token = CancellationToken::new();
    let _cancel_on_exit = CancelOnDrop(token.clone());

    let (ack_tx, mut ack_rx) = tokio::sync::mpsc::unbounded_channel();
    let (write_tx, write_rx) = tokio::sync::mpsc::channel::<TupleBuffer>(1000);
    let writer_ack_tx = ack_tx.clone();



    if ft_options.enable_backup {
        let mut file = ft_options.backup_path.expect("Backup receivers need a file path");
        file = file.join("log.bin");
        spawn_writer(token.clone(), file, write_rx, writer_ack_tx)
    }

    // TODO is it ok that we forward buffers to NES before writing to log?
    let mut pending_buffer: Option<TupleBuffer> = None;
    loop {
        if let Some(pending_buffer) = pending_buffer.take() {
            let sequence = pending_buffer.sequence();
            select! {
                _ = cancellation_token.cancelled() => {
                    return Ok(ChannelHandlerStatus::Cancelled);
                }

                write_queue_result = async {
                    if pending_buffer.closing {
                        Ok(())
                    } else {
                        buffer_queue.send(pending_buffer.clone()).await
                    }
                } => {
                    match write_queue_result {
                        Ok(_) => {
                            trace!("accepted data for sequence number {sequence:?}.");

                            if ft_options.enable_backup {
                                write_tx
                                    .send(pending_buffer)
                                    .await
                                    .map_err(|_| "writer task stopped")?;
                            } else {
                                ack_tx
                                    .send((
                                        DataChannelResponse::AckData(pending_buffer.sequence()),
                                        pending_buffer.closing,
                                    ))
                                    .map_err(|_| "failed to schedule ack")?;
                            }
                        }

                        Err(_) => {
                            return Ok(ChannelHandlerStatus::ClosedBySoftware);
                        }
                    }
                }
            }
        }

        select! {
            _ = cancellation_token.cancelled() => return Ok(ChannelHandlerStatus::Cancelled),
            // TODO can/should we send ACKs non blocking so that receiving can continue?
            Some((ack_msg, closing)) = ack_rx.recv() => {
                // Send pending ACK other side
                if closing {
                    *closed = true;
                }

                failpoint!("receiver.before_ack");
                if closing{
                    failpoint!("receiver.before_stop_ack");
                }

                let Some(result) = cancellation_token.run_until_cancelled(
                    connection_writer.send(ack_msg)
                ).await else {
                    return Ok(ChannelHandlerStatus::Cancelled);
                };
                result?;
            },
            _ = tokio::time::sleep(Duration::from_secs(10)) => {
                warn!("No data received from sender for 10 seconds");
            },
            request = connection_reader.next() => pending_buffer = {
                // Reader next could fail if the connection aborts, in which case the channel fails,
                // but will be retried after a delay. See @create_channel_handler
                match request.ok_or("Connection Lost")?.map_err(|e| e)? {
                    DataChannelRequest::Data(buffer) => {
                        Some(buffer)
                    },
                    // The other side has closed the channel. This is propagated to the registered
                    // channel by closing the queue, which will interrupt any blocking reads.
                    // Returning `ClosedByOtherSide` will not cause any retries.
                    DataChannelRequest::Close => {
                        // TODO remove
                        panic!("Should never be used");
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
    ft_options: ReceiverChannelFTOptions,
    channel_cancellation_token: CancellationToken,
    control: NetworkingServiceController,
) -> oneshot::Sender<(DataChannelReceiverReader<R>, DataChannelReceiverWriter<W>)> {
    let (tx, rx) = oneshot::channel();
    let cloned_id = channel_id.clone();
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

            let mut close_flag = false;
            let channel_handler_result = channel_handler(
                channel_cancellation_token.clone(),
                &mut buffer_queue,
                ft_options.clone(),
                &mut close_flag,
                connection_reader,
                connection_writer,
                cloned_id,
            )
            .await;

            if close_flag {
                // Propagate stop to application
                buffer_queue.close();
            }

            let status = match channel_handler_result {
                Ok(status) => status,
                Err(e) => {
                    error!("Channel Failed: {e}. Retrying");
                    control
                        .send(NetworkServiceControlCommand::RetryChannel(
                            channel,
                            buffer_queue,
                            ft_options,
                            close_flag,
                            channel_cancellation_token,
                        ))
                        .await
                        .expect("ReceiverServer should not have closed while a channel is active");
                    return;
                }
            };

            match status {
                // TODO
                ChannelHandlerStatus::ClosedByOtherSide => {
                    info!("Channel Closed by other side.");
                    control
                        .send(NetworkServiceControlCommand::RetryChannel(
                            channel,
                            buffer_queue,
                            ft_options,
                            close_flag,
                            channel_cancellation_token,
                        ))
                        .await
                        .expect("ReceiverServer should not have closed while a channel is active");
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

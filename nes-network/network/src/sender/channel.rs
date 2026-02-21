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
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::select;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, debug, info, info_span, trace, warn};

use super::control::*;
pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
/// Status returned when the channel handler terminates.
enum ChannelHandlerStatus {
    /// The receiver sent a Close message, initiating shutdown.
    ClosedByOtherSide,
    /// The local software closed the channel (via `SenderChannel::close()`),
    /// and the Close message was successfully sent to the receiver.
    ClosedBySoftware,
    /// The local software closed the channel, but sending the Close message
    /// to the receiver failed. This can happen if the network connection was
    /// already broken when attempting to propagate the close signal.
    ClosedBySoftwareButFailedToPropagate(Error),
    /// The channel was cancelled via the cancellation token.
    Cancelled,
}

/// Establishes a network connection and performs channel identification handshake.
///
/// This function creates a raw network connection, performs the identification
/// protocol to register this channel with the receiver, and upgrades the
/// connection to a framed data channel.
///
/// # Protocol Flow
///
/// 1. Connect to target using the communication layer
/// 2. Send `IAmChannel` identification request with channel identifier
/// 3. Wait for `Ok` response from receiver
/// 4. Upgrade to data channel protocol for sending/receiving data and acknowledgments
///
/// # Returns
///
/// A tuple of (reader, writer) for the data channel, ready for transmission.
async fn create_channel<C: Communication>(
    this_connection: ThisConnectionIdentifier,
    target_identifier: ConnectionIdentifier,
    channel_identifier: ChannelIdentifier,
    communication: C,
) -> Result<(
    DataChannelSenderReader<C::Reader>,
    DataChannelSenderWriter<C::Writer>,
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

/// Commands sent from `SenderChannel` to the channel handler task.
pub(super) enum ChannelCommand {
    /// Send a data buffer through the channel.
    Data(TupleBuffer),
    /// Flush the network writer and report whether all data has been acknowledged.
    /// The boolean sent through the oneshot indicates: true if both pending and
    /// in-flight queues are empty, false otherwise.
    Flush(oneshot::Sender<bool>),
}
pub(super) type ChannelCommandQueue = async_channel::Sender<ChannelCommand>;
pub(super) type ChannelCommandQueueListener = async_channel::Receiver<ChannelCommand>;

/// Core event loop handler for a sender-side data channel.
///
/// `ChannelHandler` implements a reliable, ordered data transmission protocol with
/// acknowledgment-based flow control. It manages three concurrent event sources:
///
/// 1. **Software commands** (`queue`) - Data and flush requests from `SenderChannel`
/// 2. **Network responses** (`reader`) - Acks/Nacks from the `ReceiverChannel`
/// 3. **Pending sends** - Buffers ready to be transmitted to the receiver
///
/// # Flow Control - Sliding Window Protocol
///
/// The handler maintains two queues for flow control:
/// - `pending_writes`: Buffers queued for sending but not yet transmitted
/// - `wait_for_ack`: Buffers that have been sent and are awaiting acknowledgment (max: `MAX_PENDING_ACKS`)
///
/// When `wait_for_ack` reaches `MAX_PENDING_ACKS`, the handler stops sending new
/// buffers until acknowledgments are received, providing network-level backpressure.
///
/// # Reliability - Retry Logic
///
/// - **Ack received**: Buffer is removed from `wait_for_ack` (successful delivery)
/// - **Nack received**: Buffer is moved from `wait_for_ack` back to `pending_writes` for retry
/// - **Send failure**: Buffer is returned to `pending_writes` for retry
///
/// # Lifecycle
///
/// The handler runs until one of these conditions:
/// - Receiver sends a Close message
/// - Software closes the channel via `SenderChannel::close()`
/// - Cancellation token is triggered
/// - Unrecoverable error occurs
pub(super) struct ChannelHandler<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> {
    cancellation_token: CancellationToken,
    pending_writes: VecDeque<TupleBuffer>,
    wait_for_ack: HashMap<OriginSequenceNumber, TupleBuffer>,
    writer: DataChannelSenderWriter<W>,
    reader: DataChannelSenderReader<R>,
    queue: ChannelCommandQueueListener,
    max_pending_acks: usize,
}

enum ErrorOrStatus {
    Error(Error),
    Status(ChannelHandlerStatus),
}
type InternalResult<T> = std::result::Result<T, ErrorOrStatus>;

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> ChannelHandler<R, W> {
    pub fn new(
        cancellation_token: CancellationToken,
        queue: ChannelCommandQueueListener,
        reader: DataChannelSenderReader<R>,
        writer: DataChannelSenderWriter<W>,
        max_pending_acks: usize,
    ) -> Self {
        Self {
            cancellation_token,
            pending_writes: Default::default(),
            wait_for_ack: Default::default(),
            reader,
            writer,
            queue,
            max_pending_acks,
        }
    }
    /// Handles commands from the `SenderChannel` (software side).
    ///
    /// # Commands
    ///
    /// - `Data`: Queues the buffer in `pending_writes` for transmission
    /// - `Flush`: Flushes the network writer and reports queue status
    async fn handle_request(
        &mut self,
        channel_control_message: ChannelCommand,
    ) -> InternalResult<()> {
        match channel_control_message {
            ChannelCommand::Data(data) => self.pending_writes.push_back(data),
            ChannelCommand::Flush(done) => {
                Self::cancellable(&self.cancellation_token, self.writer.flush())
                    .await?
                    .map_err(|e| ErrorOrStatus::Error(e.into()))?;
                let _ = done.send(self.pending_writes.is_empty() && self.wait_for_ack.is_empty());
            }
        }
        Ok(())
    }
    /// Handles responses from the `ReceiverChannel` (network side).
    ///
    /// # Responses
    ///
    /// - `Close`: Receiver initiated shutdown, return `ClosedByOtherSide` status
    /// - `AckData(seq)`: Buffer successfully delivered, remove from `wait_for_ack`
    /// - `NAckData(seq)`: Delivery failed, move buffer from `wait_for_ack` back to `pending_writes` for retry
    ///
    /// # Protocol Errors
    ///
    /// Returns an error if an Ack/Nack is received for an unknown sequence number,
    /// indicating a protocol violation.
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

                static PACKET_ACK_RECEIVED_EVENT: std::sync::OnceLock<ittapi::Event> = std::sync::OnceLock::new();
                let event = PACKET_ACK_RECEIVED_EVENT.get_or_init(|| ittapi::Event::new("PACKET_ACK"));
                let _ = event.start();
                trace!("Ack for {seq:?}");
            }
        }

        Ok(())
    }

    /// Attempts to send the next pending TupleBuffer to the receiver.
    ///
    /// # Behavior
    ///
    /// 1. Clones the front buffer from `pending_writes` and feeds it to the network writer
    /// 2. On success: Moves the buffer from `pending_writes` to `wait_for_ack`
    /// 3. On failure: Buffer stays in `pending_writes` for retry
    ///
    /// # Cancel Safety
    ///
    /// The buffer remains in `pending_writes` until `feed()` succeeds. This is
    /// critical because this future is used inside `select!`, which drops
    /// non-winning branches at `.await` points. If the buffer were moved to
    /// `wait_for_ack` before `feed()`, a drop would strand it there without
    /// ever being sent — the receiver would never ack it.
    /// Attempts to send the next pending buffer to the receiver.
    ///
    /// IMPORTANT: The buffer stays in `pending_writes` until `feed()` succeeds.
    /// This is critical because `select!` can drop this future at any `.await`
    /// point. If we moved the buffer to `wait_for_ack` before `feed()` and the
    /// future was dropped, the buffer would be stranded in `wait_for_ack`
    /// without ever being sent — the receiver would never ack it.
    async fn send_pending(
        cancel_token: &CancellationToken,
        writer: &mut DataChannelSenderWriter<W>,
        pending_writes: &mut VecDeque<TupleBuffer>,
        wait_for_ack: &mut HashMap<OriginSequenceNumber, TupleBuffer>,
    ) -> InternalResult<()> {
        let Some(buffer) = pending_writes.front() else {
            return Ok(());
        };
        let sequence_number = buffer.sequence();
        trace!("Sending {:?}", sequence_number);

        let Some(result) = cancel_token
            .run_until_cancelled(writer.feed(DataChannelRequest::Data(buffer.clone())))
            .await
        else {
            // Cancelled: buffer is still safely in pending_writes
            return Err(ErrorOrStatus::Status(ChannelHandlerStatus::Cancelled));
        };

        match result {
            Ok(()) => {
                // feed() succeeded — now move to wait_for_ack
                let buffer = pending_writes.pop_front().expect("BUG: was front");
                assert!(
                    wait_for_ack.insert(sequence_number, buffer).is_none(),
                    "Logic Error: Sequence Number was already in the wait_for_ack map. This indicates that the same sequence number was sent via a single channel."
                );
            }
            Err(_) => {
                // feed() failed — buffer stays in pending_writes for retry
                warn!("Sending {:?} failed", sequence_number);
            }
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
        s: &mut ChannelCommandQueueListener,
    ) -> InternalResult<ChannelCommand> {
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

    async fn flush_writes(
        cancel_token: &CancellationToken,
        writer: &mut DataChannelSenderWriter<W>,
    ) -> InternalResult<()> {
        if Self::cancellable(cancel_token, writer.flush())
            .await
            .is_err()
        {
            return Err(ErrorOrStatus::Error("Connection Lost".into()));
        };
        Ok(())
    }

    /// Main event loop that multiplexes between software commands, network responses, and pending sends.
    ///
    /// This method implements the core logic of the sliding window protocol. It uses
    /// a single `tokio::select!` with `if` guards to conditionally enable branches
    /// based on the current state:
    ///
    /// - `should_send_pending`: Sends buffered data when `pending_writes` is non-empty
    /// - `should_read_from_software`: Accepts new commands when `wait_for_ack` < `max_pending_acks`
    /// - `should_read_from_other_side`: Reads acks when `wait_for_ack` is non-empty
    ///
    /// When there is no pending data to send, the codec buffer is flushed before
    /// entering `select!` so the receiver can see previously fed data and send acks.
    async fn run_internal(&mut self) -> core::result::Result<(), ErrorOrStatus> {
        loop {
            let should_read_from_software = self.wait_for_ack.len() < self.max_pending_acks;
            let should_read_from_other_side = !self.wait_for_ack.is_empty();
            let should_send_pending = !self.pending_writes.is_empty();

            // When there's nothing left to send, flush the codec buffer so the
            // receiver actually sees the data we fed earlier and can send Acks.
            if !should_send_pending {
                Self::flush_writes(&self.cancellation_token, &mut self.writer).await?;
            }

            select! {
                response = Self::read_from_other_side(&self.cancellation_token, &mut self.reader), if should_read_from_other_side => {
                    self.handle_response(response?)?;
                },
                request = Self::read_from_software(&self.cancellation_token, &mut self.queue), if should_read_from_software => {
                    self.handle_request(request?).await?;
                },
                send_result = Self::send_pending(&self.cancellation_token, &mut self.writer, &mut self.pending_writes, &mut self.wait_for_ack), if should_send_pending => {
                    send_result?;
                },
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    warn!("No progress for 10 seconds (pending: {}, wait_for_ack: {})",
                        self.pending_writes.len(), self.wait_for_ack.len());
                },
            }
        }
    }
    /// Runs the channel handler and performs cleanup when it terminates.
    ///
    /// This method wraps `run_internal` and handles the graceful shutdown sequence.
    /// The internal event loop is designed to always terminate with a status (never
    /// returning `Ok`), so we expect an error and extract the status from it.
    ///
    /// # Close Signal Propagation
    ///
    /// When the software closes the channel (via `SenderChannel::close()`), the
    /// `read_from_software` method detects the closed queue and returns
    /// `ClosedBySoftwareButFailedToPropagate` (it doesn't have access to the writer).
    ///
    /// This method "fixes up" that status by attempting to send a `Close` message
    /// to the receiver, converting the status to either:
    /// - `ClosedBySoftware`: Close signal successfully sent
    /// - `ClosedBySoftwareButFailedToPropagate(e)`: Sending failed (e.g., network down)
    /// - `Cancelled`: Cancellation token triggered during close attempt
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

/// Creates and runs a channel handler for a single data channel.
///
/// This async function establishes a network connection to the target, performs
/// channel identification handshake, and runs the channel handler event loop.
///
/// # Connection Establishment
///
/// 1. Connects to the target connection
/// 2. Sends identification request with channel identifier
/// 3. Waits for acceptance from receiver
/// 4. Upgrades to data channel protocol
///
/// # Returns
///
/// Returns the termination status when the channel closes, or an error if
/// connection/identification fails.
async fn channel_handler(
    pending_channel: PendingChannel,
    this_connection: ThisConnectionIdentifier,
    target_connection: ConnectionIdentifier,
    communication: impl Communication,
) -> Result<ChannelHandlerStatus> {
    debug!(
        "Channel negotiated. Connecting to {target_connection} on channel {}",
        pending_channel.id
    );
    let (reader, writer) = match create_channel(
        this_connection,
        target_connection.clone(),
        pending_channel.id,
        communication,
    )
    .await
    {
        Ok((reader, writer)) => (reader, writer),
        Err(e) => {
            return Err(format!("Could not create channel to {target_connection}: {e:?}").into());
        }
    };

    let mut handler = ChannelHandler::new(
        pending_channel.cancellation,
        pending_channel.queue,
        reader,
        writer,
        pending_channel.max_pending_acks,
    );
    handler.run().await
}

/// Spawns a channel handler task with automatic error recovery.
///
/// This function spawns a background task that runs `channel_handler`. If the
/// handler terminates with an error (not a graceful status), it automatically
/// attempts to re-establish the channel, providing resilience against transient
/// network failures.
///
/// # Error Recovery
///
/// When the channel fails with an error, a `RetryChannel` message is sent to
/// the connection controller, which will spawn a new attempt to establish the
/// channel with exponential backoff.
///
/// # Graceful Termination
///
/// The following statuses do NOT trigger retry:
/// - `ClosedByOtherSide`: Receiver closed the channel
/// - `ClosedBySoftware`: Software called `SenderChannel::close()`
/// - `Cancelled`: Cancellation token was triggered
pub(super) fn create_channel_handler(
    this_connection: ThisConnectionIdentifier,
    target_connection: ConnectionIdentifier,
    pending_channel: PendingChannel,
    communication: impl Communication + 'static,
    controller: NetworkingConnectionController,
) {
    tokio::spawn(
        {
            let this_connection = this_connection.clone();
            let target_connection = target_connection.clone();
            let pending_channel = pending_channel.clone();
            async move {
                let channel_handler_result = channel_handler(
                    pending_channel.clone(),
                    this_connection,
                    target_connection,
                    communication,
                )
                .await;

                let Ok(status) = channel_handler_result else {
                    // If the channel has terminated due to an Error, the channel will be restarted.
                    // It does not really matter if this succeeds or not. If the channel or
                    // controller was terminated, then this channel wouldn't be restarted anyway.
                    let _ = pending_channel
                        .cancellation
                        .clone()
                        .run_until_cancelled(controller.send(
                            NetworkingConnectionControlCommand::RetryChannel(pending_channel),
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
        .instrument(info_span!(parent: Span::current(),"channel", channel = %pending_channel.id)),
    );
}

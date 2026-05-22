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
use std::convert::Infallible;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::select;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tracing::{Instrument, Span, debug, info, info_span, trace, warn};

use super::control::*;
pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
/// Terminal status returned by `ChannelHandler::run`. Every variant is a
/// *graceful* termination: the spawn wrapper in `create_channel_handler`
/// treats all three as "do not retry". Hard errors are returned via the
/// `Err` arm of `run` and trigger retry separately.
enum ChannelHandlerStatus {
    /// Receiver sent `DataChannelResponse::Close`; we acked it by exiting.
    ClosedByOtherSide,
    /// Local software triggered termination via one of:
    /// - `SenderChannel::close()` — explicit graceful shutdown
    /// - `SenderChannel::error()` — propagate an error to the receiver
    /// - dropping the `SenderChannel` without calling either (observed by
    ///   the handler as a `Cancellation` on the recv channel)
    /// In each case the wire-level termination message (`Close` or `Error`)
    /// was successfully sent and, where applicable, the software-facing
    /// `done` oneshot was acknowledged.
    ClosedBySoftware,
    /// Same trigger as `ClosedBySoftware`, but the wire-level termination
    /// message could not be sent — typically because the network connection
    /// was already broken. The software-facing `done` oneshot (if any) has
    /// been signalled with `false`.
    ClosedBySoftwareButFailedToPropagate(Error),
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

/// Non-terminal commands carried on the bounded data queue from
/// `SenderChannel` to the handler. Both variants are processed in FIFO
/// order; backpressure is applied by the bounded channel's capacity.
enum ChannelData {
    /// Enqueue a buffer for transmission. The handler appends to
    /// `pending_writes` and feeds it to the wire as soon as flow-control
    /// allows.
    Data(TupleBuffer),
    /// Resolve `done` once every buffer submitted before this command has
    /// been delivered and acknowledged. While a flush is pending, the
    /// handler stops reading further `ChannelData` commands; it continues
    /// to drain `pending_writes` and `wait_for_ack` until both are empty,
    /// at which point `done` is signalled and reads resume.
    Flush(oneshot::Sender<()>),
}

/// Terminal commands carried on the one-shot termination channel. Mutually
/// exclusive with each other (oneshot consumes the sender) and with any
/// further `ChannelData` traffic (since `close`/`error` consume `self`).
enum ChannelTermination {
    /// Graceful close: send `DataChannelRequest::Close` on the wire, then
    /// signal `done` with whether propagation succeeded.
    Close(oneshot::Sender<bool>),
    /// Propagate an error string to the receiver as
    /// `DataChannelRequest::Error(msg)`, then signal `done` with whether
    /// propagation succeeded.
    Error(String, oneshot::Sender<bool>),
}

/// Unified event type produced by `SenderChannelListener::recv()`.
enum ChannelCommand {
    /// A queued data-path command (Data or Flush).
    Data(ChannelData),
    /// A termination command (Close or Error).
    Terminate(ChannelTermination),
    /// The `SenderChannel` was dropped without invoking either `close()` or
    /// `error()`. Surfaces as either the data queue being closed with no
    /// senders, or the termination oneshot's sender being dropped without
    /// sending. The handler treats this as an implicit graceful close.
    Cancellation,
}

/// A handle to a registered network channel for sending tuple buffers.
///
/// `SenderChannel` represents an active data channel to a specific
/// `ReceiverChannel`. Each channel maintains its own dedicated network
/// connection with independent backpressure and flow control.
///
/// The handle is split into two communication paths to the handler:
/// - `queue`: a bounded async channel for the data path (`Data`, `Flush`).
///   Backpressure is applied by parking `send_data`/`flush` when full.
/// - `termination`: a oneshot for terminal commands (`Close`, `Error`).
///   Consuming the oneshot consumes `self`, so termination is exactly-once
///   and mutually exclusive with any subsequent data send.
///
/// Dropping `SenderChannel` without calling `close()` or `error()` is a
/// supported path: the handler observes the drop as a `Cancellation` and
/// performs an implicit graceful close.
pub struct SenderChannel {
    queue: ChannelCommandQueue,
    termination: tokio::sync::oneshot::Sender<ChannelTermination>,
}

/// Handler-side counterpart to `SenderChannel`. Multiplexes the data queue
/// and the termination oneshot into a single `ChannelCommand` stream via
/// `recv`.
pub(crate) struct SenderChannelListener {
    queue: ChannelCommandQueueListener,
    termination: tokio::sync::oneshot::Receiver<ChannelTermination>,
}

impl SenderChannelListener {
    /// Awaits the next `ChannelCommand`.
    ///
    /// Biased select: termination wins ties so that a Close issued while
    /// the data queue is also ready is observed without an extra trip
    /// through the data path. The oneshot is polled by `&mut` so the
    /// receiver is not consumed across calls; the queue's `recv` is
    /// cancel-safe (the buffered item stays in the queue if the select's
    /// other branch fires first).
    ///
    /// A `Cancellation` is produced when either side of `SenderChannel`
    /// has been dropped (oneshot sender gone, or all queue senders gone
    /// with the queue empty).
    async fn recv(&mut self) -> ChannelCommand {
        select! {
            biased;
            termination = &mut self.termination => {
                match termination {
                    Ok(termination) => ChannelCommand::Terminate(termination),
                    Err(_) => ChannelCommand::Cancellation,
                }
            }
            data = self.queue.recv() => {
                match data {
                    Ok(data) => ChannelCommand::Data(data),
                    Err(_) => ChannelCommand::Cancellation,
                }
            }
        }
    }
}

impl SenderChannel {
    pub(crate) fn new(capacity: usize) -> (Self, SenderChannelListener) {
        let (sender, queue) = async_channel::bounded(capacity);
        let (termination_sender, termination_receiver) = oneshot::channel();
        (
            SenderChannel {
                queue: sender,
                termination: termination_sender,
            },
            SenderChannelListener {
                queue,
                termination: termination_receiver,
            },
        )
    }
    /// Sends a tuple buffer through this channel, awaiting queue capacity.
    ///
    /// Returns `Err(buffer)` if the data queue has been closed (handler
    /// terminated and dropped the receiver). The second match arm is
    /// `unreachable!()` because `send_data` only ever submits `Data`
    /// variants — `async_channel::SendError` echoes back the item that
    /// failed, which can only be `Data`.
    pub async fn send_data(&self, buffer: TupleBuffer) -> std::result::Result<(), TupleBuffer> {
        match self.queue.send(ChannelData::Data(buffer)).await {
            Ok(()) => Ok(()),
            Err(async_channel::SendError(ChannelData::Data(buffer))) => Err(buffer),
            Err(_) => unreachable!("send_data only submits ChannelData::Data variants"),
        }
    }

    /// Drains the channel and resolves once the receiver has acknowledged
    /// every buffer submitted before this call.
    ///
    /// Sends a `Flush` command to the channel handler. While the flush is
    /// pending, the handler stops reading further commands from the queue and
    /// only resumes once both `pending_writes` and `wait_for_ack` are empty —
    /// i.e. once every preceding buffer has been delivered and acked.
    ///
    /// # Returns
    ///
    /// - `Ok(())`: every buffer submitted before this call has been acked.
    /// - `Err(_)`: the channel handler is no longer running (network service
    ///   shut down, handler terminated due to a network error, cancellation,
    ///   or remote close). Delivery of the in-flight buffers could not be
    ///   confirmed.
    ///
    /// Concurrent callers are serialised by the FIFO command queue: each
    /// `flush` resolves only after every buffer that was already in the queue
    /// at the time its own `Flush` command was dequeued has been acked.
    pub async fn flush(&self) -> crate::sender::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.queue
            .send(ChannelData::Flush(tx))
            .await
            .map_err(|_| "Network Service Closed")?;
        rx.await.map_err(|_| "Network Service Closed".into())
    }

    /// Closes this channel and propagates a `Close` to the `ReceiverChannel`.
    ///
    /// Consumes `self`: the data queue's sender is dropped, and the
    /// termination oneshot is fired. The handler completes any work
    /// already dequeued (including a pending flush) only insofar as the
    /// outer event loop reaches it before observing the close — pending
    /// data still sitting in `pending_writes` is *not* drained before the
    /// wire `Close` is sent.
    ///
    /// # Returns
    ///
    /// - `Ok(true)`: the handler sent `DataChannelRequest::Close` on the
    ///   wire successfully.
    /// - `Ok(false)`: the close was observed, but propagation to the
    ///   receiver failed (e.g. the network connection was already broken).
    /// - `Err(_)`: the channel handler is no longer running, so the close
    ///   was never observed.
    pub async fn close(self) -> crate::sender::Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.termination
            .send(ChannelTermination::Close(tx))
            .map_err(|_| "Network Service Closed")?;
        Ok(rx.await.map_err(|_| "Network Service Closed")?)
    }

    /// Propagates an error string to the receiver and terminates the channel.
    ///
    /// Consumes `self`. The handler sends `DataChannelRequest::Error(message)`
    /// on the wire. Otherwise identical to `close()` in lifecycle: any
    /// buffers still in `pending_writes` are not drained first.
    ///
    /// # Returns
    ///
    /// - `Ok(true)`: the error message was sent on the wire successfully.
    /// - `Ok(false)`: propagation to the receiver failed.
    /// - `Err(_)`: the handler is no longer running.
    pub async fn error(self, message: String) -> crate::sender::Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.termination
            .send(ChannelTermination::Error(message, tx))
            .map_err(|_| "Network Service Closed")?;
        rx.await.map_err(|_| "Network Service Closed".into())
    }
}

pub(super) type ChannelCommandQueue = async_channel::Sender<ChannelData>;
pub(super) type ChannelCommandQueueListener = async_channel::Receiver<ChannelData>;

/// Core event loop handler for a sender-side data channel.
///
/// `ChannelHandler` implements a reliable, ordered data transmission protocol with
/// acknowledgment-based flow control. It manages three concurrent event sources:
///
/// 1. **Software commands** (`queue: SenderChannelListener`) — Data, Flush,
///    Close, Error, and implicit Cancellation
/// 2. **Network responses** (`reader`) — Acks/Nacks/Close from the `ReceiverChannel`
/// 3. **Pending sends** — buffers ready to be transmitted to the receiver
///
/// # Flow Control - Sliding Window Protocol
///
/// The handler maintains two queues for flow control:
/// - `pending_writes`: buffers queued for sending but not yet transmitted
/// - `wait_for_ack`: buffers that have been sent and are awaiting acknowledgment
///   (max: `config.max_pending_acks`)
///
/// When `wait_for_ack` reaches `max_pending_acks`, the handler stops reading
/// new commands from the software queue until acks drain the window, providing
/// network-level backpressure.
///
/// # Reliability - Retry Logic
///
/// - **Ack received**: buffer is removed from `wait_for_ack` (successful delivery)
/// - **Nack received**: buffer is moved from `wait_for_ack` back to `pending_writes` for retry
/// - **Send failure**: buffer stays in `pending_writes` for retry (cancel-safe)
///
/// # Lifecycle
///
/// The handler runs until exactly one of:
/// - the receiver sends `DataChannelResponse::Close` → `ClosedByOtherSide`
/// - the software issues `Close` / `Error` / drops the `SenderChannel` →
///   `ClosedBySoftware[ButFailedToPropagate]`
/// - an unrecoverable network or protocol error → `Err`, triggers retry
pub(super) struct ChannelHandler<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> {
    pending_writes: VecDeque<TupleBuffer>,
    wait_for_ack: HashMap<OriginSequenceNumber, TupleBuffer>,
    /// When `Some`, a terminal command (Close/Error/Cancellation) has been
    /// dequeued. The main loop drains this at the top of the next iteration
    /// by returning the matching `ErrorOrStatus` variant. Once observed,
    /// `run_internal` does not return to the event loop.
    pending_close: Option<PendingClose>,
    /// When `Some`, a flush is in progress: command-queue reads are paused
    /// until both `pending_writes` and `wait_for_ack` drain, at which point
    /// the waker is signalled and reads resume.
    pending_flush: Option<oneshot::Sender<()>>,
    writer: DataChannelSenderWriter<W>,
    reader: DataChannelSenderReader<R>,
    queue: SenderChannelListener,
    config: ChannelConfig,
}

/// Internal latch for "a terminal command has been received, exit on the
/// next iteration". `Cancellation` is represented directly by an
/// `ErrorOrStatus::ClosedBySoftwareCancellation` returned from
/// `handle_request` rather than going through this enum, because it has no
/// software-facing `done` oneshot to reply on.
enum PendingClose {
    Error(String, oneshot::Sender<bool>),
    Graceful(oneshot::Sender<bool>),
}

/// Internal return type used by `run_internal` to communicate why the event
/// loop is exiting. Every variant except `Error` corresponds 1:1 to a
/// software-initiated termination path; `Error` covers wire-level failures
/// that will trigger a retry one layer up.
enum ErrorOrStatus {
    /// Receiver sent `DataChannelResponse::Close`.
    ClosedByOtherSide,
    /// `SenderChannel::close()` was invoked; `done` must be signalled with
    /// the propagation result once the wire `Close` has been attempted.
    ClosedBySoftware(oneshot::Sender<bool>),
    /// `SenderChannel` was dropped without calling `close`/`error`. No
    /// software-facing oneshot to reply on; `run` still attempts to
    /// propagate a wire `Close` so the receiver sees a clean shutdown.
    ClosedBySoftwareCancellation,
    /// `SenderChannel::error(msg)` was invoked; `done` must be signalled
    /// with the propagation result.
    ClosedBySoftwareWithError(String, oneshot::Sender<bool>),
    /// Wire/protocol failure. The spawn wrapper translates this into a
    /// `RetryChannel` request to the connection controller.
    Error(Error),
}
type InternalResult<T> = std::result::Result<T, ErrorOrStatus>;

#[derive(Clone)]
pub struct ChannelConfig {
    pub max_pending_acks: usize,
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> ChannelHandler<R, W> {
    pub fn new(
        queue: SenderChannelListener,
        reader: DataChannelSenderReader<R>,
        writer: DataChannelSenderWriter<W>,
        config: ChannelConfig,
    ) -> Self {
        Self {
            pending_writes: Default::default(),
            wait_for_ack: Default::default(),
            pending_flush: None,
            pending_close: None,
            reader,
            writer,
            queue,
            config,
        }
    }
    /// Dispatches a `ChannelCommand` received from `SenderChannelListener`.
    ///
    /// - `Data(Data)`: append to `pending_writes` for transmission.
    /// - `Data(Flush)`: stash `done` in `pending_flush`. The outer loop
    ///   stops reading further commands (`should_read_from_software` gate)
    ///   until `pending_writes` and `wait_for_ack` are both empty, at
    ///   which point `done` is signalled and reads resume.
    /// - `Terminate(Close)` / `Terminate(Error)`: latch the terminal
    ///   command in `pending_close`. The next iteration of `run_internal`
    ///   drains it and exits the loop.
    /// - `Cancellation`: the `SenderChannel` was dropped without
    ///   `close`/`error`. No `done` oneshot exists, so we short-circuit
    ///   the latch and return `ClosedBySoftwareCancellation` directly.
    ///
    /// The `debug_assert!` on `pending_flush` is structurally guaranteed
    /// by the outer `should_read_from_software` gate: this function is
    /// only called when `!flushing`, so a second `Flush` cannot arrive
    /// while the previous one is still pending.
    async fn handle_request(
        &mut self,
        channel_control_message: ChannelCommand,
    ) -> InternalResult<()> {
        match channel_control_message {
            ChannelCommand::Data(ChannelData::Data(data)) => self.pending_writes.push_back(data),
            ChannelCommand::Data(ChannelData::Flush(done)) => {
                debug_assert!(
                    self.pending_flush.is_none(),
                    "BUG: received Flush while another flush is pending — \
                     command-queue reads should be paused while flushing",
                );
                self.pending_flush = Some(done);
            }
            ChannelCommand::Terminate(ChannelTermination::Close(done)) => {
                self.pending_close = Some(PendingClose::Graceful(done))
            }
            ChannelCommand::Terminate(ChannelTermination::Error(message, done)) => {
                self.pending_close = Some(PendingClose::Error(message, done))
            }
            ChannelCommand::Cancellation => {
                return Err(ErrorOrStatus::ClosedBySoftwareCancellation);
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
                return Err(ErrorOrStatus::ClosedByOtherSide);
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

                static PACKET_ACK_RECEIVED_EVENT: std::sync::OnceLock<ittapi::Event> =
                    std::sync::OnceLock::new();
                let event =
                    PACKET_ACK_RECEIVED_EVENT.get_or_init(|| ittapi::Event::new("PACKET_ACK"));
                let _ = event.start();
                trace!("Ack for {seq:?}");
            }
        }

        Ok(())
    }

    /// Feeds the front of `pending_writes` to the network writer and, on
    /// success, moves the buffer into `wait_for_ack` keyed by its sequence
    /// number.
    ///
    /// # Cancel safety
    ///
    /// The buffer remains in `pending_writes` until `feed()` returns `Ok`.
    /// This matters because this future is one branch of the outer
    /// `select!`, and a losing branch is dropped at its `.await` point. If
    /// we moved the buffer into `wait_for_ack` before `feed()` succeeded,
    /// a drop would strand it there without it ever reaching the wire —
    /// the receiver would never ack it.
    ///
    /// Failure of `feed()` is non-fatal: the buffer stays in
    /// `pending_writes` and will be retried on the next iteration. A hard
    /// network error surfaces later via `read_from_other_side` (returning
    /// `Connection Lost`) or `flush_writes`.
    async fn send_pending(
        writer: &mut DataChannelSenderWriter<W>,
        pending_writes: &mut VecDeque<TupleBuffer>,
        wait_for_ack: &mut HashMap<OriginSequenceNumber, TupleBuffer>,
    ) -> InternalResult<()> {
        let Some(buffer) = pending_writes.front() else {
            return Ok(());
        };
        let sequence_number = buffer.sequence();
        trace!("Sending {:?}", sequence_number);

        let result = writer.feed(DataChannelRequest::Data(buffer.clone())).await;

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

    async fn read_from_other_side(
        s: &mut DataChannelSenderReader<R>,
    ) -> InternalResult<DataChannelResponse> {
        let Some(r) = s.next().await else {
            return Err(ErrorOrStatus::Error("Connection Lost".into()));
        };

        match r {
            Err(e) => Err(ErrorOrStatus::Error(e.into())),
            Ok(r) => Ok(r),
        }
    }

    async fn read_from_software(software: &mut SenderChannelListener) -> ChannelCommand {
        software.recv().await
    }

    async fn flush_writes(writer: &mut DataChannelSenderWriter<W>) -> InternalResult<()> {
        if writer.flush().await.is_err() {
            return Err(ErrorOrStatus::Error("Connection Lost".into()));
        };
        Ok(())
    }

    /// Main event loop multiplexing software commands, network responses,
    /// and pending sends.
    ///
    /// Implements the sliding-window protocol via a single `select!` with
    /// `if` guards that conditionally enable branches based on state:
    ///
    /// - `should_send_pending`: send buffered data when `pending_writes` is non-empty.
    /// - `should_read_from_software`: accept new commands when
    ///   `wait_for_ack.len() < max_pending_acks` *and* no flush is pending.
    ///   This combined gate also means termination (Close/Error/Cancellation)
    ///   is not observed while a flush is in flight — see `recv` for the
    ///   biased ordering inside the gate.
    /// - `should_read_from_other_side`: read acks when `wait_for_ack` is non-empty.
    ///
    /// When there is nothing left to send, the codec buffer is flushed
    /// before entering `select!` so the receiver actually sees the data
    /// fed earlier and can send acks.
    ///
    /// # Flush handling
    ///
    /// On `Flush`, the `done` oneshot is stashed in `pending_flush` and
    /// command-queue reads are paused. The loop continues to drain
    /// `pending_writes` (via `send_pending`) and `wait_for_ack` (via
    /// incoming acks). Once both are empty, `done` is signalled at the
    /// top of the next iteration and reads resume.
    ///
    /// # Termination
    ///
    /// Terminal commands set `pending_close` (or, for Cancellation, return
    /// `ClosedBySoftwareCancellation` immediately). At the top of each
    /// iteration `pending_close` is consumed and the loop exits with the
    /// matching `ErrorOrStatus`.
    async fn run_internal(&mut self) -> ErrorOrStatus {
        let outcome: InternalResult<Infallible> = async {
            loop {
                if let Some(pending_close) = self.pending_close.take() {
                    return match pending_close {
                        PendingClose::Error(msg, done) => {
                            Err(ErrorOrStatus::ClosedBySoftwareWithError(msg, done))
                        }
                        PendingClose::Graceful(done) => Err(ErrorOrStatus::ClosedBySoftware(done)),
                    };
                }

                // Resolve any pending flush as soon as the channel is fully drained.
                if self.pending_flush.is_some()
                    && self.pending_writes.is_empty()
                    && self.wait_for_ack.is_empty()
                {
                    let _ = self.pending_flush.take().unwrap().send(());
                }

                let flushing = self.pending_flush.is_some();
                let should_read_from_software =
                    !flushing && self.wait_for_ack.len() < self.config.max_pending_acks;
                let should_read_from_other_side = !self.wait_for_ack.is_empty();
                let should_send_pending = !self.pending_writes.is_empty();

                // When there's nothing left to send, flush the codec buffer so the
                // receiver actually sees the data we fed earlier and can send Acks.
                if !should_send_pending {
                    Self::flush_writes(&mut self.writer).await?;
                }

                select! {
                    response = Self::read_from_other_side(&mut self.reader), if should_read_from_other_side => {
                        self.handle_response(response?)?;
                    },
                    request = Self::read_from_software(&mut self.queue), if should_read_from_software => {
                        self.handle_request(request).await?;
                    },
                    send_result = Self::send_pending(&mut self.writer, &mut self.pending_writes, &mut self.wait_for_ack), if should_send_pending => {
                        send_result?;
                    },
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {
                        warn!("No progress for 10 seconds (pending: {}, wait_for_ack: {}, flushing: {})",
                            self.pending_writes.len(), self.wait_for_ack.len(), flushing);
                    },
                }
            }
        }
        .await;
        match outcome {
            Err(reason) => reason,
            Ok(never) => match never {},
        }
    }
    /// Runs the event loop and performs the wire-level termination
    /// handshake on exit.
    ///
    /// The match maps each terminal `ErrorOrStatus` to its wire-side action:
    ///
    /// | Variant                              | Wire message sent           | Reply to `done`     |
    /// |--------------------------------------|-----------------------------|----------------------|
    /// | `Error(e)`                           | none (giving up writer)     | n/a (no `done`)     |
    /// | `ClosedByOtherSide`                  | none (peer already closed)  | n/a                 |
    /// | `ClosedBySoftware(done)`             | `DataChannelRequest::Close` | `true` on success, `false` on writer error or drop via scopeguard |
    /// | `ClosedBySoftwareWithError(msg,done)`| `DataChannelRequest::Error` | as above            |
    /// | `ClosedBySoftwareCancellation`       | `DataChannelRequest::Close` | n/a (no `done`)     |
    ///
    /// All software-initiated variants collapse to `ChannelHandlerStatus::ClosedBySoftware`
    /// on successful propagation, or `ClosedBySoftwareButFailedToPropagate(e)` on writer
    /// error. The spawn wrapper treats both as graceful and does not retry.
    async fn run(
        mut self,
    ) -> core::result::Result<ChannelHandlerStatus, (SenderChannelListener, Error)> {
        let status = match self.run_internal().await {
            ErrorOrStatus::Error(e) => return Err((self.queue, e)),
            ErrorOrStatus::ClosedByOtherSide => ChannelHandlerStatus::ClosedByOtherSide,
            ErrorOrStatus::ClosedBySoftwareWithError(msg, done) => {
                let mut done = Some(done);
                let error_failed_on_drop = scopeguard::guard((), |_| {
                    let _ = done.take().unwrap().send(false);
                });

                let result = self.writer.send(DataChannelRequest::Error(msg)).await;
                scopeguard::ScopeGuard::into_inner(error_failed_on_drop);
                let done = done.take().unwrap();
                if let Err(e) = result {
                    let _ = done.send(false);
                    ChannelHandlerStatus::ClosedBySoftwareButFailedToPropagate(e.into())
                } else {
                    let _ = done.send(true);
                    ChannelHandlerStatus::ClosedBySoftware
                }
            }
            ErrorOrStatus::ClosedBySoftwareCancellation => {
                info!("Channel closed by software (cancel)");
                let result = self.writer.send(DataChannelRequest::Close).await;
                if let Err(e) = result {
                    ChannelHandlerStatus::ClosedBySoftwareButFailedToPropagate(e.into())
                } else {
                    ChannelHandlerStatus::ClosedBySoftware
                }
            }
            ErrorOrStatus::ClosedBySoftware(done) => {
                let mut done = Some(done);
                let close_failed_on_drop = scopeguard::guard((), |_| {
                    let _ = done.take().unwrap().send(false);
                });
                let result = self.writer.send(DataChannelRequest::Close).await;

                scopeguard::ScopeGuard::into_inner(close_failed_on_drop);
                let done = done.take().unwrap();
                if let Err(e) = result {
                    let _ = done.send(false);
                    ChannelHandlerStatus::ClosedBySoftwareButFailedToPropagate(e.into())
                } else {
                    let _ = done.send(true);
                    ChannelHandlerStatus::ClosedBySoftware
                }
            }
        };

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
) -> core::result::Result<ChannelHandlerStatus, (PendingChannel, Error)> {
    debug!(
        "Channel negotiated. Connecting to {target_connection} on channel {}",
        pending_channel.id
    );
    let (reader, writer) = match create_channel(
        this_connection,
        target_connection.clone(),
        pending_channel.id.clone(),
        communication,
    )
    .await
    {
        Ok((reader, writer)) => (reader, writer),
        Err(e) => {
            return Err((
                pending_channel,
                format!("Could not create channel to {target_connection}: {e:?}").into(),
            ));
        }
    };

    let PendingChannel { queue, id, config } = pending_channel;

    let mut handler = ChannelHandler::new(queue, reader, writer, config.clone());

    handler
        .run()
        .await
        .map_err(|(queue, e)| (PendingChannel { queue, id, config }, e))
}

/// Spawns a channel handler task with automatic error recovery.
///
/// Runs `channel_handler` on a fresh tokio task. If it terminates with an
/// `Err((pending_channel, _))`, a `RetryChannel` message is sent to the
/// connection controller, which re-spawns the establishment attempt with
/// exponential backoff.
///
/// # Graceful termination (no retry)
///
/// All variants of `ChannelHandlerStatus` are considered graceful:
/// - `ClosedByOtherSide`: receiver sent `DataChannelResponse::Close`.
/// - `ClosedBySoftware`: software invoked `close()` / `error()`, or
///   dropped `SenderChannel`; wire termination propagated.
/// - `ClosedBySoftwareButFailedToPropagate(e)`: same trigger, but the
///   wire termination could not be sent (network down). Logged as a
///   warning but still treated as terminal — there is nothing more to
///   do because the channel state is gone software-side.
pub(super) fn create_channel_handler(
    this_connection: ThisConnectionIdentifier,
    target_connection: ConnectionIdentifier,
    pending_channel: PendingChannel,
    communication: impl Communication + 'static,
    controller: NetworkingConnectionController,
) {
    let channel_id = pending_channel.id.clone();
    tokio::spawn(
        {
            let this_connection = this_connection.clone();
            let target_connection = target_connection.clone();
            async move {
                let channel_handler_result = channel_handler(
                    pending_channel,
                    this_connection,
                    target_connection,
                    communication,
                )
                .await;

                let status = match channel_handler_result {
                    Ok(status) => status,
                    Err((pending_channel, error)) => {
                        // If the channel has terminated due to an Error, the channel will be restarted.
                        warn!("Channel failed with error: {error}. Retrying...");

                        // It does not really matter if this succeeds or not. If the channel or
                        // controller was terminated, then this channel wouldn't be restarted anyway.
                        let _ = controller
                            .send(NetworkingConnectionControlCommand::RetryChannel(
                                pending_channel,
                            ))
                            .await;
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
                }
            }
        }
        .instrument(info_span!(parent: Span::current(),"channel", channel = %channel_id)),
    );
}

use super::backup::{LogBackchannelMessage, LogControlMessage, LogManager, WriteRequest, MAX_PENDING_BUFFERS};
use crate::fault_testing::fault_ffi::failpoint;
use crate::protocol::{
    ChannelIdentifier, DataChannelResponse, ThisConnectionIdentifier,
    TupleBuffer,
};
use crate::receiver::channel::DataQueue;
use crate::receiver::gateway::LogControlMessage::{CompleteCheckpoint, FlushAll};
use crate::receiver::gateway::progress::{SNDResult, SNDState};
use crate::receiver::gateway::{OriginId, SequenceNumber};
use async_channel::SendError;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use tokio::sync::mpsc::{
    Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel,
};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use crate::failpoint;
use crate::protocol::DataChannelResponse::AckData;

const CHECKPOINT_TIMEOUT_MILLIS: u64 = 5000;

// Tuple Buffer + ack_tx channel
pub(crate) type TupleBufferAndChannel = (TupleBuffer, UnboundedSender<(DataChannelResponse, bool)>);

pub enum GatewayControlMessage {
    DrainChannels(oneshot::Sender<()>),
    BarrierComplete(OriginId, u64),
}

struct CheckpointingState {
    checkpointing_epoch: u64,
    checkpoint_position: HashMap<OriginId, SequenceNumber>,
    barriers_to_inject: HashMap<OriginId, SequenceNumber>,
    inflight_barriers: HashSet<OriginId>,
}

impl CheckpointingState {
    fn new() -> Self {
        CheckpointingState {
            checkpointing_epoch: 0,
            checkpoint_position: HashMap::new(),
            barriers_to_inject: HashMap::new(),
            inflight_barriers: HashSet::new(),
        }
    }
}

pub struct ReceiverGateway {
    snd: SNDState,
    checkpointing_state: CheckpointingState,
    host: ThisConnectionIdentifier,
    channel_id: ChannelIdentifier,
    log_base: PathBuf,
    log_manager: Option<LogManager>,
    data_queue: DataQueue,
    input_rx: Receiver<TupleBufferAndChannel>,
    writer_tx: Sender<WriteRequest>,
    writer_control_tx: UnboundedSender<LogControlMessage>,
    writer_backchannel_rx: UnboundedReceiver<LogBackchannelMessage>,
    control_rx: UnboundedReceiver<GatewayControlMessage>,
    cancellation_token: CancellationToken,
}

#[derive(Clone)]
pub struct ReceiverGatewayHandle {
    pub input_tx: Sender<TupleBufferAndChannel>,
    pub control_tx: UnboundedSender<GatewayControlMessage>,
}

impl ReceiverGateway {
    pub fn new(
        base_path: PathBuf,
        host: ThisConnectionIdentifier,
        channel_id: ChannelIdentifier,
        data_queue: DataQueue,
        cancellation_token: CancellationToken,
    ) -> (Self, ReceiverGatewayHandle) {
        // TODO different queue size 
        let (input_tx, input_rx) = channel::<TupleBufferAndChannel>(MAX_PENDING_BUFFERS);
        let (control_tx, control_rx) = unbounded_channel::<GatewayControlMessage>();

        let (writer_tx, writer_rx) = channel::<WriteRequest>(MAX_PENDING_BUFFERS);
        let (writer_control_tx, writer_control_rx) = unbounded_channel::<LogControlMessage>();
        let (writer_backchannel_tx, writer_backchannel_rx) =
            unbounded_channel::<LogBackchannelMessage>();
        let manager = LogManager::new(
            base_path.clone(),
            data_queue.clone(),
            cancellation_token.clone(),
            writer_rx,
            writer_control_rx,
            writer_backchannel_tx,
        );

        let handle = ReceiverGatewayHandle {
            input_tx,
            control_tx,
        };

        let gateway = Self {
            snd: SNDState::new(),
            checkpointing_state: CheckpointingState::new(),
            host,
            channel_id,
            log_base: base_path,
            log_manager: Some(manager),
            data_queue,
            input_rx,
            writer_tx,
            writer_control_tx,
            writer_backchannel_rx,
            control_rx,
            cancellation_token,
        };

        (gateway, handle)
    }

    pub async fn start(mut self) -> bool {
        let recovery_res = self
            .log_manager
            .as_mut()
            .expect("")
            .recover(&mut self.snd)
            .await;
        match recovery_res {
            Ok((closed, ongoing_checkpoint)) => {
                if closed {
                    true
                } else {
                    self.log_manager.take().expect("start called twice").start();
                    tokio::spawn(async move { self.run_handler(ongoing_checkpoint).await });
                    false
                }
            }
            Err(e) => {
                panic!("figure out what to do here if it ever happens \n {e}")
            }
        }
    }

    async fn send_to_source(
        &mut self,
        mut buffer: TupleBuffer,
    ) -> Result<(), SendError<TupleBuffer>> {
        if buffer.sequence_number
            >= self
                .checkpointing_state
                .barriers_to_inject
                .get(&buffer.origin_id)
                .copied()
                .unwrap_or(SequenceNumber::MAX)
        {
            let host = self.host.clone();
            let channel_id = self.channel_id.clone();
            let oid = buffer.origin_id;
            let epoch = self.checkpointing_state.checkpointing_epoch;
            buffer
                .barriers
                .push(format!("{host}|{channel_id}|{oid}|{epoch}").to_string());
            //println!("Creating barrier {host}|{channel_id}|{oid}|{epoch}");
            self.checkpointing_state
                .barriers_to_inject
                .remove(&buffer.origin_id);
            self.checkpointing_state.inflight_barriers.insert(oid);
        }

        self.data_queue.send(buffer).await
    }

    async fn run_handler(mut self, ongoing_checkpoint: bool) {
        let checkpoint_timeout = std::time::Duration::from_millis(CHECKPOINT_TIMEOUT_MILLIS);
        let checkpoint_timer = tokio::time::sleep(checkpoint_timeout);
        tokio::pin!(checkpoint_timer);
        let mut checkpoint_in_progress = ongoing_checkpoint;

        loop {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => {
                    break
                },
                _ = &mut checkpoint_timer, if !checkpoint_in_progress => {
                    checkpoint_in_progress = true;
                    //let _ = self.writer_control_tx.send(LogControlMessage::StartCheckpoint);
                }
                maybe_backchannel_msg = self.writer_backchannel_rx.recv() => {
                    let Some(backchannel_msg) = maybe_backchannel_msg else { break };
                    // TODO merge backchannel and control channel. Make LogfileClosed control message
                    match backchannel_msg {
                        LogBackchannelMessage::LogfileClosed(epoch) => {
                            // HWM snapshot of the SND is not strictly in sync with the log but always at least as far ahead
                            let mut hwm_snapshot = self.snd.collect_hwms();
                            for value in hwm_snapshot.values_mut() {
                                *value += 1;
                            }
                            self.checkpointing_state.checkpoint_position = hwm_snapshot.clone();
                            self.checkpointing_state.barriers_to_inject = hwm_snapshot;
                            self.checkpointing_state.checkpointing_epoch = epoch;
                        },
                    }
                }
                maybe_control_msg = self.control_rx.recv() => {
                    let Some(control_msg) = maybe_control_msg else { break };
                    match control_msg {
                        GatewayControlMessage::DrainChannels(on_complete) => {
                            // drain input data queue from old connection
                            while self.input_rx.try_recv().is_ok() {}

                            let (tx, rx) = oneshot::channel();
                            self.writer_control_tx.send(FlushAll(tx));
                            rx.await;

                            on_complete.send(());
                        },
                        GatewayControlMessage::BarrierComplete(oid, for_epoch) => {
                            // epoch guards against late re-transmission of barriers from a previous epoch
                            // TODO right now we assume that there will be at most one network sink for each barrier
                            if for_epoch == self.checkpointing_state.checkpointing_epoch {
                                let was_unfinished = !self.checkpointing_state.inflight_barriers.is_empty();
                                let was_inflight = self.checkpointing_state.inflight_barriers.remove(&oid);
                                if was_unfinished && was_inflight && self.checkpointing_state.inflight_barriers.is_empty(){
                                    // complete checkpoint
                                    self.writer_control_tx.send(CompleteCheckpoint(self.checkpointing_state.checkpoint_position.clone()));
                                    checkpoint_in_progress = false;
                                    checkpoint_timer.as_mut().reset(tokio::time::Instant::now() + checkpoint_timeout);
                                }
                            }
                        }
                    };
                }
                maybe_buffer = self.input_rx.recv() => {
                    let Some((mut buffer, ack_tx)) = maybe_buffer else { break };
                    /*
                    TODO remove 
                    ack_tx.send((AckData(buffer.sequence(), 0), buffer.closing));
                    if !buffer.closing {
                        self.send_to_source(buffer).await;
                    }
                    continue;*/
                    if !buffer.barriers.is_empty(){
                        failpoint!("gateway.before_upstream_barrier_recieve");
                    }
                    buffer.barriers = Vec::new(); // clear barriers from previous regions

                    let snd_res = if !buffer.closing {
                        self.snd.process(buffer)
                    } else {
                        SNDResult::Buffered(buffer, SequenceNumber::MIN) // = persist and ACK
                    };


                    let ok = match snd_res {
                        SNDResult::Buffered(buffer, lwm) => {
                            // persist to disk but dont hand to engine yet
                            self.writer_tx.send(((buffer, ack_tx), lwm)).await;
                            Ok(())
                        },
                        SNDResult::PassThroughSingle(buffer, lwm)  => {
                            let engine_res = self.send_to_source(buffer.clone()).await;
                            match engine_res {
                                Err(e) => Err(e),
                                Ok(_) => {
                                    self.writer_tx.send(((buffer, ack_tx), lwm)).await;
                                    Ok(())
                                }
                            }
                        }
                        SNDResult::PassThroughBuffered(buffer, buffers, lwm) =>  {
                            let engine_res: Result<(), _> = async {
                                for b in buffers {
                                    self.send_to_source(b).await?;
                                }
                                Ok(())
                            }.await;
                            if engine_res.is_ok() {
                                self.writer_tx.send(((buffer, ack_tx), lwm)).await;
                            }
                            engine_res
                        }
                        SNDResult::Deduplicated(buffer) => {
                            // ACK immediately
                            // Cant send current LWM as it may be ahead of the log
                            ack_tx.send((DataChannelResponse::AckData(buffer.sequence(), SequenceNumber::MIN), buffer.closing));
                            Ok(())
                        }
                    };
                    if !ok.is_ok(){
                        self.input_rx.close();
                        self.control_rx.close();
                        // TODO how to handle error?
                        return;
                    }
                }
            }
        }
    }
}

use crate::fault_testing::FaultAction;
use crate::protocol::{DataChannelResponse, OriginSequenceNumber, TupleBuffer};
use crate::receiver::channel::{DataQueue, Error};
use crate::receiver::gateway::gateway::TupleBufferAndChannel;
use crate::receiver::gateway::progress::{SNDResult, SNDState};
use crate::receiver::gateway::{OriginId, SequenceNumber};
use crate::{apply_fault_action, check_disk_fault, deferred_failpoint, failpoint};
use log::warn;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::path::PathBuf;
use tokio::fs::File as TokioFile;
use tokio::fs::OpenOptions as TokioOpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{Receiver, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

// note: it is critical that WriteRequests are never re-ordered, so that the low watermarks in the ACK messages are never ahead of the log
pub(crate) type WriteRequest = (TupleBufferAndChannel, SequenceNumber); // TupleBuffer + ACK channel + SND low-watermark
pub(crate) type Result<T> = std::result::Result<T, Error>;

pub(super) const MAX_PENDING_BUFFERS: usize = 100;
const FLUSH_INTERVAL_MILLIS: u64 = 100;

pub enum LogControlMessage {
    StartCheckpoint,
    CompleteCheckpoint(HashMap<OriginId, SequenceNumber>),
    FlushAll(oneshot::Sender<()>),
}

pub enum LogBackchannelMessage {
    LogfileClosed(u64),
}

pub struct LogManager {
    base_path: PathBuf,
    current_epoch: u64,
    file: Option<TokioFile>,
    pending_acks: Vec<(
        OriginSequenceNumber,
        UnboundedSender<(DataChannelResponse, bool)>,
        SequenceNumber,
        bool,
    )>,
    cancellation_token: CancellationToken,
    writer_rx: Receiver<WriteRequest>,
    writer_control_rx: UnboundedReceiver<LogControlMessage>,
    writer_backchannel_tx: UnboundedSender<LogBackchannelMessage>,
    buffer_queue: DataQueue,
    flush_interval: tokio::time::Interval,
}

impl LogManager {
    pub fn new(
        base_path: PathBuf,
        buffer_queue: DataQueue,
        cancellation_token: CancellationToken,
        writer_rx: Receiver<WriteRequest>,
        writer_control_rx: UnboundedReceiver<LogControlMessage>,
        writer_backchannel_tx: UnboundedSender<LogBackchannelMessage>,
    ) -> Self {
        Self {
            base_path,
            current_epoch: 0,
            file: None,
            pending_acks: Vec::new(),
            cancellation_token,
            writer_rx,
            writer_control_rx,
            writer_backchannel_tx,
            buffer_queue,
            flush_interval: tokio::time::interval(std::time::Duration::from_millis(
                FLUSH_INTERVAL_MILLIS,
            )),
        }
    }

    // Return result: (is_closed, ongoing_checkpoint)
    pub async fn recover(&mut self, snd_state: &mut SNDState) -> Result<(bool, bool)> {
        fs::create_dir_all(&self.base_path)?;

        // Collect all epochs on disk (should currently only be 2)
        let mut epochs = Vec::new();
        for entry in fs::read_dir(&self.base_path)? {
            let entry = entry?;

            if !entry.file_type()?.is_dir() {
                continue;
            }

            let Some(epoch) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<u64>().ok())
            else {
                continue;
            };

            let watermarks = fs::read(entry.path().join("barriers.bin"))
                .ok()
                .and_then(|bytes| serde_cbor::from_slice(&bytes).ok());

            epochs.push((epoch, entry.path(), watermarks));
        }

        epochs.sort_unstable_by_key(|(epoch, _, _)| *epoch);
        assert!(epochs.len() <= 2);

        // Delete epochs that can be discarded (all epochs before the last epoch with watermarks)
        if let Some(start) = epochs
            .iter()
            .rposition(|(_, _, watermarks)| watermarks.is_some())
        {
            for (_, path, _) in epochs.drain(..start) {
                fs::remove_dir_all(path)?;
            }
        }

        // Restart uncompleted checkpoint immediately
        let ongoing_checkpoint = epochs.last().map_or(false, |(_, _, watermarks)| {
            watermarks.is_none() && epochs.len() > 1
        });
        if ongoing_checkpoint {
            let epoch = epochs.last().unwrap().0;
            self.writer_backchannel_tx
                .send(LogBackchannelMessage::LogfileClosed(epoch));
        }

        // Replay epochs in order
        let mut closed = false;
        for (epoch, path, watermarks) in epochs {
            self.current_epoch = epoch;

            if let Some(watermarks) = watermarks {
                snd_state.restore_watermarks(watermarks);
            }

            let log_file = path.join("log.bin");
            if log_file.is_file() {
                closed |= self.recover_epoch(log_file, snd_state).await?;
            }
        }

        Ok((closed, ongoing_checkpoint))
    }

    async fn recover_epoch(&mut self, file: PathBuf, snd_state: &mut SNDState) -> Result<bool> {
        let mut file = match OpenOptions::new().read(true).write(true).open(file) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e.into()),
        };

        let mut closed = false;
        let last_valid_offset = {
            let reader = BufReader::with_capacity(1024 * 1024, &file);
            let mut deserializer = serde_cbor::Deserializer::from_reader(reader);
            let mut last_valid_offset = 0;

            // Loop through logfile and deserialize buffers
            loop {
                let result = TupleBuffer::deserialize(&mut deserializer);

                match result {
                    Ok(buffer) => {
                        last_valid_offset = deserializer.byte_offset();

                        closed = closed || buffer.closing;

                        // We need to reconstruct the SND state from the log
                        let snd_res = if !buffer.closing {
                            snd_state.process(buffer)
                        } else {
                            SNDResult::Deduplicated(buffer) // = ignore
                        };

                        let ok = match snd_res {
                            SNDResult::Buffered(buffer, _) => Ok(()),
                            SNDResult::PassThroughSingle(buffer, _) => {
                                let engine_res = self.buffer_queue.send(buffer).await;
                                engine_res
                            }
                            SNDResult::PassThroughBuffered(_, buffers, _) => {
                                let mut engine_res = Ok(());
                                for b in buffers {
                                    engine_res = self.buffer_queue.send(b).await;
                                    if engine_res.is_err() {
                                        break;
                                    }
                                }
                                engine_res
                            }
                            SNDResult::Deduplicated(buffer) => Ok(()),
                        };

                        if !ok.is_ok() {
                            panic!("huh")
                        }
                    }

                    Err(e) if e.is_eof() => {
                        break;
                    }

                    Err(e) => {
                        panic!("error trying to read backup log \n {e}")
                    }
                }
            }
            last_valid_offset
        };

        // trim file to last *complete* TupleBuffer, truncating possible incomplete writes from a previous crash
        let result = file.set_len(last_valid_offset as u64);

        if let Err(e) = result {
            panic!("error trying to read trim backup log \n {e}")
        }

        Ok(closed)
    }

    pub fn start(mut self) {
        tokio::spawn(async move {
            self.run().await;
        });
    }

    pub async fn run(&mut self) {
        fs::create_dir_all(&self.base_path).expect("failed to create parent dirs");
        self.get_current_logfile().await;

        self.flush_interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        self.flush_interval.tick().await;

        loop {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => {
                    break;
                }
                Some(((buffer, ack_tx), lwm)) = self.writer_rx.recv() => {
                    let sequence = buffer.sequence();

                    // Push write to OS without forcing to disk immediately
                    if let Err(e) = write_buffer_to_disk(&buffer, self.file.as_mut().unwrap()).await {
                        warn!("failed to write buffer {e}");
                        return;
                    }
                    self.pending_acks.push((sequence, ack_tx, lwm, buffer.closing));

                    if self.pending_acks.len() >= MAX_PENDING_BUFFERS {
                        // Trigger 1: max num of pending ACKs reached
                        self.sync_and_ack().await;
                    }
                }

                _ = self.flush_interval.tick() => {
                    // Trigger 2: flush_interval many milliseconds elapsed since last flush
                    self.sync_and_ack().await;
                }

                // process control messages from gateway
                Some(message) = self.writer_control_rx.recv() => {
                    match message {
                        LogControlMessage::StartCheckpoint => {
                            self.sync_and_ack().await;
                            self.current_epoch += 1;
                            self.get_current_logfile().await;
                            let _ = self.writer_backchannel_tx.send(LogBackchannelMessage::LogfileClosed(self.current_epoch));
                        }
                        LogControlMessage::CompleteCheckpoint(barriers) => {
                            failpoint!("backup.before_checkpoint_complete");
                            let mut bytes = serde_cbor::to_vec(&barriers).expect("failed to serialize barriers");
                            let barriers_path = self.base_path.join(self.current_epoch.to_string()).join("barriers.bin");
                            if check_disk_fault!() {
                                break;
                            }

                            if let (Some(action)) = deferred_failpoint!("backup.during_checkpoint_complete") {
                                bytes.truncate(bytes.len() / 2);
                                tokio::fs::write(&barriers_path, bytes).await;
                                assert_eq!(action, FaultAction::Crash);
                                apply_fault_action!(action);
                                break;
                            }

                            if let Err(e) = tokio::fs::write(&barriers_path, bytes).await {
                                panic!("failed to write barriers file {e}");
                            }

                            if self.current_epoch > 0 {
                                let previous_epoch_path = self.base_path.join((self.current_epoch - 1).to_string());
                                failpoint!("backup.before_epoch_delete");
                                if check_disk_fault!() {
                                    break;
                                }
                                if let Err(e) = fs::remove_dir_all(previous_epoch_path) {
                                    warn!("failed to delete old epoch {e}");
                                }
                            }
                            failpoint!("backup.after_checkpoint_complete");

                        }
                        LogControlMessage::FlushAll(on_complete) => {
                            // drain queue to sync log with gateway state
                            while let Ok(((buffer, _), _)) = self.writer_rx.try_recv() {
                                if let Err(e) = write_buffer_to_disk(&buffer,self.file.as_mut().unwrap(),).await {
                                    warn!("failed to write buffer {e}");
                                    return;
                                }
                            }
                            self.sync_and_ack().await;
                            on_complete.send(());
                        }
                    }
                }
            }
        }
    }

    async fn get_current_logfile(&mut self) {
        let dir = self.base_path.join(self.current_epoch.to_string());
        fs::create_dir_all(&dir).expect("failed to create parent dirs");

        let file = match TokioOpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("log.bin"))
            .await
        {
            Ok(file) => file,
            Err(e) => {
                panic!("failed to create file");
            }
        };

        self.file = Some(file);
    }

    async fn sync_and_ack(&mut self) {
        let file = self.file.as_mut().unwrap();
        if let Err(e) = file.flush().await {
            panic!("failed to flush");
        }

        if let Err(e) = file.sync_data().await {
            panic!("failed to sync file");
        }

        for (sequence, ack_tx, lwm, closing) in self.pending_acks.drain(..) {
            if let Err(e) = ack_tx.send((DataChannelResponse::AckData(sequence, lwm), closing)) {
                //println!("failed to send ack");
            }
        }
    }
}

async fn write_buffer_to_disk(
    buffer: &TupleBuffer,
    file: &mut TokioFile,
) -> crate::receiver::channel::Result<()> {
    let mut encoded = serde_cbor::to_vec(buffer)?;

    failpoint!("backup.before_disk_write");
    if buffer.closing {
        failpoint!("backup.before_stop_write");
    }

    if let (Some(action)) = deferred_failpoint!("backup.during_disk_write") {
        encoded.truncate(encoded.len() / 2);
        file.write_all(&encoded).await?;
        file.flush().await;
        file.sync_data().await;
        assert_eq!(action, FaultAction::Crash);
        apply_fault_action!(action);
    }

    if check_disk_fault!() {
        return Err("".into());
    }

    file.write_all(&encoded).await?;
    Ok(())
}

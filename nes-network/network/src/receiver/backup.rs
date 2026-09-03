use std::fs;
use crate::protocol::{DataChannelResponse, OriginSequenceNumber, TupleBuffer};
use crate::receiver::channel::{DataQueue, Error};
use serde::Deserialize;
use std::fs::{OpenOptions};
use std::io::BufReader;
    use std::path::PathBuf;
use futures::future::err;
use log::warn;
use tokio::fs::File as TokioFile;
use tokio::fs::OpenOptions as TokioOpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{Receiver, UnboundedSender};
use tokio_util::sync::CancellationToken;
use crate::{apply_fault_action, check_io, deferred_failpoint, failpoint, fault_testing};
use crate::fault_testing::{FaultAction};

pub(super) type Result<T> = std::result::Result<T, Error>;
    
// TODO move config     
const MAX_PENDING_BUFFERS: usize = 100;
const FLUSH_INTERVAL_MILLIS: u64 = 100;
    

pub async fn recover_log(file: PathBuf, buffer_queue: &mut DataQueue) -> Result<bool> {
    if let Some(parent) = file.parent() {
        fs::create_dir_all(parent).expect("failed to create parent dirs");
    }
    let mut file = match OpenOptions::new()
        .read(true)
        .write(true)
        .open(file)
    {
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
                    //println!("recovered buffer {:?}", buffer.sequence());
                    closed = closed || buffer.closing;
                    if !buffer.closing {
                        buffer_queue.send(buffer).await?;
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


pub fn spawn_writer(
    cancellation_token: CancellationToken,
    file: PathBuf,
    mut write_rx: Receiver<TupleBuffer>,
    writer_ack_tx: UnboundedSender<(DataChannelResponse, bool)>,
) {
    if let Some(parent) = file.parent() {
        fs::create_dir_all(parent).expect("failed to create parent dirs");
    }

    tokio::spawn(async move {

        let mut file = match TokioOpenOptions::new()
            .create(true)
            .append(true)
            .open(file)
            .await
        {
            Ok(file) => file,
            Err(e) => {
                panic!("failed to create file");
            }
        };

        let mut pending_acks = Vec::with_capacity(MAX_PENDING_BUFFERS);

        let mut flush_interval = tokio::time::interval(std::time::Duration::from_millis(FLUSH_INTERVAL_MILLIS));
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        flush_interval.tick().await;

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                }
                Some(buffer) = write_rx.recv() => {
                    let sequence = buffer.sequence();

                    // Push write to OS without forcing to disk immediately
                    if let Err(e) = write_buffer_to_disk(&buffer, &mut file).await {
                        warn!("failed to write buffer {e}");
                        return;
                    }
                    pending_acks.push((sequence, buffer.closing));

                    if pending_acks.len() >= MAX_PENDING_BUFFERS {
                        // Trigger 1: max num of pending ACKs reached
                        sync_and_ack(&mut file, &mut pending_acks, writer_ack_tx.clone()).await;
                    }
                }

                _ = flush_interval.tick() => {
                    // Trigger 2: flush_interval many milliseconds elapsed since last flush
                    sync_and_ack(&mut file, &mut pending_acks, writer_ack_tx.clone()).await;
                }
            }
        }
    });
}

async fn sync_and_ack(
    file: &mut TokioFile,
    pending_acks: &mut Vec<(OriginSequenceNumber, bool)>,
    writer_ack_tx: UnboundedSender<(DataChannelResponse, bool)>,
) {
    if let Err(e) = file.flush().await {
        panic!("failed to flush");
    }

    if let Err(e) = file.sync_data().await {
        panic!("failed to sync file");
    }
    
    for (sequence, closing) in pending_acks.drain(..) {
        if let Err(e) = writer_ack_tx
            .send((DataChannelResponse::AckData(sequence), closing))
        {
            //println!("failed to send ack");
        }
    }
}

async fn write_buffer_to_disk(
    buffer: &TupleBuffer,
    file: &mut TokioFile,
) -> crate::receiver::channel::Result<()> {

    let mut encoded = serde_cbor::to_vec(buffer)?;

    failpoint!("backup.before_disk_write");
    if buffer.closing{
        failpoint!("backup.before_stop_write");
    }

    if let(Some(action)) = deferred_failpoint!("backup.during_disk_write"){
        encoded.truncate(encoded.len()/2);
        file.write_all(&encoded).await?;
        file.flush().await;
        file.sync_data().await;
        assert_eq!(action, FaultAction::Crash);
        apply_fault_action!(action);
    }

    if check_io!(){
        return Err("".into());
    }

    file.write_all(&encoded).await?;
    Ok(())
}

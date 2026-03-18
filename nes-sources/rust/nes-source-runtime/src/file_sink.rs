// AsyncFileSink: Reference sink implementation that writes raw binary data
// from TupleBufferHandle to a file using tokio::fs.
//
// Flushes on Flush and Close messages, not on every Data message.
// This provides the Rust-side foundation that C++ TokioSinkRegistry
// and TokioFileSink factory will call through FFI in Plan 02.

use std::path::PathBuf;

use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::schema::{SchemaField, format_schema_header};
use crate::sink::AsyncSink;
use crate::sink_context::{SinkContext, SinkMessage};
use crate::sink_error::SinkError;

/// Async file sink that writes raw binary data from TupleBufferHandle to a file.
///
/// Opens the file on first run, writes buffer contents on Data messages,
/// flushes on Flush and Close messages, and returns Ok(()) on Close.
pub struct AsyncFileSink {
    path: PathBuf,
    schema: Vec<SchemaField>,
}

impl AsyncFileSink {
    /// Create a new AsyncFileSink that will write to the given path.
    /// If `schema` is non-empty, a formatted schema header is written as the first line.
    pub fn new(path: PathBuf, schema: Vec<SchemaField>) -> Self {
        Self { path, schema }
    }
}

impl AsyncSink for AsyncFileSink {
    async fn run(&mut self, ctx: &SinkContext) -> Result<(), SinkError> {
        let mut file = File::create(&self.path)
            .await
            .map_err(|e| SinkError::new(format!("failed to open file: {}", e)))?;
        if !self.schema.is_empty() {
            let header = format_schema_header(&self.schema);
            file.write_all(header.as_bytes())
                .await
                .map_err(|e| SinkError::new(format!("failed to write schema header: {}", e)))?;
        }
        loop {
            match ctx.recv().await {
                SinkMessage::Data(mut iter) => {
                    while let Some(segment) = iter.next_segment() {
                        file.write_all(segment)
                            .await
                            .map_err(|e| SinkError::new(format!("write failed: {}", e)))?;
                    }
                }
                SinkMessage::Flush => {
                    file.flush()
                        .await
                        .map_err(|e| SinkError::new(format!("flush failed: {}", e)))?;
                }
                SinkMessage::Close => {
                    file.flush()
                        .await
                        .map_err(|e| SinkError::new(format!("final flush failed: {}", e)))?;
                    return Ok(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::TupleBufferHandle;
    use crate::buffer_iterator::BufferIterator;
    use crate::sink_context::SinkMessage;
    use tokio_util::sync::CancellationToken;

    /// Helper: create a SinkContext with a bounded channel and return (sender, ctx).
    fn make_test_ctx(cap: usize) -> (async_channel::Sender<SinkMessage>, SinkContext) {
        let (sender, receiver) = async_channel::bounded(cap);
        let token = CancellationToken::new();
        let ctx = SinkContext::new(receiver, token);
        (sender, ctx)
    }

    /// Helper: create a BufferIterator from test data.
    fn make_iter(data: &[u8]) -> BufferIterator {
        let mut buf = TupleBufferHandle::new_test();
        buf.as_mut_slice()[..data.len()].copy_from_slice(data);
        buf.set_number_of_tuples(data.len() as u64);
        BufferIterator::new(buf)
    }

    #[tokio::test]
    async fn file_sink_writes_raw_binary_data() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("nes_file_sink_test_write_{}", std::process::id()));
        let (sender, ctx) = make_test_ctx(8);

        let mut sink = AsyncFileSink::new(path.clone(), vec![]);
        let handle = tokio::spawn(async move { sink.run(&ctx).await });

        // Send 3 data segments with distinct content via BufferIterator.
        let bytes_per_buf = 8usize;
        for i in 0u64..3 {
            sender.send(SinkMessage::Data(make_iter(&i.to_le_bytes()))).await.unwrap();
        }
        sender.send(SinkMessage::Close).await.unwrap();

        let result = handle.await.unwrap();
        assert!(result.is_ok(), "sink should complete successfully");

        let contents = tokio::fs::read(&path).await.unwrap();
        assert_eq!(contents.len(), 3 * bytes_per_buf, "all 3 segments should be written");

        for i in 0u64..3 {
            let offset = i as usize * bytes_per_buf;
            let val = u64::from_le_bytes(contents[offset..offset + 8].try_into().unwrap());
            assert_eq!(val, i, "segment {} should have correct content", i);
        }

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn file_sink_flushes_on_flush_message() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("nes_file_sink_test_flush_{}", std::process::id()));
        let (sender, ctx) = make_test_ctx(8);

        let mut sink = AsyncFileSink::new(path.clone(), vec![]);
        let handle = tokio::spawn(async move { sink.run(&ctx).await });

        sender.send(SinkMessage::Data(make_iter(&42u32.to_le_bytes()))).await.unwrap();
        sender.send(SinkMessage::Flush).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let contents = tokio::fs::read(&path).await.unwrap();
        assert!(!contents.is_empty(), "data should be visible after Flush");
        let val = u32::from_le_bytes(contents[..4].try_into().unwrap());
        assert_eq!(val, 42, "flushed data should be correct");

        sender.send(SinkMessage::Close).await.unwrap();
        let _ = handle.await;
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn file_sink_flushes_on_close() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("nes_file_sink_test_close_{}", std::process::id()));
        let (sender, ctx) = make_test_ctx(8);

        let mut sink = AsyncFileSink::new(path.clone(), vec![]);
        let handle = tokio::spawn(async move { sink.run(&ctx).await });

        sender.send(SinkMessage::Data(make_iter(&99u32.to_le_bytes()))).await.unwrap();
        sender.send(SinkMessage::Close).await.unwrap();

        let result = handle.await.unwrap();
        assert!(result.is_ok(), "sink should complete successfully on Close");

        let contents = tokio::fs::read(&path).await.unwrap();
        assert!(!contents.is_empty(), "data should be flushed on Close");
        let val = u32::from_le_bytes(contents[..4].try_into().unwrap());
        assert_eq!(val, 99, "close-flushed data should be correct");

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn file_sink_returns_error_on_invalid_path() {
        let path = PathBuf::from("/nonexistent_dir_nes_test/impossible_file.bin");
        let (_sender, ctx) = make_test_ctx(8);

        let mut sink = AsyncFileSink::new(path, vec![]);
        let result = sink.run(&ctx).await;
        assert!(result.is_err(), "sink should return error on invalid path");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("failed to open file"),
            "error message should mention file open failure: {}",
            err_msg
        );
    }
}

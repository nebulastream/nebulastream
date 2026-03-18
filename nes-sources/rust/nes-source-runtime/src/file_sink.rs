// AsyncFileSink: Reference sink implementation that writes raw binary data
// from TupleBufferHandle to a file using tokio::fs.
//
// Flushes on Flush and Close messages, not on every Data message.
// This provides the Rust-side foundation that C++ TokioSinkRegistry
// and TokioFileSink factory will call through FFI in Plan 02.

use std::path::PathBuf;

use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::sink::AsyncSink;
use crate::sink_context::{SinkContext, SinkMessage};
use crate::sink_error::SinkError;

/// Async file sink that writes raw binary data from TupleBufferHandle to a file.
///
/// Opens the file on first run, writes buffer contents on Data messages,
/// flushes on Flush and Close messages, and returns Ok(()) on Close.
pub struct AsyncFileSink {
    path: PathBuf,
}

impl AsyncFileSink {
    /// Create a new AsyncFileSink that will write to the given path.
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl AsyncSink for AsyncFileSink {
    async fn run(&mut self, ctx: &SinkContext) -> Result<(), SinkError> {
        let mut file = File::create(&self.path)
            .await
            .map_err(|e| SinkError::new(format!("failed to open file: {}", e)))?;
        loop {
            match ctx.recv().await {
                SinkMessage::Data(buf) => {
                    file.write_all(buf.as_slice())
                        .await
                        .map_err(|e| SinkError::new(format!("write failed: {}", e)))?;
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
    use crate::sink_context::SinkMessage;
    use tokio_util::sync::CancellationToken;

    /// Helper: create a SinkContext with a bounded channel and return (sender, ctx).
    fn make_test_ctx(cap: usize) -> (async_channel::Sender<SinkMessage>, SinkContext) {
        let (sender, receiver) = async_channel::bounded(cap);
        let token = CancellationToken::new();
        let ctx = SinkContext::new(receiver, token);
        (sender, ctx)
    }

    #[tokio::test]
    async fn file_sink_writes_raw_binary_data() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("nes_file_sink_test_write_{}", std::process::id()));
        let (sender, ctx) = make_test_ctx(8);

        let mut sink = AsyncFileSink::new(path.clone());
        let handle = tokio::spawn(async move { sink.run(&ctx).await });

        // Send 3 data buffers with distinct content
        for i in 0u64..3 {
            let mut buf = TupleBufferHandle::new_test();
            buf.as_mut_slice()[..8].copy_from_slice(&i.to_le_bytes());
            sender.send(SinkMessage::Data(buf)).await.unwrap();
        }
        sender.send(SinkMessage::Close).await.unwrap();

        let result = handle.await.unwrap();
        assert!(result.is_ok(), "sink should complete successfully");

        // Read back and verify all 3 buffers written
        let contents = tokio::fs::read(&path).await.unwrap();
        // Each TupleBufferHandle::new_test() creates a buffer of known size
        let test_buf_size = TupleBufferHandle::new_test().as_slice().len();
        assert_eq!(contents.len(), 3 * test_buf_size, "all 3 buffers should be written");

        // Verify first 8 bytes of each buffer segment
        for i in 0u64..3 {
            let offset = i as usize * test_buf_size;
            let val = u64::from_le_bytes(contents[offset..offset + 8].try_into().unwrap());
            assert_eq!(val, i, "buffer {} should have correct content", i);
        }

        // Cleanup
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn file_sink_flushes_on_flush_message() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("nes_file_sink_test_flush_{}", std::process::id()));
        let (sender, ctx) = make_test_ctx(8);

        let mut sink = AsyncFileSink::new(path.clone());
        let handle = tokio::spawn(async move { sink.run(&ctx).await });

        // Send data, then flush, then verify data is visible on disk
        let mut buf = TupleBufferHandle::new_test();
        buf.as_mut_slice()[..4].copy_from_slice(&42u32.to_le_bytes());
        sender.send(SinkMessage::Data(buf)).await.unwrap();
        sender.send(SinkMessage::Flush).await.unwrap();

        // Give the sink a moment to process
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // After Flush, data should be visible on disk
        let contents = tokio::fs::read(&path).await.unwrap();
        assert!(!contents.is_empty(), "data should be visible after Flush");
        let val = u32::from_le_bytes(contents[..4].try_into().unwrap());
        assert_eq!(val, 42, "flushed data should be correct");

        // Clean up
        sender.send(SinkMessage::Close).await.unwrap();
        let _ = handle.await;
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn file_sink_flushes_on_close() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("nes_file_sink_test_close_{}", std::process::id()));
        let (sender, ctx) = make_test_ctx(8);

        let mut sink = AsyncFileSink::new(path.clone());
        let handle = tokio::spawn(async move { sink.run(&ctx).await });

        // Send data then close (no explicit flush)
        let mut buf = TupleBufferHandle::new_test();
        buf.as_mut_slice()[..4].copy_from_slice(&99u32.to_le_bytes());
        sender.send(SinkMessage::Data(buf)).await.unwrap();
        sender.send(SinkMessage::Close).await.unwrap();

        let result = handle.await.unwrap();
        assert!(result.is_ok(), "sink should complete successfully on Close");

        // After Close, all data should be flushed to disk
        let contents = tokio::fs::read(&path).await.unwrap();
        assert!(!contents.is_empty(), "data should be flushed on Close");
        let val = u32::from_le_bytes(contents[..4].try_into().unwrap());
        assert_eq!(val, 99, "close-flushed data should be correct");

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn file_sink_returns_error_on_invalid_path() {
        // Use a path that cannot be created (directory does not exist)
        let path = PathBuf::from("/nonexistent_dir_nes_test/impossible_file.bin");
        let (_sender, ctx) = make_test_ctx(8);

        let mut sink = AsyncFileSink::new(path);
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

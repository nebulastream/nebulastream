// TokioMqttSource: embedded MQTT broker source for exactly one topic.
//
// Uses the shared singleton broker (via nes_mqtt_broker) that is started on
// the first source/sink registration and stopped when the last one exits.
// Each source gets its own internal link subscribed to its topic — the
// rumqttd router delivers only matching messages, no userspace filtering needed.
//
// External MQTT clients connect to NebulaStream directly and publish data.

use std::collections::HashMap;
use std::time::Duration;

use tokio::time::interval;

use tracing::{debug, trace};

use nes_source_runtime::buffer::TupleBufferHandle;
use nes_source_runtime::config::{ConfigParam, ParamType};
use nes_source_runtime::context::SourceContext;
use nes_source_runtime::error::{SourceError, SourceResult};
use nes_source_runtime::source::AsyncSource;

pub const CONFIG_SCHEMA: &[ConfigParam] = &[
    ConfigParam { name: "mqtt_topic", param_type: ParamType::String, default: None },
    ConfigParam { name: "mqtt_flush_interval_ms", param_type: ParamType::U32, default: Some("100") },
    ConfigParam { name: "mqtt_message_suffix", param_type: ParamType::String, default: Some("") },
];

pub fn create_mqtt_source(config: &HashMap<String, String>) -> Result<MqttSource, String> {
    let topic = config.get("mqtt_topic")
        .ok_or("missing mqtt_topic")?
        .clone();
    let flush_interval_ms = config.get("mqtt_flush_interval_ms")
        .unwrap_or(&"100".to_string())
        .parse::<u64>()
        .map_err(|e| format!("invalid mqtt_flush_interval_ms: {e}"))?;

    // Optional suffix appended after each MQTT message payload (e.g., "\n"
    // so that single-line `mosquitto_pub -m "1,2,3"` messages are properly
    // delimited for the CSV parser). Supports C-style escape sequences.
    let message_suffix = config.get("mqtt_message_suffix")
        .filter(|s| !s.is_empty())
        .map(|s| unescape::unescape(s)
            .map(|unescaped| unescaped.into_bytes())
            .ok_or_else(|| format!("invalid escape sequence in mqtt_message_suffix: {s}")))
        .transpose()?;

    Ok(MqttSource {
        topic,
        flush_interval_ms,
        message_suffix,
    })
}

pub struct MqttSource {
    topic: String,
    flush_interval_ms: u64,
    message_suffix: Option<Vec<u8>>,
}


/// Append `payload` followed by `suffix` into a byte buffer at `cursor`,
/// emitting full buffers via the callback. Returns the new cursor position.
///
/// The `emit_full` callback is called each time the buffer fills up.
/// It receives the filled buffer and must return a fresh buffer for continued writing.
/// Returns `None` to signal that emission failed and writing should stop.
///
/// This is the core write logic extracted for testability — it is independent
/// of MQTT, tokio, or TupleBufferHandle.
fn write_message_to_buffer(
    payload: &[u8],
    suffix: &[u8],
    buf: &mut [u8],
    cursor: usize,
    mut emit_full: impl FnMut(&[u8]) -> bool,
) -> Option<usize> {
    let mut cursor = cursor;
    let cap = buf.len();

    for segment in [payload, suffix] {
        let mut offset = 0;
        while offset < segment.len() {
            let remaining = cap - cursor;
            let chunk = remaining.min(segment.len() - offset);

            buf[cursor..cursor + chunk]
                .copy_from_slice(&segment[offset..offset + chunk]);
            cursor += chunk;
            offset += chunk;

            if cursor >= cap {
                if !emit_full(buf) {
                    return None;
                }
                cursor = 0;
            }
        }
    }
    Some(cursor)
}

impl AsyncSource for MqttSource {
    async fn run(&mut self, ctx: &SourceContext) -> SourceResult {
        let cancel = ctx.cancellation_token();

        // Register with the shared broker. This starts the broker on first call
        // and creates a dedicated internal link subscribed to our topic.
        let mut handle = match nes_mqtt_broker::register_source(self.topic.clone()) {
            Ok(h) => h,
            Err(e) => return SourceResult::Error(SourceError::new(e)),
        };

        let flush_dur = Duration::from_millis(self.flush_interval_ms);
        let mut flush_timer = interval(flush_dur);
        flush_timer.reset(); // don't fire immediately

        let mut buf: Option<TupleBufferHandle> = None;
        let mut cursor: usize = 0; // bytes written into current buffer

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    // Flush remaining data before shutdown.
                    if let Some(mut b) = buf.take() {
                        if cursor > 0 {
                            b.set_number_of_tuples(cursor as u64);
                            let _ = ctx.emit(b).await;
                        }
                    }
                    return SourceResult::EndOfStream;
                }

                _ = handle.broker_stopped.changed() => {
                    // Broker thread exited (port conflict, crash, or shutdown).
                    let reason = handle.broker_stopped.borrow().clone()
                        .unwrap_or_else(|| "unknown reason".to_string());
                    return SourceResult::Error(SourceError::new(
                        format!("MQTT broker stopped: {reason}")
                    ));
                }

                notification = handle.link_rx.as_mut().unwrap().next() => {
                    let payload = match notification {
                        Ok(Some(ref n)) => match nes_mqtt_broker::forward_payload(n) {
                            Some(p) => p,
                            None => continue,
                        },
                        Ok(None) => continue,
                        Err(_) => {
                            // Link closed — flush and exit.
                            if let Some(mut b) = buf.take() {
                                if cursor > 0 {
                                    b.set_number_of_tuples(cursor as u64);
                                    let _ = ctx.emit(b).await;
                                }
                            }
                            return SourceResult::EndOfStream;
                        }
                    };

                    let suffix = self.message_suffix.as_deref().unwrap_or(&[]);
                    debug!(
                        payload_len = payload.len(),
                        suffix_len = suffix.len(),
                        ends_with_newline = payload.last() == Some(&b'\n'),
                        "MQTT message received"
                    );
                    trace!(
                        payload = %String::from_utf8_lossy(payload),
                        "MQTT message payload"
                    );

                    // Write payload + suffix into buffer(s), splitting across boundaries.
                    for segment in [payload, suffix] {
                        let mut offset = 0;
                        while offset < segment.len() {
                            if buf.is_none() {
                                buf = Some(ctx.allocate_buffer().await);
                                cursor = 0;
                            }
                            let b = buf.as_mut().unwrap();
                            let cap = b.as_mut_slice().len();
                            let remaining = cap - cursor;
                            let chunk = remaining.min(segment.len() - offset);

                            b.as_mut_slice()[cursor..cursor + chunk]
                                .copy_from_slice(&segment[offset..offset + chunk]);
                            cursor += chunk;
                            offset += chunk;

                            // Buffer full — emit and start a new one on next iteration.
                            if cursor >= cap {
                                b.set_number_of_tuples(cursor as u64);
                                debug!(bytes = cursor, "Emitting full buffer");
                                if ctx.emit(buf.take().unwrap()).await.is_err() {
                                    return SourceResult::EndOfStream;
                                }
                                cursor = 0;
                                flush_timer.reset();
                            }
                        }
                    }
                }

                _ = flush_timer.tick() => {
                    // Flush partial buffer on timer.
                    if let Some(mut b) = buf.take() {
                        if cursor > 0 {
                            b.set_number_of_tuples(cursor as u64);
                            debug!(bytes = cursor, "Flushing partial buffer on timer");
                            if ctx.emit(b).await.is_err() {
                                return SourceResult::EndOfStream;
                            }
                            cursor = 0;
                        }
                    }
                }
            }
        }
        // handle is dropped here → unregister from broker
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- write_message_to_buffer tests ---

    #[test]
    fn write_payload_no_suffix_fits_in_buffer() {
        let mut buf = [0u8; 32];
        let cursor = write_message_to_buffer(b"1,10,1000", &[], &mut buf, 0, |_| panic!("should not emit")).unwrap();
        assert_eq!(cursor, 9);
        assert_eq!(&buf[..9], b"1,10,1000");
    }

    #[test]
    fn write_payload_with_newline_suffix() {
        let mut buf = [0u8; 32];
        let cursor = write_message_to_buffer(b"1,10,1000", b"\n", &mut buf, 0, |_| panic!("should not emit")).unwrap();
        assert_eq!(cursor, 10);
        assert_eq!(&buf[..10], b"1,10,1000\n");
    }

    #[test]
    fn write_payload_with_crlf_suffix() {
        let mut buf = [0u8; 32];
        let cursor = write_message_to_buffer(b"hello", b"\r\n", &mut buf, 0, |_| panic!("should not emit")).unwrap();
        assert_eq!(cursor, 7);
        assert_eq!(&buf[..7], b"hello\r\n");
    }

    #[test]
    fn write_appends_at_cursor() {
        let mut buf = [0u8; 32];
        buf[..5].copy_from_slice(b"prev,");
        let cursor = write_message_to_buffer(b"1,2,3", b"\n", &mut buf, 5, |_| panic!("should not emit")).unwrap();
        assert_eq!(cursor, 11);
        assert_eq!(&buf[..11], b"prev,1,2,3\n");
    }

    #[test]
    fn write_multiple_messages_accumulate() {
        let mut buf = [0u8; 64];
        let cursor = write_message_to_buffer(b"1,10,1000", b"\n", &mut buf, 0, |_| panic!("no emit")).unwrap();
        let cursor = write_message_to_buffer(b"2,20,2000", b"\n", &mut buf, cursor, |_| panic!("no emit")).unwrap();
        let cursor = write_message_to_buffer(b"3,30,3000", b"\n", &mut buf, cursor, |_| panic!("no emit")).unwrap();
        assert_eq!(cursor, 30);
        assert_eq!(&buf[..30], b"1,10,1000\n2,20,2000\n3,30,3000\n");
    }

    #[test]
    fn write_emits_when_buffer_full() {
        let mut buf = [0u8; 10]; // exactly fits "1,10,1000\n"
        let mut emitted = Vec::new();
        let cursor = write_message_to_buffer(b"1,10,1000", b"\n", &mut buf, 0, |full_buf| {
            emitted.push(full_buf.to_vec());
            true
        }).unwrap();
        assert_eq!(cursor, 0); // buffer was emitted, cursor reset
        assert_eq!(emitted.len(), 1);
        assert_eq!(&emitted[0], b"1,10,1000\n");
    }

    #[test]
    fn write_payload_spans_two_buffers() {
        let mut buf = [0u8; 8]; // smaller than "1,10,1000\n" (10 bytes)
        let mut emitted = Vec::new();
        let cursor = write_message_to_buffer(b"1,10,1000", b"\n", &mut buf, 0, |full_buf| {
            emitted.push(full_buf.to_vec());
            true
        }).unwrap();
        // 10 bytes total, buffer size 8: first 8 emitted, remaining 2 in buffer
        assert_eq!(emitted.len(), 1);
        assert_eq!(&emitted[0], b"1,10,100");
        assert_eq!(cursor, 2);
        assert_eq!(&buf[..2], b"0\n");
    }

    #[test]
    fn write_suffix_spans_buffer_boundary() {
        // Payload fills buffer to exactly capacity, suffix goes into next buffer
        let mut buf = [0u8; 9]; // exactly fits "1,10,1000" without suffix
        let mut emitted = Vec::new();
        let cursor = write_message_to_buffer(b"1,10,1000", b"\n", &mut buf, 0, |full_buf| {
            emitted.push(full_buf.to_vec());
            true
        }).unwrap();
        assert_eq!(emitted.len(), 1);
        assert_eq!(&emitted[0], b"1,10,1000");
        assert_eq!(cursor, 1);
        assert_eq!(&buf[..1], b"\n");
    }

    #[test]
    fn write_large_payload_spans_many_buffers() {
        let payload = b"abcdefghijklmnopqrstuvwxyz"; // 26 bytes
        let mut buf = [0u8; 10];
        let mut emitted = Vec::new();
        let cursor = write_message_to_buffer(payload, b"\n", &mut buf, 0, |full_buf| {
            emitted.push(full_buf.to_vec());
            true
        }).unwrap();
        // 27 bytes total, buf=10: emit at 10, 20, remaining 7
        assert_eq!(emitted.len(), 2);
        assert_eq!(&emitted[0], b"abcdefghij");
        assert_eq!(&emitted[1], b"klmnopqrst");
        assert_eq!(cursor, 7);
        assert_eq!(&buf[..7], b"uvwxyz\n");
    }

    #[test]
    fn write_emit_failure_returns_none() {
        let mut buf = [0u8; 5];
        let result = write_message_to_buffer(b"1234567890", b"\n", &mut buf, 0, |_| false);
        assert!(result.is_none());
    }

    #[test]
    fn write_empty_payload_with_suffix() {
        let mut buf = [0u8; 32];
        let cursor = write_message_to_buffer(b"", b"\n", &mut buf, 0, |_| panic!("no emit")).unwrap();
        assert_eq!(cursor, 1);
        assert_eq!(&buf[..1], b"\n");
    }

    #[test]
    fn write_empty_payload_no_suffix() {
        let mut buf = [0u8; 32];
        let cursor = write_message_to_buffer(b"", &[], &mut buf, 0, |_| panic!("no emit")).unwrap();
        assert_eq!(cursor, 0);
    }

    #[test]
    fn write_cursor_at_boundary_emits_immediately() {
        let mut buf = [0u8; 4];
        buf.copy_from_slice(b"full");
        let mut emitted = Vec::new();
        // cursor == cap: first write byte triggers emit
        let cursor = write_message_to_buffer(b"AB", b"\n", &mut buf, 4, |full_buf| {
            emitted.push(full_buf.to_vec());
            true
        });
        // cursor=4, cap=4: first copy_from_slice copies 0 bytes then cursor>=cap triggers emit
        // Actually cursor=4 means remaining=0, chunk=0, no copy, but cursor>=cap triggers emit
        // then after emit, cursor=0, copies "AB\n" = 3 bytes
        assert_eq!(emitted.len(), 1);
        assert_eq!(&emitted[0], b"full");
        assert_eq!(cursor, Some(3));
        assert_eq!(&buf[..3], b"AB\n");
    }
}

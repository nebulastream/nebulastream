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

use nes_source_runtime::buffer::TupleBufferHandle;
use nes_source_runtime::config::{ConfigParam, ParamType};
use nes_source_runtime::context::SourceContext;
use nes_source_runtime::error::{SourceError, SourceResult};
use nes_source_runtime::source::AsyncSource;

pub const CONFIG_SCHEMA: &[ConfigParam] = &[
    ConfigParam { name: "mqtt_topic", param_type: ParamType::String, default: None },
    ConfigParam { name: "mqtt_flush_interval_ms", param_type: ParamType::U32, default: Some("100") },
];

pub fn create_mqtt_source(config: &HashMap<String, String>) -> Result<MqttSource, String> {
    let topic = config.get("mqtt_topic")
        .ok_or("missing mqtt_topic")?
        .clone();
    let flush_interval_ms = config.get("mqtt_flush_interval_ms")
        .unwrap_or(&"100".to_string())
        .parse::<u64>()
        .map_err(|e| format!("invalid mqtt_flush_interval_ms: {e}"))?;

    Ok(MqttSource {
        topic,
        flush_interval_ms,
    })
}

pub struct MqttSource {
    topic: String,
    flush_interval_ms: u64,
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

                    // Write payload into buffer(s), splitting across boundaries.
                    let mut offset = 0;
                    while offset < payload.len() {
                        if buf.is_none() {
                            buf = Some(ctx.allocate_buffer().await);
                            cursor = 0;
                        }
                        let b = buf.as_mut().unwrap();
                        let cap = b.as_mut_slice().len();
                        let remaining = cap - cursor;
                        let chunk = remaining.min(payload.len() - offset);

                        b.as_mut_slice()[cursor..cursor + chunk]
                            .copy_from_slice(&payload[offset..offset + chunk]);
                        cursor += chunk;
                        offset += chunk;

                        // Buffer full — emit and start a new one on next iteration.
                        if cursor >= cap {
                            b.set_number_of_tuples(cursor as u64);
                            if ctx.emit(buf.take().unwrap()).await.is_err() {
                                return SourceResult::EndOfStream;
                            }
                            cursor = 0;
                            flush_timer.reset();
                        }
                    }
                }

                _ = flush_timer.tick() => {
                    // Flush partial buffer on timer.
                    if let Some(mut b) = buf.take() {
                        if cursor > 0 {
                            b.set_number_of_tuples(cursor as u64);
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

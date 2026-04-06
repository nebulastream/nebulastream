// TokioMqttSink: publishes query output through the shared embedded MQTT broker.
//
// Uses the same singleton broker as TokioMqttSource (via nes_mqtt_broker).
// Each sink gets its own internal LinkTx for publishing to its configured topic.
// External MQTT subscribers connected to the broker receive the published data.
//
// Each SinkMessage::Data is published as a single MQTT message: all segments
// from the BufferIterator are concatenated into one payload, matching how
// the file sink writes all segments contiguously.

use std::collections::HashMap;

use bytes::Bytes;

use nes_source_runtime::config::{ConfigParam, ParamType};
use nes_source_runtime::sink_context::SinkMessage;
use nes_source_runtime::sink_error::SinkError;
use nes_source_runtime::AsyncSink;

pub const CONFIG_SCHEMA: &[ConfigParam] = &[
    ConfigParam { name: "mqtt_topic", param_type: ParamType::String, default: None },
];

pub fn create_mqtt_sink(config: &HashMap<String, String>) -> Result<MqttSink, String> {
    let topic = config.get("mqtt_topic")
        .ok_or("missing mqtt_topic")?
        .clone();

    Ok(MqttSink { topic })
}

pub struct MqttSink {
    topic: String,
}

impl AsyncSink for MqttSink {
    async fn run(
        &mut self,
        ctx: &nes_source_runtime::SinkContext,
    ) -> Result<(), SinkError> {
        let mut handle = nes_mqtt_broker::register_sink(self.topic.clone())
            .map_err(SinkError::new)?;

        let topic = Bytes::from(self.topic.clone().into_bytes());

        loop {
            tokio::select! {
                _ = handle.broker_stopped.changed() => {
                    let reason = handle.broker_stopped.borrow().clone()
                        .unwrap_or_else(|| "unknown".to_string());
                    return Err(SinkError::new(format!("MQTT broker stopped: {reason}")));
                }
                msg = ctx.recv() => {
                    match msg {
                        SinkMessage::Data(mut iter) => {
                            // Concatenate all segments into a single payload.
                            // This matches the file sink pattern where all segments
                            // are written contiguously into the output stream.
                            let mut payload = Vec::new();
                            while let Some(segment) = iter.next_segment() {
                                payload.extend_from_slice(segment);
                            }
                            if !payload.is_empty() {
                                handle.link_tx.as_mut().unwrap().async_publish(
                                    topic.clone(),
                                    Bytes::from(payload),
                                ).await.map_err(|e| SinkError::new(format!("publish failed: {e}")))?;
                            }
                        }
                        SinkMessage::Flush => {} // MQTT has no flush concept
                        SinkMessage::Close => return Ok(()),
                    }
                }
            }
        }
    }
}

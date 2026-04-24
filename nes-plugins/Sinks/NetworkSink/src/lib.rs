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

const SINK_NAME: &str = "NETWORK";

const DATA_ENDPOINT: &str = "DATA_ENDPOINT";
const BIND: &str = "BIND";
const CHANNEL: &str = "CHANNEL";
const SENDER_QUEUE_SIZE: &str = "SENDER_QUEUE_SIZE";
const MAX_PENDING_ACKS: &str = "MAX_PENDING_ACKS";
const BACKPRESSURE_UPPER_THRESHOLD: &str = "BACKPRESSURE_UPPER_THRESHOLD";
const BACKPRESSURE_LOWER_THRESHOLD: &str = "BACKPRESSURE_LOWER_THRESHOLD";
const TUPLE_SIZE: &str = "TUPLE_SIZE";

#[cfg(feature = "validation")]
mod validation {
    use super::*;
    use linkme::distributed_slice;
    use nes_sink_validation::{
        ConfigDefinition, ConfigOptionsType, ConfigOptionsTypeTag, SINK_VALIDATOR,
    };

    #[distributed_slice(SINK_VALIDATOR)]
    static NETWORK_SINK_VALIDATOR: (&'static str, &'static [ConfigDefinition]) = (
        SINK_NAME,
        &[
            ConfigDefinition::with_type(DATA_ENDPOINT, ConfigOptionsTypeTag::Text),
            ConfigDefinition::with_type(BIND, ConfigOptionsTypeTag::Text),
            ConfigDefinition::with_type(CHANNEL, ConfigOptionsTypeTag::Text),
            ConfigDefinition::with_default(SENDER_QUEUE_SIZE, ConfigOptionsType::Number(0)),
            ConfigDefinition::with_default(MAX_PENDING_ACKS, ConfigOptionsType::Number(0)),
            ConfigDefinition::with_default(BACKPRESSURE_UPPER_THRESHOLD, ConfigOptionsType::Number(1000)),
            ConfigDefinition::with_default(BACKPRESSURE_LOWER_THRESHOLD, ConfigOptionsType::Number(200)),
        ],
    );
}

#[cfg(feature = "runtime")]
mod runtime {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use linkme::distributed_slice;
    use nes_buffer_runtime::{ChildBufferIndex, TupleBuffer};
    use nes_network::protocol::{ConnectionIdentifier, TupleBuffer as NetworkBuffer};
    use nes_network::registry::{self, SenderService};
    use nes_network::sender::{SenderChannel, SenderConfig};
    use nes_sink_runtime::{AsyncSink, Result, SINK_CREATION_FUNCTIONS, SinkCreateFn};
    use nes_sink_validation::ConfigOptions;
    use std::str::FromStr;

    /// Owns a retain-counted `TupleBuffer` alongside the number of valid bytes,
    /// and exposes the valid slice as `&[u8]` so `bytes::Bytes::from_owner` can
    /// wrap it with zero copy. The underlying C++ buffer stays alive until every
    /// `Bytes` clone derived from this owner is dropped.
    struct TupleBufferSlice {
        buffer: TupleBuffer,
        len: usize,
    }

    impl AsRef<[u8]> for TupleBufferSlice {
        fn as_ref(&self) -> &[u8] {
            &self.buffer.get_data()[..self.len]
        }
    }

    struct NetworkSink {
        bind: String,
        data_endpoint: String,
        channel_id: String,
        config_override: SenderConfig,
        tuple_size: usize,
        service: Option<SenderService>,
        channel: Option<SenderChannel>,
    }

    #[async_trait]
    impl AsyncSink for NetworkSink {
        async fn start(&mut self) -> Result<()> {
            let (service, default_config) = registry::sender_service(&self.bind)?;
            let config = SenderConfig {
                sender_queue_size: if self.config_override.sender_queue_size > 0 {
                    self.config_override.sender_queue_size
                } else {
                    default_config.sender_queue_size
                },
                max_pending_acks: if self.config_override.max_pending_acks > 0 {
                    self.config_override.max_pending_acks
                } else {
                    default_config.max_pending_acks
                },
            };
            let endpoint = ConnectionIdentifier::from_str(&self.data_endpoint)
                .map_err(|e| format!("Invalid DATA_ENDPOINT `{}`: {e}", self.data_endpoint))?;
            let channel = service
                .register_channel(endpoint, self.channel_id.clone(), config)
                .await?;
            self.service = Some(service);
            self.channel = Some(channel);
            Ok(())
        }

        async fn execute(&mut self, buffer: TupleBuffer) -> Result<()> {
            let channel = self
                .channel
                .as_ref()
                .ok_or_else(|| "NetworkSink used before start()".to_string())?;
            let network_buffer = to_network_buffer(&buffer, self.tuple_size);
            channel
                .send_data(network_buffer)
                .await
                .map_err(|_| "NetworkSink was closed by the downstream receiver".to_string())
        }

        async fn stop(&mut self) -> Result<()> {
            let Some(channel) = self.channel.take() else {
                return Ok(());
            };
            channel.flush_async().await.map_err(|e| e.to_string())?;
            channel.close();
            Ok(())
        }
    }

    fn to_network_buffer(buffer: &TupleBuffer, tuple_size: usize) -> NetworkBuffer {
        let number_of_tuples = buffer.get_number_of_tuples();
        let data = Bytes::from_owner(TupleBufferSlice {
            buffer: buffer.clone(),
            len: number_of_tuples * tuple_size,
        });
        // Child buffers hold variable-sized payloads (strings, arrays); `number_of_tuples`
        // is not a byte count and is often zero. Match the original C++ NetworkSink and
        // send the full allocated memory of each child.
        let child_buffers = (0..buffer.num_children())
            .map(|i| {
                let child = buffer.load_child(ChildBufferIndex(i));
                let len = child.get_data().len();
                Bytes::from_owner(TupleBufferSlice {
                    buffer: child,
                    len,
                })
            })
            .collect();

        NetworkBuffer {
            sequence_number: buffer.get_sequence_number(),
            origin_id: buffer.get_origin_id(),
            chunk_number: buffer.get_chunk_number(),
            watermark: buffer.get_watermark(),
            number_of_tuples: number_of_tuples as u64,
            last_chunk: buffer.is_last_chunk(),
            data,
            child_buffers,
        }
    }

    fn read_usize(options: &ConfigOptions, key: &str) -> usize {
        options
            .get(key)
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or_default()
    }

    fn read_string(options: &ConfigOptions, key: &str) -> String {
        options.get(key).cloned().unwrap_or_default()
    }

    impl From<&ConfigOptions> for NetworkSink {
        fn from(value: &ConfigOptions) -> Self {
            NetworkSink {
                bind: read_string(value, BIND),
                data_endpoint: read_string(value, DATA_ENDPOINT),
                channel_id: read_string(value, CHANNEL),
                config_override: SenderConfig {
                    sender_queue_size: read_usize(value, SENDER_QUEUE_SIZE),
                    max_pending_acks: read_usize(value, MAX_PENDING_ACKS),
                },
                tuple_size: read_usize(value, TUPLE_SIZE),
                service: None,
                channel: None,
            }
        }
    }

    fn create_network_sink(config_options: &ConfigOptions) -> Box<dyn AsyncSink + Send> {
        Box::new(NetworkSink::from(config_options))
    }

    #[distributed_slice(SINK_CREATION_FUNCTIONS)]
    static NETWORK_SINK: (&'static str, &'static SinkCreateFn) = (SINK_NAME, &create_network_sink);
}

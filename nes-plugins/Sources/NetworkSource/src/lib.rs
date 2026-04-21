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

use async_trait::async_trait;
use linkme::distributed_slice;
use nes_buffer_runtime::BufferProvider;
use nes_network::receiver::{ReceiverChannel, ReceiverChannelResult};
use nes_network::registry::{self, ReceiverService};
use nes_source_runtime::{
    AsyncSource, Result, SOURCE_CREATION_FUNCTIONS, SourceCreateFn, SourceResult,
};
use nes_source_validation::{
    ConfigDefinition, ConfigOptions, ConfigOptionsType, ConfigOptionsTypeTag, SOURCE_VALIDATOR,
};

const SOURCE_NAME: &str = "NETWORK";

const BIND: &str = "BIND";
const CHANNEL: &str = "CHANNEL";
const RECEIVER_QUEUE_SIZE: &str = "RECEIVER_QUEUE_SIZE";

#[distributed_slice(SOURCE_VALIDATOR)]
static NETWORK_SOURCE_VALIDATOR: (&'static str, &'static [ConfigDefinition]) = (
    SOURCE_NAME,
    &[
        ConfigDefinition::with_type(BIND, ConfigOptionsTypeTag::Text),
        ConfigDefinition::with_type(CHANNEL, ConfigOptionsTypeTag::Text),
        ConfigDefinition::with_default(RECEIVER_QUEUE_SIZE, ConfigOptionsType::Number(0)),
    ],
);

#[distributed_slice(SOURCE_CREATION_FUNCTIONS)]
static NETWORK_SOURCE: (&'static str, &'static SourceCreateFn) = (SOURCE_NAME, &create_network_source);

struct NetworkSource {
    bind: String,
    channel_id: String,
    receiver_queue_size: usize,
    service: Option<ReceiverService>,
    channel: Option<ReceiverChannel>,
}

#[async_trait]
impl AsyncSource for NetworkSource {
    fn adds_metadata(&self) -> bool {
        true
    }

    async fn start(&mut self) -> Result<()> {
        let (service, default_queue_size) = registry::receiver_service(&self.bind)?;
        let queue_size = if self.receiver_queue_size > 0 {
            self.receiver_queue_size
        } else {
            default_queue_size
        };
        let channel = service
            .register_channel(self.channel_id.clone(), queue_size)
            .await?;
        self.service = Some(service);
        self.channel = Some(channel);
        Ok(())
    }

    async fn receive(&mut self, provider: &mut BufferProvider) -> Result<SourceResult> {
        let channel = self
            .channel
            .as_ref()
            .ok_or_else(|| "NetworkSource used before start()".to_string())?;

        match channel.receive().await {
            ReceiverChannelResult::Closed => Ok(SourceResult::EoS),
            ReceiverChannelResult::Error(e) => Err(format!("NetworkSource receive error: {}", e)),
            ReceiverChannelResult::Ok(received) => {
                let mut buffer = provider.allocate().await;
                let payload_len = received.data.len();
                if payload_len > buffer.get_data().len() {
                    return Err(format!(
                        "Received payload of {} bytes exceeds destination buffer size {}",
                        payload_len,
                        buffer.get_data().len()
                    ));
                }
                buffer.get_data_mut()[..payload_len].copy_from_slice(&received.data);

                buffer.set_sequence_number(received.sequence_number);
                buffer.set_origin_id(received.origin_id);
                buffer.set_chunk_number(received.chunk_number);
                buffer.set_watermark(received.watermark);
                buffer.set_last_chunk(received.last_chunk);
                buffer.set_number_of_tuples(received.number_of_tuples as usize);

                for child_bytes in &received.child_buffers {
                    if child_bytes.is_empty() {
                        return Err("Received empty child buffer".to_string());
                    }
                    let mut child = provider
                        .try_allocate_unpooled(child_bytes.len())
                        .ok_or_else(|| {
                            format!(
                                "Failed to allocate unpooled child buffer of size {}",
                                child_bytes.len()
                            )
                        })?;
                    child.get_data_mut()[..child_bytes.len()].copy_from_slice(child_bytes);
                    buffer.store_child(&child);
                }

                Ok(SourceResult::Data(buffer, received.number_of_tuples as usize))
            }
        }
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(channel) = self.channel.take() {
            channel.close();
        }
        self.service = None;
        Ok(())
    }
}

fn read_usize(options: &ConfigOptions, key: &str) -> usize {
    options
        .get(key)
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or_default()
}

impl From<&ConfigOptions> for NetworkSource {
    fn from(value: &ConfigOptions) -> Self {
        NetworkSource {
            bind: value.get(BIND).cloned().unwrap_or_default(),
            channel_id: value.get(CHANNEL).cloned().unwrap_or_default(),
            receiver_queue_size: read_usize(value, RECEIVER_QUEUE_SIZE),
            service: None,
            channel: None,
        }
    }
}

fn create_network_source(config_options: &ConfigOptions) -> Box<dyn AsyncSource + Send> {
    Box::new(NetworkSource::from(config_options))
}

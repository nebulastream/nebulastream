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

use nes_network::protocol::{ChannelIdentifier, ConnectionIdentifier, ThisConnectionIdentifier};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Default)]
pub struct SinkExpectation {
    pub expected_messages: Option<usize>,
    pub expected_messages_uncertainty: Option<usize>,
    pub expect_close_from_other_side: Option<bool>,
}

#[derive(Deserialize, Serialize, Default)]
pub struct SourceExpectation {
    pub expected_messages: Option<usize>,
    pub expected_messages_uncertainty: Option<usize>,
    pub expect_close_from_other_side: Option<bool>,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum Query {
    Source {
        downstream_channel: ChannelIdentifier,
        downstream_connection: ConnectionIdentifier,
        ingestion_rate_in_milliseconds: Option<u64>,
        #[serde(default)]
        sink_expectation: SinkExpectation,
        #[serde(default = "default_true")]
        close_sink: bool,
    },
    Bridge {
        input_channel: ChannelIdentifier,
        downstream_channel: ChannelIdentifier,
        downstream_connection: ConnectionIdentifier,
        ingestion_rate_in_milliseconds: Option<u64>,
        #[serde(default = "default_true")]
        close_source: bool,
        #[serde(default = "default_true")]
        close_sink: bool,
        #[serde(default)]
        sink_expectation: SinkExpectation,
        #[serde(default)]
        source_expectation: SourceExpectation,
    },
    Sink {
        input_channel: ChannelIdentifier,
        ingestion_rate_in_milliseconds: Option<u64>,
        #[serde(default = "default_true")]
        close_source: bool,
        #[serde(default)]
        source_expectation: SourceExpectation,
    },
}

fn default_true() -> bool {
    true
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum Command {
    StartQuery { q: Query },
    StopQuery { id: usize },
    Wait { millis: usize },
    Reset,
}
#[derive(Deserialize, Serialize)]
pub struct Node {
    pub(crate) connection: ThisConnectionIdentifier,
    pub commands: Vec<Command>,
}
pub(crate) fn load_config(file: &std::path::Path, index: usize) -> Node {
    let file = std::fs::File::open(file).unwrap();
    let mut nodes: Vec<Node> = serde_yaml::from_reader(&file).unwrap();
    nodes.remove(index)
}

pub(crate) fn load_all_configs(file: &std::path::Path) -> Vec<Node> {
    let file = std::fs::File::open(file).unwrap();
    serde_yaml::from_reader(&file).unwrap()
}

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

use crate::query;
use crate::query::fragment;
use crate::query::{CreateQuery, DropQuery, GetQuery};
use crate::sink;
use crate::sink::{CreateSink, DropSink, GetSink};
use crate::source::logical_source::{self, CreateLogicalSource, DropLogicalSource, GetLogicalSource};
use crate::source::physical_source::{self, CreatePhysicalSource, DropPhysicalSource, GetPhysicalSource};
use crate::worker;
use crate::worker::{CreateWorker, DropWorker, GetWorker};
use std::fmt::Debug;
use tokio::sync::oneshot;

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(tag = "tag")]
pub enum Statement {
    CreateWorker(CreateWorker),
    GetWorker(GetWorker),
    DropWorker(DropWorker),
    CreateQuery(CreateQuery),
    ExplainQuery(String),
    GetQuery(GetQuery),
    DropQuery(DropQuery),
    CreateLogicalSource(CreateLogicalSource),
    GetLogicalSource(GetLogicalSource),
    DropLogicalSource(DropLogicalSource),
    CreatePhysicalSource(CreatePhysicalSource),
    GetPhysicalSource(GetPhysicalSource),
    DropPhysicalSource(DropPhysicalSource),
    CreateSink(CreateSink),
    GetSink(GetSink),
    DropSink(DropSink),
}

pub struct Request {
    pub statement: Statement,
    pub reply_to: oneshot::Sender<anyhow::Result<StatementResponse>>,
}

impl Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "Request{:?}", self.statement)
    }
}

impl Request {
    pub fn new(statement: Statement) -> (oneshot::Receiver<anyhow::Result<StatementResponse>>, Self) {
        let (tx, rx) = oneshot::channel();
        (
            rx,
            Self {
                statement,
                reply_to: tx,
            },
        )
    }

    pub fn reply(self, response: anyhow::Result<StatementResponse>) -> Result<(), anyhow::Result<StatementResponse>> {
        self.reply_to.send(response)
    }
}

#[derive(Clone, Debug, serde::Serialize)]
pub enum StatementResponse {
    CreatedLogicalSource(logical_source::Model),
    CreatedPhysicalSource(physical_source::Model),
    CreatedSink(sink::Model),
    CreatedQuery(query::Model),
    CreatedWorker(worker::Model),
    DroppedLogicalSources(Vec<logical_source::Model>),
    DroppedPhysicalSources(Vec<physical_source::Model>),
    DroppedSinks(Vec<sink::Model>),
    DroppedQueries(Vec<query::Model>),
    DroppedWorker(Option<worker::Model>),
    LogicalSource(Vec<logical_source::Model>),
    PhysicalSources(Vec<physical_source::Model>),
    Sinks(Vec<sink::Model>),
    ExplainedQuery(String),
    Queries(Vec<(query::Model, Vec<fragment::Model>)>),
    Workers(Vec<worker::Model>),
}

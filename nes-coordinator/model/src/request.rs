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
use crate::source::logical_source::{
    self, CreateLogicalSource, DropLogicalSource, GetLogicalSource,
};
use crate::source::physical_source::{
    self, CreatePhysicalSource, DropPhysicalSource, GetPhysicalSource,
};
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

pub struct RequestInput {
    pub input: StatementInput,
    pub block_until: query::query_state::QueryState,
}

impl RequestInput {
    pub fn sql(sql: String) -> Self {
        Self {
            input: StatementInput::Sql(sql),
            block_until: query::query_state::QueryState::default(),
        }
    }

    pub fn structured(statement: Statement) -> Self {
        Self {
            input: StatementInput::Structured(statement),
            block_until: query::query_state::QueryState::default(),
        }
    }

    pub fn block_until(mut self, state: query::query_state::QueryState) -> Self {
        self.block_until = state;
        self
    }
}

pub enum StatementInput {
    Sql(String),
    Structured(Statement),
}

impl Debug for RequestInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{:?}", self.input)
    }
}

impl Debug for StatementInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Self::Sql(sql) => write!(f, "Sql({sql:?})"),
            Self::Structured(stmt) => write!(f, "{stmt:?}"),
        }
    }
}

pub struct Request {
    pub input: RequestInput,
    pub reply_to: oneshot::Sender<anyhow::Result<StatementResponse>>,
}

impl Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "Request{:?}", self.input)
    }
}

impl Request {
    pub fn new(
        statement: Statement,
    ) -> (oneshot::Receiver<anyhow::Result<StatementResponse>>, Self) {
        Self::from(RequestInput::structured(statement))
    }

    pub fn from(
        input: RequestInput,
    ) -> (oneshot::Receiver<anyhow::Result<StatementResponse>>, Self) {
        let (tx, rx) = oneshot::channel();
        (
            rx,
            Self {
                input,
                reply_to: tx,
            },
        )
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

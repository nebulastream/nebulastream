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

//! Requests submitted to the coordinator, wrapping [`statements`](crate::statement) or raw SQL.
//! A statement on a query can also state when to send the reply.

use crate::query::query_state::QueryState;
use crate::query::{CreateQuery, DropQuery, GetQuery};
use crate::statement::{ParsedStatement, StatementResult};
use anyhow::Result;
use std::fmt::Debug;
use std::time::Duration;
use tokio::sync::oneshot;

/// The reply channel is created together with the request so no caller can forget to supply one.
pub struct Request {
    pub statement: Statement,
    pub reply_to: oneshot::Sender<Result<StatementResult>>,
}

impl Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "Request({:?})", self.statement)
    }
}

impl Request {
    pub fn new(input: Statement) -> (oneshot::Receiver<Result<StatementResult>>, Self) {
        let (tx, rx) = oneshot::channel();
        (
            rx,
            Self {
                statement: input,
                reply_to: tx,
            },
        )
    }
}

#[derive(Debug)]
pub enum Statement {
    // a raw SQL statement
    Sql {
        sql: String,
        wait: Option<Wait<QueryState>>,
    },
    // an already parsed statement
    Parsed(ParsedStatement),
    CreateQuery(CreateQuery, Wait<QueryState>),
    DropQuery(DropQuery, Wait<Terminated>),
    GetQuery(GetQuery, Wait<GetUntil>),
}

/// How long the coordinator parks a request's reply.
#[derive(Debug, Clone, Copy)]
pub struct Wait<U> {
    pub until: U,
    pub timeout: Option<Duration>,
}

/// Hold a drop or a read until every affected query has terminated.
/// An elapsed timeout releases the reply with an error.
#[derive(Debug, Clone, Copy)]
pub struct Terminated;

#[derive(Debug, Clone, Copy)]
pub enum GetUntil {
    /// Hold a get until the entity reaches the target state or reaches a terminal state.
    /// An elapsed timeout releases the reply with an error.
    State(QueryState),
    /// Hold a drop or a read until every affected query has terminated.
    /// An elapsed timeout releases the reply with an error.
    Terminated,
    /// Hold a get until its status can no longer change.
    /// An elapsed timeout releases the reply with the current status.
    Converged,
}

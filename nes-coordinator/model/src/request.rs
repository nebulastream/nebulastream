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

//! Requests to the coordinator.
//! A request holds a [`statement`](crate::statement) or raw SQL.

use crate::query::{GetQuery, QueryWithFragments};
use crate::statement::{ParsedStatement, StatementResult};
use anyhow::Result;
use std::fmt::Debug;
use std::time::Duration;
use tokio::sync::oneshot;

/// A statement for the coordinator, and the channel for its reply.
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

/// What a request runs.
#[derive(Debug)]
pub enum Statement {
    /// SQL text, not yet parsed.
    Sql(String),
    Parsed(ParsedStatement),
    /// A read of queries with a reply that is held back as the [`Wait`] describes.
    /// Its result is [`StatementResult::Watched`].
    Watch(GetQuery, Wait),
}

/// When the coordinator sends the reply of a watch.
/// Without a timeout, the reply is held back until the condition holds.
#[derive(Debug, Clone, Copy)]
pub struct Wait {
    pub until: Until,
    pub timeout: Option<Duration>,
}

/// The condition on the selected queries that releases the reply of a watch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Until {
    /// Every query is running or has terminated.
    Running,
    /// Every query has terminated.
    Terminated,
    /// Every fragment's status was observed after the request,
    /// or cannot be updated (the fragment has terminated or its host is not active).
    /// The status can change again after the reply.
    Fresh,
}

/// The reply of a watch.
/// A terminated query satisfies every condition, so the caller checks the states of the rows.
/// A dropped query that was deleted has no row.
#[derive(Clone, Debug, serde::Serialize)]
pub struct Watched {
    pub rows: Vec<QueryWithFragments>,
    /// The timeout passed before the condition held, and the rows are as last read.
    pub expired: bool,
}

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

//! The request envelope submitted to the coordinator:
//! a statement to run and the wait semantics to apply before the caller's reply is sent.

use crate::query::query_state::QueryState;
use crate::statement::{Statement, StatementResult};
use anyhow::Result;
use std::fmt::Debug;
use std::time::Duration;
use tokio::sync::oneshot;

/// How long the coordinator holds back a request's reply, and the condition that releases it.
/// A `None` timeout waits indefinitely.
#[derive(Debug, Clone, Copy)]
pub enum Wait {
    /// Answer as soon as the statement has been applied to the catalog.
    None,
    /// Hold a create, or a read, until its query, or every query that it lists, reaches the target state or terminates.
    /// An elapsed timeout releases the reply with an error.
    UntilState {
        state: QueryState,
        timeout: Option<Duration>,
    },
    /// Hold a drop or a read until every affected query has terminated.
    /// An elapsed timeout releases the reply with an error.
    UntilTerminated { timeout: Option<Duration> },
    /// Hold a read until its status can no longer change.
    /// An elapsed timeout releases the reply with the current status, which is a successful return.
    Poll { timeout: Option<Duration> },
}

/// A submitted statement.
#[derive(Debug)]
pub enum StatementInput {
    // raw SQL text
    Sql(String),
    // an already parsed statement
    Parsed(Statement),
}

/// Envelope sent over the request channel.
/// The reply channel is created together with the request so no caller can forget to supply one.
pub struct Request {
    pub input: StatementInput,
    pub wait: Wait,
    pub reply_to: oneshot::Sender<Result<StatementResult>>,
}

impl Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "Request({:?}, {:?})", self.input, self.wait)
    }
}

impl Request {
    pub fn new(
        input: StatementInput,
        wait: Wait,
    ) -> (oneshot::Receiver<Result<StatementResult>>, Self) {
        let (tx, rx) = oneshot::channel();
        (
            rx,
            Self {
                input,
                wait,
                reply_to: tx,
            },
        )
    }
}

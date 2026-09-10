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

//! The request envelope submitted to the coordinator: a statement to run plus
//! the wait semantics to apply before the caller's reply is sent.

use crate::query::query_state::QueryState;
use crate::statement::{Statement, StatementResult};
use anyhow::Result;
use std::fmt::Debug;
use std::time::Duration;
use tokio::sync::oneshot;

/// How long the coordinator parks a request's reply before answering, and
/// the condition that releases it.
///
/// A request that does not need to wait answers as soon as its statement
/// has been applied to the catalog. The parking variants hold the reply
/// until their condition holds or their timeout elapses; a `None` timeout
/// waits indefinitely. Which variant is valid depends on the statement:
/// creates wait on a target state, drops and reads wait on termination,
/// and reads can poll until the status stops changing.
pub enum Wait {
    /// Answer as soon as the statement has been applied to the catalog.
    None,
    /// Park a create until its query reaches the target state or
    /// terminates. An elapsed timeout releases the reply with an error.
    UntilState {
        state: QueryState,
        timeout: Option<Duration>,
    },
    /// Park a drop or a read until every affected query has terminated. An
    /// elapsed timeout releases the reply with an error.
    UntilTerminated { timeout: Option<Duration> },
    /// Park a read until its status can no longer change. An elapsed timeout
    /// releases the reply with the current status, a successful return.
    Poll { timeout: Option<Duration> },
}

/// One unit of work submitted to the coordinator: a statement to run plus
/// the wait semantics applied before the caller's reply is sent.
pub struct Payload {
    pub input: StatementInput,
    pub wait: Wait,
}

impl Payload {
    pub fn sql(sql: String) -> Self {
        Self::from_input(StatementInput::Sql(sql))
    }

    pub fn parsed(statement: Statement) -> Self {
        Self::from_input(StatementInput::Parsed(statement))
    }

    fn from_input(input: StatementInput) -> Self {
        Self {
            input,
            wait: Wait::None,
        }
    }

    pub fn wait(mut self, wait: Wait) -> Self {
        self.wait = wait;
        self
    }

    /// Block a create until its streaming query is running.
    pub fn until_running(self, timeout: Option<Duration>) -> Self {
        self.wait(Wait::UntilState {
            state: QueryState::Running,
            timeout,
        })
    }

    /// Block a create until its batch query has completed.
    pub fn until_completed(self, timeout: Option<Duration>) -> Self {
        self.wait(Wait::UntilState {
            state: QueryState::Completed,
            timeout,
        })
    }

    /// Block a drop or a read until every affected query has terminated.
    pub fn until_terminated(self, timeout: Option<Duration>) -> Self {
        self.wait(Wait::UntilTerminated { timeout })
    }

    /// Poll a read until its status can no longer change, then answer.
    pub fn poll_for(self, timeout: Option<Duration>) -> Self {
        self.wait(Wait::Poll { timeout })
    }
}

pub enum StatementInput {
    Sql(String),
    Parsed(Statement),
}

impl Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{:?}", self.input)
    }
}

impl Debug for StatementInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            Self::Sql(sql) => write!(f, "Sql({sql:?})"),
            Self::Parsed(stmt) => write!(f, "{stmt:?}"),
        }
    }
}

/// Envelope sent over the request channel: a payload plus a oneshot
/// channel the coordinator uses to return the typed result.
pub struct Request {
    pub payload: Payload,
    pub reply_to: oneshot::Sender<Result<StatementResult>>,
}

impl Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "Request{:?}", self.payload)
    }
}

impl Request {
    pub fn new(statement: Statement) -> (oneshot::Receiver<Result<StatementResult>>, Self) {
        Self::from(Payload::parsed(statement))
    }

    pub fn from(payload: Payload) -> (oneshot::Receiver<Result<StatementResult>>, Self) {
        let (tx, rx) = oneshot::channel();
        (
            rx,
            Self {
                payload,
                reply_to: tx,
            },
        )
    }
}

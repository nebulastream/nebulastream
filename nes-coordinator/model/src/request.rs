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

use crate::query::query_state::QueryState;
use crate::statement::{Statement, StatementResult};
use anyhow::Result;
use std::fmt::Debug;
use tokio::sync::oneshot;

pub struct Payload {
    pub input: StatementInput,
    pub block_until: QueryState,
}

impl Payload {
    pub fn sql(sql: String) -> Self {
        Self {
            input: StatementInput::Sql(sql),
            block_until: QueryState::default(),
        }
    }

    pub fn structured(statement: Statement) -> Self {
        Self {
            input: StatementInput::Structured(statement),
            block_until: QueryState::default(),
        }
    }

    pub fn block_until(mut self, state: QueryState) -> Self {
        self.block_until = state;
        self
    }
}

pub enum StatementInput {
    Sql(String),
    Structured(Statement),
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
            Self::Structured(stmt) => write!(f, "{stmt:?}"),
        }
    }
}

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
    pub fn new(
        statement: Statement,
    ) -> (oneshot::Receiver<Result<StatementResult>>, Self) {
        Self::from(Payload::structured(statement))
    }

    pub fn from(
        payload: Payload,
    ) -> (oneshot::Receiver<Result<StatementResult>>, Self) {
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

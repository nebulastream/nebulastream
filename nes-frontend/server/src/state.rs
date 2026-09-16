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

use crate::Config;
use crate::error::ApiError;
use async_channel::TrySendError;
use axum::http::StatusCode;
use coordinator::{EarlyTermination, WaitTimeout};
use model::request::{Request, StatementInput, Wait};
use model::statement::StatementResult;
use std::time::Duration;

#[derive(Clone)]
pub struct AppState {
    sender: async_channel::Sender<Request>,
    wait_cap: Duration,
}

impl AppState {
    #[must_use]
    pub fn new(sender: async_channel::Sender<Request>, config: &Config) -> Self {
        Self {
            sender,
            wait_cap: config.wait_cap,
        }
    }

    pub(crate) const fn wait_cap(&self) -> Duration {
        self.wait_cap
    }

    pub(crate) async fn submit(
        &self,
        input: StatementInput,
        wait: Wait,
    ) -> Result<Outcome, ApiError> {
        let (reply, request) = Request::new(input, wait);
        self.sender.try_send(request).map_err(|error| match error {
            TrySendError::Full(_) => ApiError::overloaded(),
            TrySendError::Closed(_) => ApiError::unavailable("the coordinator has stopped"),
        })?;
        let reply = reply
            .await
            .map_err(|_| ApiError::unavailable("the coordinator dropped the request"))?;
        Outcome::from_reply(reply)
    }
}

pub(crate) enum Outcome {
    Done(StatementResult),
    TimedOut(StatementResult),
}

impl Outcome {
    pub(crate) fn from_reply(reply: anyhow::Result<StatementResult>) -> Result<Self, ApiError> {
        let error = match reply {
            Ok(result) => return Ok(Self::Done(result)),
            Err(error) => error,
        };
        let error = match error.downcast::<EarlyTermination>() {
            Ok(EarlyTermination(created)) => {
                return Ok(Self::Done(StatementResult::CreatedQuery(created)));
            }
            Err(error) => error,
        };
        match error.downcast::<WaitTimeout>() {
            Ok(WaitTimeout(result)) => Ok(Self::TimedOut(result)),
            Err(error) => Err(ApiError::from(error)),
        }
    }

    pub(crate) fn split(self, success: StatusCode) -> (StatusCode, StatementResult) {
        match self {
            Self::Done(result) => (success, result),
            Self::TimedOut(result) => (StatusCode::ACCEPTED, result),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::error::{CodedError, ErrorCode};
    use model::identifier::QueryId;
    use model::query::query_state::QueryState;
    use model::query::{self, QueryWithFragments};
    use model::statement::Statement;
    use model::worker::GetWorker;

    fn created(state: QueryState) -> QueryWithFragments {
        QueryWithFragments {
            query: query::Model {
                id: QueryId::new(1),
                name: None,
                sql: String::new(),
                state,
                start_timestamp: None,
                stop_timestamp: None,
                error: None,
            },
            fragments: Vec::new(),
        }
    }

    #[test]
    fn a_reply_is_done_as_it_is() {
        let outcome = Outcome::from_reply(Ok(StatementResult::Workers(vec![]))).unwrap();
        let (status, result) = outcome.split(StatusCode::CREATED);
        assert_eq!(status, StatusCode::CREATED);
        assert!(matches!(result, StatementResult::Workers(_)));
    }

    #[test]
    fn an_early_termination_is_done_with_the_terminal_row() {
        let error = anyhow::Error::new(EarlyTermination(created(QueryState::Failed)));
        let outcome = Outcome::from_reply(Err(error)).unwrap();
        assert!(matches!(
            outcome.split(StatusCode::CREATED),
            (StatusCode::CREATED, StatementResult::CreatedQuery(row)) if row.query.state == QueryState::Failed
        ));
    }

    #[test]
    fn a_timeout_is_accepted_with_the_rows_it_carries() {
        let error = anyhow::Error::new(WaitTimeout(StatementResult::CreatedQuery(created(
            QueryState::Running,
        ))))
        .context(CodedError::new(ErrorCode::QueryWaitTimeout, "timed out"));
        let outcome = Outcome::from_reply(Err(error)).unwrap();
        assert!(matches!(
            outcome.split(StatusCode::CREATED),
            (StatusCode::ACCEPTED, StatementResult::CreatedQuery(row)) if row.query.state == QueryState::Running
        ));
    }

    #[test]
    fn a_coded_error_is_mapped_by_its_code() {
        let error = anyhow::Error::new(CodedError::new(ErrorCode::UnknownWorker, "no such worker"))
            .context("while fetching");
        let failure = Outcome::from_reply(Err(error)).err().unwrap();
        assert_eq!(failure.status, StatusCode::NOT_FOUND);
        assert_eq!(failure.body.code, ErrorCode::UnknownWorker as u16);
        assert_eq!(failure.body.message, "while fetching: no such worker");
    }

    #[test]
    fn an_error_without_a_code_is_an_internal_one() {
        let failure = Outcome::from_reply(Err(anyhow::anyhow!("no SQL planner configured")))
            .err()
            .unwrap();
        assert_eq!(failure.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(failure.body.code, ErrorCode::UnknownException as u16);
    }

    #[tokio::test]
    async fn a_full_queue_is_refused_with_a_retry_hint() {
        let (sender, receiver) = async_channel::bounded(1);
        let state = AppState::new(sender.clone(), &Config::default());
        let (_reply, request) = Request::new(
            StatementInput::Parsed(Statement::GetWorker(GetWorker::all())),
            Wait::None,
        );
        sender.try_send(request).unwrap();
        let failure = state
            .submit(
                StatementInput::Parsed(Statement::GetWorker(GetWorker::all())),
                Wait::None,
            )
            .await
            .err()
            .unwrap();
        assert_eq!(failure.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(failure.retry_after(), Some(1));
        drop(receiver);
    }

    #[tokio::test]
    async fn a_stopped_coordinator_is_unavailable() {
        let (sender, receiver) = async_channel::bounded(1);
        let state = AppState::new(sender, &Config::default());
        drop(receiver);
        let failure = state
            .submit(
                StatementInput::Parsed(Statement::GetWorker(GetWorker::all())),
                Wait::None,
            )
            .await
            .err()
            .unwrap();
        assert_eq!(failure.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(failure.retry_after(), None);
    }

    #[tokio::test]
    async fn a_dropped_reply_is_unavailable() {
        let (sender, receiver) = async_channel::bounded(1);
        let state = AppState::new(sender, &Config::default());
        let drop_without_answering = tokio::spawn(async move {
            let request = receiver.recv().await.unwrap();
            drop(request);
        });
        let failure = state
            .submit(
                StatementInput::Parsed(Statement::GetWorker(GetWorker::all())),
                Wait::None,
            )
            .await
            .err()
            .unwrap();
        assert_eq!(failure.status, StatusCode::SERVICE_UNAVAILABLE);
        drop_without_answering.await.unwrap();
    }
}

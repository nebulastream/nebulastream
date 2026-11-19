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

//! A typed client for the request handler.
//! A watch replies with rows also when its timeout passes or a query ended early,
//! and this client turns those rows into errors,
//! so every caller reports early ends and timeouts the same way.

use anyhow::{Result, anyhow, bail};
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryId;
use model::query::query_state::QueryState;
use model::query::{GetQuery, QueryWithFragments, StopQuery};
use model::request::{Request, Statement, Until, Wait, Watched};
use model::statement::{ParsedStatement, StatementResult};
use std::fmt;
use std::time::Duration;
use thiserror::Error;

/// A query that stopped or failed while the caller waited for it to run or to complete.
#[derive(Error, Debug)]
#[error("query {} ended with state {} before it reached {expected}", .row.query.id, .row.query.state)]
pub struct EarlyTermination {
    pub row: QueryWithFragments,
    pub expected: QueryState,
}

/// The timeout of a wait passed before its condition held.
/// It holds the rows of the last read, so the caller can report their state or stop the queries.
/// It is returned with the code `QueryWaitTimeout`, and displays the queries that have not reached the condition:
/// `query 5 did not reach Running within 15s (state Pending)`, `queries 3, 7 did not terminate within 50ms`.
#[derive(Debug)]
pub struct WaitTimeout {
    pub rows: Vec<QueryWithFragments>,
    pub until: Until,
    pub timeout: Duration,
}

impl std::error::Error for WaitTimeout {}

impl fmt::Display for WaitTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (goal, reached): (&str, fn(QueryState) -> bool) = match self.until {
            Until::Running => ("reach Running", |state| {
                state == QueryState::Running || state.is_terminal()
            }),
            Until::Terminated => ("terminate", |state| state.is_terminal()),
            Until::Fresh => ("report a fresh status", |_| false),
        };
        let waiting: Vec<_> = self
            .rows
            .iter()
            .filter(|row| !reached(row.query.state))
            .collect();
        let timeout = self.timeout;
        match waiting.as_slice() {
            [row] => write!(
                f,
                "query {} did not {goal} within {timeout:?} (state {})",
                row.query.id, row.query.state
            ),
            rows => {
                let ids: Vec<_> = rows.iter().map(|row| row.query.id.to_string()).collect();
                write!(
                    f,
                    "queries {} did not {goal} within {timeout:?}",
                    ids.join(", ")
                )
            }
        }
    }
}

/// Sends requests to the request handler and waits for their replies.
#[derive(Clone)]
pub struct Client(async_channel::Sender<Request>);

impl Client {
    pub const fn new(sender: async_channel::Sender<Request>) -> Self {
        Self(sender)
    }

    pub async fn send(&self, statement: Statement) -> Result<StatementResult> {
        let (reply, request) = Request::new(statement);
        self.0
            .send(request)
            .await
            .map_err(|_| anyhow!("coordinator shut down"))?;
        reply.await.map_err(|_| anyhow!("coordinator shut down"))?
    }

    /// Waits on the queries that `read` matches when the request is handled, so a query created later is not part of the wait.
    /// A passed timeout is not an error: the reply is marked expired.
    pub async fn watch(&self, read: GetQuery, wait: Wait) -> Result<Watched> {
        match self.send(Statement::Watch(read, wait)).await? {
            StatementResult::Watched(watched) => Ok(watched),
            other => bail!("a watch returned {other:?}"),
        }
    }

    /// Runs SQL that has to create a query.
    /// Fails when the SQL is another statement, which is still applied.
    pub async fn create(&self, sql: String) -> Result<QueryWithFragments> {
        match self.send(Statement::Sql(sql)).await? {
            StatementResult::CreatedQuery(created) => Ok(created),
            other => bail!("the statement did not create a query: {other}"),
        }
    }

    /// Returns the ids of the matched queries before they have stopped.
    pub async fn stop_queries(&self, stop: StopQuery) -> Result<Vec<QueryId>> {
        match self
            .send(Statement::Parsed(ParsedStatement::StopQuery(stop)))
            .await?
        {
            StatementResult::StoppedQueries(stopped) => {
                Ok(stopped.into_iter().map(|query| query.id).collect())
            }
            other => bail!("a stop returned {other:?}"),
        }
    }

    /// Fails when a query does not exist,
    /// with [`EarlyTermination`] when a query stops or fails first,
    /// and otherwise with [`WaitTimeout`] when the timeout passes.
    pub async fn await_running(
        &self,
        ids: Vec<QueryId>,
        timeout: Option<Duration>,
    ) -> Result<Vec<QueryWithFragments>> {
        self.await_state(ids, Until::Running, timeout, Some(QueryState::Running))
            .await
    }

    /// Fails when a query does not exist,
    /// with [`EarlyTermination`] when a query stops or fails,
    /// and otherwise with [`WaitTimeout`] when the timeout passes.
    pub async fn await_completed(
        &self,
        ids: Vec<QueryId>,
        timeout: Option<Duration>,
    ) -> Result<Vec<QueryWithFragments>> {
        self.await_state(ids, Until::Terminated, timeout, Some(QueryState::Completed))
            .await
    }

    /// A stopped or failed query counts as terminated, for a caller that has stopped or dropped the queries.
    /// A dropped query that was deleted has no row in the result.
    /// Fails with [`WaitTimeout`] when the timeout passes.
    pub async fn await_terminated(
        &self,
        ids: Vec<QueryId>,
        timeout: Option<Duration>,
    ) -> Result<Vec<QueryWithFragments>> {
        self.await_state(ids, Until::Terminated, timeout, None)
            .await
    }

    /// Returns the rows of the last read when the timeout passes.
    pub async fn await_fresh(
        &self,
        read: GetQuery,
        timeout: Option<Duration>,
    ) -> Result<Vec<QueryWithFragments>> {
        let wait = Wait {
            until: Until::Fresh,
            timeout,
        };
        Ok(self.watch(read, wait).await?.rows)
    }

    async fn await_state(
        &self,
        ids: Vec<QueryId>,
        until: Until,
        timeout: Option<Duration>,
        expected: Option<QueryState>,
    ) -> Result<Vec<QueryWithFragments>> {
        let read = GetQuery::all().with_ids(ids.clone());
        let Watched { rows, expired } = self.watch(read, Wait { until, timeout }).await?;
        // The catalog deletes a dropped query once every fragment has terminated,
        // so a missing row is neither running nor completed.
        if expected.is_some()
            && let Some(missing) = ids
                .iter()
                .find(|id| !rows.iter().any(|row| row.query.id == **id))
        {
            bail!("query {missing} does not exist or was dropped");
        }
        if let Some(expected) = expected
            && let Some(row) = rows.iter().find(|row| row.query.state.ended_early())
        {
            return Err(EarlyTermination {
                row: row.clone(),
                expected,
            }
            .into());
        }
        match timeout {
            Some(timeout) if expired => Err(anyhow::Error::new(WaitTimeout {
                rows,
                until,
                timeout,
            })
            .context(CodedError::new(
                ErrorCode::QueryWaitTimeout,
                "the wait timed out",
            ))),
            _ => Ok(rows),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::query;

    fn row(id: i64, state: QueryState) -> QueryWithFragments {
        QueryWithFragments {
            query: query::Model {
                id: QueryId::new(id),
                name: None,
                sql: String::new(),
                global_plan: Vec::new(),
                state,
                start_timestamp: None,
                stop_timestamp: None,
                error: None,
                dropped: false,
            },
            fragments: Vec::new(),
        }
    }

    #[test]
    fn a_timeout_lists_the_queries_that_have_not_reached_the_condition() {
        let one = WaitTimeout {
            rows: vec![row(5, QueryState::Pending), row(6, QueryState::Running)],
            until: Until::Running,
            timeout: Duration::from_secs(15),
        };
        assert_eq!(
            one.to_string(),
            "query 5 did not reach Running within 15s (state Pending)"
        );
        let several = WaitTimeout {
            rows: vec![row(3, QueryState::Running), row(7, QueryState::Started)],
            until: Until::Terminated,
            timeout: Duration::from_millis(50),
        };
        assert_eq!(
            several.to_string(),
            "queries 3, 7 did not terminate within 50ms"
        );
    }
}

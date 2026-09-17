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

//! The request handler.
//!
//! This file contains the loop and its side effects: the channels, the catalog reads, and the reply sends.
//! The rules for which replies wait and when they are released are in `parking.rs`.

mod client;
mod execute;
mod parking;

pub use client::{Client, EarlyTermination, WaitTimeout};
pub use execute::SqlPlanner;

use chrono::Utc;
use controller::worker_client::in_process::EmbeddedWorkerLauncher;
use model::Execute;
use model::database::Database;
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryId;
use model::query::GetQuery;
use model::request::{Request, Statement, Until, Wait, Watched};
use model::statement::StatementResult;
use model::worker;
use parking::{Condition, Observation, ParkedReplies};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, watch};
use tokio::time::Instant;
use tracing::{debug, info};

/// The same value as the controller's poll interval.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// A send fails only when the client stopped waiting, so the failure is ignored.
type ReplyTo = oneshot::Sender<anyhow::Result<StatementResult>>;

/// Runs each request against the catalog and replies to it.
pub struct RequestHandler {
    receiver: async_channel::Receiver<Request>,
    db: Database,
    intent_tx: watch::Sender<()>,
    state_rx: watch::Receiver<()>,
    planner: Option<Arc<dyn SqlPlanner>>,
    launcher: Option<Arc<dyn EmbeddedWorkerLauncher>>,
    parked: ParkedReplies<ReplyTo>,
}

impl RequestHandler {
    /// Without a planner, every SQL statement fails.
    /// With a launcher the workers run in this process, otherwise they are remote.
    /// Panics after a successful statement when no receiver of `intent_tx` is left.
    pub fn new(
        receiver: async_channel::Receiver<Request>,
        db: Database,
        intent_tx: watch::Sender<()>,
        state_rx: watch::Receiver<()>,
        planner: Option<Arc<dyn SqlPlanner>>,
        launcher: Option<Arc<dyn EmbeddedWorkerLauncher>>,
    ) -> Self {
        Self {
            receiver,
            db,
            intent_tx,
            state_rx,
            planner,
            launcher,
            parked: ParkedReplies::default(),
        }
    }

    /// Returns once every sender of the request channel is dropped.
    pub async fn run(mut self) {
        info!("starting");
        loop {
            // The timer fires at least once per poll interval, also when no deadline is due,
            // so a catalog change without a notification delays a parked reply by at most that interval.
            let poll = Instant::now() + POLL_INTERVAL;
            let next_deadline = self.parked.next_deadline().unwrap_or(poll).min(poll);
            tokio::select! {
                recv_result = self.receiver.recv() => {
                    let Ok(request) = recv_result else {
                        info!("all clients have been dropped, shutting down...");
                        return;
                    };
                    self.handle(request).await;
                },
                Ok(()) = self.state_rx.changed() => self.try_release().await,
                () = tokio::time::sleep_until(next_deadline) => self.try_release().await,
            }
        }
    }

    async fn handle(&mut self, request: Request) {
        debug!("received: {request:?}");
        let result = match request.statement {
            Statement::Watch(read, wait) => return self.watch(read, wait, request.reply_to).await,
            Statement::Sql(sql) => self.execute_sql(sql).await,
            Statement::Parsed(statement) => self.execute(statement).await,
        };
        let succeeded = result.is_ok();
        // A failed statement was rolled back, so the controller has nothing to reconcile.
        if succeeded {
            // The controller holds the receiver while it runs, and the coordinator stops this handler when the controller returns.
            self.intent_tx
                .send(())
                .expect("intent channel should live as long as the request handler");
        }
        let _ = request.reply_to.send(result);
        // A statement can end a parked wait without any controller write,
        // for example a dropped worker, whose trigger fails its fragments.
        if succeeded {
            self.try_release().await;
        }
    }

    async fn watch(&mut self, mut read: GetQuery, wait: Wait, reply_to: ReplyTo) {
        let submitted_at = Utc::now();
        // This read only collects the ids, so it skips the fragments.
        // The reply rows come from the release check, which always reads the fragments.
        read.with_fragments = false;
        let ids: Vec<QueryId> = match read.execute(&self.db).await {
            Ok(rows) if !rows.is_empty() => rows.into_iter().map(|row| row.query.id).collect(),
            result => {
                let nothing = Watched {
                    rows: Vec::new(),
                    expired: false,
                };
                let _ = reply_to.send(result.map(|_| StatementResult::Watched(nothing)));
                return;
            }
        };
        // A timeout too large for the clock has no deadline, so the watch waits until its condition holds.
        let deadline = wait
            .timeout
            .and_then(|timeout| Instant::now().checked_add(timeout));
        let condition = match wait.until {
            Until::Running => Condition::Running,
            Until::Terminated => Condition::Terminated,
            Until::Fresh => Condition::FreshSince(submitted_at),
        };
        self.parked.park(ids, condition, deadline, reply_to);
        // The condition can already hold at submission (for example, a watch on terminated queries),
        // so the new entry is checked right away.
        self.try_release().await;
    }

    async fn try_release(&mut self) {
        self.parked.discard(ReplyTo::is_closed);
        // Without this check, every controller notification would cost a catalog read.
        if self.parked.is_empty() {
            return;
        }
        match self.observe(self.parked.query_ids()).await {
            Ok(observation) => {
                for (reply_to, watched) in self.parked.release(&observation) {
                    debug!("released: {watched:?}");
                    let _ = reply_to.send(Ok(StatementResult::Watched(watched)));
                }
            }
            // Without the read no condition can be checked, so only the expired replies are sent, with the read error.
            // The client can send the watch again.
            Err(e) => {
                debug!("catalog read failed, only expired replies are released: {e:#}");
                let message = format!("{e:#}");
                for reply_to in self.parked.take_expired(Instant::now()) {
                    let unavailable = CodedError::new(ErrorCode::CatalogUnavailable, &message);
                    let _ = reply_to.send(Err(unavailable.into()));
                }
            }
        }
    }

    async fn observe(&self, ids: Vec<QueryId>) -> anyhow::Result<Observation> {
        let rows = GetQuery::all()
            .with_ids(ids)
            .with_fragments()
            .execute(&self.db)
            .await?;
        // Many fragments share a host, and every listed host is one SQL parameter.
        let hosts: HashSet<_> = rows
            .iter()
            .flat_map(|row| &row.fragments)
            .map(|f| f.host_addr.clone())
            .collect();
        let workers = worker::Entity::find()
            .filter(worker::Column::HostAddr.is_in(hosts))
            .all(&self.db)
            .await?;
        Ok(Observation {
            at: Instant::now(),
            queries: rows.into_iter().map(|row| (row.query.id, row)).collect(),
            worker_states: workers
                .into_iter()
                .map(|w| (w.host_addr, w.current_state))
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::query::CreateQueryWithRefs;
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;
    use sea_orm::ConnectionTrait;

    #[tokio::test]
    async fn read_failure() {
        let db = Database::for_test().await;
        let mut req = any::<CreateQueryWithRefs>()
            .new_tree(&mut TestRunner::deterministic())
            .unwrap()
            .current();
        model::query::setup_refs(&db, &mut req).await;
        let id = req.query.execute(&db).await.unwrap().query.id;
        let (_, receiver) = async_channel::bounded(1);
        let (intent_tx, _) = watch::channel(());
        let (_, state_rx) = watch::channel(());
        let mut handler = RequestHandler::new(receiver, db, intent_tx, state_rx, None, None);
        let read = GetQuery::all().with_id(id);
        let (unbounded_to, mut unbounded) = oneshot::channel();
        let forever = Wait {
            until: Until::Terminated,
            timeout: Some(Duration::MAX),
        };
        handler.watch(read.clone(), forever, unbounded_to).await;
        // Only the release check reads the worker table, so the watch is parked before the read fails.
        handler
            .db
            .execute_unprepared("ALTER TABLE worker RENAME TO worker_gone")
            .await
            .unwrap();

        let (reply_to, reply) = oneshot::channel();
        let wait = Wait {
            until: Until::Terminated,
            timeout: Some(Duration::ZERO),
        };
        handler.watch(read, wait, reply_to).await;

        let err = reply.await.unwrap().unwrap_err();
        let code = err.downcast_ref::<CodedError>().map(|coded| coded.code);
        assert_eq!(code, Some(ErrorCode::CatalogUnavailable));
        assert!(unbounded.try_recv().is_err());
    }
}

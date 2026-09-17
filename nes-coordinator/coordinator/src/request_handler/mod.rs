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

//! The request handler, split by what can be tested without a database.
//!
//! This file is the loop that owns the side effects: the channels, the catalog reads, and the reply sends.
//! `execute.rs` runs one statement inside one catalog transaction.
//! `parking.rs` holds the rules for which replies wait and when they are released,
//! as functions over values, so its tests need neither a database nor a runtime.

mod execute;
mod parking;

pub use parking::{EarlyTermination, WaitTimeout};

use crate::SqlPlanner;
use controller::in_process::WorkerFactory;
use model::Execute;
use model::database::Database;
use model::identifier::QueryId;
use model::query::GetQuery;
use model::request::Request;
use model::statement::StatementResult;
use parking::{Observation, ParkedReplies, ReplyPlan, plan_reply};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, watch};
use tokio::time::Instant;
use tracing::{debug, info};

/// Owns the request channel and decides when each reply is sent.
///
/// Every statement runs against the catalog right away.
/// The reply can be parked: a create or a read waits for a target state, a drop or a read waits for termination,
/// and each is answered once the controller has moved the catalog far enough to satisfy the wait.
///
/// The loop wakes on a new request, on a notification that the controller wrote to the catalog,
/// or at the closest deadline.
/// On every wake-up it re-checks every parked reply and answers the ones that are ready.
pub(super) struct RequestHandler {
    receiver: async_channel::Receiver<Request>,
    db: Database,
    intent_tx: watch::Sender<()>,
    state_rx: watch::Receiver<()>,
    planner: Option<Arc<dyn SqlPlanner>>,
    factory: Option<Arc<dyn WorkerFactory>>,
    parked: ParkedReplies<oneshot::Sender<anyhow::Result<StatementResult>>>,
}

impl RequestHandler {
    pub(super) fn new(
        receiver: async_channel::Receiver<Request>,
        db: Database,
        intent_tx: watch::Sender<()>,
        state_rx: watch::Receiver<()>,
        planner: Option<Arc<dyn SqlPlanner>>,
        factory: Option<Arc<dyn WorkerFactory>>,
    ) -> Self {
        Self {
            receiver,
            db,
            intent_tx,
            state_rx,
            planner,
            factory,
            parked: ParkedReplies(Vec::new()),
        }
    }

    pub(super) async fn run(mut self) {
        info!("starting");
        loop {
            // With nothing due, the timer still fires once an hour,
            // so a catalog change that sends no notification is picked up eventually.
            let next_deadline = self
                .parked
                .next_deadline()
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(60 * 60));
            tokio::select! {
                recv_result = self.receiver.recv() => {
                    let Ok(req) = recv_result else {
                        info!("all clients have been dropped, shutting down...");
                        return;
                    };
                    self.handle(req).await;
                },
                Ok(()) = self.state_rx.changed() => {
                    self.release_parked().await;
                },
                () = tokio::time::sleep_until(next_deadline) => {
                    self.release_parked().await;
                },
            }
        }
    }

    async fn handle(&mut self, req: Request) {
        debug!("received: {req:?}");
        let Request {
            input,
            wait,
            reply_to,
        } = req;

        let result = self.execute(input).await;
        if result.is_ok() {
            self.intent_tx.send(()).expect("intent channel closed");
        }

        match result.map(|result| plan_reply(result, wait)) {
            Err(e) => {
                let _ = reply_to.send(Err(e));
            }
            Ok(ReplyPlan::Now(result)) => {
                let _ = reply_to.send(Ok(result));
            }
            Ok(ReplyPlan::Later(hold)) => {
                self.parked.park(hold, reply_to, Instant::now());
                // The condition can already hold at submission (a drop of terminated queries, say),
                // so the new entry is checked right away by the same pass that runs on later wake-ups.
                self.release_parked().await;
            }
        }
    }

    async fn release_parked(&mut self) {
        // The controller notifies after every fragment update, so the catalog is only read while a reply is waiting.
        if self.parked.is_empty() {
            return;
        }
        let Ok(observation) = self
            .observe(self.parked.query_ids())
            .await
            .inspect_err(|e| debug!("catalog read failed, parked replies stay parked: {e}"))
        else {
            return;
        };
        for (reply_to, outcome) in self.parked.release(&observation) {
            debug!("released: {outcome:?}");
            let _ = reply_to.send(outcome);
        }
    }

    async fn observe(&self, ids: Vec<QueryId>) -> anyhow::Result<Observation> {
        let rows = GetQuery::all()
            .with_ids(ids)
            .with_fragments()
            .execute(&self.db)
            .await?;
        Ok(Observation {
            at: Instant::now(),
            queries: rows.into_iter().map(|row| (row.query.id, row)).collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::parking::wait_resolved;
    use super::*;
    use model::error::{CodedError, ErrorCode};
    use model::query::query_fragment;
    use model::query::query_fragment::QueryFragmentState;
    use model::query::query_state::QueryState;
    use model::query::{CreateQueryWithRefs, DropQuery, GetQuery};
    use model::request::{Request, StatementInput, Wait};
    use model::statement::{Statement, StatementResult};
    use proptest::prelude::*;
    use sea_orm::sea_query::Expr;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
    use std::sync::Arc;

    struct TestHandle {
        rt: tokio::runtime::Runtime,
        sender: async_channel::Sender<Request>,
        db: Database,
        intent_rx: watch::Receiver<()>,
        state_tx: Arc<watch::Sender<()>>,
    }

    impl TestHandle {
        fn new(req: &mut CreateQueryWithRefs) -> Self {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap();
            let (sender, db, source_ids, sink_id, intent_rx, state_tx) = rt.block_on(async {
                use model::Execute;

                let db = Database::for_test().await;
                for w in &req.workers {
                    w.execute(&db).await.unwrap();
                }
                req.logical_source.execute(&db).await.unwrap();
                let mut source_ids = std::collections::BTreeSet::new();
                for ps in &req.physical_sources {
                    let m = ps.execute(&db).await.unwrap();
                    source_ids.insert(m.id);
                }
                let s = req.sink.execute(&db).await.unwrap();
                let (sender, receiver) = async_channel::bounded(16);
                let (intent_tx, intent_rx) = watch::channel(());
                let (state_tx, state_rx) = watch::channel(());
                let state_tx = Arc::new(state_tx);
                tokio::spawn(
                    RequestHandler::new(receiver, db.clone(), intent_tx, state_rx, None, None)
                        .run(),
                );
                (sender, db, source_ids, s.id, intent_rx, state_tx)
            });
            req.query.source_ids = source_ids;
            req.query.sink_ids = std::collections::BTreeSet::from([sink_id]);
            Self {
                rt,
                sender,
                db,
                intent_rx,
                state_tx,
            }
        }

        fn ask(&self, statement: Statement) -> anyhow::Result<StatementResult> {
            self.ask_with(statement, Wait::None)
        }

        fn ask_blocking(
            &self,
            statement: Statement,
            block_until: QueryState,
        ) -> anyhow::Result<StatementResult> {
            self.ask_with(
                statement,
                Wait::UntilState {
                    state: block_until,
                    timeout: None,
                },
            )
        }

        fn ask_with(&self, statement: Statement, wait: Wait) -> anyhow::Result<StatementResult> {
            let (rx, request) = Request::new(StatementInput::Parsed(statement), wait);
            self.sender
                .send_blocking(request)
                .expect("handler should be running");
            self.rt.block_on(rx)?
        }
    }

    /// Moves every fragment through the intermediate states that the catalog validation trigger requires before `target`.
    async fn transition_fragments(db: &Database, target: QueryFragmentState) {
        let steps = match target {
            QueryFragmentState::Started => vec![QueryFragmentState::Started],
            QueryFragmentState::Running => {
                vec![QueryFragmentState::Started, QueryFragmentState::Running]
            }
            QueryFragmentState::Completed => vec![
                QueryFragmentState::Started,
                QueryFragmentState::Running,
                QueryFragmentState::Completed,
            ],
            QueryFragmentState::Stopped => {
                vec![QueryFragmentState::Started, QueryFragmentState::Stopped]
            }
            QueryFragmentState::Failed => vec![QueryFragmentState::Failed],
            QueryFragmentState::Pending => vec![],
        };
        for step in steps {
            query_fragment::Entity::update_many()
                .col_expr(query_fragment::Column::CurrentState, Expr::value(step))
                .filter(query_fragment::Column::CurrentState.ne(step))
                .exec(db)
                .await
                .unwrap();
        }
    }

    proptest! {
        #[test]
        fn non_blocking_create_returns_pending(mut req in any::<CreateQueryWithRefs>()) {
            let handle = TestHandle::new(&mut req);
            let result = handle.ask(Statement::CreateQuery(req.query.clone())).unwrap();
            let query = match result {
                StatementResult::CreatedQuery(created) => created.query,
                other => panic!("expected CreatedQuery, got {other:?}"),
            };
            assert_eq!(query.state, QueryState::Pending);
            assert_eq!(query.sql, req.query.sql);
            assert_eq!(query.name, req.query.name);
        }

        #[test]
        fn blocking_create_resolves_correctly(
            mut req in any::<CreateQueryWithRefs>(),
            block_until in prop_oneof![
                Just(QueryState::Running),
                Just(QueryState::Completed),
            ],
        ) {
            let handle = TestHandle::new(&mut req);

            let mut intent_rx = handle.intent_rx.clone();
            let db = handle.db.clone();
            let state_tx = handle.state_tx.clone();
            handle.rt.spawn(async move {
                intent_rx.changed().await.unwrap();
                transition_fragments(&db, QueryFragmentState::Completed).await;
                state_tx.send(()).expect("state channel closed");
            });

            let result = handle.ask_blocking(Statement::CreateQuery(req.query), block_until).unwrap();
            let query = match result {
                StatementResult::CreatedQuery(created) => created.query,
                other => panic!("expected CreatedQuery, got {other:?}"),
            };
            assert!(wait_resolved(query.state, block_until));
        }

        #[test]
        fn blocking_create_early_termination(
            mut req in any::<CreateQueryWithRefs>(),
            terminal in prop_oneof![
                Just(QueryFragmentState::Stopped),
                Just(QueryFragmentState::Failed),
            ],
        ) {
            let handle = TestHandle::new(&mut req);

            let mut intent_rx = handle.intent_rx.clone();
            let db = handle.db.clone();
            let state_tx = handle.state_tx.clone();
            handle.rt.spawn(async move {
                intent_rx.changed().await.unwrap();
                transition_fragments(&db, terminal).await;
                state_tx.send(()).expect("state channel closed");
            });

            assert!(
                handle.ask_blocking(Statement::CreateQuery(req.query), QueryState::Completed).is_err(),
                "should return EarlyTermination for terminal={terminal:?}"
            );
        }

        #[test]
        fn non_blocking_drop_returns_immediately(mut req in any::<CreateQueryWithRefs>()) {
            let handle = TestHandle::new(&mut req);

            let result = handle.ask(Statement::CreateQuery(req.query)).unwrap();
            let StatementResult::CreatedQuery(created) = result else {
                panic!("expected CreatedQuery");
            };

            let drop_req = DropQuery::all()
                .with_filters(GetQuery::all().with_id(created.query.id));
            let result = handle.ask(Statement::DropQuery(drop_req)).unwrap();
            let StatementResult::DroppedQueries(dropped) = result else {
                panic!("expected DroppedQueries");
            };
            assert_eq!(dropped.len(), 1);
            assert_eq!(dropped[0].id, created.query.id);
        }

        #[test]
        fn blocking_drop_resolves_when_terminal(mut req in any::<CreateQueryWithRefs>()) {
            let handle = TestHandle::new(&mut req);

            let result = handle.ask(Statement::CreateQuery(req.query)).unwrap();
            let StatementResult::CreatedQuery(created) = result else {
                panic!("expected CreatedQuery");
            };

            let mut intent_rx = handle.intent_rx.clone();
            let db = handle.db.clone();
            let state_tx = handle.state_tx.clone();
            let query_id = created.query.id;
            handle.rt.spawn(async move {
                // The drop sends a second intent after the create's.
                // Wait for it before moving the fragments to terminal, so the handler sees the drop and the state change.
                intent_rx.changed().await.unwrap();
                transition_fragments(&db, QueryFragmentState::Stopped).await;
                state_tx.send(()).expect("state channel closed");
            });

            let drop_req = DropQuery::all()
                .with_filters(GetQuery::all().with_id(query_id));
            let result = handle.ask_with(
                Statement::DropQuery(drop_req),
                Wait::UntilTerminated { timeout: None },
            ).unwrap();
            let StatementResult::DroppedQueries(dropped) = result else {
                panic!("expected DroppedQueries");
            };
            assert!(dropped.iter().all(|q| q.state.is_terminal()));
        }

        #[test]
        fn blocking_create_times_out(mut req in any::<CreateQueryWithRefs>()) {
            let handle = TestHandle::new(&mut req);
            let err = handle.ask_with(
                Statement::CreateQuery(req.query),
                Wait::UntilState {
                    state: QueryState::Completed,
                    timeout: Some(Duration::from_millis(50)),
                },
            ).unwrap_err();
            let code = err.downcast_ref::<CodedError>().map(|coded| coded.code);
            assert_eq!(code, Some(ErrorCode::QueryWaitTimeout));
            assert!(matches!(
                err.downcast_ref::<WaitTimeout>(),
                Some(WaitTimeout(StatementResult::CreatedQuery(created))) if created.query.state == QueryState::Pending
            ));
        }
    }
}

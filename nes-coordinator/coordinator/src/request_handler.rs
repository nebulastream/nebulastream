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

use crate::SqlPlanner;
use chrono::{DateTime, Local};
use model::database::{Database, DatabaseTransaction};
use model::identifier::QueryId;
use model::query;
use model::query::query_state::QueryState;
use model::query::GetQuery;
use model::request::{Payload, Request, StatementInput};
use model::statement::{Statement, StatementResult};
use model::worker::endpoint::NetworkAddr;
use model::worker::{self, WorkerState};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{oneshot, watch};
use tokio::time::Instant;
use tracing::{debug, info};

/// Returned when a blocking create finds the query in a terminal state
/// that does not match the one the caller asked to wait for. There is
/// nothing left to wait for, and reporting success would hide that the
/// query never reached the requested state.
#[derive(Error, Debug)]
#[error("Query '{}' terminated early with state {}", .0.id, .0.state)]
struct EarlyTermination(query::Model);

struct PendingCreate {
    block_until: QueryState,
    reply_to: oneshot::Sender<anyhow::Result<StatementResult>>,
}

struct PendingDrop {
    query_ids: Vec<QueryId>,
    reply_to: oneshot::Sender<anyhow::Result<StatementResult>>,
}

struct PendingStatus {
    query: GetQuery,
    submitted_at: DateTime<Local>,
    deadline: Instant,
    reply_to: oneshot::Sender<anyhow::Result<StatementResult>>,
}

/// Owns the request channel and decides when each reply is sent.
///
/// Every statement runs against the catalog right away. The reply,
/// however, can be parked: a create that asks to block until the query
/// reaches a target state, a drop that asks to block until the query
/// terminates, and a status read that asks to poll for a window all
/// return their reply later — once the controller has driven the
/// catalog far enough for the wait to be satisfied.
///
/// The loop wakes on three signals: a new request, a notification that
/// the controller has written something to the catalog, or the closest
/// poll deadline. On every wake-up it re-checks every parked reply and
/// answers the ones that are now ready.
pub(super) struct RequestHandler {
    receiver: flume::Receiver<Request>,
    db: Database,
    intent_tx: watch::Sender<()>,
    state_rx: watch::Receiver<()>,
    planner: Option<Arc<dyn SqlPlanner>>,
    pending_query_creates: HashMap<QueryId, PendingCreate>,
    pending_query_drops: Vec<PendingDrop>,
    pending_polls: Vec<PendingStatus>,
}

impl RequestHandler {
    pub(super) fn new(
        receiver: flume::Receiver<Request>,
        db: Database,
        intent_tx: watch::Sender<()>,
        state_rx: watch::Receiver<()>,
        planner: Option<Arc<dyn SqlPlanner>>,
    ) -> Self {
        Self {
            receiver,
            db,
            intent_tx,
            state_rx,
            planner,
            pending_query_creates: HashMap::new(),
            pending_query_drops: Vec::new(),
            pending_polls: Vec::new(),
        }
    }

    pub(super) async fn run(mut self) {
        info!("starting");
        loop {
            let next_deadline = self.next_pending_deadline();
            tokio::select! {
                recv_result = self.receiver.recv_async() => {
                    let Ok(req) = recv_result else {
                        info!("all clients have been dropped, shutting down...");
                        return;
                    };
                    self.handle(req).await;
                },
                Ok(()) = self.state_rx.changed() => {
                    self.resolve_pending_queries().await;
                },
                () = tokio::time::sleep_until(next_deadline) => {
                    self.resolve_pending_queries().await;
                },
            }
        }
    }

    fn next_pending_deadline(&self) -> Instant {
        self.pending_polls
            .iter()
            .map(|p| p.deadline)
            .min()
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(60 * 60))
    }

    async fn execute(&self, input: StatementInput) -> anyhow::Result<StatementResult> {
        match input {
            StatementInput::Sql(sql) => {
                let planner = self
                    .planner
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("no SQL planner configured"))?;
                let txn = self.db.begin().await?;
                #[allow(deprecated)]
                let (statement, txn): (Statement, DatabaseTransaction) =
                    tokio::task::spawn_blocking(move || planner.plan(txn, &sql)).await??;
                let rsp = statement.execute_on(&txn).await?;
                txn.commit().await?;
                Ok(rsp)
            }
            StatementInput::Structured(statement) => {
                let txn = self.db.begin().await?;
                let rsp = statement.execute_on(&txn).await?;
                txn.commit().await?;
                Ok(rsp)
            }
        }
    }

    async fn handle(&mut self, req: Request) {
        debug!("received: {req:?}");
        let Request {
            payload:
                Payload {
                    input,
                    block_until,
                    block_until_dropped,
                    poll_for,
                },
            reply_to,
        } = req;

        // Pair the get-query filter with its caller-supplied poll window
        // before consuming `input`. Pairing them eliminates a second match
        // and a pair of correlated `Option`s further down.
        let poll = match (&input, poll_for) {
            (StatementInput::Structured(Statement::GetQuery(q)), Some(secs)) => {
                Some((q.clone(), secs))
            }
            _ => None,
        };

        let result = self.execute(input).await;

        if result.is_ok() {
            self.intent_tx.send(()).expect("intent channel closed");
        }

        match result {
            Ok(StatementResult::CreatedQuery(query, _)) if block_until != QueryState::default() => {
                self.pending_query_creates.insert(
                    query.id,
                    PendingCreate {
                        block_until,
                        reply_to,
                    },
                );
            }
            Ok(StatementResult::DroppedQueries(queries)) if block_until_dropped => {
                let query_ids: Vec<_> = queries.iter().map(|q| q.id).collect();
                if query_ids.is_empty() || queries.iter().all(|q| q.state.is_terminal()) {
                    info!("completed: DroppedQueries({} queries)", queries.len());
                    let _ = reply_to.send(Ok(StatementResult::DroppedQueries(queries)));
                } else {
                    self.pending_query_drops.push(PendingDrop {
                        query_ids,
                        reply_to,
                    });
                }
            }
            Ok(StatementResult::Queries(queries)) if poll.is_some() => {
                let (query, wait_secs) = poll.unwrap();
                let submitted_at = Local::now();
                let worker_states = self.worker_states_for(&queries).await;
                if status_satisfied(&queries, &worker_states, submitted_at) {
                    info!("completed: Queries({} queries)", queries.len());
                    reply_to
                        .send(Ok(StatementResult::Queries(queries)))
                        .expect("requester should be alive");
                } else {
                    self.pending_polls.push(PendingStatus {
                        query,
                        submitted_at,
                        deadline: Instant::now() + Duration::from_secs(wait_secs),
                        reply_to,
                    });
                }
            }
            Ok(resp) => {
                reply_to.send(Ok(resp)).expect("requester should be alive");
            }
            Err(e) => {
                reply_to.send(Err(e)).expect("requester should be alive");
            }
        }
    }

    async fn resolve_pending_queries(&mut self) {
        if self.pending_query_creates.is_empty()
            && self.pending_query_drops.is_empty()
            && self.pending_polls.is_empty()
        {
            return;
        }

        let mut all_ids: Vec<_> = self.pending_query_creates.keys().copied().collect();
        for pending in &self.pending_query_drops {
            all_ids.extend(&pending.query_ids);
        }

        let result = Statement::GetQuery(GetQuery::all().with_ids(all_ids))
            .execute_with(&self.db)
            .await;
        let Ok(StatementResult::Queries(query_list)) = result else {
            return;
        };
        let queries: HashMap<QueryId, (query::Model, Vec<query::fragment::Model>)> = query_list
            .into_iter()
            .map(|(query, fragments)| (query.id, (query, fragments)))
            .collect();

        let resolved_ids: Vec<_> = queries
            .iter()
            .filter(|(id, (query, _))| {
                self.pending_query_creates
                    .get(id)
                    .is_some_and(|pending| reached(query.state, pending.block_until))
            })
            .map(|(&id, _)| id)
            .collect();

        for id in resolved_ids {
            let pending = self.pending_query_creates.remove(&id).unwrap();
            let (query, fragments) = queries[&id].clone();

            // Only abnormal terminations (Stopped, Failed) count as
            // early — reaching Completed past a less-advanced wait
            // target is still a successful resolve from the caller's
            // point of view.
            let early_termination = matches!(query.state, QueryState::Stopped | QueryState::Failed)
                && query.state != pending.block_until;
            if early_termination {
                let _ = pending.reply_to.send(Err(EarlyTermination(query).into()));
            } else {
                info!("completed: CreatedQuery({})", query.id);
                let _ = pending
                    .reply_to
                    .send(Ok(StatementResult::CreatedQuery(query, fragments)));
            }
        }

        let pending_drops = std::mem::take(&mut self.pending_query_drops);
        for pending in pending_drops {
            let all_terminal = pending.query_ids.iter().all(|id| {
                queries
                    .get(id)
                    .is_some_and(|(query, _)| query.state.is_terminal())
            });
            if all_terminal {
                let dropped: Vec<_> = pending
                    .query_ids
                    .iter()
                    .filter_map(|id| queries.get(id).map(|(query, _)| query.clone()))
                    .collect();
                info!("completed: DroppedQueries({} queries)", dropped.len());
                let _ = pending
                    .reply_to
                    .send(Ok(StatementResult::DroppedQueries(dropped)));
            } else {
                self.pending_query_drops.push(pending);
            }
        }

        let pending_polls = std::mem::take(&mut self.pending_polls);
        for pending in pending_polls {
            let result = Statement::GetQuery(pending.query.clone())
                .execute_with(&self.db)
                .await;
            let now = Instant::now();
            match result {
                Ok(StatementResult::Queries(queries)) => {
                    let worker_states = self.worker_states_for(&queries).await;
                    if status_satisfied(&queries, &worker_states, pending.submitted_at)
                        || now >= pending.deadline
                    {
                        info!("completed: Queries({} queries)", queries.len());
                        let _ = pending.reply_to.send(Ok(StatementResult::Queries(queries)));
                    } else {
                        self.pending_polls.push(pending);
                    }
                }
                Ok(other) => {
                    let _ = pending.reply_to.send(Ok(other));
                }
                Err(e) => {
                    let _ = pending.reply_to.send(Err(e));
                }
            }
        }
    }

    async fn worker_states_for(
        &self,
        queries: &[(query::Model, Vec<query::fragment::Model>)],
    ) -> HashMap<NetworkAddr, WorkerState> {
        let hosts: Vec<NetworkAddr> = queries
            .iter()
            .flat_map(|(_, fragments)| fragments.iter().map(|f| f.host_addr.clone()))
            .collect();
        if hosts.is_empty() {
            return HashMap::new();
        }
        let workers = worker::Entity::find()
            .filter(worker::Column::HostAddr.is_in(hosts))
            .all(&self.db)
            .await
            .unwrap_or_default();
        workers
            .into_iter()
            .map(|w| (w.host_addr, w.current_state))
            .collect()
    }
}

/// True once `current` has either entered the requested state or terminated
/// in some other way the caller cannot make further progress past.
fn reached(current: QueryState, target: QueryState) -> bool {
    matches!(
        (current, target),
        (QueryState::Pending, QueryState::Pending)
            | (
                QueryState::Registered,
                QueryState::Pending | QueryState::Registered
            )
            | (
                QueryState::Running,
                QueryState::Pending | QueryState::Registered | QueryState::Running
            )
    ) || current.is_terminal()
}

/// True for a fragment whose status reply the caller can already trust:
/// either it has terminated, its host is not active (so further updates
/// cannot arrive), or it has been observed at least once after the poll was
/// submitted.
fn status_satisfied(
    queries: &[(query::Model, Vec<query::fragment::Model>)],
    worker_states: &HashMap<NetworkAddr, WorkerState>,
    submitted_at: DateTime<Local>,
) -> bool {
    queries.iter().all(|(_, fragments)| {
        fragments.iter().all(|f| {
            f.current_state.is_terminal()
                || worker_states.get(&f.host_addr) != Some(&WorkerState::Active)
                || f.last_observed_at.is_some_and(|t| t >= submitted_at)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::query::fragment;
    use model::query::fragment::FragmentState;
    use model::query::query_state::QueryState;
    use model::query::{CreateQueryWithRefs, DropQuery, GetQuery};
    use model::request::Request;
    use model::statement::{Statement, StatementResult};
    use proptest::prelude::*;
    use sea_orm::sea_query::Expr;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
    use std::sync::Arc;

    struct TestHandle {
        rt: tokio::runtime::Runtime,
        sender: flume::Sender<Request>,
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
                let mut source_ids = Vec::new();
                for ps in &req.physical_sources {
                    let m = ps.execute(&db).await.unwrap();
                    source_ids.push(m.id);
                }
                let s = req.sink.execute(&db).await.unwrap();
                let (sender, receiver) = flume::bounded(16);
                let (intent_tx, intent_rx) = watch::channel(());
                let (state_tx, state_rx) = watch::channel(());
                let state_tx = Arc::new(state_tx);
                tokio::spawn(
                    RequestHandler::new(receiver, db.clone(), intent_tx, state_rx, None).run(),
                );
                (sender, db, source_ids, s.id, intent_rx, state_tx)
            });
            req.query.source_ids = source_ids;
            req.query.sink_ids = vec![sink_id];
            Self {
                rt,
                sender,
                db,
                intent_rx,
                state_tx,
            }
        }

        fn ask(&self, statement: Statement) -> anyhow::Result<StatementResult> {
            self.ask_with(Payload::structured(statement))
        }

        fn ask_blocking(
            &self,
            statement: Statement,
            block_until: QueryState,
        ) -> anyhow::Result<StatementResult> {
            self.ask_with(Payload::structured(statement).block_until(block_until))
        }

        fn ask_with(&self, input: Payload) -> anyhow::Result<StatementResult> {
            let (rx, request) = Request::from(input);
            self.sender
                .send(request)
                .expect("handler should be running");
            self.rt.block_on(rx)?
        }
    }

    /// Walk a query's fragments through the intermediate states the catalog
    /// validation trigger requires to land them on `target`.
    async fn transition_fragments(db: &Database, target: FragmentState) {
        let steps = match target {
            FragmentState::Registered => vec![FragmentState::Registered],
            FragmentState::Running => vec![FragmentState::Registered, FragmentState::Running],
            FragmentState::Completed => vec![
                FragmentState::Registered,
                FragmentState::Running,
                FragmentState::Completed,
            ],
            FragmentState::Stopped => vec![FragmentState::Registered, FragmentState::Stopped],
            FragmentState::Failed => vec![FragmentState::Failed],
            FragmentState::Pending => vec![],
        };
        for step in steps {
            fragment::Entity::update_many()
                .col_expr(fragment::Column::CurrentState, Expr::value(step))
                .filter(fragment::Column::CurrentState.ne(step))
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
                StatementResult::CreatedQuery(q, _) => q,
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
                Just(QueryState::Registered),
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
                transition_fragments(&db, FragmentState::Completed).await;
                state_tx.send(()).expect("state channel closed");
            });

            let result = handle.ask_blocking(Statement::CreateQuery(req.query), block_until).unwrap();
            let query = match result {
                StatementResult::CreatedQuery(q, _) => q,
                other => panic!("expected CreatedQuery, got {other:?}"),
            };
            assert!(reached(query.state, block_until));
        }

        #[test]
        fn blocking_create_early_termination(
            mut req in any::<CreateQueryWithRefs>(),
            terminal in prop_oneof![
                Just(FragmentState::Stopped),
                Just(FragmentState::Failed),
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
            let StatementResult::CreatedQuery(created, _) = result else {
                panic!("expected CreatedQuery");
            };

            let drop_req = DropQuery::all()
                .with_filters(GetQuery::all().with_id(created.id));
            let result = handle.ask(Statement::DropQuery(drop_req)).unwrap();
            let StatementResult::DroppedQueries(dropped) = result else {
                panic!("expected DroppedQueries");
            };
            assert_eq!(dropped.len(), 1);
            assert_eq!(dropped[0].id, created.id);
        }

        #[test]
        fn blocking_drop_resolves_when_terminal(mut req in any::<CreateQueryWithRefs>()) {
            let handle = TestHandle::new(&mut req);

            let result = handle.ask(Statement::CreateQuery(req.query)).unwrap();
            let StatementResult::CreatedQuery(created, _) = result else {
                panic!("expected CreatedQuery");
            };

            let mut intent_rx = handle.intent_rx.clone();
            let db = handle.db.clone();
            let state_tx = handle.state_tx.clone();
            let query_id = created.id;
            handle.rt.spawn(async move {
                // Drop emits a second intent after the create; wait for it
                // before flipping fragments to terminal so the handler sees
                // the drop *and* the state change.
                intent_rx.changed().await.unwrap();
                transition_fragments(&db, FragmentState::Stopped).await;
                state_tx.send(()).expect("state channel closed");
            });

            let drop_req = DropQuery::all()
                .with_filters(GetQuery::all().with_id(query_id));
            let result = handle.ask_with(
                Payload::structured(Statement::DropQuery(drop_req)).block_until_dropped(),
            ).unwrap();
            let StatementResult::DroppedQueries(dropped) = result else {
                panic!("expected DroppedQueries");
            };
            assert!(dropped.iter().all(|q| q.state.is_terminal()));
        }
    }
}

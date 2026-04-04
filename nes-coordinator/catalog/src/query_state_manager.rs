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

use crate::database::Database;
use crate::notification::{NotificationChannel, Reconcilable};
use anyhow::Result;
use model::query::{QueryId, fragment};
use model::query;
#[cfg(test)]
use sea_orm::ActiveValue::Set;
use sea_orm::sea_query::Expr;
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, TransactionTrait};
use std::sync::Arc;
use tokio::sync::watch;

pub struct QueryStateManager {
    db: Database,
    listeners: NotificationChannel,
}

impl QueryStateManager {
    pub fn from(db: Database) -> Arc<Self> {
        Arc::new(Self {
            db,
            listeners: NotificationChannel::new(),
        })
    }

    pub async fn get_fragments(&self, query_id: i64) -> Result<Vec<fragment::Model>> {
        let (_, fragments) = self
            .db
            .with_retry(|conn| async move {
                query::Entity::find_by_id(query_id)
                    .find_with_related(fragment::Entity)
                    .all(&conn)
                    .await
            })
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("Query with id {query_id} not found"))?;
        Ok(fragments)
    }

    pub async fn update_fragment_states(
        &self,
        query_id: i64,
        updates: Vec<fragment::ActiveModel>,
    ) -> Result<(query::Model, Vec<fragment::Model>)> {
        debug_assert!(
            updates
                .iter()
                .all(|u| u.query_id.clone().unwrap() == query_id)
        );

        let result = self
            .db
            .with_retry(|conn| {
                let updates = updates.clone();
                async move {
                    conn.transaction::<_, (query::Model, Vec<fragment::Model>), sea_orm::DbErr>(
                        |txn| {
                            Box::pin(async move {
                                for fragment in updates {
                                    fragment.update(txn).await?;
                                }

                                let updated_query = query::Entity::find_by_id(query_id)
                                    .one(txn)
                                    .await?
                                    .ok_or(sea_orm::DbErr::RecordNotFound(format!(
                                        "Query {query_id} not found"
                                    )))?;

                                let fragments = fragment::Entity::find()
                                    .filter(fragment::Column::QueryId.eq(query_id))
                                    .all(txn)
                                    .await?;

                                Ok((updated_query, fragments))
                            })
                        },
                    )
                    .await
                    .map_err(|e| match e {
                        sea_orm::TransactionError::Connection(e)
                        | sea_orm::TransactionError::Transaction(e) => e,
                    })
                }
            })
            .await?;
        self.listeners.notify_state();
        Ok(result)
    }

    pub fn notify_intent(&self) {
        self.listeners.notify_intent();
    }
}

impl Reconcilable for QueryStateManager {
    type Model = query::Model;

    fn subscribe_intent(&self) -> watch::Receiver<()> {
        self.listeners.subscribe_intent()
    }
    fn subscribe_state(&self) -> watch::Receiver<()> {
        self.listeners.subscribe_state()
    }

    async fn get_mismatch(&self) -> Result<Vec<query::Model>> {
        Ok(self
            .db
            .with_retry(|conn| async move {
                query::Entity::find()
                    .filter(Expr::cust("current_state <> desired_state"))
                    .filter(Expr::cust(
                        "current_state NOT IN ('Completed', 'Stopped', 'Failed')",
                    ))
                    .all(&conn)
                    .await
            })
            .await?)
    }
}

impl QueryStateManager {
    pub async fn transition_to(
        &self,
        query_id: QueryId,
        fragments: &[fragment::Model],
        target: fragment::FragmentState,
    ) -> (query::Model, Vec<fragment::Model>) {
        use fragment::FragmentState::*;
        let path = match target {
            Pending => vec![],
            Registered => vec![Registered],
            Started => vec![Registered, Started],
            Running => vec![Registered, Started, Running],
            Completed => vec![Registered, Started, Running, Completed],
            Stopped => vec![Stopped],
            Failed => vec![Failed],
        };
        let mut current_query = query::Entity::find_by_id(query_id)
            .one(&self.db.conn)
            .await
            .unwrap()
            .expect("query not found");
        let mut current_fragments = fragments.to_vec();
        for state in path {
            let (q, f) = self
                .update_fragment_states(
                    query_id,
                    current_fragments
                        .iter()
                        .map(|f| f.with_state(state))
                        .collect(),
                )
                .await
                .unwrap();
            current_query = q;
            current_fragments = f;
        }
        (current_query, current_fragments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use crate::crud::execute_statement;
    use model::query::fragment::{FragmentError, FragmentState};
    use model::query::query_state::{DesiredQueryState, QueryState};
    use model::query::{CreateQueryWithRefs, DropQuery, GetQuery, StopMode};
    use model::request::{Statement, StatementResponse};
    use model::worker::GetWorker;
    use proptest::prelude::*;
    use sea_orm::sqlx::types::chrono;
    use std::collections::HashMap;
    use test_strategy::proptest;

    async fn setup(req: &mut CreateQueryWithRefs) -> (Database, Arc<QueryStateManager>) {
        let db = Database::for_test().await;
        let qsm = QueryStateManager::from(db.clone());
        for worker in &req.workers {
            execute_statement(&db, Statement::CreateWorker(worker.clone())).await.unwrap();
        }
        execute_statement(&db, Statement::CreateLogicalSource(req.logical_source.clone()))
            .await
            .unwrap();
        let mut source_ids = Vec::new();
        for ps in &req.physical_sources {
            let StatementResponse::CreatedPhysicalSource(m) =
                execute_statement(&db, Statement::CreatePhysicalSource(ps.clone()))
                    .await
                    .unwrap()
            else {
                panic!("expected CreatedPhysicalSource")
            };
            source_ids.push(m.id);
        }
        let StatementResponse::CreatedSink(s) =
            execute_statement(&db, Statement::CreateSink(req.sink.clone())).await.unwrap()
        else {
            panic!("expected CreatedSink")
        };
        req.query.source_ids = source_ids;
        req.query.sink_ids = vec![s.id];
        (db, qsm)
    }

    fn ts_from_ms(ms: i64) -> chrono::DateTime<chrono::Local> {
        chrono::DateTime::from_timestamp_millis(ms)
            .unwrap()
            .with_timezone(&chrono::Local)
    }

    #[proptest(async = "tokio")]
    async fn create_and_get_query(mut req: CreateQueryWithRefs) {
        let (db, _) = setup(&mut req).await;
        let StatementResponse::CreatedQuery(created) = execute_statement(&db,Statement::CreateQuery(req.query.clone())).await.unwrap() else { panic!("unexpected") };

        assert_eq!(created.name, req.query.name);
        assert_eq!(created.statement, req.query.sql);
        assert_eq!(created.current_state, QueryState::Pending);
        assert_eq!(created.desired_state, DesiredQueryState::Completed);

        let StatementResponse::Queries(results) = execute_statement(&db,Statement::GetQuery(GetQuery::all().with_id(created.id))).await.unwrap() else { panic!("unexpected") };
        let queries: Vec<query::Model> = results.into_iter().map(|(q, _)| q).collect();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0], created);
    }

    #[proptest(async = "tokio")]
    async fn drop_query(mut req: CreateQueryWithRefs, stop_mode: StopMode) {
        let (db, _) = setup(&mut req).await;
        let StatementResponse::CreatedQuery(created) = execute_statement(&db,Statement::CreateQuery(req.query)).await.unwrap() else { panic!("unexpected") };

        let drop_req = DropQuery::all()
            .with_stop_mode(stop_mode)
            .with_filters(GetQuery::all().with_id(created.id));
        let StatementResponse::DroppedQueries(updated) = execute_statement(&db,Statement::DropQuery(drop_req)).await.unwrap() else { panic!("unexpected") };

        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0].desired_state, DesiredQueryState::Stopped);
        assert_eq!(updated[0].stop_mode, Some(stop_mode));
    }

    #[proptest(async = "tokio")]
    async fn update_fragment_states_empty_noop(mut req: CreateQueryWithRefs) {
        let (db, qsm) = setup(&mut req).await;
        let StatementResponse::CreatedQuery(query) = execute_statement(&db,Statement::CreateQuery(req.query)).await.unwrap() else { panic!("unexpected") };
        let (returned, fragments) = qsm
            .update_fragment_states(query.id, vec![])
            .await
            .expect("empty update should succeed as no-op");
        assert_eq!(returned, query);
        assert!(fragments.into_iter().all(|f| f.current_state == FragmentState::Pending));
    }

    #[proptest(async = "tokio")]
    async fn fragment_negative_capacity_rejected(mut req: CreateQueryWithRefs) {
        req.query.fragments[0].num_operators = -1;
        let (db, _) = setup(&mut req).await;
        assert!(
            execute_statement(&db,Statement::CreateQuery(req.query)).await.is_err(),
            "fragment with negative used capacity should be rejected"
        );
    }

    #[proptest(async = "tokio")]
    async fn fragment_exceeding_capacity_rejected(mut req: CreateQueryWithRefs) {
        for w in &mut req.workers {
            w.max_operators = Some(w.max_operators.unwrap_or(0));
        }
        req.query.fragments[0].num_operators = req
            .workers.iter()
            .find(|w| w.host_addr == req.query.fragments[0].host_addr)
            .unwrap().max_operators.unwrap() + 1;

        let (db, _) = setup(&mut req).await;
        assert!(
            execute_statement(&db,Statement::CreateQuery(req.query)).await.is_err(),
            "Fragment exceeding worker capacity should be rejected"
        );
    }

    #[proptest(async = "tokio")]
    async fn fragment_creation_reserves_capacity(mut req: CreateQueryWithRefs) {
        let initial: HashMap<_, _> = req.workers.iter().map(|w| (w.host_addr.clone(), w.max_operators)).collect();
        let used: HashMap<_, i32> = req.query.fragments.iter().fold(HashMap::new(), |mut acc, f| {
            *acc.entry(f.host_addr.clone()).or_default() += f.num_operators;
            acc
        });

        let (db, _) = setup(&mut req).await;
        execute_statement(&db,Statement::CreateQuery(req.query)).await.expect("valid fragments should succeed");

        let StatementResponse::Workers(workers) = execute_statement(&db,Statement::GetWorker(GetWorker::all())).await.unwrap() else { panic!("unexpected") };
        assert!(workers.iter().all(|w| {
            let init = initial[&w.host_addr];
            let u = used.get(&w.host_addr).copied().unwrap_or(0);
            w.max_operators == init.map(|c| c - u)
        }));
    }

    #[proptest(async = "tokio")]
    async fn zero_capacity(mut req: CreateQueryWithRefs) {
        for w in &mut req.workers { w.max_operators = Some(0); }
        for f in &mut req.query.fragments { f.num_operators = 0; }

        let (db, _) = setup(&mut req).await;
        execute_statement(&db,Statement::CreateQuery(req.query)).await
            .expect("fragment with zero capacity on zero-capacity worker should succeed");
    }

    #[proptest(async = "tokio")]
    async fn capacity_released_on_fragment_terminal(
        mut req: CreateQueryWithRefs,
        #[strategy(prop_oneof![
            Just(FragmentState::Completed),
            Just(FragmentState::Stopped),
            Just(FragmentState::Failed),
        ])]
        terminal_state: FragmentState,
    ) {
        let (db, qsm) = setup(&mut req).await;
        let initial_total: i32 = req.workers.iter().filter_map(|w| w.max_operators).sum();

        let StatementResponse::CreatedQuery(query) = execute_statement(&db,Statement::CreateQuery(req.query)).await.unwrap() else { panic!("unexpected") };
        let fragments = qsm.get_fragments(query.id).await.unwrap();
        qsm.transition_to(query.id, &fragments, terminal_state).await;

        let StatementResponse::Workers(final_workers) = execute_statement(&db,Statement::GetWorker(GetWorker::all())).await.unwrap() else { panic!("unexpected") };
        let final_total: i32 = final_workers.iter().filter_map(|w| w.max_operators).sum();
        assert_eq!(initial_total, final_total, "capacity must be fully restored after terminal state");
    }

    #[proptest(async = "tokio")]
    async fn query_state_derived_from_fragments(mut req: CreateQueryWithRefs) {
        let (db, qsm) = setup(&mut req).await;
        let StatementResponse::CreatedQuery(query) = execute_statement(&db,Statement::CreateQuery(req.query)).await.unwrap() else { panic!("unexpected") };
        let mut fragments = qsm.get_fragments(query.id).await.unwrap();

        let steps: &[(FragmentState, QueryState)] = &[
            (FragmentState::Pending, QueryState::Pending),
            (FragmentState::Registered, QueryState::Registered),
            (FragmentState::Started, QueryState::Running),
            (FragmentState::Running, QueryState::Running),
            (FragmentState::Completed, QueryState::Completed),
        ];

        for &(fragment_state, query_state) in steps {
            let updates: Vec<_> = fragments.iter().map(|f| f.with_state(fragment_state)).collect();
            let (q, updated) = qsm.update_fragment_states(query.id, updates).await.unwrap();
            fragments = updated;
            assert_eq!(q.current_state, query_state);
        }
    }

    #[proptest(async = "tokio")]
    async fn fragments_stored_with_defaults(mut req: CreateQueryWithRefs) {
        let (db, qsm) = setup(&mut req).await;
        let StatementResponse::CreatedQuery(query) = execute_statement(&db,Statement::CreateQuery(req.query)).await.unwrap() else { panic!("unexpected") };
        let fragments = qsm.get_fragments(query.id).await.unwrap();

        assert!(fragments.iter().all(|f| {
            f.query_id == query.id
                && f.current_state == FragmentState::Pending
                && f.start_timestamp.is_none()
                && f.stop_timestamp.is_none()
                && f.error.is_none()
        }));
    }

    #[proptest(async = "tokio")]
    async fn fragments_reject_missing_worker(req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        assert!(
            execute_statement(&db, Statement::CreateQuery(req.query)).await.is_err(),
            "Fragments referencing non-existent workers should be rejected"
        );
    }

    #[proptest(async = "tokio")]
    async fn get_mismatch_query_correctness(mut req: CreateQueryWithRefs) {
        let (db, qsm) = setup(&mut req).await;
        let StatementResponse::CreatedQuery(query) = execute_statement(&db,Statement::CreateQuery(req.query)).await.unwrap() else { panic!("unexpected") };
        let fragments = qsm.get_fragments(query.id).await.unwrap();

        let mismatched = qsm.get_mismatch().await.unwrap();
        assert_eq!(mismatched.len(), 1, "query {:?} should be a mismatch", query);

        let (query, fragments) = qsm
            .transition_to(query.id, &fragments, FragmentState::Registered).await;
        let mismatched = qsm.get_mismatch().await.unwrap();
        assert_eq!(mismatched.len(), 1, "query {:?} should be a mismatch", query);

        qsm.transition_to(query.id, &fragments, FragmentState::Completed).await;
        let mismatched = qsm.get_mismatch().await.unwrap();
        assert!(mismatched.is_empty(), "query should no longer be a mismatch");
    }

    #[proptest(async = "tokio")]
    async fn one_failed_fragment_fails_query(mut req: CreateQueryWithRefs) {
        let (db, qsm) = setup(&mut req).await;
        let StatementResponse::CreatedQuery(query) = execute_statement(&db,Statement::CreateQuery(req.query)).await.unwrap() else { panic!("unexpected") };
        let fragments = qsm.get_fragments(query.id).await.unwrap();

        let (query, fragments) = qsm
            .transition_to(query.id, &fragments, FragmentState::Running).await;

        let mut updates: Vec<_> = fragments.iter().map(|f| f.with_state(FragmentState::Running)).collect();
        updates[0].current_state = Set(FragmentState::Failed);
        updates[0].error = Set(Some(FragmentError::WorkerCommunication {
            msg: "connection lost".to_string(),
        }));

        let (query, _) = qsm.update_fragment_states(query.id, updates).await.unwrap();
        assert_eq!(query.current_state, QueryState::Failed);
    }

    #[proptest(async = "tokio")]
    async fn worker_internal_error_aggregated(mut req: CreateQueryWithRefs) {
        let (db, qsm) = setup(&mut req).await;
        let StatementResponse::CreatedQuery(query) = execute_statement(&db,Statement::CreateQuery(req.query)).await.unwrap() else { panic!("unexpected") };
        let fragments = qsm.get_fragments(query.id).await.unwrap();

        let (query, fragments) = qsm
            .transition_to(query.id, &fragments, FragmentState::Running).await;

        let mut update: fragment::ActiveModel = fragments.first().unwrap().clone().into();
        update.current_state = Set(FragmentState::Failed);
        update.error = Set(Some(FragmentError::WorkerInternal {
            code: 42,
            msg: "segfault in operator".to_string(),
            trace: "stack trace here".to_string(),
        }));

        let (query, _) = qsm.update_fragment_states(query.id, vec![update]).await.unwrap();
        assert_eq!(query.current_state, QueryState::Failed);
        let error = query.error.expect("query should have aggregated error");
        let error_map = error.as_object().expect("error should be a JSON object");
        assert_eq!(
            error_map[&fragments.first().unwrap().host_addr.to_string()],
            serde_json::Value::String("segfault in operator".to_string())
        );
    }

    #[proptest(async = "tokio")]
    async fn start_timestamp_is_min(mut req: CreateQueryWithRefs) {
        let (db, qsm) = setup(&mut req).await;
        let StatementResponse::CreatedQuery(query) = execute_statement(&db,Statement::CreateQuery(req.query)).await.unwrap() else { panic!("unexpected") };
        let fragments = qsm.get_fragments(query.id).await.unwrap();
        let (_, fragments) = qsm
            .transition_to(query.id, &fragments, FragmentState::Started)
            .await;

        let updates: Vec<_> = fragments.iter().enumerate().map(|(i, f)| {
            let mut fragment: fragment::ActiveModel = f.clone().into();
            fragment.current_state = Set(FragmentState::Running);
            fragment.start_timestamp = Set(Some(ts_from_ms((i as i64 + 1) * 1000)));
            fragment
        }).collect();

        let (query, _) = qsm.update_fragment_states(query.id, updates).await.unwrap();
        assert_eq!(query.start_timestamp, Some(ts_from_ms(1000)));
        assert!(query.stop_timestamp.is_none());
    }

    #[proptest(async = "tokio")]
    async fn stop_timestamp_is_max(mut req: CreateQueryWithRefs) {
        let (db, qsm) = setup(&mut req).await;
        let StatementResponse::CreatedQuery(query) = execute_statement(&db,Statement::CreateQuery(req.query)).await.unwrap() else { panic!("unexpected") };
        let fragments = qsm.get_fragments(query.id).await.unwrap();
        let (_, fragments) = qsm
            .transition_to(query.id, &fragments, FragmentState::Running)
            .await;

        let updates: Vec<_> = fragments.iter().enumerate().map(|(i, f)| {
            let mut fragment: fragment::ActiveModel = f.clone().into();
            fragment.current_state = Set(FragmentState::Completed);
            fragment.stop_timestamp = Set(Some(ts_from_ms((i as i64 + 1) * 1000)));
            fragment
        }).collect();

        let (query, _) = qsm.update_fragment_states(query.id, updates).await.unwrap();
        assert_eq!(query.current_state, QueryState::Completed);
        assert_eq!(query.stop_timestamp, Some(ts_from_ms(fragments.len() as i64 * 1000)));
    }
}

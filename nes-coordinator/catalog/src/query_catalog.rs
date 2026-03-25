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
use model::query::query_state::DesiredQueryState;
use model::query::{CreateQuery, DropQuery, GetQuery, QueryId, fragment};
use model::{IntoCondition, query};
#[cfg(test)]
use sea_orm::ActiveValue::Set;
use sea_orm::sea_query::Expr;
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, TransactionTrait};
use std::sync::Arc;
use tokio::sync::watch;

pub struct QueryCatalog {
    db: Database,
    listeners: NotificationChannel,
}

impl QueryCatalog {
    pub fn from(db: Database) -> Arc<Self> {
        Arc::new(Self {
            db,
            listeners: NotificationChannel::new(),
        })
    }

    pub async fn create_query(&self, req: CreateQuery) -> Result<(query::Model, Vec<fragment::Model>)> {
        anyhow::ensure!(
            !req.fragments.is_empty(),
            "a query must have at least one fragment"
        );
        let fragments = req.fragments.clone();
        let result = self
            .db
            .with_retry(|conn| {
                let req = req.clone();
                let fragments = fragments.clone();
                async move {
                    conn.transaction::<_, (query::Model, Vec<fragment::Model>), sea_orm::DbErr>(|txn| {
                        Box::pin(async move {
                            let query = query::ActiveModel::from(req).insert(txn).await?;
                            fragment::Entity::insert_many(
                                fragments.into_iter().map(|f| f.into_active_model(query.id)),
                            )
                            .exec(txn)
                            .await?;
                            let fragments = fragment::Entity::find()
                                .filter(fragment::Column::QueryId.eq(query.id))
                                .all(txn)
                                .await?;
                            Ok((query, fragments))
                        })
                    })
                    .await
                    .map_err(|e| match e {
                        sea_orm::TransactionError::Connection(e)
                        | sea_orm::TransactionError::Transaction(e) => e,
                    })
                }
            })
            .await?;
        self.listeners.notify_intent();
        Ok(result)
    }

    pub async fn drop_query(&self, req: DropQuery) -> Result<Vec<query::Model>> {
        let updated = self
            .db
            .with_retry(|conn| {
                let req = req.clone();
                async move {
                    conn.transaction::<_, Vec<query::Model>, sea_orm::DbErr>(|txn| {
                        Box::pin(async move {
                            query::Entity::update_many()
                                .col_expr(
                                    query::Column::DesiredState,
                                    Expr::value(DesiredQueryState::Stopped),
                                )
                                .col_expr(query::Column::StopMode, Expr::value(req.stop_mode))
                                .filter(req.filters.clone().into_condition())
                                .exec(txn)
                                .await?;
                            query::Entity::find()
                                .filter(req.filters.into_condition())
                                .all(txn)
                                .await
                        })
                    })
                    .await
                    .map_err(|e| match e {
                        sea_orm::TransactionError::Connection(e)
                        | sea_orm::TransactionError::Transaction(e) => e,
                    })
                }
            })
            .await?;
        self.listeners.notify_intent();
        Ok(updated)
    }

    pub async fn get_query(&self, req: GetQuery) -> Result<Vec<query::Model>> {
        Ok(query::Entity::find()
            .filter(req.into_condition())
            .all(&self.db.conn)
            .await?)
    }

    pub async fn get_query_with_fragments(
        &self,
        req: GetQuery,
    ) -> Result<Vec<(query::Model, Vec<fragment::Model>)>> {
        Ok(self
            .db
            .with_retry(|conn| {
                let req = req.clone();
                async move {
                    query::Entity::find()
                        .filter(req.into_condition())
                        .find_with_related(fragment::Entity)
                        .all(&conn)
                        .await
                }
            })
            .await?)
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
                                for am in updates {
                                    am.update(txn).await?;
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
}

impl Reconcilable for QueryCatalog {
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

impl QueryCatalog {
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
        let mut current_query = self
            .get_query(GetQuery::all().with_id(query_id))
            .await
            .unwrap()
            .pop()
            .unwrap();
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
    use crate::Catalog;
    use model::query::fragment::{FragmentError, FragmentState};
    use model::query::query_state::QueryState;
    use model::query::{CreateQueryWithRefs, StopMode};
    use model::worker::GetWorker;
    use proptest::prelude::*;
    use sea_orm::sqlx::types::chrono;
    use std::collections::HashMap;
    use test_strategy::proptest;

    async fn setup(req: &CreateQueryWithRefs) -> Arc<Catalog> {
        let catalog = Catalog::for_test().await;
        for worker in &req.workers {
            catalog.worker.create_worker(worker.clone()).await.unwrap();
        }
        catalog
    }

    fn ts_from_ms(ms: i64) -> chrono::DateTime<chrono::Local> {
        chrono::DateTime::from_timestamp_millis(ms)
            .unwrap()
            .with_timezone(&chrono::Local)
    }

    #[proptest(async = "tokio")]
    async fn create_and_get_query(req: CreateQueryWithRefs) {
        let catalog = setup(&req).await;
        let (created, _) = catalog.query.create_query(req.query.clone()).await.unwrap();

        assert_eq!(created.name, req.query.name);
        assert_eq!(created.statement, req.query.sql_statement);
        assert_eq!(created.current_state, QueryState::Pending);
        assert_eq!(created.desired_state, DesiredQueryState::Completed);

        let queries = catalog.query.get_query(GetQuery::all().with_id(created.id)).await.unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0], created);
    }

    #[proptest(async = "tokio")]
    async fn drop_query(req: CreateQueryWithRefs, stop_mode: StopMode) {
        let catalog = setup(&req).await;
        let (created, _) = catalog.query.create_query(req.query).await.unwrap();

        let drop_req = DropQuery::all()
            .with_stop_mode(stop_mode)
            .with_filters(GetQuery::all().with_id(created.id));
        let updated = catalog.query.drop_query(drop_req).await.unwrap();

        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0].desired_state, DesiredQueryState::Stopped);
        assert_eq!(updated[0].stop_mode, Some(stop_mode));
    }

    #[proptest(async = "tokio")]
    async fn update_fragment_states_empty_noop(req: CreateQueryWithRefs) {
        let catalog = setup(&req).await;
        let (query, _) = catalog.query.create_query(req.query).await.unwrap();
        let (returned, fragments) = catalog
            .query
            .update_fragment_states(query.id, vec![])
            .await
            .expect("empty update should succeed as no-op");
        assert_eq!(returned, query);
        assert!(fragments.into_iter().all(|f| f.current_state == FragmentState::Pending));
    }

    #[proptest(async = "tokio")]
    async fn fragment_negative_capacity_rejected(mut req: CreateQueryWithRefs) {
        req.query.fragments[0].used_capacity = -1;
        let catalog = setup(&req).await;
        assert!(
            catalog.query.create_query(req.query).await.is_err(),
            "fragment with negative used capacity should be rejected"
        );
    }

    #[proptest(async = "tokio")]
    async fn fragment_exceeding_capacity_rejected(mut req: CreateQueryWithRefs) {
        for w in &mut req.workers {
            w.capacity = Some(w.capacity.unwrap_or(0));
        }
        req.query.fragments[0].used_capacity = req
            .workers.iter()
            .find(|w| w.host_addr == req.query.fragments[0].host_addr)
            .unwrap().capacity.unwrap() + 1;

        let catalog = setup(&req).await;
        assert!(
            catalog.query.create_query(req.query).await.is_err(),
            "Fragment exceeding worker capacity should be rejected"
        );
    }

    #[proptest(async = "tokio")]
    async fn fragment_creation_reserves_capacity(req: CreateQueryWithRefs) {
        let initial: HashMap<_, _> = req.workers.iter().map(|w| (&w.host_addr, w.capacity)).collect();
        let used: HashMap<_, i32> = req.query.fragments.iter().fold(HashMap::new(), |mut acc, f| {
            *acc.entry(f.host_addr.clone()).or_default() += f.used_capacity;
            acc
        });

        let catalog = setup(&req).await;
        catalog.query.create_query(req.query).await.expect("valid fragments should succeed");

        let workers = catalog.worker.get_worker(GetWorker::all()).await.unwrap();
        assert!(workers.iter().all(|w| {
            let init = initial[&w.host_addr];
            let u = used.get(&w.host_addr).copied().unwrap_or(0);
            w.capacity == init.map(|c| c - u)
        }));
    }

    #[proptest(async = "tokio")]
    async fn zero_capacity(mut req: CreateQueryWithRefs) {
        for w in &mut req.workers { w.capacity = Some(0); }
        for f in &mut req.query.fragments { f.used_capacity = 0; }

        let catalog = setup(&req).await;
        catalog.query.create_query(req.query).await
            .expect("fragment with zero capacity on zero-capacity worker should succeed");
    }

    #[proptest(async = "tokio")]
    async fn capacity_released_on_fragment_terminal(
        req: CreateQueryWithRefs,
        #[strategy(prop_oneof![
            Just(FragmentState::Completed),
            Just(FragmentState::Stopped),
            Just(FragmentState::Failed),
        ])]
        terminal_state: FragmentState,
    ) {
        let catalog = setup(&req).await;
        let initial_total: i32 = req.workers.iter().filter_map(|w| w.capacity).sum();

        let (query, fragments) = catalog.query.create_query(req.query).await.unwrap();
        catalog.query.transition_to(query.id, &fragments, terminal_state).await;

        let final_total: i32 = catalog.worker.get_worker(GetWorker::all()).await.unwrap()
            .iter().filter_map(|w| w.capacity).sum();
        assert_eq!(initial_total, final_total, "capacity must be fully restored after terminal state");
    }

    #[proptest(async = "tokio")]
    async fn query_state_derived_from_fragments(req: CreateQueryWithRefs) {
        let catalog = setup(&req).await;
        let (query, mut fragments) = catalog.query.create_query(req.query).await.unwrap();

        let steps: &[(FragmentState, QueryState)] = &[
            (FragmentState::Pending, QueryState::Pending),
            (FragmentState::Registered, QueryState::Registered),
            (FragmentState::Started, QueryState::Running),
            (FragmentState::Running, QueryState::Running),
            (FragmentState::Completed, QueryState::Completed),
        ];

        for &(fragment_state, query_state) in steps {
            let updates: Vec<_> = fragments.iter().map(|f| f.with_state(fragment_state)).collect();
            let (q, updated) = catalog.query.update_fragment_states(query.id, updates).await.unwrap();
            fragments = updated;
            assert_eq!(q.current_state, query_state);
        }
    }

    #[proptest(async = "tokio")]
    async fn fragments_stored_with_defaults(req: CreateQueryWithRefs) {
        let catalog = setup(&req).await;
        let (query, fragments) = catalog.query.create_query(req.query).await.unwrap();

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
        let catalog = Catalog::for_test().await;
        assert!(
            catalog.query.create_query(req.query).await.is_err(),
            "Fragments referencing non-existent workers should be rejected"
        );
    }

    #[proptest(async = "tokio")]
    async fn get_mismatch_query_correctness(req: CreateQueryWithRefs) {
        let catalog = setup(&req).await;
        let (query, fragments) = catalog.query.create_query(req.query).await.unwrap();

        let mismatched = catalog.query.get_mismatch().await.unwrap();
        assert_eq!(mismatched.len(), 1, "query {:?} should be a mismatch", query);

        let (query, fragments) = catalog.query
            .transition_to(query.id, &fragments, FragmentState::Registered).await;
        let mismatched = catalog.query.get_mismatch().await.unwrap();
        assert_eq!(mismatched.len(), 1, "query {:?} should be a mismatch", query);

        catalog.query.transition_to(query.id, &fragments, FragmentState::Completed).await;
        let mismatched = catalog.query.get_mismatch().await.unwrap();
        assert!(mismatched.is_empty(), "query should no longer be a mismatch");
    }

    #[proptest(async = "tokio")]
    async fn one_failed_fragment_fails_query(req: CreateQueryWithRefs) {
        let catalog = setup(&req).await;
        let (query, fragments) = catalog.query.create_query(req.query).await.unwrap();

        let (query, fragments) = catalog.query
            .transition_to(query.id, &fragments, FragmentState::Running).await;

        let mut updates: Vec<_> = fragments.iter().map(|f| f.with_state(FragmentState::Running)).collect();
        updates[0].current_state = Set(FragmentState::Failed);
        updates[0].error = Set(Some(FragmentError::WorkerCommunication {
            msg: "connection lost".to_string(),
        }));

        let (query, _) = catalog.query.update_fragment_states(query.id, updates).await.unwrap();
        assert_eq!(query.current_state, QueryState::Failed);
    }

    #[proptest(async = "tokio")]
    async fn worker_internal_error_aggregated(req: CreateQueryWithRefs) {
        let catalog = setup(&req).await;
        let (query, fragments) = catalog.query.create_query(req.query).await.unwrap();

        let (query, fragments) = catalog.query
            .transition_to(query.id, &fragments, FragmentState::Running).await;

        let mut update: fragment::ActiveModel = fragments.first().unwrap().clone().into();
        update.current_state = Set(FragmentState::Failed);
        update.error = Set(Some(FragmentError::WorkerInternal {
            code: 42,
            msg: "segfault in operator".to_string(),
            trace: "stack trace here".to_string(),
        }));

        let (query, _) = catalog.query.update_fragment_states(query.id, vec![update]).await.unwrap();
        assert_eq!(query.current_state, QueryState::Failed);
        let error = query.error.expect("query should have aggregated error");
        let error_map = error.as_object().expect("error should be a JSON object");
        assert_eq!(
            error_map[&fragments.first().unwrap().host_addr.to_string()],
            serde_json::Value::String("segfault in operator".to_string())
        );
    }

    #[proptest(async = "tokio")]
    async fn start_timestamp_is_min(req: CreateQueryWithRefs) {
        let catalog = setup(&req).await;
        let (query, fragments) = catalog.query.create_query(req.query).await.unwrap();
        let (_, fragments) = catalog.query
            .transition_to(query.id, &fragments, FragmentState::Started)
            .await;

        let updates: Vec<_> = fragments.iter().enumerate().map(|(i, f)| {
            let mut am: fragment::ActiveModel = f.clone().into();
            am.current_state = Set(FragmentState::Running);
            am.start_timestamp = Set(Some(ts_from_ms((i as i64 + 1) * 1000)));
            am
        }).collect();

        let (query, _) = catalog.query.update_fragment_states(query.id, updates).await.unwrap();
        assert_eq!(query.start_timestamp, Some(ts_from_ms(1000)));
        assert!(query.stop_timestamp.is_none());
    }

    #[proptest(async = "tokio")]
    async fn stop_timestamp_is_max(req: CreateQueryWithRefs) {
        let catalog = setup(&req).await;
        let (query, fragments) = catalog.query.create_query(req.query).await.unwrap();
        let (_, fragments) = catalog.query
            .transition_to(query.id, &fragments, FragmentState::Running)
            .await;

        let updates: Vec<_> = fragments.iter().enumerate().map(|(i, f)| {
            let mut am: fragment::ActiveModel = f.clone().into();
            am.current_state = Set(FragmentState::Completed);
            am.stop_timestamp = Set(Some(ts_from_ms((i as i64 + 1) * 1000)));
            am
        }).collect();

        let (query, _) = catalog.query.update_fragment_states(query.id, updates).await.unwrap();
        assert_eq!(query.current_state, QueryState::Completed);
        assert_eq!(query.stop_timestamp, Some(ts_from_ms(fragments.len() as i64 * 1000)));
    }
}

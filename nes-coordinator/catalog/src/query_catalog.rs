use crate::database::Database;
use crate::notification::{NotificationChannel, Reconcilable};
use anyhow::Result;
use model::query::fragment::CreateFragment;
#[cfg(test)]
use model::query::fragment::FragmentState;
use model::query::query_state::{DesiredQueryState, QueryState};
use model::query::{CreateQuery, DropQuery, GetQuery, fragment};
use model::{IntoCondition, query};
use sea_orm::sea_query::Expr;
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Related, Set, TransactionTrait};
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

    pub async fn create_query(&self, req: CreateQuery) -> Result<query::Model> {
        let query = query::ActiveModel::from(req).insert(&self.db.conn).await?;
        self.listeners.notify_intent();
        Ok(query)
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

    pub async fn create_fragments_for_query(
        &self,
        requests: Vec<CreateFragment>,
    ) -> Result<Vec<fragment::Model>> {
        anyhow::ensure!(
            !requests.is_empty(),
            "a query must have at least one fragment"
        );
        let query_id = requests.first().unwrap().query_id;
        debug_assert!(requests.iter().all(|r| r.query_id == query_id));

        let result = self
            .db
            .with_retry(|conn| {
                let requests = requests.clone();
                async move {
                    conn.transaction::<_, Vec<fragment::Model>, sea_orm::DbErr>(
                        |txn| {
                            Box::pin(async move {
                                fragment::Entity::insert_many(
                                    requests.into_iter().map(fragment::ActiveModel::from),
                                )
                                .exec(txn)
                                .await?;

                                fragment::Entity::find()
                                    .filter(fragment::Column::QueryId.eq(query_id))
                                    .all(txn)
                                    .await
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
        debug_assert!(updates.iter().all(|u| u.query_id.clone().unwrap() == query_id));

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

    #[cfg(test)]
    async fn set_query_state(
        &self,
        query: &query::Model,
        f: impl FnOnce(QueryState) -> QueryState,
    ) -> Result<query::Model> {
        let new_state = f(query.current_state);
        let updated = self
            .db
            .with_retry(|conn| {
                let query = query.clone();
                async move {
                    let mut am: query::ActiveModel = query.into();
                    am.current_state = Set(new_state);
                    am.update(&conn).await
                }
            })
            .await?;
        self.listeners.notify_state();
        Ok(updated)
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

#[cfg(test)]
impl QueryCatalog {
    async fn walk_fragments_to_state(
        &self,
        query_id: i64,
        fragments: &[fragment::Model],
        target: FragmentState,
    ) {
        let path = match target {
            FragmentState::Pending => vec![],
            FragmentState::Registered => vec![FragmentState::Registered],
            FragmentState::Started => vec![FragmentState::Registered, FragmentState::Started],
            FragmentState::Running => {
                vec![
                    FragmentState::Registered,
                    FragmentState::Started,
                    FragmentState::Running,
                ]
            }
            FragmentState::Completed => vec![
                FragmentState::Registered,
                FragmentState::Started,
                FragmentState::Running,
                FragmentState::Completed,
            ],
            FragmentState::Stopped => vec![FragmentState::Stopped],
            FragmentState::Failed => vec![FragmentState::Failed],
        };
        for state in path {
            let updates: Vec<_> = fragments.iter().map(|f| f.with_state(state)).collect();
            self.update_fragment_states(query_id, updates)
                .await
                .unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{test_prop, Catalog};
    use crate::testing::walk_query_via_fragments;
    use model::Generate;
    use model::query::StopMode;
    use model::query::fragment::ValidFragments;
    use model::query::fragment::{FragmentError, FragmentId, FragmentState};
    use model::query::query_state::QueryState;
    use model::worker::{CreateWorker, GetWorker};
    use proptest::prelude::*;
    use sea_orm::sqlx::types::chrono;
    use std::collections::HashMap;

    async fn prop_create_and_get_query(req: CreateQuery) {
        let catalog = Catalog::for_test().await.query.clone();
        let created = catalog.create_query(req.clone()).await.unwrap();

        assert_eq!(created.name, req.name);
        assert_eq!(created.statement, req.sql_statement);
        assert_eq!(created.current_state, QueryState::Pending);
        assert_eq!(created.desired_state, DesiredQueryState::Completed);

        let queries = catalog
            .get_query(GetQuery::all().with_name(req.name))
            .await
            .unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0], created);
    }

    async fn prop_drop_query(req: CreateQuery, stop_mode: StopMode) {
        let catalog = Catalog::for_test().await.query.clone();
        catalog.create_query(req.clone()).await.unwrap();

        let drop_req = DropQuery::all()
            .stop_mode(stop_mode)
            .with_filters(GetQuery::all().with_name(req.name));
        let updated = catalog.drop_query(drop_req).await.unwrap();

        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0].desired_state, DesiredQueryState::Stopped);
        assert_eq!(updated[0].stop_mode, Some(stop_mode));
    }

    async fn prop_update_fragment_states_empty_noop(req: CreateQuery) {
        let catalog = Catalog::for_test().await;
        let query = catalog.query.create_query(req).await.unwrap();
        let (returned, fragments) = catalog
            .query
            .update_fragment_states(query.id, vec![])
            .await
            .expect("empty update should succeed as no-op");
        assert_eq!(returned, query);
        assert!(fragments.is_empty());
    }

    async fn prop_fragment_negative_capacity_rejected(worker: CreateWorker, req: CreateQuery) {
        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(worker.clone()).await.unwrap();
        let query = catalog.query.create_query(req).await.unwrap();

        assert!(
            catalog
                .query
                .create_fragments_for_query(
                    vec![CreateFragment {
                        query_id: query.id,
                        host_addr: worker.host_addr,
                        grpc_addr: worker.grpc_addr,
                        plan: serde_json::json!({}),
                        used_capacity: -1,
                        has_source: false,
                    }]
                )
                .await
                .is_err(),
            "Fragment with negative used_capacity should be rejected"
        );
    }

    async fn prop_fragment_exceeding_capacity_rejected(worker: CreateWorker, req: CreateQuery) {
        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(worker.clone()).await.unwrap();
        let query = catalog.query.create_query(req).await.unwrap();

        assert!(
            catalog
                .query
                .create_fragments_for_query(
                    vec![CreateFragment {
                        query_id: query.id,
                        host_addr: worker.host_addr,
                        grpc_addr: worker.grpc_addr,
                        plan: serde_json::json!({}),
                        used_capacity: worker.capacity + 1,
                        has_source: false,
                    }]
                )
                .await
                .is_err(),
            "Fragment exceeding worker capacity should be rejected"
        );
    }

    async fn prop_fragment_exactly_exhausts_capacity(worker: CreateWorker, req: CreateQuery) {
        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(worker.clone()).await.unwrap();
        let query = catalog.query.create_query(req).await.unwrap();

        let half = worker.capacity / 2;
        let rest = worker.capacity - half;

        catalog
            .query
            .create_fragments_for_query(
                vec![
                    CreateFragment {
                        query_id: query.id,
                        host_addr: worker.host_addr.clone(),
                        grpc_addr: worker.grpc_addr.clone(),
                        plan: serde_json::json!({}),
                        used_capacity: half,
                        has_source: false,
                    },
                    CreateFragment {
                        query_id: query.id,
                        host_addr: worker.host_addr.clone(),
                        grpc_addr: worker.grpc_addr,
                        plan: serde_json::json!({}),
                        used_capacity: rest,
                        has_source: false,
                    },
                ],
            )
            .await
            .expect("fragments exactly exhausting capacity should succeed");

        let workers = catalog
            .worker
            .get_worker(GetWorker::all().with_host_addr(worker.host_addr))
            .await
            .unwrap();
        assert_eq!(workers[0].capacity, 0);
    }

    async fn prop_fragment_zero_capacity(worker: CreateWorker, req: CreateQuery) {
        let zero_worker = CreateWorker::new(worker.host_addr.clone(), worker.grpc_addr.clone(), 0);
        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(zero_worker).await.unwrap();
        let query = catalog.query.create_query(req).await.unwrap();

        catalog
            .query
            .create_fragments_for_query(
                vec![CreateFragment {
                    query_id: query.id,
                    host_addr: worker.host_addr,
                    grpc_addr: worker.grpc_addr,
                    plan: serde_json::json!({}),
                    used_capacity: 0,
                    has_source: false,
                }],
            )
            .await
            .expect("fragment with zero capacity on zero-capacity worker should succeed");
    }

    async fn prop_fragment_creation_reserves_capacity(req: CreateQuery, setup: ValidFragments) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let initial_capacities: HashMap<_, _> = setup
            .workers
            .iter()
            .map(|w| (w.host_addr.clone(), w.capacity))
            .collect();

        let query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(query.id);
        let expected_usage: HashMap<_, i32> =
            fragment_reqs.iter().fold(HashMap::new(), |mut acc, f| {
                *acc.entry(f.host_addr.clone()).or_default() += f.used_capacity;
                acc
            });

        catalog
            .query
            .create_fragments_for_query(fragment_reqs)
            .await
            .unwrap();

        let workers = catalog.worker.get_worker(GetWorker::all()).await.unwrap();
        for w in &workers {
            let initial = initial_capacities.get(&w.host_addr).copied().unwrap_or(0);
            let used = expected_usage.get(&w.host_addr).copied().unwrap_or(0);
            assert_eq!(
                w.capacity,
                initial - used,
                "Worker {} capacity should be {} - {} = {}",
                w.host_addr,
                initial,
                used,
                initial - used
            );
        }
    }

    async fn prop_capacity_released_on_fragment_terminal(
        req: CreateQuery,
        setup: ValidFragments,
        terminal_state: FragmentState,
    ) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let initial_total: i32 = setup.workers.iter().map(|w| w.capacity).sum();

        let query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(query.id);
        let total_used: i32 = fragment_reqs.iter().map(|f| f.used_capacity).sum();
        let fragments = catalog
            .query
            .create_fragments_for_query(fragment_reqs)
            .await
            .unwrap();

        let current_total = || {
            let catalog = catalog.clone();
            async move {
                let workers = catalog.worker.get_worker(GetWorker::all()).await.unwrap();
                workers.iter().map(|w| w.capacity).sum::<i32>()
            }
        };

        assert_eq!(
            current_total().await,
            initial_total - total_used,
            "Capacity should be reduced after fragment creation"
        );

        catalog
            .query
            .walk_fragments_to_state(query.id, &fragments, FragmentState::Running)
            .await;
        assert_eq!(
            current_total().await,
            initial_total - total_used,
            "Capacity should be unchanged during non-terminal transitions"
        );

        let terminal_updates: Vec<_> =
            fragments.iter().map(|f| f.with_state(terminal_state)).collect();
        catalog
            .query
            .update_fragment_states(query.id, terminal_updates)
            .await
            .unwrap();
        assert_eq!(
            current_total().await,
            initial_total,
            "Capacity should be fully restored after terminal state"
        );
    }

    async fn prop_query_state_path_valid(
        req: CreateQuery,
        setup: ValidFragments,
        path: Vec<QueryState>,
    ) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let created = catalog.query.create_query(req).await.unwrap();
        let models = walk_query_via_fragments(&catalog, &created, &setup, &path).await;

        for (model, expected_state) in models.iter().zip(path.iter()) {
            assert_eq!(model.current_state, *expected_state);
        }

        let results = catalog
            .query
            .get_query(GetQuery::all().with_id(created.id))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], *models.last().unwrap());
    }

    async fn prop_terminal_state_preserves_identity(
        req: CreateQuery,
        setup: ValidFragments,
        path: Vec<QueryState>,
    ) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let created = catalog.query.create_query(req.clone()).await.unwrap();
        let models = walk_query_via_fragments(&catalog, &created, &setup, &path).await;
        let final_model = models.last().unwrap();

        assert_eq!(final_model.name, req.name);
        assert_eq!(final_model.statement, req.sql_statement);
        assert!(final_model.current_state.is_terminal());
    }

    async fn prop_invalid_transitions_rejected(
        req: CreateQuery,
        setup: ValidFragments,
        path: Vec<QueryState>,
    ) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let created = catalog.query.create_query(req).await.unwrap();

        let non_terminal_path = &path[..path.len() - 1];
        let models = walk_query_via_fragments(&catalog, &created, &setup, non_terminal_path).await;
        let current = models.last().unwrap();

        let invalid_states = current.current_state.invalid_transitions();
        for invalid_target in invalid_states {
            assert!(
                catalog
                    .query
                    .set_query_state(&current, |_| invalid_target)
                    .await
                    .is_err(),
                "Transition {:?} -> {:?} should be rejected",
                current.current_state,
                invalid_target
            );
        }

        let refetched = catalog
            .query
            .get_query(GetQuery::all().with_id(created.id))
            .await
            .unwrap();
        assert_eq!(refetched.len(), 1);
        assert_eq!(refetched[0], *current);
    }

    async fn prop_fragments_stored_with_correct_defaults(req: CreateQuery, setup: ValidFragments) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let created_query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(created_query.id);

        let created = catalog
            .query
            .create_fragments_for_query(fragment_reqs.clone())
            .await
            .expect("fragment creation with valid refs should succeed");

        assert_eq!(created.len(), fragment_reqs.len());

        let mut seen_ids = std::collections::HashSet::new();
        for (model, req) in created.iter().zip(fragment_reqs.iter()) {
            assert_eq!(model.query_id, created_query.id);
            assert_eq!(model.host_addr, req.host_addr);
            assert_eq!(model.grpc_addr, req.grpc_addr);
            assert_eq!(model.used_capacity, req.used_capacity);
            assert_eq!(model.has_source, req.has_source);
            assert_eq!(model.plan, req.plan);
            assert_eq!(model.current_state, FragmentState::Pending);
            assert!(model.start_timestamp.is_none());
            assert!(model.stop_timestamp.is_none());
            assert!(model.error.is_none());

            assert!(
                seen_ids.insert(model.id),
                "Duplicate fragment_id {}",
                model.id
            );
        }
    }

    async fn prop_fragments_reject_missing_query(setup: ValidFragments) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let fragment_reqs = setup.create_fragments(i64::MAX);

        assert!(
            catalog
                .query
                .create_fragments_for_query(fragment_reqs)
                .await
                .is_err(),
            "Fragments referencing non-existent query should be rejected"
        );
    }

    async fn prop_fragments_reject_missing_worker(req: CreateQuery, setup: ValidFragments) {
        let catalog = Catalog::for_test().await.query.clone();
        let created_query = catalog.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(created_query.id);

        assert!(
            catalog
                .create_fragments_for_query(fragment_reqs)
                .await
                .is_err(),
            "Fragments referencing non-existent workers should be rejected"
        );
    }

    async fn prop_capacity_conserved_on_fragment_terminal(
        req: CreateQuery,
        setup: ValidFragments,
        terminal_state: FragmentState,
    ) {
        let catalog = Catalog::for_test().await;

        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let initial_total: i32 = setup.workers.iter().map(|w| w.capacity).sum();

        let created_query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(created_query.id);
        let fragments = catalog
            .query
            .create_fragments_for_query(fragment_reqs)
            .await
            .unwrap();

        catalog
            .query
            .walk_fragments_to_state(created_query.id, &fragments, terminal_state)
            .await;

        let workers = catalog
            .worker
            .get_worker(model::worker::GetWorker::all())
            .await
            .unwrap();
        let final_total: i32 = workers.iter().map(|w| w.capacity).sum();

        assert_eq!(
            initial_total, final_total,
            "Total capacity must be conserved: initial={}, final={}",
            initial_total, final_total,
        );
    }

    fn arb_fragment_state() -> impl Strategy<Value = FragmentState> {
        prop_oneof![
            Just(FragmentState::Pending),
            Just(FragmentState::Registered),
            Just(FragmentState::Started),
            Just(FragmentState::Running),
            Just(FragmentState::Completed),
            Just(FragmentState::Stopped),
            Just(FragmentState::Failed),
        ]
    }

    fn arb_forward_fragment_state() -> impl Strategy<Value = FragmentState> {
        prop_oneof![
            Just(FragmentState::Completed),
            Just(FragmentState::Stopped),
            Just(FragmentState::Failed),
        ]
    }

    async fn prop_update_fragment_states_applied(
        req: CreateQuery,
        setup: ValidFragments,
        target_state: FragmentState,
    ) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(query.id);
        let created = catalog
            .query
            .create_fragments_for_query(fragment_reqs)
            .await
            .unwrap();

        catalog
            .query
            .walk_fragments_to_state(query.id, &created, target_state)
            .await;

        let fetched = catalog.query.get_fragments(query.id).await.unwrap();
        for fragment in &fetched {
            assert_eq!(
                fragment.current_state, target_state,
                "Fragment {} should be {:?} but was {:?}",
                fragment.id, target_state, fragment.current_state
            );
        }
    }

    async fn prop_update_fragment_states_partial(
        req: CreateQuery,
        setup: ValidFragments,
        target_state: FragmentState,
    ) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(query.id);
        let created = catalog
            .query
            .create_fragments_for_query(fragment_reqs)
            .await
            .unwrap();

        catalog
            .query
            .walk_fragments_to_state(query.id, &created, FragmentState::Running)
            .await;

        let updated_ids: std::collections::HashSet<FragmentId> = created
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 2 == 0)
            .map(|(_, f)| f.id)
            .collect();

        let updates: Vec<fragment::ActiveModel> = created
            .iter()
            .filter(|f| updated_ids.contains(&f.id))
            .map(|f| f.with_state(target_state))
            .collect();

        catalog
            .query
            .update_fragment_states(query.id, updates)
            .await
            .unwrap();

        let fetched = catalog.query.get_fragments(query.id).await.unwrap();
        for fragment in &fetched {
            let expected = if updated_ids.contains(&fragment.id) {
                target_state
            } else {
                FragmentState::Running
            };
            assert_eq!(
                fragment.current_state, expected,
                "Fragment {} should be {:?} but was {:?}",
                fragment.id, expected, fragment.current_state
            );
        }
    }

    async fn prop_get_mismatch_query_correctness(queries: Vec<CreateQuery>, setup: ValidFragments) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }

        let mut created = Vec::new();
        for req in &queries {
            created.push(catalog.query.create_query(req.clone()).await.unwrap());
        }

        let completed_path = vec![
            QueryState::Pending,
            QueryState::Registered,
            QueryState::Running,
            QueryState::Completed,
        ];
        for (i, query) in created.into_iter().enumerate() {
            if i % 2 == 0 {
                walk_query_via_fragments(&catalog, &query, &setup, &completed_path).await;
            }
        }

        let mismatched = catalog.query.get_mismatch().await.unwrap();
        let all_queries = catalog.query.get_query(GetQuery::all()).await.unwrap();

        let mismatched_ids: std::collections::HashSet<_> =
            mismatched.iter().map(|q| q.id).collect();

        for query in &all_queries {
            let is_mismatched = query.current_state.to_string() != query.desired_state.to_string();
            let in_result = mismatched_ids.contains(&query.id);

            assert_eq!(
                is_mismatched,
                in_result,
                "Query '{}' (id={}): current={:?}, desired={:?}, \
                 is_mismatched={}, in_result={}",
                query.name,
                query.id,
                query.current_state,
                query.desired_state,
                is_mismatched,
                in_result
            );
        }
    }

    async fn prop_create_fragments_keeps_pending(req: CreateQuery, setup: ValidFragments) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let query = catalog.query.create_query(req).await.unwrap();
        assert_eq!(query.current_state, QueryState::Pending);

        let fragment_reqs = setup.create_fragments(query.id);
        catalog
            .query
            .create_fragments_for_query(fragment_reqs)
            .await
            .unwrap();

        let refetched = catalog
            .query
            .get_query(GetQuery::all().with_id(query.id))
            .await
            .unwrap();
        assert_eq!(refetched.len(), 1);
        assert_eq!(refetched[0].current_state, QueryState::Pending);
    }

    async fn prop_fragment_state_derives_query_state(req: CreateQuery, setup: ValidFragments) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(query.id);
        let fragments = catalog
            .query
            .create_fragments_for_query(fragment_reqs)
            .await
            .unwrap();

        let check_derived = |catalog: &Catalog, expected: QueryState| {
            let catalog = catalog.clone();
            let query_id = query.id;
            async move {
                let q = catalog
                    .query
                    .get_query(GetQuery::all().with_id(query_id))
                    .await
                    .unwrap();
                assert_eq!(
                    q[0].current_state, expected,
                    "Expected query state {:?} but got {:?}",
                    expected, q[0].current_state
                );
            }
        };

        let advance_all = |catalog: &Catalog, state: FragmentState| {
            let catalog = catalog.clone();
            let fragments = fragments.clone();
            let query_id = query.id;
            async move {
                let updates: Vec<_> = fragments
                    .iter()
                    .map(|f| f.with_state(state))
                    .collect();
                catalog
                    .query
                    .update_fragment_states(query_id, updates)
                    .await
                    .unwrap();
            }
        };

        advance_all(&catalog, FragmentState::Registered).await;
        check_derived(&catalog, QueryState::Registered).await;

        advance_all(&catalog, FragmentState::Started).await;
        check_derived(&catalog, QueryState::Running).await;

        advance_all(&catalog, FragmentState::Running).await;
        check_derived(&catalog, QueryState::Running).await;

        advance_all(&catalog, FragmentState::Completed).await;
        check_derived(&catalog, QueryState::Completed).await;
    }

    async fn prop_one_failed_fragment_fails_query(req: CreateQuery, setup: ValidFragments) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(query.id);
        let fragments = catalog
            .query
            .create_fragments_for_query(fragment_reqs)
            .await
            .unwrap();

        catalog
            .query
            .walk_fragments_to_state(query.id, &fragments, FragmentState::Running)
            .await;

        let mut updates: Vec<_> = fragments
            .iter()
            .map(|f| f.with_state(FragmentState::Running))
            .collect();
        updates[0].current_state = Set(FragmentState::Failed);
        updates[0].error = Set(Some(FragmentError::WorkerCommunication {
            msg: "connection lost".to_string(),
        }));

        let (query, _) = catalog
            .query
            .update_fragment_states(query.id, updates)
            .await
            .unwrap();
        assert_eq!(query.current_state, QueryState::Failed);
        let error = query.error.expect("query should have aggregated error");
        let error_map = error.as_object().expect("error should be a JSON object");
        let host_key = fragments[0].host_addr.to_string();
        assert!(
            error_map.contains_key(&host_key),
            "Error map should contain the failed fragment's host_addr"
        );
        assert_eq!(
            error_map[&host_key],
            serde_json::Value::String("connection lost".to_string())
        );
    }

    async fn prop_worker_internal_error_aggregated(req: CreateQuery, setup: ValidFragments) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(query.id);
        let fragments = catalog
            .query
            .create_fragments_for_query(fragment_reqs)
            .await
            .unwrap();

        catalog
            .query
            .walk_fragments_to_state(query.id, &fragments, FragmentState::Running)
            .await;

        let mut updates: Vec<fragment::ActiveModel> = fragments
            .iter()
            .map(|f| f.with_state(FragmentState::Running))
            .collect();
        updates[0].current_state = Set(FragmentState::Failed);
        updates[0].error = Set(Some(FragmentError::WorkerInternal {
            code: 42,
            msg: "segfault in operator".to_string(),
            trace: "stack trace here".to_string(),
        }));

        let (query, _) = catalog
            .query
            .update_fragment_states(query.id, updates)
            .await
            .unwrap();
        assert_eq!(query.current_state, QueryState::Failed);
        let error = query.error.expect("query should have aggregated error");
        let error_map = error.as_object().expect("error should be a JSON object");
        let host_key = fragments[0].host_addr.to_string();
        assert!(error_map.contains_key(&host_key));
        assert_eq!(
            error_map[&host_key],
            serde_json::Value::String("segfault in operator".to_string())
        );
    }

    async fn prop_all_fragments_failed_errors_aggregated(req: CreateQuery, setup: ValidFragments) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(query.id);
        let fragments = catalog
            .query
            .create_fragments_for_query(fragment_reqs)
            .await
            .unwrap();

        catalog
            .query
            .walk_fragments_to_state(query.id, &fragments, FragmentState::Running)
            .await;

        let updates: Vec<fragment::ActiveModel> = fragments
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let mut am: fragment::ActiveModel = f.clone().into();
                am.current_state = Set(FragmentState::Failed);
                am.error = Set(Some(if i % 2 == 0 {
                    FragmentError::WorkerCommunication {
                        msg: format!("connection lost on fragment {i}"),
                    }
                } else {
                    FragmentError::WorkerInternal {
                        code: i as u64,
                        msg: format!("internal error on fragment {i}"),
                        trace: "trace".to_string(),
                    }
                }));
                am
            })
            .collect();

        let (query, _) = catalog
            .query
            .update_fragment_states(query.id, updates)
            .await
            .unwrap();
        assert_eq!(query.current_state, QueryState::Failed);
        let error = query.error.expect("query should have aggregated error");
        let error_map = error.as_object().expect("error should be a JSON object");

        let unique_hosts: std::collections::HashSet<String> =
            fragments.iter().map(|f| f.host_addr.to_string()).collect();

        assert_eq!(
            error_map.len(),
            unique_hosts.len(),
            "Error map should have one entry per unique host_addr"
        );
        for host in &unique_hosts {
            assert!(
                error_map.contains_key(host),
                "Error map should contain host_addr {host}"
            );
            assert!(
                error_map[host].as_str().unwrap().contains("fragment"),
                "Error message should contain fragment-specific text"
            );
        }
    }

    fn ts_from_ms(ms: i64) -> chrono::DateTime<chrono::Local> {
        chrono::DateTime::from_timestamp_millis(ms)
            .unwrap()
            .with_timezone(&chrono::Local)
    }

    async fn prop_timestamps_propagated(req: CreateQuery, setup: ValidFragments) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(query.id);
        let fragments = catalog
            .query
            .create_fragments_for_query(fragment_reqs)
            .await
            .unwrap();

        catalog
            .query
            .walk_fragments_to_state(query.id, &fragments, FragmentState::Running)
            .await;

        const START_BASE_MS: i64 = 1_700_000_000_000;
        let updates: Vec<fragment::ActiveModel> = fragments
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let mut am: fragment::ActiveModel = f.clone().into();
                am.current_state = Set(FragmentState::Running);
                am.start_timestamp = Set(Some(ts_from_ms(START_BASE_MS + (i as i64) * 1000)));
                am
            })
            .collect();

        catalog
            .query
            .update_fragment_states(query.id, updates)
            .await
            .unwrap();

        let q = catalog
            .query
            .get_query(GetQuery::all().with_id(query.id))
            .await
            .unwrap();

        let expected_min_start = ts_from_ms(START_BASE_MS);
        assert_eq!(
            q[0].start_timestamp,
            Some(expected_min_start),
            "Query start_timestamp should be MIN of fragment start timestamps"
        );
        assert!(
            q[0].stop_timestamp.is_none(),
            "Query stop_timestamp should still be None (not terminal yet)"
        );

        const STOP_BASE_MS: i64 = 1_700_001_000_000;
        let stop_updates: Vec<fragment::ActiveModel> = fragments
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let mut am: fragment::ActiveModel = f.clone().into();
                am.current_state = Set(FragmentState::Completed);
                am.stop_timestamp = Set(Some(ts_from_ms(STOP_BASE_MS + (i as i64) * 1000)));
                am
            })
            .collect();

        catalog
            .query
            .update_fragment_states(query.id, stop_updates)
            .await
            .unwrap();

        let q = catalog
            .query
            .get_query(GetQuery::all().with_id(query.id))
            .await
            .unwrap();

        assert_eq!(q[0].current_state, QueryState::Completed);
        assert_eq!(
            q[0].start_timestamp,
            Some(expected_min_start),
            "start_timestamp should be preserved"
        );

        let expected_max_stop = ts_from_ms(STOP_BASE_MS + (fragments.len() as i64 - 1) * 1000);
        assert_eq!(
            q[0].stop_timestamp,
            Some(expected_max_stop),
            "Query stop_timestamp should be MAX of fragment stop timestamps"
        );
    }

    async fn prop_get_fragments_returns_created(req: CreateQuery, setup: ValidFragments) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(query.id);
        let created = catalog
            .query
            .create_fragments_for_query(fragment_reqs)
            .await
            .unwrap();

        let mut fetched = catalog.query.get_fragments(query.id).await.unwrap();
        fetched.sort_by_key(|f| f.id);
        let mut created = created;
        created.sort_by_key(|f| f.id);
        assert_eq!(fetched, created);
    }

    proptest! {
        #[test]
        fn create_and_get_query(req in CreateQuery::generate()) {
            test_prop(|| async move {
                prop_create_and_get_query(req).await;
            });
        }

        #[test]
        fn drop_query(req in CreateQuery::generate(), stop_mode in any::<StopMode>()) {
            test_prop(|| async move {
                prop_drop_query(req, stop_mode).await;
            });
        }

        #[test]
        fn update_fragment_states_empty_noop(req in CreateQuery::generate()) {
            test_prop(|| async move {
                prop_update_fragment_states_empty_noop(req).await;
            });
        }

        #[test]
        fn fragment_negative_capacity_rejected(
            worker in CreateWorker::generate(),
            req in CreateQuery::generate(),
        ) {
            test_prop(|| async move {
                prop_fragment_negative_capacity_rejected(worker, req).await;
            });
        }

        #[test]
        fn fragment_exceeding_capacity_rejected(
            worker in CreateWorker::generate(),
            req in CreateQuery::generate(),
        ) {
            test_prop(|| async move {
                prop_fragment_exceeding_capacity_rejected(worker, req).await;
            });
        }

        #[test]
        fn fragment_exactly_exhausts_capacity(
            worker in CreateWorker::generate(),
            req in CreateQuery::generate(),
        ) {
            test_prop(|| async move {
                prop_fragment_exactly_exhausts_capacity(worker, req).await;
            });
        }

        #[test]
        fn fragment_zero_capacity(
            worker in CreateWorker::generate(),
            req in CreateQuery::generate(),
        ) {
            test_prop(|| async move {
                prop_fragment_zero_capacity(worker, req).await;
            });
        }

        #[test]
        fn fragment_creation_reserves_capacity(
            req in CreateQuery::generate(),
            setup in ValidFragments::generate(),
        ) {
            test_prop(|| async move {
                prop_fragment_creation_reserves_capacity(req, setup).await;
            });
        }

        #[test]
        fn capacity_released_on_fragment_terminal(
            req in CreateQuery::generate(),
            setup in ValidFragments::generate(),
            terminal_state in prop_oneof![
                Just(FragmentState::Completed),
                Just(FragmentState::Stopped),
                Just(FragmentState::Failed),
            ],
        ) {
            test_prop(|| async move {
                prop_capacity_released_on_fragment_terminal(req, setup, terminal_state).await;
            });
        }

        #[test]
        fn query_state_path_valid(
            req in CreateQuery::generate(),
            setup in ValidFragments::generate(),
            path in <Vec<QueryState> as Generate>::generate(),
        ) {
            test_prop(|| async move {
                prop_query_state_path_valid(req, setup, path).await;
            });
        }

        #[test]
        fn terminal_state_preserves_identity(
            req in CreateQuery::generate(),
            setup in ValidFragments::generate(),
            path in <Vec<QueryState> as Generate>::generate(),
        ) {
            test_prop(|| async move {
                prop_terminal_state_preserves_identity(req, setup, path).await;
            });
        }

        #[test]
        fn invalid_transitions_rejected(
            req in CreateQuery::generate(),
            setup in ValidFragments::generate(),
            path in <Vec<QueryState> as Generate>::generate(),
        ) {
            test_prop(|| async move {
                prop_invalid_transitions_rejected(req, setup, path).await;
            });
        }

        #[test]
        fn fragments_stored_with_correct_defaults(req in CreateQuery::generate(), setup in ValidFragments::generate()) {
            test_prop(|| async move {
                prop_fragments_stored_with_correct_defaults(req, setup).await;
            });
        }

        #[test]
        fn fragments_reject_missing_query(setup in ValidFragments::generate()) {
            test_prop(|| async move {
                prop_fragments_reject_missing_query(setup).await;
            });
        }

        #[test]
        fn fragments_reject_missing_worker(req in CreateQuery::generate(), setup in ValidFragments::generate()) {
            test_prop(|| async move {
                prop_fragments_reject_missing_worker(req, setup).await;
            });
        }

        #[test]
        fn capacity_conserved_on_fragment_terminal(
            req in CreateQuery::generate(),
            setup in ValidFragments::generate(),
            terminal_state in prop_oneof![
                Just(FragmentState::Completed),
                Just(FragmentState::Stopped),
                Just(FragmentState::Failed),
            ],
        ) {
            test_prop(|| async move {
                prop_capacity_conserved_on_fragment_terminal(req, setup, terminal_state).await;
            });
        }

        #[test]
        fn update_fragment_states_applied(
            req in CreateQuery::generate(),
            setup in ValidFragments::generate(),
            target_state in arb_fragment_state(),
        ) {
            test_prop(|| async move {
                prop_update_fragment_states_applied(req, setup, target_state).await;
            });
        }

        #[test]
        fn update_fragment_states_partial(
            req in CreateQuery::generate(),
            setup in ValidFragments::generate(),
            target_state in arb_forward_fragment_state(),
        ) {
            test_prop(|| async move {
                prop_update_fragment_states_partial(req, setup, target_state).await;
            });
        }

        #[test]
        fn get_mismatch_query_correctness(
            queries in prop::collection::vec(CreateQuery::generate(), 1..=5usize),
            setup in ValidFragments::generate(),
        ) {
            test_prop(|| async move {
                prop_get_mismatch_query_correctness(queries, setup).await;
            });
        }

        #[test]
        fn get_fragments_returns_created(
            req in CreateQuery::generate(),
            setup in ValidFragments::generate(),
        ) {
            test_prop(|| async move {
                prop_get_fragments_returns_created(req, setup).await;
            });
        }

        #[test]
        fn create_fragments_keeps_pending(
            req in CreateQuery::generate(),
            setup in ValidFragments::generate(),
        ) {
            test_prop(|| async move {
                prop_create_fragments_keeps_pending(req, setup).await;
            });
        }

        #[test]
        fn fragment_state_derives_query_state(
            req in CreateQuery::generate(),
            setup in ValidFragments::generate(),
        ) {
            test_prop(|| async move {
                prop_fragment_state_derives_query_state(req, setup).await;
            });
        }

        #[test]
        fn one_failed_fragment_fails_query(
            req in CreateQuery::generate(),
            setup in ValidFragments::generate(),
        ) {
            test_prop(|| async move {
                prop_one_failed_fragment_fails_query(req, setup).await;
            });
        }

        #[test]
        fn worker_internal_error_aggregated(
            req in CreateQuery::generate(),
            setup in ValidFragments::generate(),
        ) {
            test_prop(|| async move {
                prop_worker_internal_error_aggregated(req, setup).await;
            });
        }

        #[test]
        fn all_fragments_failed_errors_aggregated(
            req in CreateQuery::generate(),
            setup in ValidFragments::generate(),
        ) {
            test_prop(|| async move {
                prop_all_fragments_failed_errors_aggregated(req, setup).await;
            });
        }

        #[test]
        fn timestamps_propagated(
            req in CreateQuery::generate(),
            setup in ValidFragments::generate(),
        ) {
            test_prop(|| async move {
                prop_timestamps_propagated(req, setup).await;
            });
        }
    }
}

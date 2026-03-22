use crate::database::Database;
use crate::notification::{NotificationChannel, Reconcilable};
use anyhow::{Result, ensure};
use model::IntoCondition;
use model::worker::network_link;
use model::worker::topology::TopologyGraph;
use model::worker::{
    self, CreateWorker, DesiredWorkerState, DropWorker, Entity as WorkerEntity, GetWorker,
    WorkerState,
};
use sea_orm::sea_query::Expr;
use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, EntityTrait, QueryFilter, TransactionTrait,
};
use std::sync::Arc;

pub struct WorkerCatalog {
    db: Database,
    listeners: NotificationChannel,
}

impl WorkerCatalog {
    pub fn from(db: Database) -> Arc<Self> {
        Arc::new(Self {
            db,
            listeners: NotificationChannel::new(),
        })
    }

    pub async fn create_worker(&self, req: CreateWorker) -> Result<worker::Model> {
        let worker = self.db.with_retry(|conn| {
            let req = req.clone();
            async move {
                conn.transaction::<_, worker::Model, sea_orm::DbErr>(|txn| {
                    Box::pin(async move {
                        let host_addr = req.host_addr.clone();
                        let peers = req.peers.clone();
                        let worker = worker::ActiveModel::from(req).insert(txn).await?;

                        if !peers.is_empty() {
                            network_link::Entity::insert_many(peers.into_iter().map(|peer| {
                                network_link::ActiveModel {
                                    source_host_addr: Set(host_addr.clone()),
                                    target_host_addr: Set(peer),
                                }
                            }))
                            .exec(txn)
                            .await?;
                        }

                        Ok(worker)
                    })
                })
                .await
                .map_err(|e| match e {
                    sea_orm::TransactionError::Connection(e) => e,
                    sea_orm::TransactionError::Transaction(e) => e,
                })
            }
        }).await?;

        self.listeners.notify_intent();
        Ok(worker)
    }

    pub async fn get_worker(&self, req: GetWorker) -> Result<Vec<worker::Model>> {
        Ok(WorkerEntity::find()
            .filter(req.into_condition())
            .all(&self.db.conn)
            .await?)
    }

    pub async fn drop_worker(&self, req: DropWorker) -> Result<Option<worker::Model>> {
        let worker = WorkerEntity::find_by_id(req.host_addr)
            .one(&self.db.conn)
            .await?;
        let Some(worker) = worker else {
            return Ok(None);
        };
        let mut active: worker::ActiveModel = worker.into();
        active.desired_state = Set(DesiredWorkerState::Removed);
        let updated = active.update(&self.db.conn).await?;
        self.listeners.notify_intent();
        Ok(Some(updated))
    }

    pub async fn mark_worker(
        &self,
        mut worker: worker::ActiveModel,
        new_state: WorkerState,
    ) -> Result<worker::Model> {
        worker.current_state = Set(new_state);
        let updated = worker.update(&self.db.conn).await?;
        self.listeners.notify_state();
        Ok(updated)
    }

    pub async fn get_topology(&self) -> Result<TopologyGraph> {
        let workers = WorkerEntity::find().all(&self.db.conn).await?;
        let links = network_link::Entity::find().all(&self.db.conn).await?;
        let edges = links
            .into_iter()
            .map(|l| (l.source_host_addr, l.target_host_addr))
            .collect();
        Ok(TopologyGraph::from_parts(workers, edges)?)
    }
}

impl Reconcilable for WorkerCatalog {
    type Model = worker::Model;

    fn subscribe_intent(&self) -> tokio::sync::watch::Receiver<()> {
        self.listeners.subscribe_intent()
    }

    fn subscribe_state(&self) -> tokio::sync::watch::Receiver<()> {
        self.listeners.subscribe_state()
    }

    async fn get_mismatch(&self) -> Result<Vec<worker::Model>> {
        Ok(WorkerEntity::find()
            .filter(Expr::cust("current_state <> desired_state"))
            .all(&self.db.conn)
            .await?)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use super::*;
    use crate::{test_prop, Catalog};
    use model::Generate;
    use model::query::CreateQuery;
    use model::query::fragment::CreateFragment;
    use model::worker::topology::CycleDetected;
    use proptest::prelude::*;

    /// Upper bound on workers generated per test case. Kept low because potential
    /// edges grow quadratically (n*(n-1)/2), so larger values slow proptest
    /// shrinking and make minimal failing cases harder to find.
    const MAX_TEST_WORKERS: usize = 8;

    async fn prop_create_and_get_worker(req: CreateWorker) {
        let catalog = Catalog::for_test().await;

        let created = catalog
            .worker
            .create_worker(req.clone())
            .await
            .expect("worker creation should succeed");

        assert_eq!(created.host_addr, req.host_addr);
        assert_eq!(created.grpc_addr, req.grpc_addr);
        assert_eq!(created.capacity, req.capacity);
        assert_eq!(created.current_state, WorkerState::Pending);
        assert_eq!(created.desired_state, DesiredWorkerState::Active);

        let workers = catalog
            .worker
            .get_worker(GetWorker::all().with_host_addr(req.host_addr.clone()))
            .await
            .expect("get worker should succeed");

        assert_eq!(workers.len(), 1);
        assert_eq!(workers.first().unwrap().host_addr, req.host_addr);
    }

    async fn prop_drop_and_remove_worker(req: CreateWorker) {
        let catalog = Catalog::for_test().await;
        let created = catalog.worker.create_worker(req.clone()).await.unwrap();

        let updated = catalog
            .worker
            .drop_worker(DropWorker::new(req.host_addr.clone()))
            .await
            .expect("drop should succeed")
            .expect("worker should exist");
        assert_eq!(updated.desired_state, DesiredWorkerState::Removed);

        let removed = catalog
            .worker
            .mark_worker(created.into(), WorkerState::Removed)
            .await
            .expect("remove should succeed");
        assert_eq!(removed.current_state, WorkerState::Removed);

        let workers = catalog
            .worker
            .get_worker(GetWorker::all().with_host_addr(req.host_addr))
            .await
            .unwrap();
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].current_state, WorkerState::Removed);
    }

    async fn prop_mark_worker_state(req: CreateWorker) {
        let catalog = Catalog::for_test().await;
        let created = catalog.worker.create_worker(req.clone()).await.unwrap();

        catalog
            .worker
            .mark_worker(created.into(), WorkerState::Active)
            .await
            .expect("mark should succeed");

        let workers = catalog
            .worker
            .get_worker(GetWorker::all().with_host_addr(req.host_addr))
            .await
            .unwrap();
        assert_eq!(workers.first().unwrap().current_state, WorkerState::Active);
    }

    async fn prop_host_grpc_neq(req: CreateWorker) {
        let catalog = Catalog::for_test().await;
        let same_addr = CreateWorker::new(req.host_addr.clone(), req.host_addr, req.capacity);

        assert!(
            catalog.worker.create_worker(same_addr).await.is_err(),
            "Worker creation should fail when host_addr equals grpc_addr"
        );
    }

    async fn prop_grpc_addr_unique(w1: CreateWorker, mut w2: CreateWorker) {
        if w1.host_addr == w2.host_addr {
            return;
        }
        w2.grpc_addr = w1.grpc_addr.clone();
        if w2.host_addr == w2.grpc_addr {
            return;
        }

        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(w1).await.unwrap();
        assert!(
            catalog.worker.create_worker(w2).await.is_err(),
            "Duplicate grpc_addr should be rejected"
        );
    }

    async fn prop_worker_drop_blocked_by_active_fragments(req: CreateWorker) {
        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(req.clone()).await.unwrap();

        let query = catalog
            .query
            .create_query(CreateQuery::new("SELECT x FROM y".to_string()))
            .await
            .unwrap();
        catalog
            .query
            .create_fragments_for_query(
                vec![CreateFragment {
                    query_id: query.id,
                    host_addr: req.host_addr.clone(),
                    grpc_addr: req.grpc_addr,
                    plan: serde_json::json!({}),
                    used_capacity: 0,
                    has_source: false,
                }],
            )
            .await
            .unwrap();

        assert!(
            catalog
                .worker
                .drop_worker(DropWorker::new(req.host_addr))
                .await
                .is_err(),
            "Worker with active fragments should not be droppable"
        );
    }

    async fn prop_worker_self_peer_rejected(req: CreateWorker) {
        let catalog = Catalog::for_test().await;
        let worker = req.clone().with_peers(vec![req.host_addr]);

        assert!(
            catalog.worker.create_worker(worker).await.is_err(),
            "Worker referencing itself as peer should be rejected"
        );
    }

    async fn prop_duplicate_peer_rejected(w1: CreateWorker, w2: CreateWorker) {
        if w1.host_addr == w2.host_addr || w1.grpc_addr == w2.grpc_addr {
            return;
        }

        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(w2.clone()).await.unwrap();

        let w1_dup_peer = w1.with_peers(vec![w2.host_addr.clone(), w2.host_addr]);
        assert!(
            catalog.worker.create_worker(w1_dup_peer).await.is_err(),
            "Duplicate peer entries should violate composite PK"
        );
    }

    async fn prop_worker_addr_unique(req: CreateWorker) {
        let catalog = Catalog::for_test().await;

        catalog
            .worker
            .create_worker(req.clone())
            .await
            .expect("first worker creation should succeed");

        assert!(
            catalog.worker.create_worker(req.clone()).await.is_err(),
            "Duplicate worker host_addr '{}' should be rejected",
            req.host_addr
        );
    }

    async fn prop_mismatch_correct(workers: Vec<CreateWorker>) {
        let catalog = Catalog::for_test().await;

        let mut created: Vec<worker::Model> = Vec::new();
        for worker in &workers {
            let model = catalog.worker.create_worker(worker.clone()).await.unwrap();
            created.push(model);
        }

        for (i, worker) in created.into_iter().enumerate() {
            if i % 2 == 0 {
                catalog
                    .worker
                    .mark_worker(worker.into(), WorkerState::Active)
                    .await
                    .unwrap();
            }
        }

        let mismatched = catalog.worker.get_mismatch().await.unwrap();
        let all_workers = catalog.worker.get_worker(GetWorker::all()).await.unwrap();

        let mismatched_addrs: HashSet<_> =
            mismatched.iter().map(|w| &w.host_addr).collect();

        for worker in &all_workers {
            let is_mismatched =
                worker.current_state.to_string() != worker.desired_state.to_string();
            let in_result = mismatched_addrs.contains(&worker.host_addr);

            assert_eq!(
                is_mismatched,
                in_result,
                "Worker '{}': is_mismatched={} but in_result={}. State: {:?}/{:?}",
                worker.host_addr,
                is_mismatched,
                in_result,
                worker.current_state,
                worker.desired_state
            );
        }
    }

    async fn prop_dag_topology_round_trip(workers: Vec<CreateWorker>) {
        let catalog = Catalog::for_test().await;

        for worker in &workers {
            catalog.worker.create_worker(worker.clone()).await.unwrap();
        }

        let result = catalog.worker.get_topology().await;
        assert!(
            result.is_ok(),
            "Generated DAG should validate: {:?}",
            result.err()
        );
        let topo = result.unwrap();
        assert_eq!(topo.len(), workers.len());
    }

    async fn prop_cycle_detection(w1: CreateWorker, w2: CreateWorker) {
        if w1.host_addr == w2.host_addr || w1.grpc_addr == w2.grpc_addr {
            return;
        }

        let catalog = Catalog::for_test().await;

        let w1 = w1.with_peers(vec![w2.host_addr.clone()]);
        let w2 = w2.with_peers(vec![w1.host_addr.clone()]);
        catalog.worker.create_worker(w1).await.unwrap();
        catalog.worker.create_worker(w2).await.unwrap();

        let result = catalog.worker.get_topology().await;
        assert!(result.is_err(), "Mutual peers should form a cycle");
        let err = result.unwrap_err();
        assert!(err.downcast_ref::<CycleDetected>().is_some());
    }

    proptest! {
        #[test]
        fn create_and_get_worker(req in CreateWorker::generate()) {
            test_prop(|| async move {
                prop_create_and_get_worker(req).await;
            });
        }

        #[test]
        fn drop_and_remove_worker(req in CreateWorker::generate()) {
            test_prop(|| async move {
                prop_drop_and_remove_worker(req).await;
            });
        }

        #[test]
        fn mark_worker_state(req in CreateWorker::generate()) {
            test_prop(|| async move {
                prop_mark_worker_state(req).await;
            });
        }

        #[test]
        fn host_addr_grpc_addr_must_differ(req in CreateWorker::generate()) {
            test_prop(|| async move {
                prop_host_grpc_neq(req).await;
            });
        }

        #[test]
        fn grpc_addr_unique((w1, w2) in (CreateWorker::generate(), CreateWorker::generate())) {
            test_prop(|| async move {
                prop_grpc_addr_unique(w1, w2).await;
            });
        }

        #[test]
        fn worker_drop_blocked_by_active_fragments(req in CreateWorker::generate()) {
            test_prop(|| async move {
                prop_worker_drop_blocked_by_active_fragments(req).await;
            });
        }

        #[test]
        fn worker_self_peer_rejected(req in CreateWorker::generate()) {
            test_prop(|| async move {
                prop_worker_self_peer_rejected(req).await;
            });
        }

        #[test]
        fn duplicate_peer_rejected((w1, w2) in (CreateWorker::generate(), CreateWorker::generate())) {
            test_prop(|| async move {
                prop_duplicate_peer_rejected(w1, w2).await;
            });
        }

        #[test]
        fn worker_host_addr_unique(req in CreateWorker::generate()) {
            test_prop(|| async move {
                prop_worker_addr_unique(req).await;
            });
        }

        #[test]
        fn get_mismatch_correctness(workers in CreateWorker::dag_topology(MAX_TEST_WORKERS)) {
            test_prop(|| async move {
                prop_mismatch_correct(workers).await;
            });
        }

        #[test]
        fn dag_topology_round_trip(workers in CreateWorker::dag_topology(MAX_TEST_WORKERS)) {
            test_prop(|| async move {
                prop_dag_topology_round_trip(workers).await;
            });
        }

        #[test]
        fn cycle_detection((w1, w2) in (CreateWorker::generate(), CreateWorker::generate())) {
            test_prop(|| async move {
                prop_cycle_detection(w1, w2).await;
            });
        }
    }
}

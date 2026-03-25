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
use model::IntoCondition;
use model::worker::endpoint::NetworkAddr;
use model::worker::network_link;
use model::worker::{
    self, CreateWorker, DesiredWorkerState, DropWorker, Entity as WorkerEntity, GetWorker,
    WorkerState,
};
use sea_orm::sea_query::Expr;
use sea_orm::{ActiveModelTrait, ActiveValue::Set, EntityTrait, QueryFilter, TransactionTrait};
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
        let worker = self
            .db
            .with_retry(|conn| {
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
            })
            .await?;

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

    pub async fn get_topology(
        &self,
    ) -> Result<(Vec<worker::Model>, Vec<(NetworkAddr, NetworkAddr)>)> {
        let workers = WorkerEntity::find().all(&self.db.conn).await?;
        let links = network_link::Entity::find().all(&self.db.conn).await?;
        let edges = links
            .into_iter()
            .map(|l| (l.source_host_addr, l.target_host_addr))
            .collect();
        Ok((workers, edges))
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
    use super::*;
    use crate::Catalog;
    use model::query::CreateQueryWithRefs;
    use model::worker::n_unique_workers;
    use std::collections::HashSet;
    use test_strategy::proptest;

    const MAX_TEST_WORKERS: u8 = 32;

    #[proptest(async = "tokio")]
    async fn create_and_get_worker(req: CreateWorker) {
        let catalog = Catalog::for_test().await;

        let created = catalog
            .worker
            .create_worker(req.clone())
            .await
            .expect("worker creation should succeed");

        assert_eq!(created.host_addr, req.host_addr);
        assert_eq!(created.data_addr, req.data_addr);
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

    #[proptest(async = "tokio")]
    async fn drop_and_remove_worker(req: CreateWorker) {
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

    #[proptest(async = "tokio")]
    async fn mark_worker_state(req: CreateWorker) {
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

    #[proptest(async = "tokio")]
    async fn host_addr_data_addr_must_differ(mut req: CreateWorker) {
        let catalog = Catalog::for_test().await;
        req.data_addr = req.host_addr.clone();

        assert!(
            catalog.worker.create_worker(req).await.is_err(),
            "Worker creation should fail when host_addr equals data_addr"
        );
    }

    #[proptest(async = "tokio")]
    async fn data_addr_unique(#[strategy(n_unique_workers(2))] mut workers: Vec<CreateWorker>) {
        let dup_addr = workers[0].data_addr.clone();
        workers[1].data_addr = dup_addr;

        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(workers[0].clone()).await.unwrap();
        assert!(
            catalog.worker.create_worker(workers[1].clone()).await.is_err(),
            "Duplicate data_addr should be rejected"
        );
    }

    #[proptest(async = "tokio")]
    async fn worker_drop_blocked_by_active_fragments(req: CreateQueryWithRefs) {
        let catalog = Catalog::for_test().await;
        for w in &req.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let _ = catalog.query.create_query(req.query.clone()).await.unwrap();

        // Pick a worker that actually has a fragment placed on it
        let fragment_host = &req.query.fragments[0].host_addr;
        assert!(
            catalog.worker.drop_worker(DropWorker::new(fragment_host.clone())).await.is_err(),
            "Worker with active fragments should not be droppable"
        );
    }

    #[proptest(async = "tokio")]
    async fn worker_self_peer_rejected(mut req: CreateWorker) {
        let catalog = Catalog::for_test().await;
        req.peers = vec![req.host_addr.clone()];

        assert!(
            catalog.worker.create_worker(req).await.is_err(),
            "Worker referencing itself as peer should be rejected"
        );
    }

    #[proptest(async = "tokio")]
    async fn duplicate_peer_rejected(#[strategy(n_unique_workers(2))] mut workers: Vec<CreateWorker>) {
        let peer_addr = workers[1].host_addr.clone();
        workers[0].peers = vec![peer_addr.clone(), peer_addr];

        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(workers[1].clone()).await.unwrap();
        assert!(
            catalog.worker.create_worker(workers[0].clone()).await.is_err(),
            "Duplicate peer entries should violate composite PK"
        );
    }

    #[proptest(async = "tokio")]
    async fn worker_host_addr_unique(req: CreateWorker) {
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

    #[proptest(async = "tokio")]
    async fn get_mismatch_correctness(
        #[strategy(CreateWorker::topology_dag(2, MAX_TEST_WORKERS))] workers: Vec<CreateWorker>,
    ) {
        let catalog = Catalog::for_test().await;
        for worker in &workers {
            catalog.worker.create_worker(worker.clone()).await.unwrap();
        }

        // Mark every other worker as Active, creating a mismatch with desired=Active
        // for the ones still in Pending.
        let all = catalog.worker.get_worker(GetWorker::all()).await.unwrap();
        for (i, w) in all.into_iter().enumerate() {
            if i % 2 == 0 {
                catalog.worker.mark_worker(w.into(), WorkerState::Active).await.unwrap();
            }
        }

        let mismatched: HashSet<_> = catalog.worker.get_mismatch().await.unwrap()
            .into_iter().map(|w| w.host_addr).collect();
        let all = catalog.worker.get_worker(GetWorker::all()).await.unwrap();

        for w in &all {
            let expect_mismatch = w.current_state != WorkerState::Active;
            assert_eq!(
                mismatched.contains(&w.host_addr), expect_mismatch,
                "Worker '{}': current={:?}, expected mismatch={}", w.host_addr, w.current_state, expect_mismatch
            );
        }
    }
}

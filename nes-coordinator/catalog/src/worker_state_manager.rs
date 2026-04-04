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
use model::worker::{self, Entity as WorkerEntity, GetWorker, WorkerState};
use sea_orm::sea_query::Expr;
use sea_orm::{ActiveModelTrait, ActiveValue::Set, EntityTrait, QueryFilter};
use std::sync::Arc;

pub struct WorkerStateManager {
    db: Database,
    listeners: NotificationChannel,
}

impl WorkerStateManager {
    pub fn from(db: Database) -> Arc<Self> {
        Arc::new(Self {
            db,
            listeners: NotificationChannel::new(),
        })
    }

    pub fn notify_intent(&self) {
        self.listeners.notify_intent();
    }

    pub async fn get_worker(&self, req: GetWorker) -> Result<Vec<worker::Model>> {
        Ok(WorkerEntity::find()
            .filter(req.into_condition())
            .all(&self.db.conn)
            .await?)
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

impl Reconcilable for WorkerStateManager {
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
    use crate::crud::execute_statement;
    use model::request::{Statement, StatementResponse};
    use model::worker::{CreateWorker, GetWorker};
    use std::collections::HashSet;
    use test_strategy::proptest;

    const MAX_TEST_WORKERS: u8 = 32;

    #[proptest(async = "tokio")]
    async fn mark_worker_state(req: CreateWorker) {
        let db = Database::for_test().await;
        let wsm = WorkerStateManager::from(db.clone());
        let StatementResponse::CreatedWorker(created) =
            execute_statement(&db, Statement::CreateWorker(req.clone())).await.unwrap()
        else {
            panic!("unexpected response");
        };

        wsm.mark_worker(created.into(), WorkerState::Active)
            .await
            .expect("mark should succeed");

        let StatementResponse::Workers(workers) =
            execute_statement(&db, Statement::GetWorker(GetWorker::all().with_host_addr(req.host_addr))).await.unwrap()
        else {
            panic!("unexpected response");
        };
        assert_eq!(workers.first().unwrap().current_state, WorkerState::Active);
    }

    #[proptest(async = "tokio")]
    async fn get_mismatch_correctness(
        #[strategy(CreateWorker::topology_dag(2, MAX_TEST_WORKERS))] workers: Vec<CreateWorker>,
    ) {
        let db = Database::for_test().await;
        let wsm = WorkerStateManager::from(db.clone());
        for worker in &workers {
            execute_statement(&db, Statement::CreateWorker(worker.clone())).await.unwrap();
        }

        let StatementResponse::Workers(all) =
            execute_statement(&db, Statement::GetWorker(GetWorker::all())).await.unwrap()
        else {
            panic!("unexpected response");
        };
        for (i, w) in all.into_iter().enumerate() {
            if i % 2 == 0 {
                wsm.mark_worker(w.into(), WorkerState::Active).await.unwrap();
            }
        }

        let mismatched: HashSet<_> = wsm.get_mismatch().await.unwrap()
            .into_iter().map(|w| w.host_addr).collect();
        let StatementResponse::Workers(all) =
            execute_statement(&db, Statement::GetWorker(GetWorker::all())).await.unwrap()
        else {
            panic!("unexpected response");
        };

        for w in &all {
            let expect_mismatch = w.current_state != WorkerState::Active;
            assert_eq!(
                mismatched.contains(&w.host_addr), expect_mismatch,
                "Worker '{}': current={:?}, expected mismatch={}", w.host_addr, w.current_state, expect_mismatch
            );
        }
    }
}

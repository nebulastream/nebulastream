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

use crate::error::{ErrorCode, catalog_write};
use crate::worker::endpoint::NetworkAddr;
use crate::worker::{
    ActiveModel, DesiredWorkerState, Entity as WorkerEntity, WorkerState, network_link,
};
use crate::{Execute, coded_bail};
use anyhow::{Context, Result};
use sea_orm::ActiveValue::Set;
use sea_orm::{ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, NotSet, QueryFilter};
use serde::Deserialize;

use super::Model;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateWorker {
    pub host_addr: NetworkAddr,
    pub data_addr: NetworkAddr,
    pub max_operators: Option<i32>,
    pub peers: Vec<NetworkAddr>,
    pub config: serde_json::Value,
    /// Return the existing row instead of erroring when a matching one already exists,
    /// as with SQL `CREATE ... IF NOT EXISTS`.
    #[serde(default)]
    pub if_not_exists: bool,
}

impl From<CreateWorker> for ActiveModel {
    fn from(req: CreateWorker) -> Self {
        Self {
            host_addr: Set(req.host_addr),
            data_addr: Set(req.data_addr),
            max_operators: Set(req.max_operators),
            config: Set(req.config),
            current_state: NotSet,
            desired_state: NotSet,
        }
    }
}

impl CreateWorker {
    async fn register_again(&self, removed: Model, conn: &impl ConnectionTrait) -> Result<Model> {
        if removed.current_state != WorkerState::Removed {
            coded_bail!(
                ErrorCode::WorkerAlreadyExists,
                "worker {} is still being removed",
                self.host_addr
            );
        }
        let mut registered = ActiveModel::from(self.clone());
        registered.current_state = Set(WorkerState::Pending);
        registered.desired_state = Set(DesiredWorkerState::Active);
        let worker = registered
            .update(conn)
            .await
            .map_err(catalog_write(
                ErrorCode::WorkerAlreadyExists,
                ErrorCode::UnknownWorker,
            ))
            .context("failed to update worker")?;
        network_link::Entity::delete_many()
            .filter(network_link::Column::SourceHostAddr.eq(self.host_addr.clone()))
            .exec(conn)
            .await
            .context("failed to delete peers")?;
        self.insert_peers(conn).await?;
        Ok(worker)
    }

    async fn insert_peers(&self, conn: &impl ConnectionTrait) -> Result<()> {
        if !self.peers.is_empty() {
            network_link::Entity::insert_many(self.peers.iter().map(|peer| {
                network_link::ActiveModel {
                    source_host_addr: Set(self.host_addr.clone()),
                    target_host_addr: Set(peer.clone()),
                }
            }))
            .exec(conn)
            .await
            .context("failed to insert peers")?;
        }
        Ok(())
    }
}

impl Execute for CreateWorker {
    type Response = Model;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Model> {
        let existing = WorkerEntity::find_by_id(self.host_addr.clone())
            .one(conn)
            .await
            .context("failed to fetch worker")?;
        if let Some(existing) = existing {
            if existing.desired_state == DesiredWorkerState::Removed {
                return self.register_again(existing, conn).await;
            }
            if self.if_not_exists {
                // When the worker already exists, return it as is
                // without applying this request's peers or config (create-if-absent, not update).
                return Ok(existing);
            }
        }
        let worker = ActiveModel::from(self.clone())
            .insert(conn)
            .await
            .map_err(catalog_write(
                ErrorCode::WorkerAlreadyExists,
                ErrorCode::UnknownWorker,
            ))
            .context("failed to insert worker")?;
        self.insert_peers(conn).await?;
        Ok(worker)
    }
}

#[cfg(any(test, feature = "testing"))]
mod arbitrary;

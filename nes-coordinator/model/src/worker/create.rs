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

use crate::Execute;
use crate::error::{ErrorCode, catalog_write};
use crate::worker::endpoint::NetworkAddr;
use crate::worker::{ActiveModel, Entity as WorkerEntity, network_link};
use anyhow::{Context, Result};
use sea_orm::ActiveValue::Set;
use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait, NotSet};
use serde::Deserialize;

use super::Model;

#[derive(Debug, Clone, Deserialize)]
pub struct CreateWorker {
    pub host_addr: NetworkAddr,
    pub data_addr: NetworkAddr,
    pub max_operators: Option<i32>,
    pub peers: Vec<NetworkAddr>,
    pub config: serde_json::Value,
    /// Return the existing row instead of erroring when a matching one
    /// already exists, as with SQL `CREATE ... IF NOT EXISTS`.
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

impl Execute for CreateWorker {
    type Response = Model;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Model> {
        if self.if_not_exists {
            // No-op when the worker already exists: return it as-is, without
            // reconciling this request's peers or config (create-if-absent,
            // not update).
            if let Some(existing) = WorkerEntity::find_by_id(self.host_addr.clone())
                .one(conn)
                .await
                .context("failed to fetch worker")?
            {
                return Ok(existing);
            }
        }
        let host_addr = self.host_addr.clone();
        let peers = self.peers.clone();
        let worker = ActiveModel::from(self.clone())
            .insert(conn)
            .await
            .map_err(catalog_write(
                ErrorCode::WorkerAlreadyExists,
                ErrorCode::UnknownWorker,
            ))
            .context("failed to insert worker")?;
        if !peers.is_empty() {
            network_link::Entity::insert_many(peers.into_iter().map(|peer| {
                network_link::ActiveModel {
                    source_host_addr: Set(host_addr.clone()),
                    target_host_addr: Set(peer),
                }
            }))
            .exec(conn)
            .await
            .context("failed to insert peers")?;
        }
        Ok(worker)
    }
}

#[cfg(test)]
pub(crate) use arbitrary::gen_unique_workers;

#[cfg(any(test, feature = "testing"))]
mod arbitrary;

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

//! The read request that returns a worker together with the fragments
//! currently placed on it.

use crate::Execute;
use crate::query::query_fragment;
use crate::worker::endpoint::NetworkAddr;
use crate::worker::{self, Column};
use anyhow::{Context, Result};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct GetWorkerStatus {
    pub host_addr: NetworkAddr,
}

impl GetWorkerStatus {
    pub fn new(host_addr: NetworkAddr) -> Self {
        Self { host_addr }
    }
}

impl Execute for GetWorkerStatus {
    type Response = (worker::Model, Vec<query_fragment::Model>);

    async fn execute(
        &self,
        conn: &impl ConnectionTrait,
    ) -> Result<(worker::Model, Vec<query_fragment::Model>)> {
        worker::Entity::find()
            .filter(Column::HostAddr.eq(self.host_addr.clone()))
            .find_with_related(query_fragment::Entity)
            .all(conn)
            .await
            .context(format!(
                "failed to fetch fragments for worker {}",
                self.host_addr
            ))?
            .into_iter()
            .next()
            .context("worker not found")
    }
}

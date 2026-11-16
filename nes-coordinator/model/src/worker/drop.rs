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

use crate::worker::endpoint::NetworkAddr;
use crate::worker::{DesiredWorkerState, Model};
use crate::{Execute, worker};
use anyhow::{Context, Result};
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
use serde::Deserialize;

/// Soft removal: sets the target state to `Removed` rather than deleting the row.
/// Any active fragments on the worker are then failed automatically.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DropWorker {
    pub host_addr: NetworkAddr,
}

impl DropWorker {
    pub fn new(host_addr: NetworkAddr) -> Self {
        Self { host_addr }
    }
}

impl Execute for DropWorker {
    type Response = Option<Model>;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Option<Model>> {
        let workers = worker::Entity::update_many()
            .col_expr(
                worker::Column::DesiredState,
                Expr::value(DesiredWorkerState::Removed),
            )
            .filter(worker::Column::HostAddr.eq(self.host_addr.clone()))
            .exec_with_returning(conn)
            .await
            .context("failed to update worker")?;
        Ok(workers.into_iter().next())
    }
}

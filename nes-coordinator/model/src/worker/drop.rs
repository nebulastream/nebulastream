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
use anyhow::Result;
use sea_orm::ActiveValue::Set;
use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
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
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<Option<Model>> {
        let worker = worker::Entity::find_by_id(self.host_addr).one(conn).await?;
        let Some(worker) = worker else {
            return Ok(None);
        };
        let mut active: worker::ActiveModel = worker.into();
        active.desired_state = Set(DesiredWorkerState::Removed);
        Ok(Some(active.update(conn).await?))
    }
}

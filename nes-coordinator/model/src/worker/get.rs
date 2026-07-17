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
use crate::worker::{Column, DesiredWorkerState, Model, WorkerState};
use crate::{Execute, IntoCondition, worker};
use anyhow::{Context, Result};
use sea_orm::{ColumnTrait, Condition, ConnectionTrait};
use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct GetWorker {
    pub host_addr: Option<NetworkAddr>,
    pub current_state: Option<WorkerState>,
    pub desired_state: Option<DesiredWorkerState>,
}

impl GetWorker {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_host_addr(mut self, host_addr: NetworkAddr) -> Self {
        self.host_addr = Some(host_addr);
        self
    }

    pub fn with_current_state(mut self, state: WorkerState) -> Self {
        self.current_state = Some(state);
        self
    }
}

impl IntoCondition for GetWorker {
    fn to_condition(&self) -> Condition {
        Condition::all()
            .add_option(self.host_addr.clone().map(|v| Column::HostAddr.eq(v)))
            .add_option(self.desired_state.map(|v| Column::DesiredState.eq(v)))
            .add_option(self.current_state.map(|v| Column::CurrentState.eq(v)))
    }
}

impl Execute for GetWorker {
    type Response = Vec<Model>;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Vec<Model>> {
        crate::find_all::<worker::Entity>(self.to_condition(), conn)
            .await
            .context("failed to fetch worker(s)")
    }
}

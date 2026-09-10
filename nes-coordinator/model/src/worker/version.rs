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

//! The read request that reports which build each worker runs.
//!
//! A version is not written down anywhere, so answering means asking the workers
//! themselves. This request only selects which ones to ask; the asking happens
//! where the connections to them live.

use crate::Execute;
use crate::worker::endpoint::NetworkAddr;
use crate::worker::{Column, Model};
use crate::{IntoCondition, worker};
use anyhow::{Context, Result};
use sea_orm::{ColumnTrait, Condition, ConnectionTrait};
use serde::{Deserialize, Serialize};

/// Selects the workers to ask. An absent address asks every worker the catalog holds.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GetWorkerVersion {
    pub host_addr: Option<NetworkAddr>,
}

impl GetWorkerVersion {
    #[must_use]
    pub fn all() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_host_addr(mut self, host_addr: NetworkAddr) -> Self {
        self.host_addr = Some(host_addr);
        self
    }
}

impl IntoCondition for GetWorkerVersion {
    fn to_condition(&self) -> Condition {
        // Every worker is asked, whatever state the catalog last recorded for it. A worker that is
        // registered but not yet up is exactly the one a caller wants told apart from a healthy one,
        // and reporting that it could not be reached says that, while leaving it out says nothing.
        Condition::all().add_option(self.host_addr.clone().map(|v| Column::HostAddr.eq(v)))
    }
}

impl Execute for GetWorkerVersion {
    /// The workers to ask, rather than their versions, because reading a version is not a catalog read.
    type Response = Vec<Model>;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Vec<Model>> {
        crate::get_all::<worker::Entity>(self, conn)
            .await
            .context("failed to fetch the workers to ask for their version")
    }
}

/// What one worker answered when asked for its version.
///
/// A worker that could not be reached reports why instead of a version, so one unreachable worker
/// does not hide what the others answered. Exactly one of the two is set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkerVersion {
    pub worker: NetworkAddr,
    pub version: Option<String>,
    pub error: Option<String>,
}

impl WorkerVersion {
    #[must_use]
    pub fn reported(worker: NetworkAddr, version: impl Into<String>) -> Self {
        Self {
            worker,
            version: Some(version.into()),
            error: None,
        }
    }

    #[must_use]
    pub fn unreachable(worker: NetworkAddr, error: impl Into<String>) -> Self {
        Self {
            worker,
            version: None,
            error: Some(error.into()),
        }
    }
}

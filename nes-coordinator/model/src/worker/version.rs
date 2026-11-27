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
//! A version is not stored in the catalog, so only the workers themselves can report it.
//! This request selects the workers to contact,
//! and the component that holds the worker connections requests their versions.

use crate::Execute;
use crate::worker::endpoint::NetworkAddr;
use crate::worker::{Column, Model};
use crate::{IntoCondition, worker};
use anyhow::{Context, Result};
use sea_orm::{ColumnTrait, Condition, ConnectionTrait};
use serde::{Deserialize, Serialize};

/// An absent address selects every worker in the catalog.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
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
        // Every worker is contacted, whatever state the catalog last recorded for it.
        // This includes a worker that is registered but not yet up.
        // Its entry reports that it could not be reached, and leaving it out would hide that.
        Condition::all().add_option(self.host_addr.clone().map(|v| Column::HostAddr.eq(v)))
    }
}

impl Execute for GetWorkerVersion {
    /// The workers to contact, not their versions.
    type Response = Vec<Model>;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Vec<Model>> {
        crate::get_all::<worker::Entity>(self, conn)
            .await
            .context("failed to fetch the workers to ask for their version")
    }
}

/// One worker's version, or the reason why it could not be reached.
///
/// An unreachable worker does not fail the request, so the versions of the other workers are still returned.
/// The constructors set exactly one of the two.
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

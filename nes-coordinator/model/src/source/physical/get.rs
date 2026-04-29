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

use crate::identifier::SourceId;
use crate::{ConnectorKind, Execute, IntoCondition};
use crate::worker::endpoint::NetworkAddr;
use crate::source::physical::{Column, Entity, Model};
use anyhow::Result;
use sea_orm::{ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter};
use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct GetPhysicalSource {
    pub id: Option<SourceId>,
    pub host_addr: Option<NetworkAddr>,
    pub logical_source: Option<String>,
    pub kind: Option<ConnectorKind>,
}

impl GetPhysicalSource {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_id(mut self, id: SourceId) -> Self {
        self.id = Some(id);
        self
    }

    pub fn with_host_addr(mut self, host_addr: NetworkAddr) -> Self {
        self.host_addr = Some(host_addr);
        self
    }

    pub fn with_logical_source(mut self, logical_source: String) -> Self {
        self.logical_source = Some(logical_source);
        self
    }

    pub fn with_kind(mut self, kind: ConnectorKind) -> Self {
        self.kind = Some(kind);
        self
    }
}

impl IntoCondition for GetPhysicalSource {
    fn to_condition(&self) -> Condition {
        Condition::all()
            .add_option(self.id.map(|v| Column::Id.eq(v)))
            .add_option(self.host_addr.clone().map(|v| Column::HostAddr.eq(v)))
            .add_option(self.logical_source.clone().map(|v| Column::LogicalSource.eq(v)))
            .add_option(self.kind.map(|v| Column::Kind.eq(v)))
    }
}

impl Execute for GetPhysicalSource {
    type Response = Vec<Model>;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Vec<Model>> {
        Ok(Entity::find()
            .filter(self.to_condition())
            .all(conn)
            .await?)
    }
}

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

use crate::{ConnectorKind, Execute};
use crate::worker::endpoint::NetworkAddr;
use crate::source::physical::{ActiveModel, Column, Entity, Model, SourceType};
use anyhow::Result;
use sea_orm::ActiveValue::{NotSet, Set};
use sea_orm::{ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct CreatePhysicalSource {
    pub logical_source: String,
    pub host_addr: NetworkAddr,
    pub source_type: SourceType,
    #[serde(default)]
    pub source_config: serde_json::Value,
    #[serde(default)]
    pub parser_config: serde_json::Value,
    #[serde(default)]
    pub if_not_exists: bool,
}

impl From<CreatePhysicalSource> for ActiveModel {
    fn from(req: CreatePhysicalSource) -> Self {
        Self {
            id: NotSet,
            logical_source: Set(Some(req.logical_source)),
            host_addr: Set(Some(req.host_addr)),
            source_type: Set(req.source_type),
            source_config: Set(req.source_config),
            parser_config: Set(req.parser_config),
            kind: Set(ConnectorKind::Shared),
        }
    }
}

impl Execute for CreatePhysicalSource {
    type Response = Model;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Model> {
        if self.if_not_exists {
            let existing = Entity::find()
                .filter(Column::LogicalSource.eq(&self.logical_source))
                .filter(Column::HostAddr.eq(self.host_addr.to_string()))
                .filter(Column::SourceType.eq(self.source_type.to_string()))
                .one(conn)
                .await?;
            if let Some(existing) = existing {
                return Ok(existing);
            }
        }
        Ok(ActiveModel::from(self.clone()).insert(conn).await?)
    }
}

#[derive(Clone, Debug)]
pub struct CreateInlineSource {
    pub source_type: SourceType,
    pub source_config: serde_json::Value,
    pub parser_config: serde_json::Value,
    pub host_addr: Option<NetworkAddr>,
    pub internal: bool,
}

impl From<CreateInlineSource> for ActiveModel {
    fn from(req: CreateInlineSource) -> Self {
        Self {
            id: NotSet,
            logical_source: Set(None),
            host_addr: Set(req.host_addr),
            source_type: Set(req.source_type),
            source_config: Set(req.source_config),
            parser_config: Set(req.parser_config),
            kind: Set(if req.internal {
                ConnectorKind::Internal
            } else {
                ConnectorKind::Inline
            }),
        }
    }
}

impl Execute for CreateInlineSource {
    type Response = Model;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Model> {
        Ok(ActiveModel::from(self.clone()).insert(conn).await?)
    }
}

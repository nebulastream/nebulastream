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
use crate::source::physical::{ActiveModel, Column, Entity, Model};
use crate::worker::endpoint::NetworkAddr;
use crate::{ConnectorKind, Execute};
use anyhow::Result;
use sea_orm::ActiveValue::{NotSet, Set};
use sea_orm::{ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct CreatePhysicalSource {
    #[serde(alias = "logical")]
    pub logical_source: String,
    #[serde(alias = "host")]
    pub host_addr: NetworkAddr,
    #[serde(alias = "type")]
    pub source_type: String,
    #[serde(default)]
    pub source_config: serde_json::Value,
    #[serde(default)]
    pub parser_config: serde_json::Value,
    /// Return the existing row instead of erroring when a matching one
    /// already exists (matched on the full definition), as with SQL
    /// `CREATE ... IF NOT EXISTS`.
    #[serde(default)]
    pub if_not_exists: bool,
}

impl From<CreatePhysicalSource> for ActiveModel {
    fn from(req: CreatePhysicalSource) -> Self {
        Self {
            id: NotSet,
            logical_source: Set(Some(req.logical_source)),
            host_addr: Set(req.host_addr),
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
            // Match the full definition, not just (logical, host, type):
            // two sources that agree on those three but differ in config
            // (e.g. two FILE sources reading different paths) are distinct.
            let existing = Entity::find()
                .filter(Column::LogicalSource.eq(&self.logical_source))
                .filter(Column::HostAddr.eq(self.host_addr.to_string()))
                .filter(Column::SourceType.eq(&self.source_type))
                .filter(Column::SourceConfig.eq(self.source_config.clone()))
                .filter(Column::ParserConfig.eq(self.parser_config.clone()))
                .one(conn)
                .await?;
            if let Some(existing) = existing {
                return Ok(existing);
            }
        }
        // A physical source names the logical source it binds to and the worker it runs on.
        // The logical source is what a statement gets wrong, so a missing reference is reported as that.
        Ok(ActiveModel::from(self.clone())
            .insert(conn)
            .await
            .map_err(catalog_write(
                ErrorCode::SourceAlreadyExists,
                ErrorCode::UnknownSourceName,
            ))?)
    }
}

#[derive(Clone, Debug)]
pub struct CreateAnonymousSource {
    pub source_type: String,
    pub source_config: serde_json::Value,
    pub parser_config: serde_json::Value,
    pub host_addr: NetworkAddr,
    pub internal: bool,
}

impl From<CreateAnonymousSource> for ActiveModel {
    fn from(req: CreateAnonymousSource) -> Self {
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
                ConnectorKind::Anonymous
            }),
        }
    }
}

impl Execute for CreateAnonymousSource {
    type Response = Model;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Model> {
        // A physical source names the logical source it binds to and the worker it runs on.
        // The logical source is what a statement gets wrong, so a missing reference is reported as that.
        Ok(ActiveModel::from(self.clone())
            .insert(conn)
            .await
            .map_err(catalog_write(
                ErrorCode::SourceAlreadyExists,
                ErrorCode::UnknownSourceName,
            ))?)
    }
}

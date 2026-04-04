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

use crate::ConnectorKind;
use crate::Execute;
use crate::worker::endpoint::NetworkAddr;
use super::{ActiveModel, Column, Entity as SinkEntity, Model, SinkType};
use anyhow::Result;
use sea_orm::ActiveValue::{NotSet, Set};
use sea_orm::{ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct CreateSink {
    pub name: String,
    pub host_addr: NetworkAddr,
    pub sink_type: SinkType,
    pub schema: serde_json::value::Value,
    #[serde(default)]
    pub config: serde_json::Value,
    #[serde(default)]
    pub if_not_exists: bool,
}

impl From<CreateSink> for ActiveModel {
    fn from(req: CreateSink) -> Self {
        Self {
            id: NotSet,
            name: Set(Some(req.name)),
            host_addr: Set(Some(req.host_addr)),
            sink_type: Set(req.sink_type),
            schema: Set(req.schema),
            config: Set(req.config),
            kind: Set(ConnectorKind::Shared),
        }
    }
}

impl Execute for CreateSink {
    type Response = Model;
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<Model> {
        if self.if_not_exists {
            let existing = SinkEntity::find()
                .filter(Column::Name.eq(&self.name))
                .one(conn)
                .await?;
            if let Some(existing) = existing {
                return Ok(existing);
            }
        }
        Ok(ActiveModel::from(self).insert(conn).await?)
    }
}

#[derive(Clone, Debug)]
pub struct CreateInlineSink {
    pub sink_type: SinkType,
    pub schema: serde_json::value::Value,
    pub config: serde_json::Value,
    pub host_addr: Option<NetworkAddr>,
    pub internal: bool,
}

impl From<CreateInlineSink> for ActiveModel {
    fn from(req: CreateInlineSink) -> Self {
        Self {
            id: NotSet,
            name: Set(None),
            host_addr: Set(req.host_addr),
            sink_type: Set(req.sink_type),
            schema: Set(req.schema),
            config: Set(req.config),
            kind: Set(if req.internal {
                ConnectorKind::Internal
            } else {
                ConnectorKind::Inline
            }),
        }
    }
}

impl Execute for CreateInlineSink {
    type Response = Model;
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<Model> {
        Ok(ActiveModel::from(self).insert(conn).await?)
    }
}

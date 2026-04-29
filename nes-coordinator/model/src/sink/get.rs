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

use super::{Column, Entity as SinkEntity, Model};
use crate::identifier::SinkId;
use crate::{ConnectorKind, Execute, IntoCondition};
use anyhow::{Context, Result};
use sea_orm::{ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter};
use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct GetSink {
    pub id: Option<SinkId>,
    pub name: Option<String>,
    pub kind: Option<ConnectorKind>,
}

impl GetSink {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_id(mut self, id: SinkId) -> Self {
        self.id = Some(id);
        self
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn with_kind(mut self, kind: ConnectorKind) -> Self {
        self.kind = Some(kind);
        self
    }
}

impl IntoCondition for GetSink {
    fn to_condition(&self) -> Condition {
        Condition::all()
            .add_option(self.id.map(|v| Column::Id.eq(v)))
            .add_option(self.name.clone().map(|v| Column::Name.eq(v)))
            .add_option(self.kind.map(|v| Column::Kind.eq(v)))
    }
}

impl Execute for GetSink {
    type Response = Vec<Model>;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Vec<Model>> {
        Ok(SinkEntity::find()
            .filter(self.to_condition())
            .all(conn)
            .await
            .context("failed to fetch sink(s)")?)
    }
}

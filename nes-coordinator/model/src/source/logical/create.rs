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

use crate::Execute;
use crate::error::{ErrorCode, catalog_write};
use crate::source::logical::{ActiveModel, Entity, Model};
use anyhow::{Context, Result};
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait};
use serde::Deserialize;

/// Register a logical source by name. `if_not_exists` makes the call
/// idempotent and returns the existing row instead of erroring.
#[derive(Clone, Debug, Deserialize)]
pub struct CreateLogicalSource {
    pub name: String,
    pub schema: Json,
    /// Return the existing row instead of erroring when a matching one
    /// already exists, as with SQL `CREATE ... IF NOT EXISTS`.
    #[serde(default)]
    pub if_not_exists: bool,
}

#[cfg(any(test, feature = "testing"))]
mod arbitrary;

impl From<CreateLogicalSource> for ActiveModel {
    fn from(req: CreateLogicalSource) -> Self {
        Self {
            name: Set(req.name),
            schema: Set(req.schema),
        }
    }
}

impl Execute for CreateLogicalSource {
    type Response = Model;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Model> {
        if self.if_not_exists
            && let Some(existing) = Entity::find_by_id(&self.name)
                .one(conn)
                .await
                .context("failed to fetch existing logical source")?
        {
            return Ok(existing);
        }
        ActiveModel::from(self.clone())
            .insert(conn)
            .await
            // A logical source refers to nothing, so a reference cannot be what the write broke.
            .map_err(catalog_write(
                ErrorCode::SourceAlreadyExists,
                ErrorCode::CatalogWriteRejected,
            ))
            .context("failed to insert logical source")
    }
}

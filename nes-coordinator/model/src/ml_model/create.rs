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
use crate::ml_model::{ActiveModel, Entity, Model};
use anyhow::{Context, Result};
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct CreateMlModel {
    pub name: String,
    pub path: String,
    pub input_schema: Json,
    pub output_schema: Json,
    #[serde(default)]
    pub imported: serde_json::Value,
    /// Return the existing row instead of erroring when a matching one
    /// already exists, as with SQL `CREATE ... IF NOT EXISTS`.
    #[serde(default)]
    pub if_not_exists: bool,
}

#[cfg(any(test, feature = "testing"))]
mod arbitrary;

impl From<CreateMlModel> for ActiveModel {
    fn from(req: CreateMlModel) -> Self {
        Self {
            name: Set(req.name),
            path: Set(req.path),
            input_schema: Set(req.input_schema),
            output_schema: Set(req.output_schema),
            imported: Set(req.imported),
        }
    }
}

impl Execute for CreateMlModel {
    type Response = Model;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Model> {
        if self.if_not_exists
            && let Some(existing) = Entity::find_by_id(&self.name)
                .one(conn)
                .await
                .context("failed to fetch existing ml_model")?
        {
            return Ok(existing);
        }
        ActiveModel::from(self.clone())
            .insert(conn)
            .await
            .map_err(catalog_write(
                ErrorCode::ModelAlreadyExists,
                ErrorCode::CatalogWriteRejected,
            ))
            .context("failed to insert ml_model")
    }
}

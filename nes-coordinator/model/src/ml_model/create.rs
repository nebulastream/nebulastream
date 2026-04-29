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

use crate::ml_model::{ActiveModel, Entity, Model};
use crate::Execute;
use anyhow::{Context, Result};
use proptest::arbitrary::Arbitrary;
use sea_orm::entity::prelude::*;
use sea_orm::ActiveValue::Set;
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
    #[serde(default)]
    pub if_not_exists: bool,
}

impl Arbitrary for CreateMlModel {
    type Parameters = ();
    fn arbitrary_with(_: ()) -> Self::Strategy {
        use proptest::prelude::*;
        ("[a-z][a-z0-9_]{2,29}", "/[a-z]{1,8}/[a-z]{1,8}\\.onnx")
            .prop_map(|(name, path)| Self {
                name,
                path,
                input_schema: serde_json::json!({}),
                output_schema: serde_json::json!({}),
                imported: serde_json::json!({}),
                if_not_exists: false,
            })
            .boxed()
    }
    type Strategy = proptest::strategy::BoxedStrategy<Self>;
}

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
        if self.if_not_exists {
            if let Some(existing) = Entity::find_by_id(&self.name)
                .one(conn)
                .await
                .context("failed to fetch existing ml_model")?
            {
                return Ok(existing);
            }
        }
        Ok(ActiveModel::from(self.clone())
            .insert(conn)
            .await
            .context("failed to insert ml_model")?)
    }
}

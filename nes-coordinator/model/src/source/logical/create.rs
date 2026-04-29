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
use crate::source::logical::{ActiveModel, Entity, Model};
use anyhow::{Context, Result};
use proptest::arbitrary::Arbitrary;
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct CreateLogicalSource {
    pub name: String,
    pub schema: Json,
    #[serde(default)]
    pub if_not_exists: bool,
}

impl Arbitrary for CreateLogicalSource {
    type Parameters = ();
    fn arbitrary_with(_: ()) -> Self::Strategy {
        use proptest::prelude::*;
        "[a-z][a-z0-9_]{2,29}"
            .prop_map(|name| Self {
                name,
                schema: serde_json::json!({}),
                if_not_exists: false,
            })
            .boxed()
    }
    type Strategy = proptest::strategy::BoxedStrategy<Self>;
}

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
        if self.if_not_exists {
            if let Some(existing) = Entity::find_by_id(&self.name)
                .one(conn)
                .await
                .context("failed to fetch existing logical source")?
            {
                return Ok(existing);
            }
        }
        Ok(ActiveModel::from(self.clone())
            .insert(conn)
            .await
            .context("failed to insert logical source")?)
    }
}

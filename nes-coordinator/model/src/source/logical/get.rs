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

use crate::source::logical::{Column, Entity, Model};
use crate::{Execute, IntoCondition};
use anyhow::{Context, Result};
use sea_orm::{ColumnTrait, Condition, ConnectionTrait};
use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct GetLogicalSource {
    pub name: Option<String>,
}

impl GetLogicalSource {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }
}

impl IntoCondition for GetLogicalSource {
    fn to_condition(&self) -> Condition {
        Condition::all().add_option(self.name.clone().map(|name| Column::Name.eq(name)))
    }
}

impl Execute for GetLogicalSource {
    type Response = Vec<Model>;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Vec<Model>> {
        crate::find_all::<Entity>(self.to_condition(), conn)
            .await
            .context("failed to fetch logical source(s)")
    }
}

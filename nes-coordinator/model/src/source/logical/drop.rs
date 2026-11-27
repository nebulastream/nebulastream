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
use crate::name::Identifier;
use crate::source::logical::{Column, Entity, Model};
use crate::{Execute, IntoCondition};
use anyhow::{Context, Result};
use sea_orm::{ColumnTrait, Condition, ConnectionTrait};
use serde::Deserialize;

/// Deletes the matching logical sources and returns the deleted rows.
/// Fails with `CatalogWriteRejected` while a physical source is bound to a matched logical source.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DropLogicalSource {
    pub name: Option<Identifier>,
}

impl DropLogicalSource {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_name(mut self, name: Identifier) -> Self {
        self.name = Some(name);
        self
    }
}

impl IntoCondition for DropLogicalSource {
    fn to_condition(&self) -> Condition {
        Condition::all().add_option(self.name.clone().map(|n| Column::Name.eq(n)))
    }
}

impl Execute for DropLogicalSource {
    type Response = Vec<Model>;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Vec<Model>> {
        crate::drop_all::<Entity>(self, conn)
            .await
            .map_err(catalog_write(
                ErrorCode::CatalogWriteRejected,
                ErrorCode::CatalogWriteRejected,
            ))
            .context("failed to drop logical source(s)")
    }
}

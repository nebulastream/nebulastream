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

use super::schema::Schema;
use crate::IntoCondition;
use proptest_derive::Arbitrary;
use sea_orm::ActiveValue::Set;
use sea_orm::Condition;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "logical_source")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub name: String,
    #[sea_orm(column_type = "JsonBinary")]
    pub schema: Schema,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::physical_source::Entity")]
    PhysicalSource,
}

impl Related<super::physical_source::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PhysicalSource.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Arbitrary, Clone, Debug, serde::Deserialize)]
pub struct CreateLogicalSource {
    #[proptest(regex = "[a-z][a-z0-9_]{2,29}")]
    pub name: String,
    pub schema: Schema,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
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
    fn into_condition(self) -> Condition {
        Condition::all().add_option(self.name.map(|name| Column::Name.eq(name)))
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct DropLogicalSource {
    pub with_name: Option<String>,
}

impl IntoCondition for DropLogicalSource {
    fn into_condition(self) -> Condition {
        Condition::all().add_option(self.with_name.map(|name| Column::Name.eq(name)))
    }
}

impl From<CreateLogicalSource> for ActiveModel {
    fn from(req: CreateLogicalSource) -> Self {
        Self {
            name: Set(req.name),
            schema: Set(req.schema),
        }
    }
}

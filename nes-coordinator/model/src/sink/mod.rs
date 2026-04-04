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
use crate::worker::CreateWorker;
use crate::worker::endpoint::NetworkAddr;
use proptest::arbitrary::{Arbitrary, any};
use proptest::strategy::{BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use sea_orm::ActiveValue::NotSet;
use sea_orm::entity::prelude::*;
use sea_orm::{Condition, Set};
use serde::{Deserialize, Serialize};
use strum::Display;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "sink")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    #[sea_orm(unique)]
    pub name: Option<String>,
    pub host_addr: Option<NetworkAddr>,
    pub sink_type: SinkType,
    #[sea_orm(column_type = "JsonBinary")]
    pub schema: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub config: Json,
    pub kind: ConnectorKind,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "crate::worker::Entity",
        from = "Column::HostAddr",
        to = "crate::worker::Column::HostAddr",
        on_update = "Restrict",
        on_delete = "Restrict"
    )]
    Worker,
}

impl Related<crate::worker::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Worker.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct CreateSink {
    pub name: String,
    pub host_addr: NetworkAddr,
    pub sink_type: SinkType,
    pub schema: Json,
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

#[derive(Clone, Debug)]
pub struct CreateInlineSink {
    pub sink_type: SinkType,
    pub schema: Json,
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

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub struct GetSink {
    pub id: Option<i64>,
    pub name: Option<String>,
    pub kind: Option<ConnectorKind>,
}

impl GetSink {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_id(mut self, id: i64) -> Self {
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

impl crate::IntoCondition for GetSink {
    fn into_condition(self) -> Condition {
        Condition::all()
            .add_option(self.id.map(|v| Column::Id.eq(v)))
            .add_option(self.name.map(|v| Column::Name.eq(v)))
            .add_option(self.kind.map(|v| Column::Kind.eq(v)))
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct DropSink {
    pub name: Option<String>,
    pub id: Option<i64>,
}

impl crate::IntoCondition for DropSink {
    fn into_condition(self) -> Condition {
        Condition::all()
            .add_option(self.name.map(|v| Column::Name.eq(v)))
            .add_option(self.id.map(|v| Column::Id.eq(v)))
    }
}

#[derive(
    Arbitrary,
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    Display,
    Serialize,
    Deserialize,
    DeriveActiveEnum,
    EnumIter,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
pub enum SinkType {
    File,
    Print,
    Void,
}

#[derive(Debug, Clone)]
pub struct SinkWithRefs {
    pub worker: CreateWorker,
    pub sink: CreateSink,
}

impl Arbitrary for SinkWithRefs {
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            "[a-zA-Z]{1,10}",
            any::<CreateWorker>(),
            any::<SinkType>(),
        )
            .prop_map(|(name, worker, sink_type)| {
                let sink = CreateSink {
                    name,
                    host_addr: worker.host_addr.clone(),
                    sink_type,
                    schema: serde_json::json!({}),
                    config: serde_json::json!({}),
                    if_not_exists: false,
                };
                Self { worker, sink }
            })
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

use crate::source::schema::Schema;
use crate::worker::endpoint::HostAddr;
#[cfg(feature = "testing")]
use proptest_derive::Arbitrary;
use sea_orm::entity::prelude::*;
use sea_orm::{Condition, Set};
use serde::{Deserialize, Serialize};
use strum::Display;
#[cfg(feature = "testing")]
use crate::worker::CreateWorker;

pub type SinkName = String;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "sink")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub name: SinkName,
    pub host_addr: HostAddr,
    pub sink_type: SinkType,
    #[sea_orm(column_type = "JsonBinary")]
    pub schema: Schema,
    #[sea_orm(column_type = "Json")]
    pub config: Json,
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

#[derive(Clone, Debug)]
pub struct CreateSink {
    pub name: SinkName,
    pub host_addr: HostAddr,
    pub sink_type: SinkType,
    pub schema: Schema,
    pub config: serde_json::Value,
}

impl From<CreateSink> for ActiveModel {
    fn from(req: CreateSink) -> Self {
        Self {
            name: Set(req.name),
            host_addr: Set(req.host_addr),
            sink_type: Set(req.sink_type),
            schema: Set(req.schema),
            config: Set(req.config),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct GetSink {
    pub name: Option<SinkName>,
}

impl GetSink {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_name(mut self, name: SinkName) -> Self {
        self.name = Some(name);
        self
    }
}

impl crate::IntoCondition for GetSink {
    fn into_condition(self) -> Condition {
        Condition::all()
            .add_option(self.name.map(|v| Column::Name.eq(v)))
    }
}

#[derive(Clone, Debug)]
pub struct DropSink {
    pub name: SinkName,
}

impl crate::IntoCondition for DropSink {
    fn into_condition(self) -> Condition {
        Condition::all()
            .add(Column::Name.eq(self.name))
    }
}

#[cfg_attr(feature = "testing", derive(Arbitrary))]
#[derive(
    Debug, Copy, Clone, PartialEq, Eq, Display, Serialize, Deserialize, DeriveActiveEnum, EnumIter,
)]
#[sea_orm(
    rs_type = "String",
    db_type = "Text",
    rename_all = "PascalCase"
)]
pub enum SinkType {
    File,
    Print,
}

#[cfg(feature = "testing")]
#[derive(Debug, Clone)]
pub struct SinkWithRefs {
    pub worker: CreateWorker,
    pub sink: CreateSink,
}

#[cfg(feature = "testing")]
fn sink_name_chars() -> impl proptest::strategy::Strategy<Value = char> {
    use proptest::prelude::*;
    prop_oneof![
        proptest::char::range('a', 'z'),
        proptest::char::range('0', '9'),
        Just('_')
    ]
}

#[cfg(feature = "testing")]
impl crate::Generate for SinkWithRefs {
    fn generate() -> proptest::strategy::BoxedStrategy<Self> {
        use proptest::prelude::*;
        let name = (
            proptest::char::range('a', 'z'),
            proptest::collection::vec(sink_name_chars(), 2..=29),
        )
            .prop_map(|(first, rest)| {
                std::iter::once(first).chain(rest).collect::<String>()
            });
        (
            name,
            CreateWorker::generate(),
            any::<SinkType>(),
            any::<Schema>(),
        )
            .prop_map(|(name, worker, sink_type, schema)| {
                let sink = CreateSink {
                    name,
                    host_addr: worker.host_addr.clone(),
                    sink_type,
                    schema,
                    config: serde_json::json!({}),
                };
                SinkWithRefs { worker, sink }
            })
            .boxed()
    }
}

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

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
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
    pub by_name: Option<SinkName>,
    pub by_host_addr: Option<HostAddr>,
    pub by_type: Option<SinkType>,
}

impl GetSink {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn by_name(mut self, name: SinkName) -> Self {
        self.by_name = Some(name);
        self
    }

    pub fn by_host_addr(mut self, host_addr: HostAddr) -> Self {
        self.by_host_addr = Some(host_addr);
        self
    }

    pub fn by_sink_type(mut self, sink_type: SinkType) -> Self {
        self.by_type = Some(sink_type);
        self
    }
}

impl crate::IntoCondition for GetSink {
    fn into_condition(self) -> Condition {
        Condition::all()
            .add_option(self.by_name.map(|v| Column::Name.eq(v)))
            .add_option(self.by_host_addr.map(|v| Column::HostAddr.eq(v)))
            .add_option(self.by_type.map(|v| Column::SinkType.eq(v)))
    }
}

#[derive(Clone, Debug, Default)]
pub struct DropSink {
    pub with_name: Option<SinkName>,
    pub with_host_addr: Option<HostAddr>,
    pub with_type: Option<SinkType>,
}

impl DropSink {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_name(mut self, name: SinkName) -> Self {
        self.with_name = Some(name);
        self
    }

    pub fn with_host_addr(mut self, host_addr: HostAddr) -> Self {
        self.with_host_addr = Some(host_addr);
        self
    }

    pub fn with_sink_type(mut self, sink_type: SinkType) -> Self {
        self.with_type = Some(sink_type);
        self
    }
}

impl crate::IntoCondition for DropSink {
    fn into_condition(self) -> Condition {
        Condition::all()
            .add_option(self.with_name.map(|v| Column::Name.eq(v)))
            .add_option(self.with_host_addr.map(|v| Column::HostAddr.eq(v)))
            .add_option(self.with_type.map(|v| Column::SinkType.eq(v)))
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

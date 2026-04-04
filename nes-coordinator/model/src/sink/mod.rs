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

mod create;
mod drop;
mod get;

pub use create::{CreateInlineSink, CreateSink};
pub use drop::DropSink;
pub use get::GetSink;

use crate::identifier::SinkId;
use crate::worker::CreateWorker;
use crate::worker::endpoint::NetworkAddr;
use crate::{ConnectorKind, worker};
use proptest::arbitrary::{Arbitrary, any};
use proptest::strategy::{BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
use strum::Display;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "sink")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: SinkId,
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

impl Related<worker::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Worker.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

#[cfg(test)]
mod tests {
    use crate::Execute;
    use crate::database::Database;
    use crate::sink::{DropSink, GetSink, SinkWithRefs};
    use test_strategy::proptest;

    #[proptest(async = "tokio")]
    async fn create_and_get(req: SinkWithRefs) {
        let db = Database::for_test().await;
        req.worker.execute(&db.conn).await.unwrap();
        let created = req.sink.clone().execute(&db.conn).await.unwrap();
        assert_eq!(created.name, Some(req.sink.name.clone()));
        assert_eq!(created.sink_type, req.sink.sink_type);

        let sinks = GetSink::all()
            .with_name(req.sink.name)
            .execute(&db.conn)
            .await
            .unwrap();
        assert_eq!(sinks.len(), 1);
        assert_eq!(sinks[0].id, created.id);
    }

    #[proptest(async = "tokio")]
    async fn drop_sink(req: SinkWithRefs) {
        let db = Database::for_test().await;
        req.worker.execute(&db.conn).await.unwrap();
        req.sink.clone().execute(&db.conn).await.unwrap();

        let dropped = (DropSink { name: Some(req.sink.name.clone()), id: None })
            .execute(&db.conn)
            .await
            .unwrap();
        assert_eq!(dropped.len(), 1);

        let remaining = GetSink::all()
            .with_name(req.sink.name)
            .execute(&db.conn)
            .await
            .unwrap();
        assert!(remaining.is_empty());
    }

    #[proptest(async = "tokio")]
    async fn name_unique(req: SinkWithRefs) {
        let db = Database::for_test().await;
        req.worker.execute(&db.conn).await.unwrap();
        req.sink.clone().execute(&db.conn).await.unwrap();
        assert!(req.sink.execute(&db.conn).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn worker_ref_required(req: SinkWithRefs) {
        let db = Database::for_test().await;
        assert!(req.sink.clone().execute(&db.conn).await.is_err());

        req.worker.execute(&db.conn).await.unwrap();
        req.sink.execute(&db.conn).await.unwrap();
    }

    #[proptest(async = "tokio")]
    async fn create_drop_create(req: SinkWithRefs) {
        let db = Database::for_test().await;
        req.worker.execute(&db.conn).await.unwrap();
        req.sink.clone().execute(&db.conn).await.unwrap();
        (DropSink { name: Some(req.sink.name.clone()), id: None })
            .execute(&db.conn)
            .await
            .unwrap();
        req.sink.execute(&db.conn).await.unwrap();
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
    Network,
}

#[derive(Debug, Clone)]
pub(crate) struct SinkWithRefs {
    pub(crate) worker: CreateWorker,
    pub(crate) sink: CreateSink,
}

impl Arbitrary for SinkWithRefs {
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        ("[a-zA-Z]{1,10}", any::<CreateWorker>(), any::<SinkType>())
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

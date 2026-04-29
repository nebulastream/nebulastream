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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, DeriveEntityModel)]
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
                    schema: Default::default(),
                    config: Default::default(),
                    if_not_exists: false,
                };
                Self { worker, sink }
            })
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

#[derive(Debug, Clone)]
pub(crate) struct InlineSinkWithRefs {
    pub(crate) worker: CreateWorker,
    pub(crate) sink: CreateInlineSink,
}

impl Arbitrary for InlineSinkWithRefs {
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (any::<CreateWorker>(), any::<SinkType>(), any::<bool>())
            .prop_map(|(worker, sink_type, internal)| {
                let sink = CreateInlineSink {
                    sink_type,
                    schema: Default::default(),
                    config: Default::default(),
                    host_addr: Some(worker.host_addr.clone()),
                    internal,
                };
                Self { worker, sink }
            })
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

#[cfg(test)]
mod tests {
    use crate::ConnectorKind;
    use crate::Execute;
    use crate::database::Database;
    use crate::sink::{
        CreateInlineSink, CreateSink, DropSink, GetSink, InlineSinkWithRefs, SinkType, SinkWithRefs,
    };
    use test_strategy::proptest;

    #[proptest(async = "tokio")]
    async fn create_and_get(req: SinkWithRefs) {
        let db = Database::for_test().await;
        req.worker.execute(&db).await.unwrap();
        let created = req.sink.execute(&db).await.unwrap();
        assert_eq!(created.name, Some(req.sink.name.clone()));
        assert_eq!(created.sink_type, req.sink.sink_type);

        let sinks = GetSink::all()
            .with_name(req.sink.name)
            .execute(&db)
            .await
            .unwrap();
        assert_eq!(sinks.len(), 1);
        assert_eq!(sinks[0].id, created.id);
    }

    #[proptest(async = "tokio")]
    async fn drop_sink(req: SinkWithRefs) {
        let db = Database::for_test().await;
        req.worker.execute(&db).await.unwrap();
        req.sink.execute(&db).await.unwrap();

        let dropped = (DropSink {
            name: Some(req.sink.name.clone()),
            id: None,
        })
        .execute(&db)
        .await
        .unwrap();
        assert_eq!(dropped.len(), 1);

        let remaining = GetSink::all()
            .with_name(req.sink.name)
            .execute(&db)
            .await
            .unwrap();
        assert!(remaining.is_empty());
    }

    #[proptest(async = "tokio")]
    async fn name_unique(req: SinkWithRefs) {
        let db = Database::for_test().await;
        req.worker.execute(&db).await.unwrap();
        req.sink.execute(&db).await.unwrap();
        assert!(req.sink.execute(&db).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn worker_ref_required(req: SinkWithRefs) {
        let db = Database::for_test().await;
        assert!(req.sink.execute(&db).await.is_err());

        req.worker.execute(&db).await.unwrap();
        req.sink.execute(&db).await.unwrap();
    }

    #[proptest(async = "tokio")]
    async fn create_drop_create(req: SinkWithRefs) {
        let db = Database::for_test().await;
        req.worker.execute(&db).await.unwrap();
        req.sink.execute(&db).await.unwrap();
        (DropSink {
            name: Some(req.sink.name.clone()),
            id: None,
        })
        .execute(&db)
        .await
        .unwrap();
        req.sink.execute(&db).await.unwrap();
    }

    #[proptest(async = "tokio")]
    async fn if_not_exists_returns_existing(req: SinkWithRefs) {
        let db = Database::for_test().await;
        req.worker.execute(&db).await.unwrap();
        let original = req.sink.execute(&db).await.unwrap();

        let retry = CreateSink {
            name: req.sink.name.clone(),
            host_addr: req.worker.host_addr.clone(),
            sink_type: req.sink.sink_type,
            schema: serde_json::json!({"different": "schema"}),
            config: serde_json::json!({"different": "config"}),
            if_not_exists: true,
        };
        let returned = retry.execute(&db).await.unwrap();
        assert_eq!(returned, original);

        let all = GetSink::all().execute(&db).await.unwrap();
        assert_eq!(all.len(), 1);
    }

    #[proptest(async = "tokio")]
    async fn if_not_exists_creates_new_name(req: SinkWithRefs) {
        let db = Database::for_test().await;
        req.worker.execute(&db).await.unwrap();
        let original = req.sink.execute(&db).await.unwrap();

        let mut retry = req.sink;
        retry.name.push_str("_");
        let new = retry.execute(&db).await.unwrap();
        assert_ne!(new.id, original.id);
        assert_eq!(new.name, Some(retry.name));

        let all = GetSink::all().execute(&db).await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[proptest(async = "tokio")]
    async fn identical_inline_sinks(req1: InlineSinkWithRefs, req2: InlineSinkWithRefs) {
        let db = Database::for_test().await;
        req1.worker.execute(&db).await.unwrap();
        req2.worker.execute(&db).await.unwrap();

        let first = req1.sink.execute(&db).await.unwrap();
        let second = req2.sink.execute(&db).await.unwrap();

        assert_ne!(first.id, second.id);
        assert_eq!(first.name, None);
        assert_eq!(second.name, None);
    }

    #[proptest(async = "tokio")]
    async fn drop_blocked_by_active_query(mut req: crate::query::CreateQueryWithRefs) {
        use crate::query::{fragment::FragmentState, setup, walk_all};
        use crate::sink;
        use sea_orm::EntityTrait;

        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        let sink_id = req.query.sink_ids[0];

        assert!(sink::Entity::delete_by_id(sink_id).exec(&db).await.is_err());

        walk_all(&fragments, FragmentState::Completed, &db).await;

        sink::Entity::delete_by_id(sink_id).exec(&db).await.unwrap();
    }
}

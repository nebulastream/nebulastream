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

//! The sink entity and the requests that manage it.

mod create;
mod drop;
mod get;

pub use create::{CreateAnonymousSink, CreateSink};
pub use drop::DropSink;
pub use get::GetSink;

use crate::identifier::SinkId;
#[cfg(test)]
use crate::worker::CreateWorker;
use crate::worker::endpoint::NetworkAddr;
use crate::{ConnectorKind, worker};
#[cfg(test)]
use proptest::strategy::Strategy;
use sea_orm::entity::prelude::*;
use serde::Serialize;

/// An output that queries write to, placed on one worker.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "sink")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: SinkId,
    /// Nullable for query-owned (anonymous/internal) sinks;
    /// required and unique for shared, user-managed sinks.
    #[sea_orm(unique)]
    pub name: Option<String>,
    pub host_addr: NetworkAddr,
    /// Free-form connector type owned by the C++ side (for example `VOID` or `NETWORK`);
    /// the catalog only stores and returns it.
    pub sink_type: String,
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
#[derive(Debug, Clone, test_strategy::Arbitrary)]
pub(crate) struct SinkWithRefs {
    pub(crate) worker: CreateWorker,
    #[strategy(("[a-zA-Z]{1,10}", "[A-Za-z]{1,10}").prop_map(move |(name, sink_type)| CreateSink {
        name,
        host_addr: #worker.host_addr.clone(),
        sink_type,
        schema: Default::default(),
        config: Default::default(),
        if_not_exists: false,
    }))]
    pub(crate) sink: CreateSink,
}

#[cfg(test)]
mod tests {
    use crate::database::Database;
    use crate::error::{ErrorCode, code_of};
    use crate::sink::{CreateAnonymousSink, CreateSink, DropSink, GetSink, SinkWithRefs};
    use crate::{ConnectorKind, Execute};
    use test_strategy::proptest;

    #[proptest(async = "tokio")]
    async fn create_and_get(req: SinkWithRefs) {
        let db = Database::for_test().await;
        let unplaced = code_of(req.sink.execute(&db).await);
        assert_eq!(unplaced, Some(ErrorCode::UnknownWorker));

        req.worker.execute(&db).await.unwrap();
        let created = req.sink.execute(&db).await.unwrap();
        assert_eq!(created.name, Some(req.sink.name.clone()));
        assert_eq!(created.sink_type, req.sink.sink_type);
        let taken = code_of(req.sink.execute(&db).await);
        assert_eq!(taken, Some(ErrorCode::SinkAlreadyExists));

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
            .with_name(req.sink.name.clone())
            .execute(&db)
            .await
            .unwrap();
        assert!(remaining.is_empty());
        req.sink.execute(&db).await.unwrap();
    }

    #[proptest(async = "tokio")]
    async fn if_not_exists_matches_on_the_name(req: SinkWithRefs) {
        let db = Database::for_test().await;
        req.worker.execute(&db).await.unwrap();
        let original = req.sink.execute(&db).await.unwrap();

        let mut retry = CreateSink {
            schema: serde_json::json!({"different": "schema"}),
            config: serde_json::json!({"different": "config"}),
            if_not_exists: true,
            ..req.sink
        };
        assert_eq!(retry.execute(&db).await.unwrap(), original);

        retry.name.push_str("_");
        let new = retry.execute(&db).await.unwrap();
        assert_ne!(new.id, original.id);
        assert_eq!(new.name, Some(retry.name));

        let all = GetSink::all().execute(&db).await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[proptest(async = "tokio")]
    async fn filters_select_one_of_several(req: SinkWithRefs, internal: bool) {
        let db = &Database::for_test().await;
        req.worker.execute(db).await.unwrap();
        let first = req.sink.execute(db).await.unwrap();
        let second = CreateSink {
            name: format!("{}_", req.sink.name),
            ..req.sink.clone()
        };
        let second = second.execute(db).await.unwrap();
        let anonymous = CreateAnonymousSink {
            sink_type: req.sink.sink_type,
            schema: req.sink.schema,
            config: req.sink.config,
            host_addr: req.sink.host_addr,
            internal,
        };
        let anonymous = anonymous.execute(db).await.unwrap();
        assert_eq!(anonymous.kind == ConnectorKind::Internal, internal);

        let get = |request: GetSink| async move { request.execute(db).await.unwrap() };
        let by_kind = GetSink {
            kind: Some(anonymous.kind),
            ..GetSink::all()
        };
        assert_eq!(
            get(GetSink::all().with_name(req.sink.name)).await,
            [first.clone()]
        );
        assert_eq!(
            get(GetSink::all().with_id(second.id)).await,
            [second.clone()]
        );
        assert_eq!(get(by_kind).await, [anonymous.clone()]);
        assert_eq!(get(GetSink::all()).await.len(), 3);

        let by_id = DropSink {
            id: Some(first.id),
            name: None,
        };
        assert_eq!(by_id.execute(db).await.unwrap(), [first]);
        assert_eq!(DropSink::all().execute(db).await.unwrap(), [second]);
        assert_eq!(get(GetSink::all()).await, [anonymous]);
    }

    #[proptest(async = "tokio")]
    async fn identical_inline_sinks(req: SinkWithRefs, internal: bool) {
        let db = Database::for_test().await;
        req.worker.execute(&db).await.unwrap();
        let anonymous = CreateAnonymousSink {
            sink_type: req.sink.sink_type,
            schema: req.sink.schema,
            config: req.sink.config,
            host_addr: req.sink.host_addr,
            internal,
        };

        let first = anonymous.execute(&db).await.unwrap();
        let second = anonymous.execute(&db).await.unwrap();

        assert_ne!(first.id, second.id);
        assert_eq!((first.name, second.name), (None, None));
    }
}

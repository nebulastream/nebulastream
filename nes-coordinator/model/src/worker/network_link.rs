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

//! The network-link entity: a directed edge between two workers. Links can be
//! declared independently of worker registration and are not backed by a
//! foreign key.

use crate::worker::endpoint::NetworkAddr;
use sea_orm::entity::prelude::*;

/// A directed network edge between two workers. Both columns logically
/// reference `worker.host_addr`, but no foreign key is enforced. This is
/// deliberate: a foreign key would force clients to insert both workers
/// before declaring a link between them, whereas we want links to be
/// declarable ahead of (or independently of) worker registration. For
/// the same reason, removing a worker does not delete its links; they
/// stay as dangling edges. Both cases are expected, and query planning
/// resolves links against the live worker set, ignoring dangling ones.
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "network_link")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub source_host_addr: NetworkAddr,
    #[sea_orm(primary_key, auto_increment = false)]
    pub target_host_addr: NetworkAddr,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "crate::worker::Entity",
        from = "Column::SourceHostAddr",
        to = "crate::worker::Column::HostAddr"
    )]
    SourceWorker,
    #[sea_orm(
        belongs_to = "crate::worker::Entity",
        from = "Column::TargetHostAddr",
        to = "crate::worker::Column::HostAddr"
    )]
    TargetWorker,
}

impl ActiveModelBehavior for ActiveModel {}

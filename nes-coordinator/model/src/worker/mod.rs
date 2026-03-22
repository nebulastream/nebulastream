pub mod endpoint;
pub mod network_link;
pub mod topology;

use endpoint::{GrpcAddr, HostAddr};
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use sea_orm::{Condition, NotSet};
use strum::{Display, EnumIter};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Display, EnumIter, serde::Serialize, DeriveActiveEnum)]
#[sea_orm(
    rs_type = "String",
    db_type = "Text",
    rename_all = "PascalCase"
)]
pub enum WorkerState {
    #[default]
    Pending,
    Active,
    Unreachable,
    Removed,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Display, EnumIter, serde::Serialize, DeriveActiveEnum)]
#[sea_orm(
    rs_type = "String",
    db_type = "Text",
    rename_all = "PascalCase"
)]
pub enum DesiredWorkerState {
    #[default]
    Active,
    Removed,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "worker")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub host_addr: HostAddr,
    #[sea_orm(unique)]
    pub grpc_addr: GrpcAddr,
    pub capacity: i32,
    pub current_state: WorkerState,
    pub desired_state: DesiredWorkerState,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "crate::source::physical_source::Entity")]
    PhysicalSource,
    #[sea_orm(has_many = "crate::sink::Entity")]
    Sink,
    #[sea_orm(has_many = "crate::query::fragment::Entity")]
    Fragment,
}

impl Related<crate::source::physical_source::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PhysicalSource.def()
    }
}

impl Related<crate::sink::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Sink.def()
    }
}

impl Related<crate::query::fragment::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Fragment.def()
    }
}

#[async_trait::async_trait]
impl ActiveModelBehavior for ActiveModel {
    async fn before_save<C>(self, _db: &C, _insert: bool) -> Result<Self, DbErr>
    where
        C: ConnectionTrait,
    {
        if self.host_addr == self.grpc_addr {
            return Err(DbErr::Custom(format!(
                "GrpcAddr must be different from HostAddr: {:?}",
                self.host_addr
            )));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone)]
pub struct CreateWorker {
    pub host_addr: HostAddr,
    pub grpc_addr: GrpcAddr,
    pub capacity: i32,
    pub peers: Vec<HostAddr>,
    pub config: serde_json::Value,
}

impl CreateWorker {
    pub fn new(host_addr: HostAddr, grpc_addr: GrpcAddr, capacity: i32) -> Self {
        Self {
            host_addr,
            grpc_addr,
            capacity,
            peers: Vec::new(),
            config: serde_json::Value::Object(Default::default()),
        }
    }

    pub fn with_peers(mut self, peers: Vec<HostAddr>) -> Self {
        self.peers = peers;
        self
    }
}

impl From<CreateWorker> for ActiveModel {
    fn from(req: CreateWorker) -> Self {
        Self {
            host_addr: Set(req.host_addr),
            grpc_addr: Set(req.grpc_addr),
            capacity: Set(req.capacity),
            current_state: NotSet,
            desired_state: NotSet,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct GetWorker {
    pub host_addr: Option<HostAddr>,
    pub desired_state: Option<DesiredWorkerState>,
}

impl GetWorker {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_host_addr(mut self, host_addr: HostAddr) -> Self {
        self.host_addr = Some(host_addr);
        self
    }

    pub fn with_desired_state(mut self, state: DesiredWorkerState) -> Self {
        self.desired_state = Some(state);
        self
    }
}

impl crate::IntoCondition for GetWorker {
    fn into_condition(self) -> Condition {
        Condition::all()
            .add_option(self.host_addr.map(|v| Column::HostAddr.eq(v)))
            .add_option(self.desired_state.map(|v| Column::DesiredState.eq(v)))
    }
}

#[derive(Clone, Debug)]
pub struct DropWorker {
    pub host_addr: HostAddr,
}

impl DropWorker {
    pub fn new(host_addr: HostAddr) -> Self {
        Self { host_addr }
    }
}

#[cfg(feature = "testing")]
impl crate::Generate for CreateWorker {
    fn generate() -> proptest::strategy::BoxedStrategy<Self> {
        use proptest::prelude::*;
        (
            endpoint::NetworkAddr::generate(),
            1024..65535u16,
            Self::arb_capacity(),
        )
            .prop_map(|(host_addr, grpc_port, capacity)| {
                let grpc_port = if grpc_port == host_addr.port {
                    if grpc_port < 65534 { grpc_port + 1 } else { grpc_port - 1 }
                } else {
                    grpc_port
                };
                let grpc_addr = endpoint::NetworkAddr::new(host_addr.host.clone(), grpc_port);
                CreateWorker::new(host_addr, grpc_addr, capacity)
            })
            .boxed()
    }
}

#[cfg(feature = "testing")]
impl CreateWorker {
    fn arb_capacity() -> impl proptest::prelude::Strategy<Value = i32> {
        use proptest::prelude::*;
        prop_oneof![
            1 => Just(0i32),
            5 => 1..=16i32,
            3 => 17..=128i32,
            1 => 129..=1024i32,
        ]
    }

    pub fn topology(min_workers: u8) -> proptest::strategy::BoxedStrategy<Vec<CreateWorker>> {
        use proptest::prelude::*;
        const MAX_SIM_WORKERS: u8 = 16;
        (min_workers..=MAX_SIM_WORKERS)
            .prop_flat_map(|n| {
                prop::collection::vec(Self::arb_capacity(), n as usize..=n as usize).prop_map(
                    |capacities| {
                        capacities
                            .into_iter()
                            .enumerate()
                            .map(|(i, capacity)| {
                                let octet = (i + 1) as u8;
                                let host = endpoint::NetworkAddr::new(
                                    format!("192.168.2.{octet}"),
                                    endpoint::DEFAULT_DATA_PORT,
                                );
                                let grpc = endpoint::NetworkAddr::new(
                                    format!("192.168.2.{octet}"),
                                    endpoint::DEFAULT_GRPC_PORT,
                                );
                                CreateWorker::new(host, grpc, capacity)
                            })
                            .collect()
                    },
                )
            })
            .boxed()
    }

    pub fn dag_topology(
        max_workers: usize,
    ) -> proptest::strategy::BoxedStrategy<Vec<CreateWorker>> {
        use crate::Generate as _;
        use proptest::prelude::*;
        (2..=max_workers)
            .prop_flat_map(|n| {
                let max_edges = n * (n - 1) / 2;
                (
                    prop::collection::vec(CreateWorker::generate(), n..=n).prop_map(|workers| {
                        workers
                            .into_iter()
                            .enumerate()
                            .map(|(i, mut w)| {
                                w.host_addr.port = 10000 + i as u16;
                                w.grpc_addr.port = 20000 + i as u16;
                                w
                            })
                            .collect::<Vec<_>>()
                    }),
                    prop::collection::vec(any::<bool>(), max_edges..=max_edges),
                )
                    .prop_map(|(mut workers, flags)| {
                        let mut peers_per_worker: Vec<Vec<endpoint::HostAddr>> =
                            vec![Vec::new(); workers.len()];
                        let mut idx = 0;
                        for i in 0..workers.len() {
                            for j in (i + 1)..workers.len() {
                                if flags[idx] {
                                    peers_per_worker[i].push(workers[j].host_addr.clone());
                                }
                                idx += 1;
                            }
                        }
                        for (i, peers) in peers_per_worker.into_iter().enumerate() {
                            workers[i].peers = peers;
                        }
                        workers
                    })
            })
            .boxed()
    }
}

#[cfg(feature = "testing")]
impl crate::Generate for Vec<CreateWorker> {
    fn generate() -> proptest::strategy::BoxedStrategy<Self> {
        CreateWorker::topology(1)
    }
}

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

//! What the client sent to the coordinator and what the coordinator replied.
//!
//! The ledger never reads the catalog, so a write that the coordinator confirmed and later lost shows up in the end-state checks.
//! Driver workloads draw their next statement from it.
//! A trial starts with a fixed logical source, physical source and sink, on two workers that are never dropped.

use crate::harness::{Harness, build_ip};
use anyhow::{Error, Result};
use madsim::rand::seq::IteratorRandom;
use madsim::rand::{Rng, thread_rng};
use model::error::{CodedError, ErrorCode};
use model::identifier::{QueryFragmentId, QueryId, SinkId, SourceId};
use model::query::query_fragment::CreateQueryFragment;
use model::query::{CreateQuery, DropQuery, GetQuery, QueryWithFragments};
use model::sink::CreateSink;
use model::source::logical::CreateLogicalSource;
use model::source::physical::CreatePhysicalSource;
use model::statement::{ParsedStatement, StatementResult};
use model::worker::endpoint::{DEFAULT_DATA_PORT, DEFAULT_HOST_PORT, NetworkAddr};
use model::worker::{CreateWorker, DropWorker};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use tracing::info;

const LOGICAL_SOURCE_NAME: &str = "source";
const SINK_NAME: &str = "sink";
const SOURCE_HOST_OCTET: u8 = 2;
const SINK_HOST_OCTET: u8 = 3;
const FIRST_WORKER_OCTET: u8 = 4;

/// The source host and the sink host are the first two registrations.
const PINNED: [RegistrationId; 2] = [0, 1];

/// One registration of an address.
/// An address that is dropped and registered again has a second registration,
/// and a query placed on the first one still fails when the second one runs.
pub type RegistrationId = usize;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    Created,
    /// The drop was sent and its reply is outstanding.
    Dropping,
    Dropped,
}

struct Registration {
    addr: NetworkAddr,
    lifecycle: Lifecycle,
}

pub struct LedgerQuery {
    lifecycle: Lifecycle,
    fragments: BTreeMap<QueryFragmentId, RegistrationId>,
}

impl LedgerQuery {
    pub fn is_dropped(&self) -> bool {
        self.lifecycle == Lifecycle::Dropped
    }

    pub const fn fragments(&self) -> &BTreeMap<QueryFragmentId, RegistrationId> {
        &self.fragments
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Operation {
    CreateWorker,
    DropWorker,
    CreateQuery,
    DropQuery,
}

/// What the ledger needs to record the reply of a sent statement.
pub enum Sent {
    CreateWorker(NetworkAddr),
    DropWorker(RegistrationId),
    CreateQuery(HashMap<NetworkAddr, RegistrationId>),
    DropQuery(QueryId),
}

pub struct Ledger {
    source: SourceId,
    sink: SinkId,
    registrations: Vec<Registration>,
    /// Addresses with a create in flight, so no second create races it.
    registering: HashSet<NetworkAddr>,
    next_octet: u8,
    queries: BTreeMap<QueryId, LedgerQuery>,
}

impl Ledger {
    /// Creates the source, the sink and their two hosts.
    pub async fn setup(harness: &Harness) -> Self {
        let source_host = build_addr(SOURCE_HOST_OCTET);
        let sink_host = build_addr(SINK_HOST_OCTET);

        send_expecting(
            harness,
            ParsedStatement::CreateLogicalSource(CreateLogicalSource {
                name: LOGICAL_SOURCE_NAME.parse().unwrap(),
                schema: Default::default(),
                if_not_exists: false,
            }),
        )
        .await;
        for addr in [&source_host, &sink_host] {
            send_expecting(harness, build_create_worker(addr.clone())).await;
        }
        let StatementResult::CreatedPhysicalSource(source) = send_expecting(
            harness,
            ParsedStatement::CreatePhysicalSource(CreatePhysicalSource {
                logical_source: LOGICAL_SOURCE_NAME.parse().unwrap(),
                host_addr: source_host.clone(),
                source_type: "File".parse().unwrap(),
                source_config: Default::default(),
                parser_config: Default::default(),
                if_not_exists: false,
            }),
        )
        .await
        else {
            panic!("a physical source create returned another kind of result");
        };
        let StatementResult::CreatedSink(sink) = send_expecting(
            harness,
            ParsedStatement::CreateSink(CreateSink {
                name: SINK_NAME.parse().unwrap(),
                host_addr: sink_host.clone(),
                sink_type: "File".parse().unwrap(),
                schema: Default::default(),
                config: Default::default(),
                if_not_exists: false,
            }),
        )
        .await
        else {
            panic!("a sink create returned another kind of result");
        };

        Self {
            source: source.id,
            sink: sink.id,
            registrations: [source_host, sink_host]
                .into_iter()
                .map(|addr| Registration {
                    addr,
                    lifecycle: Lifecycle::Created,
                })
                .collect(),
            registering: HashSet::new(),
            next_octet: FIRST_WORKER_OCTET,
            queries: BTreeMap::new(),
        }
    }

    /// Returns `None` if the operation has no target, such as a drop with nothing left to drop.
    /// A drop reserves its target, so no concurrent drop picks it again.
    pub fn plan(&mut self, op: Operation) -> Option<(ParsedStatement, Sent)> {
        match op {
            Operation::CreateWorker => Some(self.plan_create_worker()),
            Operation::DropWorker => self.plan_drop_worker(),
            Operation::CreateQuery => Some(self.plan_create_query()),
            Operation::DropQuery => self.plan_drop_query(),
        }
    }

    /// Panics on a reply that contradicts what the client sent.
    pub fn record(&mut self, sent: Sent, reply: Result<StatementResult>) {
        match sent {
            Sent::CreateWorker(addr) => self.record_create_worker(addr, reply),
            Sent::DropWorker(id) => self.record_drop_worker(id, reply),
            Sent::CreateQuery(hosts) => self.record_create_query(&hosts, reply),
            Sent::DropQuery(id) => self.record_drop_query(id, reply),
        }
    }

    /// Registers a dropped address again half of the time, and a new address otherwise.
    fn plan_create_worker(&mut self) -> (ParsedStatement, Sent) {
        let mut rng = thread_rng();
        let reusable = self
            .get_dropped_addrs()
            .into_iter()
            .filter(|addr| !self.registering.contains(addr))
            .choose(&mut rng);
        let addr = match reusable {
            Some(addr) if rng.gen_bool(0.5) => {
                info!("registering {addr} again");
                addr
            }
            _ => {
                let octet = self.next_octet;
                self.next_octet = octet.checked_add(1).expect("ran out of worker IPs");
                build_addr(octet)
            }
        };
        self.registering.insert(addr.clone());
        (build_create_worker(addr.clone()), Sent::CreateWorker(addr))
    }

    /// The addresses whose latest registration is dropped.
    fn get_dropped_addrs(&self) -> Vec<NetworkAddr> {
        self.registrations
            .iter()
            .enumerate()
            .filter(|&(id, registration)| {
                registration.lifecycle == Lifecycle::Dropped
                    && !self.registrations[id + 1..]
                        .iter()
                        .any(|later| later.addr == registration.addr)
            })
            .map(|(_, registration)| registration.addr.clone())
            .collect()
    }

    fn plan_drop_worker(&mut self) -> Option<(ParsedStatement, Sent)> {
        let id = self
            .get_created_workers()
            .filter(|id| !PINNED.contains(id))
            .choose(&mut thread_rng())?;
        let registration = &mut self.registrations[id];
        registration.lifecycle = Lifecycle::Dropping;
        Some((
            ParsedStatement::DropWorker(DropWorker::new(registration.addr.clone())),
            Sent::DropWorker(id),
        ))
    }

    /// Places fragments only on workers without a drop in flight.
    /// The coordinator applies statements in the order that they arrive,
    /// so every fragment exists before a later drop of its host, and that drop fails it.
    fn plan_create_query(&self) -> (ParsedStatement, Sent) {
        let mut rng = thread_rng();
        let droppable: Vec<RegistrationId> = self
            .get_created_workers()
            .filter(|id| !PINNED.contains(id))
            .collect();
        let count = rng.gen_range(0..=droppable.len());
        let hosts: HashMap<NetworkAddr, RegistrationId> = droppable
            .into_iter()
            .choose_multiple(&mut rng, count)
            .into_iter()
            .chain(PINNED)
            .map(|id| (self.registrations[id].addr.clone(), id))
            .collect();
        let source_host = &self.registrations[PINNED[0]].addr;

        let statement = ParsedStatement::CreateQuery(CreateQuery {
            name: None,
            sql: format!("SELECT * FROM {LOGICAL_SOURCE_NAME} INTO {SINK_NAME}"),
            global_plan: vec![],
            fragments: hosts
                .keys()
                .map(|addr| CreateQueryFragment {
                    host_addr: addr.clone(),
                    plan: vec![],
                    num_operators: 1,
                    has_source: addr == source_host,
                })
                .collect(),
            source_ids: BTreeSet::from([self.source]),
            sink_ids: BTreeSet::from([self.sink]),
        });
        (statement, Sent::CreateQuery(hosts))
    }

    fn plan_drop_query(&mut self) -> Option<(ParsedStatement, Sent)> {
        let (&id, query) = self
            .queries
            .iter_mut()
            .filter(|(_, query)| query.lifecycle == Lifecycle::Created)
            .choose(&mut thread_rng())?;
        query.lifecycle = Lifecycle::Dropping;
        Some((
            ParsedStatement::DropQuery(DropQuery::all().with_filters(GetQuery::all().with_id(id))),
            Sent::DropQuery(id),
        ))
    }

    /// A create of an address whose earlier registration is still being removed fails with `WorkerAlreadyExists`.
    fn record_create_worker(&mut self, addr: NetworkAddr, reply: Result<StatementResult>) {
        self.registering.remove(&addr);
        match reply {
            Ok(StatementResult::CreatedWorker(worker)) => {
                assert_eq!(
                    worker.host_addr, addr,
                    "a worker create returned another worker"
                );
                self.registrations.push(Registration {
                    addr,
                    lifecycle: Lifecycle::Created,
                });
            }
            Err(error) if get_code(&error) == Some(ErrorCode::WorkerAlreadyExists) => {
                assert!(
                    self.get_dropped_addrs().contains(&addr),
                    "the create of {addr} failed as already existing, but the client never dropped it: {error:#}"
                );
                info!("{addr} is still being removed");
            }
            other => panic!("unexpected reply to the create of worker {addr}: {other:?}"),
        }
    }

    fn record_drop_worker(&mut self, id: RegistrationId, reply: Result<StatementResult>) {
        let registration = &mut self.registrations[id];
        let addr = &registration.addr;
        match reply {
            Ok(StatementResult::DroppedWorker(Some(worker))) => {
                assert_eq!(
                    &worker.host_addr, addr,
                    "a worker drop returned another worker"
                );
            }
            Ok(StatementResult::DroppedWorker(None)) => {
                panic!("the catalog had no worker {addr} to drop")
            }
            other => panic!("unexpected reply to the drop of worker {addr}: {other:?}"),
        }
        registration.lifecycle = Lifecycle::Dropped;
    }

    fn record_create_query(
        &mut self,
        hosts: &HashMap<NetworkAddr, RegistrationId>,
        reply: Result<StatementResult>,
    ) {
        let Ok(StatementResult::CreatedQuery(QueryWithFragments { query, fragments })) = reply
        else {
            panic!("unexpected reply to a query create: {reply:?}");
        };
        assert!(
            !self.queries.contains_key(&query.id),
            "query {} already created",
            query.id
        );
        let fragments = fragments
            .into_iter()
            .map(|fragment| {
                let host = hosts.get(&fragment.host_addr).unwrap_or_else(|| {
                    panic!(
                        "query {} has fragment {} on {}, which the client did not place it on",
                        query.id, fragment.id, fragment.host_addr
                    )
                });
                (fragment.id, *host)
            })
            .collect();
        self.queries.insert(
            query.id,
            LedgerQuery {
                lifecycle: Lifecycle::Created,
                fragments,
            },
        );
    }

    fn record_drop_query(&mut self, id: QueryId, reply: Result<StatementResult>) {
        let Ok(StatementResult::DroppedQueries(dropped)) = reply else {
            panic!("unexpected reply to the drop of query {id}: {reply:?}");
        };
        let [dropped] = dropped.as_slice() else {
            panic!(
                "a drop by id must remove one query, removed {}",
                dropped.len()
            );
        };
        assert_eq!(
            dropped.id, id,
            "the drop of query {id} removed another query"
        );
        let query = self.queries.get_mut(&id).expect("dropped an unknown query");
        assert!(
            query.lifecycle == Lifecycle::Dropping,
            "query {id} dropped twice"
        );
        query.lifecycle = Lifecycle::Dropped;
    }

    fn get_created_workers(&self) -> impl Iterator<Item = RegistrationId> + '_ {
        self.registrations
            .iter()
            .enumerate()
            .filter(|(_, registration)| registration.lifecycle == Lifecycle::Created)
            .map(|(id, _)| id)
    }

    /// Only after every workload finished, when no reply is outstanding.
    pub fn get_registered_addrs(&self) -> Vec<NetworkAddr> {
        self.get_created_workers()
            .map(|id| self.registrations[id].addr.clone())
            .collect()
    }

    pub fn get_addr(&self, id: RegistrationId) -> &NetworkAddr {
        &self.registrations[id].addr
    }

    pub fn is_dropped(&self, id: RegistrationId) -> bool {
        self.registrations[id].lifecycle == Lifecycle::Dropped
    }

    pub const fn queries(&self) -> &BTreeMap<QueryId, LedgerQuery> {
        &self.queries
    }
}

fn build_addr(octet: u8) -> NetworkAddr {
    NetworkAddr::new(build_ip(octet).to_string(), DEFAULT_HOST_PORT).unwrap()
}

fn build_create_worker(host_addr: NetworkAddr) -> ParsedStatement {
    let data_addr = NetworkAddr::new(host_addr.host(), DEFAULT_DATA_PORT).unwrap();
    ParsedStatement::CreateWorker(CreateWorker {
        host_addr,
        data_addr,
        max_operators: None,
        peers: vec![],
        config: Default::default(),
        if_not_exists: false,
    })
}

async fn send_expecting(harness: &Harness, statement: ParsedStatement) -> StatementResult {
    let description = format!("{statement:?}");
    harness
        .send(statement)
        .await
        .unwrap_or_else(|e| panic!("the coordinator rejected {description}: {e:#}"))
}

fn get_code(error: &Error) -> Option<ErrorCode> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<CodedError>())
        .map(|coded| coded.code)
}

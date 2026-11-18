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

//! In-memory mirror of what the workloads expect the coordinator's catalog to contain.
//! Invariants compare it against the real catalog and against worker-reported state.
//!
//! Workloads do not send statements directly.
//! A workload picks an operation, the model resolves it into a ready statement or the next prerequisite,
//! the workload sends the statement, and the model observes the response.
//! Resolving every operation through the model keeps the mirror and the assertions consistent
//! without catalog details in every workload.

use crate::harness::{FIRST_WORKER_OCTET, build_ip};
use anyhow::{Context, Result};
use madsim::rand::seq::IteratorRandom;
use madsim::rand::{Rng, thread_rng};
use model::identifier::{QueryFragmentId, QueryId, SinkId, SourceId};
use model::query::query_fragment::CreateQueryFragment;
use model::query::{CreateQuery, DropQuery, GetQuery, QueryWithFragments};
use model::sink::CreateSink;
use model::source::logical::CreateLogicalSource;
use model::source::physical::CreatePhysicalSource;
use model::statement::{ParsedStatement, StatementResult};
use model::worker::endpoint::{DEFAULT_DATA_PORT, DEFAULT_HOST_PORT, NetworkAddr};
use model::worker::{CreateWorker, DropWorker};
use std::collections::{BTreeSet, HashMap, HashSet};

const LOGICAL_SOURCE_NAME: &str = "source";
const SINK_NAME: &str = "sink";

#[derive(Clone, Copy, Debug)]
pub enum Operation {
    CreateWorker,
    DropWorker,
    CreateQuery,
    DropQuery,
}

/// The next statement to send for an operation.
/// After a prerequisite's response is observed, the operation is resolved again.
pub enum Step {
    /// A statement that must succeed before the operation's own statement can be built.
    Prerequisite(ParsedStatement),
    /// The operation's own statement.
    Ready(ParsedStatement),
}

impl Step {
    fn into_prerequisite(self) -> Self {
        match self {
            Self::Ready(s) | Self::Prerequisite(s) => Self::Prerequisite(s),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum QueryStatus {
    Active,
    Dropped,
    /// A fragment of the query was on a worker when the worker was dropped.
    Failed,
}

struct Query {
    status: QueryStatus,
    fragments: HashMap<QueryFragmentId, NetworkAddr>,
}

#[derive(Default)]
pub struct ModelState {
    active_workers: HashSet<NetworkAddr>,
    dropped_workers: HashSet<NetworkAddr>,
    pinned_workers: HashSet<NetworkAddr>,
    leaving_workers: HashSet<NetworkAddr>,
    allocated_worker_ips: u8,

    has_logical_source: bool,
    source: Option<(SourceId, NetworkAddr)>,

    sink: Option<(SinkId, NetworkAddr)>,

    queries: HashMap<QueryId, Query>,
    leaving_queries: HashSet<QueryId>,
}

impl ModelState {
    pub fn resolve(&mut self, op: Operation) -> Result<Step> {
        match op {
            Operation::CreateWorker => self.create_worker(),
            Operation::DropWorker => self.drop_worker(),
            Operation::CreateQuery => self.create_query(),
            Operation::DropQuery => self.drop_query(),
        }
    }

    /// Applies a statement response to the model.
    /// Every response has to come back through here,
    /// otherwise the model and the real catalog diverge and the invariants report a mismatch that the system did not cause.
    pub fn observe(&mut self, reply: StatementResult) {
        match reply {
            StatementResult::CreatedWorker(worker) => {
                self.active_workers.insert(worker.host_addr);
            }
            StatementResult::DroppedWorker(worker) => {
                let addr = worker
                    .expect("the catalog dropped a worker it does not know")
                    .host_addr;
                assert!(self.active_workers.remove(&addr), "unknown worker {addr}");
                for query in self.queries.values_mut() {
                    if query.status == QueryStatus::Active
                        && query.fragments.values().any(|a| *a == addr)
                    {
                        query.status = QueryStatus::Failed;
                    }
                }
                self.dropped_workers.insert(addr);
            }
            StatementResult::CreatedLogicalSource(_) => {
                self.has_logical_source = true;
            }
            StatementResult::CreatedPhysicalSource(source) => {
                self.pinned_workers.insert(source.host_addr.clone());
                self.source = Some((source.id, source.host_addr));
            }
            StatementResult::CreatedSink(sink) => {
                self.pinned_workers.insert(sink.host_addr.clone());
                self.sink = Some((sink.id, sink.host_addr));
            }
            StatementResult::CreatedQuery(QueryWithFragments { query, fragments }) => {
                assert!(
                    !self.queries.contains_key(&query.id),
                    "query {} already created",
                    query.id
                );
                // Workloads run concurrently, so the model can observe a worker drop between this query's placement and its response.
                let on_dropped_worker = fragments
                    .iter()
                    .any(|f| self.dropped_workers.contains(&f.host_addr));
                let status = if on_dropped_worker {
                    QueryStatus::Failed
                } else {
                    QueryStatus::Active
                };
                self.queries.insert(
                    query.id,
                    Query {
                        status,
                        fragments: fragments.into_iter().map(|f| (f.id, f.host_addr)).collect(),
                    },
                );
            }
            StatementResult::DroppedQueries(dropped) => {
                let [dropped] = dropped.as_slice() else {
                    panic!(
                        "a drop by id must remove one query, removed {}",
                        dropped.len()
                    );
                };
                let query = self.queries.get_mut(&dropped.id).unwrap_or_else(|| {
                    panic!("query {} dropped but not active in the model", dropped.id)
                });
                match query.status {
                    // A worker drop that was observed before this response has already failed the query.
                    QueryStatus::Failed => {}
                    QueryStatus::Active => query.status = QueryStatus::Dropped,
                    QueryStatus::Dropped => panic!("query {} dropped twice", dropped.id),
                }
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    fn create_query(&mut self) -> Result<Step> {
        let Some((source_id, source_addr)) = &self.source else {
            return self.create_physical_source().map(Step::into_prerequisite);
        };
        let Some((sink_id, sink_addr)) = &self.sink else {
            return self.create_sink().map(Step::into_prerequisite);
        };
        let mut rng = thread_rng();
        // The source and sink hosts always get a fragment.
        // The other fragments go to a random subset of the workers.
        let k = rng.gen_range(0..=self.active_workers.len());
        let workers: HashSet<&NetworkAddr> = self
            .active_workers
            .iter()
            .choose_multiple(&mut rng, k)
            .into_iter()
            .chain([source_addr, sink_addr])
            .collect();

        Ok(Step::Ready(ParsedStatement::CreateQuery(CreateQuery {
            name: None,
            sql: format!("SELECT * FROM {LOGICAL_SOURCE_NAME} INTO {SINK_NAME}"),
            fragments: workers
                .into_iter()
                .map(|addr| CreateQueryFragment {
                    host_addr: addr.clone(),
                    plan: vec![],
                    num_operators: 1,
                    has_source: addr == source_addr,
                })
                .collect(),
            source_ids: BTreeSet::from([*source_id]),
            sink_ids: BTreeSet::from([*sink_id]),
        })))
    }

    fn create_sink(&mut self) -> Result<Step> {
        let Some(addr) = self.pin_worker() else {
            return self.create_worker().map(Step::into_prerequisite);
        };
        Ok(Step::Ready(ParsedStatement::CreateSink(CreateSink {
            name: SINK_NAME.to_string(),
            host_addr: addr,
            sink_type: "File".to_string(),
            schema: Default::default(),
            config: Default::default(),
            if_not_exists: false,
        })))
    }

    fn create_logical_source() -> Step {
        Step::Ready(ParsedStatement::CreateLogicalSource(CreateLogicalSource {
            name: LOGICAL_SOURCE_NAME.to_string(),
            schema: Default::default(),
            if_not_exists: true,
        }))
    }

    fn create_physical_source(&mut self) -> Result<Step> {
        if !self.has_logical_source {
            return Ok(Self::create_logical_source().into_prerequisite());
        }
        let Some(addr) = self.pin_worker() else {
            return self.create_worker().map(Step::into_prerequisite);
        };
        Ok(Step::Ready(ParsedStatement::CreatePhysicalSource(
            CreatePhysicalSource {
                logical_source: LOGICAL_SOURCE_NAME.to_string(),
                host_addr: addr,
                source_type: "File".to_string(),
                source_config: Default::default(),
                parser_config: Default::default(),
                if_not_exists: false,
            },
        )))
    }

    fn drop_query(&mut self) -> Result<Step> {
        let qid = self
            .queries
            .iter()
            // Regression: only dropped queries are skipped, so a failed query can be picked.
            .filter(|(_, query)| query.status != QueryStatus::Dropped)
            .map(|(&id, _)| id)
            // A concurrent drop of the same query would fail the model on its second reply.
            .filter(|id| !self.leaving_queries.contains(id))
            .choose(&mut thread_rng())
            .context("no query to drop")?;
        self.leaving_queries.insert(qid);
        Ok(Step::Ready(ParsedStatement::DropQuery(
            DropQuery::all().with_filters(GetQuery::all().with_id(qid)),
        )))
    }

    fn drop_worker(&mut self) -> Result<Step> {
        let addr = self
            .active_workers
            .iter()
            // The model uses its source and sink for every later query, so their hosts stay.
            // The model drops a worker at most once.
            .filter(|addr| {
                !self.pinned_workers.contains(*addr) && !self.leaving_workers.contains(*addr)
            })
            .choose(&mut thread_rng())
            .context("no workers to drop")?
            .clone();
        self.leaving_workers.insert(addr.clone());
        Ok(Step::Ready(ParsedStatement::DropWorker(DropWorker::new(
            addr,
        ))))
    }

    fn create_worker(&mut self) -> Result<Step> {
        let octet = FIRST_WORKER_OCTET
            .checked_add(self.allocated_worker_ips)
            .expect("ran out of worker IPs");
        self.allocated_worker_ips += 1;
        let ip = build_ip(octet).to_string();
        let host_addr = NetworkAddr::new(&ip, DEFAULT_HOST_PORT)?;
        let data_addr = NetworkAddr::new(&ip, DEFAULT_DATA_PORT)?;
        Ok(Step::Ready(ParsedStatement::CreateWorker(CreateWorker {
            host_addr,
            data_addr,
            max_operators: None,
            peers: vec![],
            config: Default::default(),
            if_not_exists: false,
        })))
    }

    fn get_queries(&self, status: QueryStatus) -> impl Iterator<Item = (QueryId, &Query)> {
        self.queries
            .iter()
            .filter(move |(_, query)| query.status == status)
            .map(|(&id, query)| (id, query))
    }

    fn get_query_ids(&self, status: QueryStatus) -> Vec<QueryId> {
        self.get_queries(status).map(|(id, _)| id).collect()
    }

    fn get_fragment_ids(&self, status: QueryStatus) -> HashSet<QueryFragmentId> {
        self.get_queries(status)
            .flat_map(|(_, query)| query.fragments.keys())
            .copied()
            .collect()
    }

    pub fn active_queries(&self) -> Vec<QueryId> {
        self.get_query_ids(QueryStatus::Active)
    }

    pub fn active_fragments(&self) -> HashSet<QueryFragmentId> {
        self.get_fragment_ids(QueryStatus::Active)
    }

    pub fn dropped_queries(&self) -> Vec<QueryId> {
        self.get_query_ids(QueryStatus::Dropped)
    }

    pub fn dropped_fragments(&self) -> HashSet<QueryFragmentId> {
        self.get_fragment_ids(QueryStatus::Dropped)
    }

    pub fn failed_queries(&self) -> Vec<QueryId> {
        self.get_query_ids(QueryStatus::Failed)
    }

    pub fn failed_fragments(&self) -> HashSet<QueryFragmentId> {
        self.get_fragment_ids(QueryStatus::Failed)
    }

    pub const fn active_workers(&self) -> &HashSet<NetworkAddr> {
        &self.active_workers
    }

    pub const fn dropped_workers(&self) -> &HashSet<NetworkAddr> {
        &self.dropped_workers
    }

    /// Reserves the host of a source or sink when the create is resolved, because a concurrent worker drop can resolve before its reply.
    fn pin_worker(&mut self) -> Option<NetworkAddr> {
        let addr = self
            .active_workers
            .difference(&self.leaving_workers)
            .choose(&mut thread_rng())?
            .clone();
        self.pinned_workers.insert(addr.clone());
        Some(addr)
    }
}

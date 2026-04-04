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

#![cfg(madsim)]

use crate::harness::next_worker_ip;
use anyhow::{Context, Result, anyhow, bail};
use madsim::rand::seq::IteratorRandom;
use madsim::rand::{Rng, thread_rng};
use model::identifier::{FragmentId, QueryId, SinkId, SourceId};
use model::query::fragment::{CreateFragment, StopMode};
use model::query::{CreateQuery, DropQuery, GetQuery};
use model::sink::{CreateSink, DropSink, SinkType};
use model::source::logical::CreateLogicalSource;
use model::source::physical::{CreatePhysicalSource, DropPhysicalSource, SourceType};
use model::statement::{Statement, StatementResult};
use model::worker::endpoint::{DEFAULT_DATA_PORT, DEFAULT_HOST_PORT, NetworkAddr};
use model::worker::{CreateWorker, DropWorker};
use std::collections::{HashMap, HashSet};

const LOGICAL_SOURCE_NAME: &str = "source";
const SINK_NAME: &str = "sink";

#[derive(Clone, Copy, Debug)]
pub enum Operation {
    CreateWorker,
    DropWorker,
    CreateLogicalSource,
    CreatePhysicalSource,
    DropPhysicalSource,
    CreateSink,
    DropSink,
    CreateQuery,
    DropQuery,
}

pub enum Step {
    /// A prerequisite must be fulfilled first. Execute this statement,
    /// call `observe` with the response, then call `resolve` again.
    Prerequisite(Statement),
    /// All prerequisites are met. Execute this final statement.
    Ready(Statement),
}

impl Step {
    fn into_prerequisite(self) -> Self {
        match self {
            Step::Ready(s) | Step::Prerequisite(s) => Step::Prerequisite(s),
        }
    }
}

#[derive(Default)]
pub struct ModelState {
    created_workers: HashSet<NetworkAddr>,
    dropped_workers: HashSet<NetworkAddr>,

    logical: Option<String>,
    physical: Option<(SourceId, NetworkAddr)>,

    sink: Option<(SinkId, NetworkAddr)>,

    active_queries: HashSet<QueryId>,
    dropped_queries: HashSet<QueryId>,
    fragments: HashMap<QueryId, HashMap<FragmentId, NetworkAddr>>,
}

impl ModelState {
    /// Check prerequisites for `op` and return the next statement to execute.
    /// If prerequisites are missing, returns `Step::Prerequisite` — execute it,
    /// call `observe`, then call `resolve` again for the same operation.
    pub fn resolve(&self, op: Operation) -> Result<Step> {
        match op {
            Operation::CreateWorker => self.create_worker(),
            Operation::DropWorker => self.drop_worker(),
            Operation::CreateLogicalSource => Ok(Step::Ready(Statement::CreateLogicalSource(
                CreateLogicalSource {
                    name: LOGICAL_SOURCE_NAME.to_string(),
                    schema: Default::default(),
                    if_not_exists: true,
                },
            ))),
            Operation::CreatePhysicalSource => self.create_physical_source(),
            Operation::DropPhysicalSource => {
                let (id, _) = self
                    .physical
                    .as_ref()
                    .ok_or_else(|| anyhow!("no source to drop"))?;
                Ok(Step::Ready(Statement::DropPhysicalSource(
                    DropPhysicalSource::all().with_id(*id),
                )))
            }
            Operation::CreateSink => self.create_sink(),
            Operation::DropSink => {
                let (id, _) = self
                    .sink
                    .as_ref()
                    .ok_or_else(|| anyhow!("no sink to drop"))?;
                Ok(Step::Ready(Statement::DropSink(DropSink {
                    name: None,
                    id: Some(*id),
                })))
            }
            Operation::CreateQuery => self.create_query(),
            Operation::DropQuery => {
                let qid = self
                    .active_queries
                    .iter()
                    .choose(&mut thread_rng())
                    .ok_or_else(|| anyhow!("no query to drop"))?;
                Ok(Step::Ready(Statement::DropQuery(
                    DropQuery::all()
                        .with_filters(GetQuery::all().with_id(*qid))
                        .with_stop_mode(StopMode::Forceful),
                )))
            }
        }
    }

    /// Update internal state based on a statement response.
    pub fn observe(&mut self, rsp: StatementResult) {
        match rsp {
            StatementResult::CreatedWorker(worker) => {
                self.created_workers.insert(worker.host_addr);
            }
            StatementResult::DroppedWorker(worker) => {
                let addr = worker.unwrap().host_addr;
                assert!(self.created_workers.remove(&addr), "unknown worker {addr}");
                assert!(
                    self.dropped_workers.insert(addr.clone()),
                    "worker {addr} dropped twice"
                );
            }
            StatementResult::CreatedLogicalSource(source) => {
                self.logical = Some(source.name);
            }
            StatementResult::CreatedPhysicalSource(source) => {
                self.physical = Some((source.id, source.host_addr.unwrap()));
            }
            StatementResult::DroppedPhysicalSources(dropped) => {
                let (id, _) = self.physical.as_ref().unwrap();
                assert!(dropped.len() == 1 && dropped.first().unwrap().id == *id);
                self.physical = None;
            }
            StatementResult::CreatedSink(sink) => {
                self.sink = Some((sink.id, sink.host_addr.unwrap()));
            }
            StatementResult::DroppedSinks(dropped) => {
                let (id, _) = self.sink.as_ref().unwrap();
                assert!(dropped.len() == 1 && dropped.first().unwrap().id == *id);
                self.sink = None;
            }
            StatementResult::CreatedQuery(query, fragments) => {
                assert!(
                    self.active_queries.insert(query.id),
                    "query {} already created",
                    query.id
                );
                self.fragments.insert(
                    query.id,
                    fragments.into_iter().map(|f| (f.id, f.host_addr)).collect(),
                );
            }
            StatementResult::DroppedQueries(dropped) => {
                assert_eq!(dropped.len(), 1);
                let dropped = dropped.first().unwrap();
                assert!(self.active_queries.remove(&dropped.id));
                assert!(
                    self.dropped_queries.insert(dropped.id),
                    "query {} dropped twice",
                    dropped.id
                );
                // TODO: drop fragments
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    fn create_query(&self) -> Result<Step> {
        if self.physical.is_none() {
            return self
                .resolve(Operation::CreatePhysicalSource)
                .map(Step::into_prerequisite);
        }
        if self.sink.is_none() {
            return self
                .resolve(Operation::CreateSink)
                .map(Step::into_prerequisite);
        }
        let (source_id, source_addr) = self.physical.as_ref().unwrap();
        let (sink_id, sink_addr) = self.sink.as_ref().unwrap();
        let mut rng = thread_rng();
        // pick a random subset among the currently active workers
        let k = rng.gen_range(0..=self.created_workers.len());
        let workers: HashSet<&NetworkAddr> = self
            .created_workers
            .iter()
            .choose_multiple(&mut rng, k)
            .into_iter()
            .chain([source_addr, sink_addr])
            .collect::<HashSet<_>>();

        Ok(Step::Ready(Statement::CreateQuery(CreateQuery {
            name: None,
            sql: format!("SELECT * FROM {LOGICAL_SOURCE_NAME} INTO {SINK_NAME}"),
            fragments: workers
                .into_iter()
                .map(|addr| CreateFragment {
                    host_addr: addr.clone(),
                    plan: vec![],
                    num_operators: 1,
                    has_source: addr == source_addr,
                })
                .collect(),
            source_ids: vec![*source_id],
            sink_ids: vec![*sink_id],
        })))
    }

    fn create_sink(&self) -> Result<Step> {
        if self.created_workers.is_empty() {
            return self
                .resolve(Operation::CreateWorker)
                .map(Step::into_prerequisite);
        }
        let addr = self
            .created_workers
            .iter()
            .choose(&mut thread_rng())
            .unwrap();
        Ok(Step::Ready(Statement::CreateSink(CreateSink {
            name: SINK_NAME.to_string(),
            host_addr: addr.clone(),
            sink_type: SinkType::File,
            schema: Default::default(),
            config: Default::default(),
            if_not_exists: false,
        })))
    }

    fn create_physical_source(&self) -> Result<Step> {
        if self.created_workers.is_empty() {
            return self
                .resolve(Operation::CreateWorker)
                .map(Step::into_prerequisite);
        }
        if self.logical.is_none() {
            return self
                .resolve(Operation::CreateLogicalSource)
                .map(Step::into_prerequisite);
        }
        let addr = self
            .created_workers
            .iter()
            .choose(&mut thread_rng())
            .unwrap();
        Ok(Step::Ready(Statement::CreatePhysicalSource(
            CreatePhysicalSource {
                logical_source: LOGICAL_SOURCE_NAME.to_string(),
                host_addr: addr.clone(),
                source_type: SourceType::File,
                source_config: Default::default(),
                parser_config: Default::default(),
                if_not_exists: false,
            },
        )))
    }

    fn drop_worker(&self) -> Result<Step> {
        let addr = self
            .created_workers
            .iter()
            .min_by_key(|addr| self.entities_on(addr))
            .context("no workers to drop")?
            .clone();

        if let Some(qid) = self.active_query_with_fragment_on(&addr) {
            return Ok(Step::Prerequisite(Statement::DropQuery(
                DropQuery::all()
                    .with_filters(GetQuery::all().with_id(qid))
                    .with_stop_mode(StopMode::Forceful),
            )));
        }
        if self.any_query_with_fragment_on(&addr) {
            bail!("fragments still terminating on {addr}");
        }
        Ok(Step::Ready(Statement::DropWorker(DropWorker::new(addr))))
    }

    fn create_worker(&self) -> Result<Step> {
        let ip = next_worker_ip();
        let host_addr = NetworkAddr::new(&ip, DEFAULT_HOST_PORT);
        let data_addr = NetworkAddr::new(&ip, DEFAULT_DATA_PORT);
        Ok(Step::Ready(Statement::CreateWorker(CreateWorker {
            host_addr,
            data_addr,
            max_operators: None,
            peers: vec![],
            config: Default::default(),
            if_not_exists: false,
        })))
    }

    pub fn active_queries(&self) -> Vec<QueryId> {
        self.active_queries.iter().cloned().collect()
    }

    pub fn active_fragments(&self) -> HashSet<FragmentId> {
        self.active_queries
            .iter()
            .flat_map(|id| self.fragments.get(id).unwrap().keys())
            .copied()
            .collect()
    }

    pub fn dropped_queries(&self) -> Vec<QueryId> {
        self.dropped_queries.iter().copied().collect()
    }

    pub fn dropped_fragments(&self) -> HashSet<FragmentId> {
        self.dropped_queries
            .iter()
            .flat_map(|id| self.fragments.get(id).unwrap().keys())
            .copied()
            .collect()
    }

    pub fn active_workers(&self) -> Vec<NetworkAddr> {
        self.created_workers.iter().cloned().collect()
    }

    pub fn dropped_workers(&self) -> &HashSet<NetworkAddr> {
        &self.dropped_workers
    }

    fn entities_on(&self, addr: &NetworkAddr) -> usize {
        let fragments = self
            .active_queries
            .iter()
            .flat_map(|qid| self.fragments.get(qid).unwrap().values())
            .filter(|a| *a == addr)
            .count();
        let has_source = self.physical.as_ref().is_some_and(|(_, a)| a == addr);
        let has_sink = self.sink.as_ref().is_some_and(|(_, a)| a == addr);
        fragments + has_source as usize + has_sink as usize
    }

    fn active_query_with_fragment_on(&self, addr: &NetworkAddr) -> Option<QueryId> {
        self.active_queries
            .iter()
            .find(|qid| {
                self.fragments
                    .get(qid)
                    .expect("BUG: fragment not found for query")
                    .values()
                    .any(|a| a == addr)
            })
            .copied()
    }

    fn any_query_with_fragment_on(&self, addr: &NetworkAddr) -> bool {
        self.fragments.values().any(|frags| frags.values().any(|a| a == addr))
    }
}

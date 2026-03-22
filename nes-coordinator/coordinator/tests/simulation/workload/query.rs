#![cfg(madsim)]
use crate::harness::{TestHarness, arb};
use crate::workload::{
    Invariant, InvariantContext, Workload, WorkloadFactory, check_invariants, parse_options,
    precompute_times,
};
use async_trait::async_trait;
use madsim::rand::Rng;
use model::query::query_state::QueryState;
use model::query::{CreateQuery, DropQuery, GetQuery, StopMode};
use model::{Generate, query};
use proptest::prelude::*;
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use tracing::{debug, info};

const DEFAULT_NUM_QUERIES: usize = 15;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct QueryConfig {
    num_queries: usize,
    begin: u64,
    end: Option<u64>,
    drop_fraction: f64,
    drop_begin: Option<u64>,
    drop_end: Option<u64>,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            num_queries: DEFAULT_NUM_QUERIES,
            begin: 0,
            end: None,
            drop_fraction: 0.0,
            drop_begin: None,
            drop_end: None,
        }
    }
}

pub struct QueryWorkload {
    num_queries: usize,
    begin: Duration,
    end: Duration,
    num_drops: usize,
    drop_begin: Duration,
    drop_end: Duration,
    timeline: Vec<(Duration, QueryOp)>,
    state: RefCell<QueryWorkloadState>,
}

#[derive(Default)]
struct QueryWorkloadState {
    created_query_ids: Vec<i64>,
    dropped_query_ids: Vec<i64>,
}

#[derive(Clone, Debug)]
enum QueryOp {
    CreateQuery(usize),
    DropQuery(usize),
}

fn build_timeline(
    num_queries: usize,
    begin: Duration,
    end: Duration,
    num_drops: usize,
    drop_begin: Duration,
    drop_end: Duration,
) -> Vec<(Duration, QueryOp)> {
    let mut rng = madsim::rand::thread_rng();

    let create_times = precompute_times(num_queries, begin, end - begin);
    let mut timeline: Vec<(Duration, QueryOp)> = create_times
        .into_iter()
        .enumerate()
        .map(|(i, t)| (t, QueryOp::CreateQuery(i)))
        .collect();

    if num_drops > 0 {
        let mut drop_indices: Vec<usize> = (0..num_queries).collect();
        for i in (1..drop_indices.len()).rev() {
            let j = rng.gen_range(0..=i);
            drop_indices.swap(i, j);
        }
        drop_indices.truncate(num_drops);

        let raw_drop_times = precompute_times(num_drops, drop_begin, drop_end - drop_begin);

        let mut drop_entries: Vec<(usize, Duration)> =
            drop_indices.into_iter().zip(raw_drop_times).collect();
        drop_entries.sort_by_key(|(qi, _)| *qi);

        for (qi, drop_time) in drop_entries {
            let create_time = timeline
                .iter()
                .find_map(|(t, op)| match op {
                    QueryOp::CreateQuery(i) if *i == qi => Some(*t),
                    _ => None,
                })
                .unwrap();
            let effective_time = drop_time.max(create_time + Duration::from_millis(1));
            timeline.push((effective_time, QueryOp::DropQuery(qi)));
        }
    }

    timeline.sort_by_key(|(t, _)| *t);
    timeline
}

impl QueryWorkload {
    pub const NAME: &str = "Query";

    pub fn from_options(options: &HashMap<String, toml::Value>) -> Self {
        let c: QueryConfig = parse_options(options);
        let end = c.end.unwrap_or(c.begin);
        let span = end - c.begin;
        let drop_begin = c.drop_begin.unwrap_or(end);
        let drop_end = c.drop_end.unwrap_or(drop_begin + span);
        let num_drops = (c.num_queries as f64 * c.drop_fraction).round() as usize;
        Self {
            num_queries: c.num_queries,
            begin: Duration::from_secs(c.begin),
            end: Duration::from_secs(end),
            num_drops,
            drop_begin: Duration::from_secs(drop_begin),
            drop_end: Duration::from_secs(drop_end),
            timeline: Vec::new(),
            state: RefCell::new(QueryWorkloadState::default()),
        }
    }

    async fn execute_op(
        &self,
        harness: &TestHarness,
        i: usize,
        op: &QueryOp,
        created_ids: &mut Vec<Option<i64>>,
        dropped_ids: &mut Vec<i64>,
    ) {
        match *op {
            QueryOp::CreateQuery(idx) => {
                let mut query = arb(CreateQuery::generate());
                query.block_until = QueryState::Pending;
                let resp = harness.create_query(query).await;
                match resp {
                    Ok(q) => {
                        debug!("[{i}] create({idx}) -> id={}", q.id);
                        created_ids[idx] = Some(q.id);
                    }
                    Err(e) => info!("[{i}] create({idx}) failed: {e}"),
                }
            }
            QueryOp::DropQuery(idx) => {
                if let Some(qid) = created_ids[idx] {
                    let stop_mode: StopMode = arb(any::<StopMode>());
                    let resp = harness
                        .drop_queries(
                            DropQuery::all()
                                .stop_mode(stop_mode)
                                .with_filters(GetQuery::all().with_id(qid)),
                        )
                        .await;
                    match resp {
                        Ok(_) => {
                            debug!("[{i}] drop({idx}) id={qid}");
                            dropped_ids.push(qid);
                        }
                        Err(e) => info!("[{i}] drop({idx}) id={qid} failed: {e}"),
                    }
                }
            }
        }
    }
}

#[async_trait(?Send)]
impl Workload for QueryWorkload {
    fn name(&self) -> &str {
        Self::NAME
    }

    async fn setup(&mut self, _harness: &TestHarness) {
        self.timeline = build_timeline(
            self.num_queries,
            self.begin,
            self.end,
            self.num_drops,
            self.drop_begin,
            self.drop_end,
        );
    }

    async fn start(&self, harness: &TestHarness) {
        info!(
            "{}: running {} creates + {} drops ({} ops)",
            self.name(),
            self.num_queries,
            self.num_drops,
            self.timeline.len(),
        );

        let mut created_ids: Vec<Option<i64>> = vec![None; self.num_queries];
        let mut dropped_ids = Vec::new();
        let mut elapsed = Duration::ZERO;

        for (i, (scheduled, op)) in self.timeline.iter().enumerate() {
            if *scheduled > elapsed {
                tokio::time::sleep(*scheduled - elapsed).await;
                elapsed = *scheduled;
            }
            self.execute_op(harness, i, op, &mut created_ids, &mut dropped_ids)
                .await;
        }
        info!(
            "{}: completed {} ops",
            self.name(),
            self.num_queries + self.num_drops
        );

        let mut state = self.state.borrow_mut();
        state.created_query_ids = created_ids.into_iter().flatten().collect();
        state.dropped_query_ids = dropped_ids;
    }

    async fn check(&self, harness: &TestHarness) {
        let (created_ids, dropped_ids) = {
            let state = self.state.borrow();
            (
                state.created_query_ids.clone(),
                state.dropped_query_ids.clone(),
            )
        };

        let ctx = InvariantContext::build(harness, created_ids, dropped_ids).await;

        check_invariants(
            &[&QueryLiveness, &QueryTermination, &Leakage],
            harness,
            &ctx,
        )
        .await;
    }
}

/// Queries that were created but never dropped must be in Running state, and every
/// fragment must be reported as active by the specific worker it was placed on.
pub struct QueryLiveness;

#[async_trait(?Send)]
impl Invariant for QueryLiveness {
    fn name(&self) -> &str {
        "query_liveness"
    }

    async fn check(&self, _harness: &TestHarness, ctx: &InvariantContext) {
        let live: HashSet<i64> = ctx.live_query_ids().iter().copied().collect();

        for (q, fragments) in &ctx.queries {
            if !live.contains(&q.id) {
                continue;
            }

            assert_eq!(
                q.current_state,
                QueryState::Running,
                "query_liveness: query {} in state {:?}",
                q.id,
                q.current_state,
            );

            for f in fragments {
                let active = ctx
                    .active_by_worker
                    .get(&f.grpc_addr)
                    .map_or(false, |ids| ids.contains(&(f.id as u64)));
                assert!(
                    active,
                    "query_liveness: fragment {} of query {} not active on placed worker {}",
                    f.id, q.id, f.grpc_addr,
                );
            }
        }
    }
}

/// Queries that were explicitly dropped must be in Stopped state, and no worker
/// should still report any of their fragments as active.
pub struct QueryTermination;

#[async_trait(?Send)]
impl Invariant for QueryTermination {
    fn name(&self) -> &str {
        "query_termination"
    }

    async fn check(&self, _harness: &TestHarness, ctx: &InvariantContext) {
        let dropped: HashSet<i64> = ctx.dropped_query_ids().iter().copied().collect();

        for (q, fragments) in &ctx.queries {
            if !dropped.contains(&q.id) {
                continue;
            }

            assert_eq!(
                q.current_state,
                QueryState::Stopped,
                "query_termination: dropped query {} in state {:?}",
                q.id,
                q.current_state,
            );

            for f in fragments {
                let active = ctx
                    .active_by_worker
                    .get(&f.grpc_addr)
                    .map_or(false, |ids| ids.contains(&(f.id as u64)));
                assert!(
                    !active,
                    "query_termination: fragment {} of dropped query {} still active on worker {}",
                    f.id, q.id, f.grpc_addr,
                );
            }
        }
    }
}

/// Every fragment ID reported as active by a worker must correspond to a fragment
/// the coordinator knows about. Catches leaked fragments where a stop RPC failed
/// silently but the coordinator moved on.
pub struct Leakage;

#[async_trait(?Send)]
impl Invariant for Leakage {
    fn name(&self) -> &str {
        "leakage"
    }

    async fn check(&self, _harness: &TestHarness, ctx: &InvariantContext) {
        let known: HashSet<u64> = ctx
            .queries
            .iter()
            .flat_map(|(_, fragments)| fragments.iter().map(|f| f.id as u64))
            .collect();

        for (addr, active_ids) in &ctx.active_by_worker {
            for id in active_ids {
                assert!(
                    known.contains(id),
                    "leakage: worker {} reports fragment {} active but coordinator has no record of it",
                    addr,
                    id,
                );
            }
        }
    }
}

inventory::submit! {
    WorkloadFactory {
        name: QueryWorkload::NAME,
        create: |opts| Box::new(QueryWorkload::from_options(opts)),
    }
}

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

mod lifecycle;
mod task;

pub(super) use task::QueryFragmentTask;

use crate::remote::worker_rpc_service::nes::SerializableQueryPlan;
use crate::util::task_map::TaskMap;
use model::identifier::QueryFragmentId;
use model::query::query_fragment::{self, DesiredQueryFragmentState, QueryFragmentError};
use model::worker::endpoint::NetworkAddr;
use sea_orm::DatabaseConnection;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{Instrument, info_span, warn};

pub(crate) struct QueryFragmentTasks {
    tasks: TaskMap<QueryFragmentId>,
    stop_signals: HashMap<QueryFragmentId, watch::Sender<DesiredQueryFragmentState>>,
}

impl QueryFragmentTasks {
    pub(crate) fn new() -> Self {
        Self {
            tasks: TaskMap::new(),
            stop_signals: HashMap::new(),
        }
    }

    pub(crate) fn tasks(&mut self) -> &mut TaskMap<QueryFragmentId> {
        &mut self.tasks
    }

    pub(crate) async fn reconcile_from_catalog<T: Transport + 'static>(
        &mut self,
        db: &DatabaseConnection,
        host: &NetworkAddr,
        state_tx: &Arc<watch::Sender<()>>,
        make_transport: impl Fn() -> T,
    ) {
        let rows = match query_fragment::Entity::actionable(db, host).await {
            Ok(rows) => rows,
            Err(e) => {
                warn!("failed to load fragments: {e}");
                return;
            }
        };
        self.reconcile(rows, db, state_tx, make_transport);
    }

    pub(crate) fn reconcile<T: Transport + 'static>(
        &mut self,
        rows: Vec<query_fragment::Model>,
        db: &DatabaseConnection,
        state_tx: &Arc<watch::Sender<()>>,
        make_transport: impl Fn() -> T,
    ) {
        let ids: HashSet<QueryFragmentId> = rows.iter().map(|row| row.id).collect();
        self.stop_signals.retain(|id, _| ids.contains(id));
        for row in rows {
            if self.tasks.is_tracked(&row.id) {
                if let Some(desired) = self.stop_signals.get(&row.id) {
                    desired.send_if_modified(|current| {
                        let changed = *current != row.desired_state;
                        *current = row.desired_state;
                        changed
                    });
                }
                continue;
            }
            let (desired_tx, desired_rx) = watch::channel(row.desired_state);
            self.stop_signals.insert(row.id, desired_tx);
            let span = info_span!("query_fragment", id = %row.id, query_id = %row.query_id);
            let db = db.clone();
            let state_tx = state_tx.clone();
            let transport = make_transport();
            self.tasks.spawn_if_untracked(row.id, move || {
                QueryFragmentTask::new(row, db, transport, state_tx, desired_rx)
                    .run()
                    .instrument(span)
            });
        }
    }
}

pub(crate) enum CallError {
    /// The worker has no record of the query_fragment id.
    NotFound,
    /// The call may succeed when repeated (within a certain budget).
    Transient(QueryFragmentError),
    /// A terminal, unrecoverable error occurred.
    Failed(QueryFragmentError),
}

/// A query_fragment status reported by the worker, before the state is decoded.
pub struct RawStatus {
    pub state: i32,
    pub start_ms: Option<u64>,
    pub stop_ms: Option<u64>,
    pub error: Option<QueryFragmentError>,
}

/// Interaction with a worker, current implementors are the in-process worker reached via FFI,
/// and the remote worker reached via gRPC.
pub(crate) trait Transport: Send + Sync {
    /// Duration between status calls of a query_fragment.
    /// Implementors may choose different values here,
    /// as an in-process status call is cheaper than one over the network.
    fn poll_interval(&self) -> Duration;

    fn start(
        &self,
        plan: SerializableQueryPlan,
    ) -> impl Future<Output = Result<(), CallError>> + Send;

    fn stop(&self, id: QueryFragmentId) -> impl Future<Output = Result<(), CallError>> + Send;

    fn status(
        &self,
        id: QueryFragmentId,
    ) -> impl Future<Output = Result<RawStatus, CallError>> + Send;
}

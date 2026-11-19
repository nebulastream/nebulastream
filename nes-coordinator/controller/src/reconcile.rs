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

//! The policy that brings a worker in line with the catalog, without I/O:
//! a worker's status and its managed catalog rows go in, and the calls and catalog writes come out.

use crate::remote::worker_rpc_service::nes::SerializableQueryId;
use crate::remote::worker_rpc_service::{Error, WorkerStatusResponse};
use chrono::{DateTime, Utc};
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryFragmentId;
use model::query::query_fragment::{
    DesiredQueryFragmentState, ManagedFragment, QueryFragmentError, QueryFragmentState,
    QueryFragmentTransition,
};
use std::collections::BTreeMap;

/// The fragments that a worker runs, and those that terminated since the requested time.
/// A fragment that the worker does not know is absent.
#[derive(Debug)]
pub(crate) struct WorkerStatus {
    pub fragments: BTreeMap<QueryFragmentId, FragmentStatus>,
    /// The worker's status in milliseconds since the Unix epoch.
    pub until: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FragmentStatus {
    /// Accepted and still compiling or starting.
    Started,
    Running {
        start: DateTime<Utc>,
    },
    /// Without an error, the fragment finished or was stopped; the worker lists both the same way.
    Terminated {
        start: Option<DateTime<Utc>>,
        stop: DateTime<Utc>,
        error: Option<QueryFragmentError>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Correction {
    Start(QueryFragmentId),
    Stop(QueryFragmentId),
    Record(QueryFragmentId, QueryFragmentTransition),
}

impl From<WorkerStatusResponse> for WorkerStatus {
    fn from(response: WorkerStatusResponse) -> Self {
        // The controller did not start a query with an id that is not a fragment id, so it is left out and never stopped as an orphan.
        let get_fragment_id =
            |id: Option<SerializableQueryId>| id?.get_fragment_id().map(QueryFragmentId::new);
        let active = response.active_queries.into_iter().filter_map(|query| {
            let fragment = query
                .started_unix_timestamp_in_milli_seconds
                .map(to_datetime)
                .map_or(FragmentStatus::Started, |start| FragmentStatus::Running {
                    start,
                });
            Some((get_fragment_id(query.query_id)?, fragment))
        });
        let terminated = response.terminated_queries.into_iter().filter_map(|query| {
            let fragment = FragmentStatus::Terminated {
                start: query
                    .started_unix_timestamp_in_milli_seconds
                    .map(to_datetime),
                stop: to_datetime(query.terminated_unix_timestamp_in_milli_seconds),
                error: query.error.as_ref().map(QueryFragmentError::from),
            };
            Some((get_fragment_id(query.query_id)?, fragment))
        });
        Self {
            fragments: active.chain(terminated).collect(),
            until: response.until_unix_timestamp_in_milli_seconds,
        }
    }
}

impl From<&Error> for QueryFragmentError {
    fn from(err: &Error) -> Self {
        let code =
            u16::try_from(err.code).map_or(ErrorCode::UnknownException, ErrorCode::from_code);
        Self::Internal(CodedError::relayed(
            code,
            err.message.clone(),
            err.stack_trace.clone(),
        ))
    }
}

fn to_datetime(ms: u64) -> DateTime<Utc> {
    i64::try_from(ms)
        .ok()
        .and_then(DateTime::from_timestamp_millis)
        .unwrap_or_default()
}

/// Returns the starts, stops and catalog writes that bring the worker in line with `managed`, its non-terminal catalog rows.
/// A state from the status is recorded only when it is ahead of the row's state, so a late or reordered status never moves a row back.
pub(crate) fn reconcile(
    status: &WorkerStatus,
    managed_fragments: &[ManagedFragment],
) -> Vec<Correction> {
    let mut corrections = Vec::new();
    for managed_fragment in managed_fragments {
        let should_stop = managed_fragment.desired_state == DesiredQueryFragmentState::Stopped;
        let fragment = match status.fragments.get(&managed_fragment.id) {
            Some(fragment) => fragment.clone(),
            None if should_stop => FragmentStatus::Terminated {
                start: None,
                stop: to_datetime(status.until),
                error: None,
            },
            None => {
                corrections.push(Correction::Start(managed_fragment.id));
                continue;
            }
        };
        let live = !matches!(fragment, FragmentStatus::Terminated { .. });
        let transition = match fragment {
            FragmentStatus::Started => (managed_fragment.current_state
                < QueryFragmentState::Started)
                .then_some(QueryFragmentTransition::Started),
            FragmentStatus::Running { start } => (managed_fragment.current_state
                < QueryFragmentState::Running)
                .then_some(QueryFragmentTransition::Running {
                    start_timestamp: start,
                }),
            FragmentStatus::Terminated {
                start: start_timestamp,
                stop: stop_timestamp,
                error: Some(error),
            } => Some(QueryFragmentTransition::Failed {
                start_timestamp,
                stop_timestamp,
                error,
            }),
            FragmentStatus::Terminated {
                start: start_timestamp,
                stop: stop_timestamp,
                error: None,
            } if should_stop => Some(QueryFragmentTransition::Stopped {
                start_timestamp,
                stop_timestamp,
            }),
            FragmentStatus::Terminated {
                start: start_timestamp,
                stop: stop_timestamp,
                error: None,
            } => Some(QueryFragmentTransition::Completed {
                start_timestamp,
                stop_timestamp,
            }),
        };
        corrections.extend(
            transition.map(|transition| Correction::Record(managed_fragment.id, transition)),
        );
        if live && should_stop {
            corrections.push(Correction::Stop(managed_fragment.id));
        }
    }
    let orphans = status.fragments.iter().filter(|(id, fragment)| {
        !matches!(fragment, FragmentStatus::Terminated { .. })
            && !managed_fragments.iter().any(|row| row.id == **id)
    });
    corrections.extend(orphans.map(|(id, _)| Correction::Stop(*id)));
    corrections
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote::worker_rpc_service::worker_status_response::{ActiveQuery, TerminatedQuery};
    use Correction::{Record, Start, Stop};
    use DesiredQueryFragmentState::{Completed as DesiredCompleted, Stopped as DesiredStopped};
    use QueryFragmentState::{Pending, Running, Started};
    use QueryFragmentTransition as Transition;

    /// The rows of the state table in the controller README for a fragment with a catalog row.
    #[test]
    #[allow(clippy::too_many_lines, reason = "one case per row of the state table")]
    fn state_table() {
        let id = QueryFragmentId::new(1);
        let (start, stop, until) = (to_datetime(1), to_datetime(2), 3);
        let running = FragmentStatus::Running { start };
        let terminated = |error| FragmentStatus::Terminated {
            start: Some(start),
            stop,
            error,
        };
        let error = QueryFragmentError::Transport { msg: String::new() };
        let cases = [
            (Pending, DesiredCompleted, None, vec![Start(id)]),
            (Running, DesiredCompleted, None, vec![Start(id)]),
            (
                Started,
                DesiredStopped,
                None,
                vec![Record(
                    id,
                    Transition::Stopped {
                        start_timestamp: None,
                        stop_timestamp: to_datetime(until),
                    },
                )],
            ),
            (
                Pending,
                DesiredCompleted,
                Some(FragmentStatus::Started),
                vec![Record(id, Transition::Started)],
            ),
            (
                Started,
                DesiredCompleted,
                Some(FragmentStatus::Started),
                vec![],
            ),
            (
                Running,
                DesiredCompleted,
                Some(FragmentStatus::Started),
                vec![],
            ),
            (
                Started,
                DesiredCompleted,
                Some(running.clone()),
                vec![Record(
                    id,
                    Transition::Running {
                        start_timestamp: start,
                    },
                )],
            ),
            (
                Pending,
                DesiredStopped,
                Some(FragmentStatus::Started),
                vec![Record(id, Transition::Started), Stop(id)],
            ),
            (Running, DesiredStopped, Some(running), vec![Stop(id)]),
            (
                Running,
                DesiredCompleted,
                Some(terminated(None)),
                vec![Record(
                    id,
                    Transition::Completed {
                        start_timestamp: Some(start),
                        stop_timestamp: stop,
                    },
                )],
            ),
            (
                Running,
                DesiredStopped,
                Some(terminated(None)),
                vec![Record(
                    id,
                    Transition::Stopped {
                        start_timestamp: Some(start),
                        stop_timestamp: stop,
                    },
                )],
            ),
            (
                Pending,
                DesiredStopped,
                Some(terminated(Some(error.clone()))),
                vec![Record(
                    id,
                    Transition::Failed {
                        start_timestamp: Some(start),
                        stop_timestamp: stop,
                        error,
                    },
                )],
            ),
        ];
        for (current_state, desired_state, fragment, expected) in cases {
            let status = WorkerStatus {
                fragments: fragment.iter().map(|f| (id, f.clone())).collect(),
                until,
            };
            let managed = [ManagedFragment {
                id,
                current_state,
                desired_state,
            }];
            assert_eq!(
                reconcile(&status, &managed),
                expected,
                "{current_state:?} {desired_state:?} {fragment:?}"
            );
        }
    }

    /// The last row of the state table in the controller README.
    #[test]
    fn unmanaged_fragment_stopped() {
        let id = QueryFragmentId::new;
        let status = WorkerStatus {
            fragments: BTreeMap::from([
                (id(1), FragmentStatus::Started),
                (
                    id(2),
                    FragmentStatus::Running {
                        start: to_datetime(1),
                    },
                ),
                (
                    id(3),
                    FragmentStatus::Terminated {
                        start: None,
                        stop: to_datetime(2),
                        error: None,
                    },
                ),
            ]),
            until: 3,
        };
        assert_eq!(reconcile(&status, &[]), vec![Stop(id(1)), Stop(id(2))]);
    }

    #[test]
    fn status_from_response() {
        let query_id = |value| Some(SerializableQueryId::from_fragment_id(value));
        let response = WorkerStatusResponse {
            after_unix_timestamp_in_milli_seconds: 0,
            until_unix_timestamp_in_milli_seconds: 3,
            active_queries: vec![
                ActiveQuery {
                    query_id: query_id(1),
                    started_unix_timestamp_in_milli_seconds: None,
                },
                ActiveQuery {
                    query_id: query_id(2),
                    started_unix_timestamp_in_milli_seconds: Some(1),
                },
                ActiveQuery {
                    query_id: Some(SerializableQueryId {
                        local_query_id: "5f0c2a8e-3b1d-4c7a-9e2f-6d8b1a4c0e37".to_string(),
                        distributed_query_id: String::new(),
                    }),
                    started_unix_timestamp_in_milli_seconds: None,
                },
            ],
            terminated_queries: vec![TerminatedQuery {
                query_id: query_id(3),
                started_unix_timestamp_in_milli_seconds: Some(1),
                terminated_unix_timestamp_in_milli_seconds: 2,
                error: Some(Error::default()),
            }],
        };
        let status = WorkerStatus::from(response);
        let id = QueryFragmentId::new;
        assert_eq!(status.until, 3);
        assert_eq!(
            status.fragments.len(),
            3,
            "a query without a fragment id is left out"
        );
        assert_eq!(status.fragments[&id(1)], FragmentStatus::Started);
        assert_eq!(
            status.fragments[&id(2)],
            FragmentStatus::Running {
                start: to_datetime(1)
            }
        );
        assert!(matches!(
            &status.fragments[&id(3)],
            FragmentStatus::Terminated { start: Some(start), error: Some(_), stop }
                if *start == to_datetime(1) && *stop == to_datetime(2)
        ));
    }
}

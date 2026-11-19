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
//! a worker's status report and its wanted catalog rows go in, and the calls and catalog writes come out.

use crate::remote::worker_rpc_service::WorkerStatusResponse;
use crate::remote::worker_rpc_service::nes::SerializableQueryId;
use chrono::{DateTime, Utc};
use model::identifier::QueryFragmentId;
use model::query::query_fragment::{
    ActionableFragment, DesiredQueryFragmentState, QueryFragmentError, QueryFragmentState,
    QueryFragmentTransition,
};
use std::collections::BTreeMap;

/// The fragments that a worker runs, and those that terminated since the requested time.
/// A fragment that the worker does not know is absent.
#[derive(Debug, Default)]
pub(crate) struct WorkerReport {
    pub fragments: BTreeMap<QueryFragmentId, Reported>,
    /// The worker's time of the report in milliseconds since the Unix epoch.
    pub until: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Reported {
    /// Accepted and still compiling or starting.
    Starting,
    Running {
        since: DateTime<Utc>,
    },
    /// Without an error, the fragment finished or was stopped; the worker reports both the same way.
    Terminated {
        start: Option<DateTime<Utc>>,
        stop: DateTime<Utc>,
        error: Option<QueryFragmentError>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Start(QueryFragmentId),
    Stop(QueryFragmentId),
    Record(QueryFragmentId, QueryFragmentTransition),
}

impl From<WorkerStatusResponse> for WorkerReport {
    fn from(response: WorkerStatusResponse) -> Self {
        // A query whose id is not a fragment id was not started by the controller,
        // so it is left out and never stopped as an orphan.
        let fragment_id = |id: Option<SerializableQueryId>| {
            id.as_ref()
                .and_then(SerializableQueryId::fragment_id)
                .map(QueryFragmentId::new)
        };
        let active = response.active_queries.into_iter().filter_map(|query| {
            let reported =
                query
                    .started_unix_timestamp_in_milli_seconds
                    .map_or(Reported::Starting, |since| Reported::Running {
                        since: to_datetime(since),
                    });
            Some((fragment_id(query.query_id)?, reported))
        });
        let terminated = response.terminated_queries.into_iter().filter_map(|query| {
            let reported = Reported::Terminated {
                start: query
                    .started_unix_timestamp_in_milli_seconds
                    .map(to_datetime),
                stop: to_datetime(query.terminated_unix_timestamp_in_milli_seconds),
                error: query.error.as_ref().map(QueryFragmentError::from),
            };
            Some((fragment_id(query.query_id)?, reported))
        });
        Self {
            fragments: active.chain(terminated).collect(),
            until: response.until_unix_timestamp_in_milli_seconds,
        }
    }
}

fn to_datetime(ms: u64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(i64::try_from(ms).unwrap_or(i64::MAX)).unwrap_or_default()
}

/// `wanted` holds the worker's non-terminal catalog rows.
/// A reported state is recorded only when it is ahead of the row's state,
/// so a late or reordered report never moves a row back.
pub(crate) fn decide(report: &WorkerReport, wanted: &[ActionableFragment]) -> Vec<Action> {
    let mut actions = Vec::new();
    for row in wanted {
        let stop = row.desired_state == DesiredQueryFragmentState::Stopped;
        let Some(reported) = report.fragments.get(&row.id) else {
            actions.push(if stop {
                Action::Record(
                    row.id,
                    QueryFragmentTransition::Stopped {
                        start_timestamp: None,
                        stop_timestamp: to_datetime(report.until),
                    },
                )
            } else {
                Action::Start(row.id)
            });
            continue;
        };
        let (ahead, live) = match reported {
            Reported::Starting => (row.current_state < QueryFragmentState::Started, true),
            Reported::Running { .. } => (row.current_state < QueryFragmentState::Running, true),
            Reported::Terminated { .. } => (true, false),
        };
        if ahead {
            actions.push(Action::Record(row.id, transition(reported, stop)));
        }
        if stop && live {
            actions.push(Action::Stop(row.id));
        }
    }
    let orphans = report.fragments.iter().filter(|(id, reported)| {
        !matches!(reported, Reported::Terminated { .. }) && !wanted.iter().any(|row| row.id == **id)
    });
    actions.extend(orphans.map(|(id, _)| Action::Stop(*id)));
    actions
}

fn transition(reported: &Reported, stop: bool) -> QueryFragmentTransition {
    match reported.clone() {
        Reported::Starting => QueryFragmentTransition::Started,
        Reported::Running { since } => QueryFragmentTransition::Running {
            start_timestamp: since,
        },
        Reported::Terminated {
            start,
            stop: stop_timestamp,
            error: Some(error),
        } => QueryFragmentTransition::Failed {
            start_timestamp: start,
            stop_timestamp,
            error,
        },
        Reported::Terminated {
            start,
            stop: stop_timestamp,
            error: None,
        } if stop => QueryFragmentTransition::Stopped {
            start_timestamp: start,
            stop_timestamp,
        },
        Reported::Terminated {
            start,
            stop: stop_timestamp,
            error: None,
        } => QueryFragmentTransition::Completed {
            start_timestamp: start,
            stop_timestamp,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote::worker_rpc_service::Error;
    use crate::remote::worker_rpc_service::worker_status_response::{ActiveQuery, TerminatedQuery};
    use DesiredQueryFragmentState::{Completed as WantCompleted, Stopped as WantStopped};
    use QueryFragmentState::{Pending, Running, Started};
    use model::identifier::QueryId;

    const ID: i64 = 7;
    const UNTIL: u64 = 9_000;

    fn id(value: i64) -> QueryFragmentId {
        QueryFragmentId::new(value)
    }

    fn at(ms: u64) -> DateTime<Utc> {
        to_datetime(ms)
    }

    fn row(
        current_state: QueryFragmentState,
        desired_state: DesiredQueryFragmentState,
    ) -> ActionableFragment {
        ActionableFragment {
            id: id(ID),
            query_id: QueryId::new(1),
            current_state,
            desired_state,
        }
    }

    fn report(fragments: &[(i64, Reported)]) -> WorkerReport {
        WorkerReport {
            fragments: fragments.iter().map(|(i, r)| (id(*i), r.clone())).collect(),
            until: UNTIL,
        }
    }

    fn running() -> Reported {
        Reported::Running { since: at(1_000) }
    }

    fn terminated(error: Option<QueryFragmentError>) -> Reported {
        Reported::Terminated {
            start: Some(at(1_000)),
            stop: at(2_000),
            error,
        }
    }

    fn crashed() -> QueryFragmentError {
        QueryFragmentError::Transport {
            msg: "crashed".to_string(),
        }
    }

    type Case = (
        &'static str,
        QueryFragmentState,
        DesiredQueryFragmentState,
        Option<Reported>,
        Vec<Action>,
    );

    fn check(cases: Vec<Case>) {
        for (name, current, desired, reported, expected) in cases {
            let fragments: Vec<_> = reported.into_iter().map(|r| (ID, r)).collect();
            let actions = decide(&report(&fragments), &[row(current, desired)]);
            assert_eq!(actions, expected, "{name}");
        }
    }

    fn record(transition: QueryFragmentTransition) -> Action {
        Action::Record(id(ID), transition)
    }

    /// The rows of the state table in the controller README for a fragment that the worker does not know or runs.
    #[test]
    fn rows_for_an_unknown_or_live_fragment() {
        use Action::{Start, Stop};
        check(vec![
            (
                "unknown, wanted running",
                Pending,
                WantCompleted,
                None,
                vec![Start(id(ID))],
            ),
            (
                "unknown after a start",
                Running,
                WantCompleted,
                None,
                vec![Start(id(ID))],
            ),
            (
                "unknown, wanted stopped",
                Started,
                WantStopped,
                None,
                vec![record(QueryFragmentTransition::Stopped {
                    start_timestamp: None,
                    stop_timestamp: at(UNTIL),
                })],
            ),
            (
                "starting, ahead",
                Pending,
                WantCompleted,
                Some(Reported::Starting),
                vec![record(QueryFragmentTransition::Started)],
            ),
            (
                "starting, behind",
                Running,
                WantCompleted,
                Some(Reported::Starting),
                vec![],
            ),
            (
                "running, ahead",
                Started,
                WantCompleted,
                Some(running()),
                vec![record(QueryFragmentTransition::Running {
                    start_timestamp: at(1_000),
                })],
            ),
            (
                "running, recorded",
                Running,
                WantCompleted,
                Some(running()),
                vec![],
            ),
            (
                "starting, ahead, wanted stopped",
                Pending,
                WantStopped,
                Some(Reported::Starting),
                vec![record(QueryFragmentTransition::Started), Stop(id(ID))],
            ),
            (
                "running, recorded, wanted stopped",
                Running,
                WantStopped,
                Some(running()),
                vec![Stop(id(ID))],
            ),
        ]);
    }

    /// The row of the state table in the controller README for a terminated fragment.
    #[test]
    fn rows_for_a_terminated_fragment() {
        check(vec![
            (
                "wanted running",
                Running,
                WantCompleted,
                Some(terminated(None)),
                vec![record(QueryFragmentTransition::Completed {
                    start_timestamp: Some(at(1_000)),
                    stop_timestamp: at(2_000),
                })],
            ),
            (
                "wanted stopped",
                Running,
                WantStopped,
                Some(terminated(None)),
                vec![record(QueryFragmentTransition::Stopped {
                    start_timestamp: Some(at(1_000)),
                    stop_timestamp: at(2_000),
                })],
            ),
            (
                "with an error",
                Pending,
                WantStopped,
                Some(terminated(Some(crashed()))),
                vec![record(QueryFragmentTransition::Failed {
                    start_timestamp: Some(at(1_000)),
                    stop_timestamp: at(2_000),
                    error: crashed(),
                })],
            ),
        ]);
    }

    /// The last row of the state table in the controller README.
    #[test]
    fn an_active_fragment_that_is_not_wanted_is_stopped() {
        let fragments = [
            (1, Reported::Starting),
            (2, running()),
            (3, terminated(None)),
        ];
        assert_eq!(
            decide(&report(&fragments), &[]),
            vec![Action::Stop(id(1)), Action::Stop(id(2))]
        );
    }

    #[test]
    fn a_report_lists_active_and_terminated_fragments_by_fragment_id() {
        let query_id = |value| Some(SerializableQueryId::from_fragment_id(value));
        let response = WorkerStatusResponse {
            after_unix_timestamp_in_milli_seconds: 0,
            until_unix_timestamp_in_milli_seconds: UNTIL,
            active_queries: vec![
                ActiveQuery {
                    query_id: query_id(1),
                    started_unix_timestamp_in_milli_seconds: None,
                },
                ActiveQuery {
                    query_id: query_id(2),
                    started_unix_timestamp_in_milli_seconds: Some(1_000),
                },
                ActiveQuery {
                    query_id: query_id(0),
                    started_unix_timestamp_in_milli_seconds: None,
                },
            ],
            terminated_queries: vec![TerminatedQuery {
                query_id: query_id(3),
                started_unix_timestamp_in_milli_seconds: None,
                terminated_unix_timestamp_in_milli_seconds: 2_000,
                error: Some(Error {
                    code: 1,
                    message: "crashed".to_string(),
                    ..Default::default()
                }),
            }],
        };
        let report = WorkerReport::from(response);
        assert_eq!(report.until, UNTIL);
        assert_eq!(
            report.fragments.len(),
            3,
            "a query without a fragment id is left out"
        );
        assert_eq!(report.fragments[&id(1)], Reported::Starting);
        assert_eq!(report.fragments[&id(2)], running());
        assert!(matches!(
            &report.fragments[&id(3)],
            Reported::Terminated { start: None, error: Some(_), stop } if *stop == at(2_000)
        ));
    }
}

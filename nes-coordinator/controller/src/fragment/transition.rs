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

//! The policy for a worker's fragments, without I/O.
//!
//! A fragment's row only moves forward through its states:
//!
//! ```text
//! Pending ──▶ Started ──▶ Running ──▶ Completed | Stopped | Failed
//! ```
//!
//! A status shows only the latest state of a fragment, so a row can skip states,
//! e.g. a fragment that compiled and finished between two statuses goes from Pending straight to Completed.

use crate::status::{WorkerFragmentState, WorkerStatus, to_datetime};
use model::identifier::QueryFragmentId;
use model::query::query_fragment::{
    DesiredQueryFragmentState, ManagedFragment, QueryFragmentState, QueryFragmentTransition,
};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Correction {
    Start(QueryFragmentId),
    Stop(QueryFragmentId),
    Record(QueryFragmentId, QueryFragmentTransition),
}

/// Returns the starts, stops and catalog writes that bring the worker in line with its managed rows.
/// The rows must be all non-terminal rows of the worker, because every other live fragment that it lists is stopped.
/// A state from the status is recorded only when it is ahead of the row's state,
/// so a late or reordered status never moves a row back.
pub(crate) fn reconcile(
    status: &WorkerStatus,
    managed_fragments: &[ManagedFragment],
) -> Vec<Correction> {
    let mut corrections = Vec::new();
    for managed_fragment in managed_fragments {
        let should_stop = managed_fragment.desired_state == DesiredQueryFragmentState::Stopped;
        let actual_state = match status.fragments.get(&managed_fragment.id) {
            Some(fragment) => fragment.clone(),
            // The worker does not list the fragment, so its stop is recorded at the worker's time.
            // A start that is still in flight can land afterwards, and a later round then stops the fragment as unmanaged.
            None if should_stop => WorkerFragmentState::Terminated {
                start: None,
                stop: to_datetime(status.until),
                error: None,
            },
            // A termination stays listed until it is recorded,
            // so the worker never accepted the fragment or no longer knows it, e.g. after a restart.
            None => {
                corrections.push(Correction::Start(managed_fragment.id));
                continue;
            }
        };
        let live = !matches!(actual_state, WorkerFragmentState::Terminated { .. });
        let transition = match actual_state {
            WorkerFragmentState::Started => (managed_fragment.current_state
                < QueryFragmentState::Started)
                .then_some(QueryFragmentTransition::Started),
            WorkerFragmentState::Running { start } => (managed_fragment.current_state
                < QueryFragmentState::Running)
                .then_some(QueryFragmentTransition::Running {
                    start_timestamp: start,
                }),
            WorkerFragmentState::Terminated {
                start: start_timestamp,
                stop: stop_timestamp,
                error,
            } => Some(match error {
                Some(error) => QueryFragmentTransition::Failed {
                    start_timestamp,
                    stop_timestamp,
                    error,
                },
                None if should_stop => QueryFragmentTransition::Stopped {
                    start_timestamp,
                    stop_timestamp,
                },
                None => QueryFragmentTransition::Completed {
                    start_timestamp,
                    stop_timestamp,
                },
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
        !matches!(fragment, WorkerFragmentState::Terminated { .. })
            && !managed_fragments.iter().any(|row| row.id == **id)
    });
    corrections.extend(orphans.map(|(id, _)| Correction::Stop(*id)));
    corrections
}

#[cfg(test)]
mod tests {
    use super::*;
    use Correction::{Record, Start, Stop};
    use DesiredQueryFragmentState::{Completed as DesiredCompleted, Stopped as DesiredStopped};
    use QueryFragmentState::{Pending, Running, Started};
    use QueryFragmentTransition as Transition;
    use model::query::query_fragment::QueryFragmentError;

    /// The rows of the correction table in the controller README for a fragment with a catalog row.
    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one or more cases per row of the correction table"
    )]
    fn correction_table() {
        let id = QueryFragmentId::new(1);
        let (start, stop, until) = (to_datetime(1), to_datetime(2), 3);
        let running = WorkerFragmentState::Running { start };
        let terminated = |error| WorkerFragmentState::Terminated {
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
                Some(WorkerFragmentState::Started),
                vec![Record(id, Transition::Started)],
            ),
            (
                Started,
                DesiredCompleted,
                Some(WorkerFragmentState::Started),
                vec![],
            ),
            (
                Running,
                DesiredCompleted,
                Some(WorkerFragmentState::Started),
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
                Some(WorkerFragmentState::Started),
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
}

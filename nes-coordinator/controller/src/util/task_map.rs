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

use crate::config::TASK_RESPAWN_DELAY;
use crate::util::poly_join_set::{AbortHandle, JoinSet};
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use tokio::task::JoinError;
use tracing::error;

/// At most one running task per key.
/// A finished task frees its key when it is joined; a task that returned an error first holds it for the respawn delay.
pub(crate) struct TaskMap<K> {
    tasks: JoinSet<K>,
    handles: HashMap<K, AbortHandle>,
}

impl<K: Eq + Hash + Clone + Send + 'static> TaskMap<K> {
    pub(crate) fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
            handles: HashMap::new(),
        }
    }

    /// Completed tasks are identified by their return value (the key),
    /// since the madsim stand-in has no task ids.
    /// A panicking task returns no key, so its entry stays tracked,
    /// and the reconcile loop re-raises the panic instead of spawning the key again.
    pub(crate) fn spawn_if_untracked<C, F, E>(&mut self, key: K, make_task: C)
    where
        K: Display,
        C: FnOnce() -> F,
        F: Future<Output = Result<(), E>> + Send + 'static,
        E: Display + Send + 'static,
    {
        if !self.handles.contains_key(&key) {
            let task = make_task();
            let k = key.clone();
            let handle = self.tasks.spawn(async move {
                if let Err(e) = task.await {
                    error!(key = %k, error = %format_args!("{e:#}"), "task failed");
                    tokio::time::sleep(TASK_RESPAWN_DELAY).await;
                }
                k
            });
            self.handles.insert(key, handle);
        }
    }

    pub(crate) fn is_tracked(&self, key: &K) -> bool {
        self.handles.contains_key(key)
    }

    pub(crate) fn abort(&mut self, key: &K) {
        if let Some(handle) = self.handles.remove(key) {
            handle.abort();
        }
    }

    pub(crate) async fn join_next(&mut self) -> Option<Result<K, JoinError>> {
        let result = self.tasks.join_next().await?;
        if let Ok(ref key) = result {
            // The key may have been aborted and respawned between the task finishing and this join.
            if self.handles.get(key).is_some_and(AbortHandle::is_finished) {
                self.handles.remove(key);
            }
        }
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_panicking_task_reaches_the_caller_as_a_join_error() {
        let mut tasks = TaskMap::new();
        tasks.spawn_if_untracked(1, || async { panic!("boom") as Result<(), String> });
        let result = tasks.join_next().await.expect("one task was spawned");
        assert!(result.is_err_and(|err| err.is_panic()));
    }

    #[tokio::test]
    async fn a_finished_task_frees_its_key() {
        let mut tasks = TaskMap::new();
        tasks.spawn_if_untracked(1, || async { Ok::<(), String>(()) });
        assert!(
            tasks
                .join_next()
                .await
                .expect("one task was spawned")
                .is_ok()
        );
        assert!(!tasks.is_tracked(&1));
    }

    #[tokio::test]
    async fn a_failed_task_holds_its_key_for_the_respawn_delay() {
        let mut tasks = TaskMap::new();
        let start = std::time::Instant::now();
        tasks.spawn_if_untracked(1, || async { Err::<(), _>("failed") });
        assert!(
            tasks
                .join_next()
                .await
                .expect("one task was spawned")
                .is_ok()
        );
        assert!(start.elapsed() >= TASK_RESPAWN_DELAY);
    }
}

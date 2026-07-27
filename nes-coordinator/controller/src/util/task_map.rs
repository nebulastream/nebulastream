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

//! A set of running tasks keyed so at most one task runs per key, used to keep
//! one child task per resource.

use crate::util::poly_join_set::{AbortHandle, JoinSet};
use futures::FutureExt;
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::panic::AssertUnwindSafe;
use tokio::task::JoinError;
use tracing::error;

/// Keyed task set: at most one running task per key. Spawning the same key
/// twice is a no-op while the first task is still alive, and completed
/// tasks are removed from the map on join so the key becomes spawnable
/// again. Used to keep one reconciler-child per resource without races.
#[derive(Default)]
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

    /// Spawns a task if no task is currently tracked for `key`.
    ///
    /// We identify completed tasks by their return value (the key), since
    /// madsim's `JoinSet` doesn't support task IDs. This means panics must be
    /// caught here. Otherwise the key is never returned and the entry leaks,
    /// permanently blocking respawns for that key.
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
                match AssertUnwindSafe(task).catch_unwind().await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => error!(key = %k, "task failed: {e}"),
                    Err(_) => error!(key = %k, "task panicked"),
                }
                k
            });
            self.handles.insert(key, handle);
        }
    }

    pub(crate) fn abort(&mut self, key: &K) {
        if let Some(handle) = self.handles.remove(key) {
            handle.abort();
        }
    }

    pub(crate) async fn join_next(&mut self) -> Option<Result<K, JoinError>> {
        let result = self.tasks.join_next().await?;
        if let Ok(ref key) = result {
            // The key may have been aborted and respawned between the task
            // finishing and this join. Only drop the tracked handle if it is
            // the one that just finished, so a respawned handle stays tracked.
            if self.handles.get(key).is_some_and(AbortHandle::is_finished) {
                self.handles.remove(key);
            }
        }
        Some(result)
    }
}

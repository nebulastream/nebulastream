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

use crate::util::poly_join_set::{AbortHandle, JoinSet};
use futures_util::FutureExt;
use std::collections::HashMap;
use std::hash::Hash;
use std::panic::AssertUnwindSafe;
use tokio::task::JoinError;
use tracing::error;

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
    /// madsim's JoinSet doesn't support task IDs. This means panics must be
    /// caught here — otherwise the key is never returned and the entry leaks,
    /// permanently blocking respawns for that key.
    pub(crate) fn spawn_if_untracked<F, T>(&mut self, key: K, task: F)
    where
        F: Future<Output = T> + Send + 'static,
    {
        if !self.handles.contains_key(&key) {
            let k = key.clone();
            let handle = self.tasks.spawn(async move {
                if let Err(_) = AssertUnwindSafe(task).catch_unwind().await {
                    error!("task panicked");
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
            self.handles.remove(key);
        }
        Some(result)
    }
}

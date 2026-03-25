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

#[cfg(madsim)]
use tokio::task::JoinHandle;

#[cfg(not(madsim))]
pub use tokio::task::JoinSet;

#[cfg(madsim)]
pub struct JoinSet<T> {
    tasks: futures_util::stream::FuturesUnordered<JoinHandle<T>>,
    abort_handles: Vec<tokio::task::AbortHandle>,
}

#[cfg(madsim)]
impl<T> Default for JoinSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(madsim)]
impl<T> JoinSet<T> {
    pub fn new() -> Self {
        Self {
            tasks: futures_util::stream::FuturesUnordered::new(),
            abort_handles: Vec::new(),
        }
    }

    pub fn spawn<F>(&mut self, task: F) -> AbortHandle
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let handle = tokio::spawn(task);
        let abort_handle = handle.abort_handle();
        self.abort_handles.push(abort_handle.clone());
        self.tasks.push(handle);
        AbortHandle(abort_handle)
    }

    pub async fn join_next(&mut self) -> Option<Result<T, tokio::task::JoinError>> {
        use futures_util::StreamExt;
        self.tasks.next().await
    }

    pub async fn join_all(mut self) -> Vec<T> {
        use futures_util::StreamExt;
        let mut results = Vec::new();
        while let Some(result) = self.tasks.next().await {
            if let Ok(value) = result {
                results.push(value);
            }
        }
        results
    }
}

// Dropping a madsim JoinHandle detaches the task instead of aborting it.
// We must abort explicitly to match tokio::task::JoinSet's drop semantics.
#[cfg(madsim)]
impl<T> Drop for JoinSet<T> {
    fn drop(&mut self) {
        for handle in &self.abort_handles {
            handle.abort();
        }
    }
}

#[cfg(madsim)]
pub struct AbortHandle(tokio::task::AbortHandle);

#[cfg(madsim)]
impl AbortHandle {
    pub fn abort(&self) {
        self.0.abort()
    }

    pub fn is_finished(&self) -> bool {
        self.0.is_finished()
    }
}

#[cfg(not(madsim))]
pub use tokio::task::AbortHandle;

use std::collections::HashMap;
use std::hash::Hash;
use std::panic::AssertUnwindSafe;
use futures_util::FutureExt;
use tokio::task::JoinError;

#[derive(Default)]
pub struct TaskMap<K, V = ()> {
    tasks: JoinSet<K>,
    entries: HashMap<K, (AbortHandle, V)>,
}

impl<K: Eq + Hash + Clone + Send + 'static, V> TaskMap<K, V> {
    pub fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
            entries: HashMap::new(),
        }
    }

    pub fn spawn_if_untracked<F>(&mut self, key: K, value: V, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if !self.contains_key(&key) {
            let k = key.clone();
            let handle = self.tasks.spawn(async move {
                let _ = AssertUnwindSafe(task).catch_unwind().await;
                k
            });
            self.entries.insert(key, (handle, value));
        }
    }

    pub fn spawn<F>(&mut self, key: K, value: V, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let k = key.clone();
        let handle = self.tasks.spawn(async move {
            let _ = AssertUnwindSafe(task).catch_unwind().await;
            k
        });
        self.entries.insert(key, (handle, value));
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.entries.get(key).map(|(_, v)| v)
    }

    pub fn abort(&mut self, key: &K) {
        if let Some((handle, _)) = self.entries.remove(key) {
            handle.abort();
        }
    }

    pub async fn join_next(&mut self) -> Option<Result<K, JoinError>> {
        let result = self.tasks.join_next().await?;
        if let Ok(ref key) = result {
            self.entries.remove(key);
        }
        Some(result)
    }
}
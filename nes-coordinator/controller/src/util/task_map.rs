use std::collections::HashMap;
use std::hash::Hash;
use std::panic::AssertUnwindSafe;
use futures_util::FutureExt;
use tokio::task::JoinError;
use crate::util::join_set::{AbortHandle, JoinSet};

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

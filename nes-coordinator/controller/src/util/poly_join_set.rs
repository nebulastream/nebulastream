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

//! `JoinSet` is `tokio`'s own outside madsim and an in-crate equivalent under `cfg(madsim)`,
//! which covers only the part of the API that this crate uses.

#[cfg(madsim)]
use tokio::task::JoinHandle;

#[cfg(not(madsim))]
pub(crate) use tokio::task::JoinSet;

#[cfg(madsim)]
pub(crate) struct JoinSet<T> {
    tasks: futures::stream::FuturesUnordered<JoinHandle<T>>,
    abort_handles: Vec<tokio::task::AbortHandle>,
}

#[cfg(madsim)]
impl<T> JoinSet<T> {
    pub fn new() -> Self {
        Self {
            tasks: futures::stream::FuturesUnordered::new(),
            abort_handles: Vec::new(),
        }
    }

    pub fn spawn<F>(&mut self, task: F) -> tokio::task::AbortHandle
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let handle = tokio::spawn(task);
        let abort_handle = handle.abort_handle();
        self.abort_handles.push(abort_handle.clone());
        self.tasks.push(handle);
        abort_handle
    }

    pub async fn join_next(&mut self) -> Option<Result<T, tokio::task::JoinError>> {
        use futures::StreamExt;
        self.tasks.next().await
    }
}

// Dropping a madsim JoinHandle detaches the task instead of aborting it.
// Abort explicitly to match the drop semantics of tokio's JoinSet.
#[cfg(madsim)]
impl<T> Drop for JoinSet<T> {
    fn drop(&mut self) {
        for handle in &self.abort_handles {
            handle.abort();
        }
    }
}

pub(crate) use tokio::task::AbortHandle;

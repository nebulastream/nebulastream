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

//! tokio's `JoinSet`, and under madsim, which has none, a stand-in with the part of its API that the crate uses.

#[cfg(not(madsim))]
pub(crate) use tokio::task::JoinSet;

#[cfg(madsim)]
pub(crate) use sim::JoinSet;

#[cfg(madsim)]
mod sim {
    use futures::StreamExt;
    use futures::stream::FuturesUnordered;
    use std::any::Any;
    use tokio::task::{AbortHandle, JoinHandle};

    pub(crate) struct JoinSet<T> {
        tasks: FuturesUnordered<JoinHandle<T>>,
        abort_handles: Vec<AbortHandle>,
    }

    /// A madsim join error has no panic payload, so its message stands in for it.
    pub(crate) struct JoinError(tokio::task::JoinError);

    impl JoinError {
        pub(crate) fn into_panic(self) -> Box<dyn Any + Send> {
            Box::new(self.0.to_string())
        }
    }

    impl<T: Send + 'static> JoinSet<T> {
        pub(crate) fn new() -> Self {
            Self {
                tasks: FuturesUnordered::new(),
                abort_handles: Vec::new(),
            }
        }

        pub(crate) fn spawn(&mut self, task: impl Future<Output = T> + Send + 'static) {
            let handle = tokio::spawn(task);
            self.abort_handles.push(handle.abort_handle());
            self.tasks.push(handle);
        }

        pub(crate) async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
            Some(self.tasks.next().await?.map_err(JoinError))
        }
    }

    // Dropping a madsim join handle detaches its task, so the stand-in aborts its tasks itself, as tokio's join set does.
    // Otherwise a call of an ended worker loop, such as a start that is still being repeated, outlives the loop under madsim.
    impl<T> Drop for JoinSet<T> {
        fn drop(&mut self) {
            for handle in &self.abort_handles {
                handle.abort();
            }
        }
    }
}

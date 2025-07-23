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

use tokio_util::sync::CancellationToken;

/// Tracks a list of cancellation tokens, which are canceled when dropped.
/// Useful for cooperative cancellation of multiple tasks.
#[derive(Default)]
pub struct ActiveTokens {
    tokens: Vec<CancellationToken>,
}
impl ActiveTokens {
    pub fn add_token(&mut self, token: CancellationToken) {
        self.tokens.push(token);
        self.tokens.retain(|t| !t.is_cancelled());
    }
}
impl Drop for ActiveTokens {
    fn drop(&mut self) {
        self.tokens.iter().for_each(|t| t.cancel());
    }
}

/// Utility around a Tokio::JoinHandle. By default, JoinHandles are not aborted on drop.
/// Using a custom drop implementation, a ScopedTask will invoke `abort` on a join handle if it is dropped.
pub struct ScopedTask<T> {
    task: tokio::task::JoinHandle<T>,
}
impl<T> ScopedTask<T> {
    pub fn new(task: tokio::task::JoinHandle<T>) -> Self {
        ScopedTask { task }
    }
}

impl<T> Drop for ScopedTask<T> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

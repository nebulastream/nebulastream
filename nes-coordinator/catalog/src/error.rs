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

use sea_orm::{DbErr, RuntimeErr, SqlxError};

// To be implemented by retryable error types.
// The implementation should categorize on the source error type, e.g., by pattern matching self.
pub trait Retryable {
    fn retryable(&self) -> bool;
}

impl Retryable for DbErr {
    // IO and connection-related DB errors are usually retryable
    fn retryable(&self) -> bool {
        match self {
            DbErr::ConnectionAcquire(_) => true,
            DbErr::Conn(RuntimeErr::SqlxError(err))
            | DbErr::Exec(RuntimeErr::SqlxError(err))
            | DbErr::Query(RuntimeErr::SqlxError(err)) => {
                matches!(err, SqlxError::PoolTimedOut | SqlxError::Io(_))
            }
            _ => false,
        }
    }
}

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
use anyhow::anyhow;
use anyhow::Result;
use catalog::database::StateBackend;
use model::request::{Request, Statement};

#[cxx::bridge]
pub mod ffi {
    extern "Rust" {
        type CoordinatorHandle;

        /// Start the coordinator and return a handle for submitting statements.
        fn start_coordinator(db_path: &str) -> Box<CoordinatorHandle>;

        /// Submit a SQL string and block until the coordinator returns a result.
        /// Returns JSON-serialized StatementResponse on success.
        fn execute_statement(handle: &CoordinatorHandle, sql: &str) -> Result<String>;
    }
}

pub struct CoordinatorHandle {
    sender: flume::Sender<Request>,
}

fn start_coordinator(db_path: &str) -> Box<CoordinatorHandle> {
    let sender = coordinator::start(
        Some(StateBackend::sqlite(db_path)),
        None,
    );
    Box::new(CoordinatorHandle { sender })
}

fn execute_statement(handle: &CoordinatorHandle, sql: &str) -> Result<String> {
    let statement = Statement::Sql(sql.to_string());
    let (rx, req) = Request::new(statement);
    handle
        .sender
        .send(req)
        .map_err(|_| anyhow!("coordinator channel closed"))?;
    let response = rx
        .blocking_recv()
        .map_err(|_| anyhow!("coordinator dropped the request"))??;
    Ok(serde_json::to_string(&response)?)
}

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

use sea_orm::DbBackend;

const SQLITE_UP: &str = include_str!("sqlite.up.sql");
const SQLITE_DOWN: &str = include_str!("sqlite.down.sql");

pub fn up(backend: DbBackend) -> Option<&'static str> {
    match backend {
        DbBackend::Sqlite => Some(SQLITE_UP),
        _ => None,
    }
}

pub fn down(backend: DbBackend) -> Option<&'static str> {
    match backend {
        DbBackend::Sqlite => Some(SQLITE_DOWN),
        _ => None,
    }
}

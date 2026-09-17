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

//! The catalog's migration runner.
//! It lists the schema migrations in the order in which they are applied.

pub use sea_orm_migration::prelude::*;

mod m20260204_185834_init;
mod triggers;

/// SeaORM migrator for the catalog.
/// Runs when the coordinator starts to create the schema and its triggers.
pub struct Migrator;

/// Panic if any of the listed tables already exists.
/// Runs at the top of an `up` so a migration is never applied on top of a leftover schema.
macro_rules! assert_not_has_tables {
    ($manager:expr, $( $table:ident ),+) => {
        $(
            assert!(
                !$manager
                    .has_table($table::Table.to_string())
                    .await?,
                "Table `{}` already exists",
                $table::Table.to_string()
            );
        )+
    };
}

/// Drop the listed tables with `IF EXISTS ... CASCADE`.
/// SQLite has no `CASCADE` clause (the builder omits it) and enforces foreign keys,
/// so the caller must list children before parents and drop triggers first.
macro_rules! drop_tables {
    ($manager:expr, $( $table:ident ),+) => {
        $(
            $manager
                .drop_table(
                    sea_orm_migration::prelude::Table::drop()
                        .table($table::Table)
                        .if_exists()
                        .cascade()
                        .to_owned(),
                )
                .await?;
        )+
    };
}

pub(crate) use {assert_not_has_tables, drop_tables};

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(m20260204_185834_init::Migration)]
    }
}

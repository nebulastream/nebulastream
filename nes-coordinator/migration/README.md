# Migrations

The `migration` crate contains the coordinator catalog schema as a sequence of SeaORM migrations.    
A migration is an ordered change to the database schema that can create or alter tables, indexes, and triggers.   
The coordinator applies pending migrations when it opens the catalog.  
The CLI below provides commands for running them manually.

## Catalog Schema

The catalog stores the coordinator state across the tables shown below.  
Solid connections represent foreign key relationships.  
The dotted connections show the worker addresses that `network_link` refers to, which no foreign key enforces.

`PK` identifies a primary key, `FK` a foreign key, and `UK` a unique key.  
Fields marked as nullable do not require a value.

```mermaid
erDiagram
    logical_source o|--o{ physical_source : binds
    worker ||--o{ physical_source : hosts
    worker ||--o{ sink : hosts
    worker ||--o{ query_fragment : hosts

    query ||--o{ query_fragment : contains
    query ||--o{ query_source : uses
    physical_source ||--o{ query_source : references
    query ||--o{ query_sink : uses
    sink ||--o{ query_sink : references
    query |o--o{ physical_source : owns
    query |o--o{ sink : owns

    worker ||..o{ network_link : "source endpoint"
    worker ||..o{ network_link : "target endpoint"

    logical_source {
        string name PK
        json schema
    }

    worker {
        string host_addr PK
        string data_addr UK
        integer max_operators "nullable"
        json config
        string current_state
        string desired_state
    }

    physical_source {
        bigint id PK
        string logical_source FK "nullable"
        string host_addr FK
        string source_type
        json source_config
        json parser_config
        string kind
        bigint query_id FK "nullable"
    }

    sink {
        bigint id PK
        string name UK "nullable"
        string host_addr FK
        string sink_type
        json schema
        json config
        string kind
        bigint query_id FK "nullable"
    }

    network_link {
        string source_host_addr PK
        string target_host_addr PK
    }

    query {
        bigint id PK
        string name "nullable"
        string sql
        string state
        datetime start_timestamp "nullable"
        datetime stop_timestamp "nullable"
        json error "nullable"
        boolean dropped
    }

    query_fragment {
        bigint id PK
        bigint query_id FK
        string host_addr FK
        binary plan
        integer num_operators
        boolean has_source
        string current_state
        string desired_state
        datetime start_timestamp "nullable"
        datetime stop_timestamp "nullable"
        json error "nullable"
        datetime last_observed_at "nullable"
    }

    query_source {
        bigint query_id PK, FK
        bigint source_id PK, FK
    }

    query_sink {
        bigint query_id PK, FK
        bigint sink_id PK, FK
    }

    ml_model {
        string name PK
        string path
        json input_schema
        json output_schema
        json imported
    }
```

`worker.max_operators` holds the configured capacity and changes only when a removed worker registers again.  
The free capacity is not stored: the model derives it as the configured value minus the operators of the worker's non-terminal fragments.

## Database Rules

CHECK constraints on `query_fragment` require the columns of each state:
a `Running` fragment has a start timestamp, a terminal one a stop timestamp, and a `Failed` one also an error.  
They apply to every write, including the writes of the triggers.

The init migration creates five triggers, and its SQL is in `src/m20260204_185834_init/`:

| Trigger | Effect |
| --- | --- |
| `validate_query_fragment_state_transition` | Rejects a fragment state change out of a terminal state or from `Running` back to `Started`. |
| `derive_query_state_on_query_fragment_update` | Derives a query's state, timestamps and error from its fragments. |
| `cascade_query_failed_to_query_fragments` | Sets the desired state of a failed query's other fragments to `Stopped`. |
| `cascade_worker_removed_to_query_fragments` | Fails the non-terminal fragments of a worker that is marked for removal. |
| `delete_dropped_query_on_termination` | Deletes a dropped query once none of its fragments can run. |

The rules that need an error with context are checked in the model's requests:
creating a query fails when a worker's fragments need more operators than its capacity,
and dropping a source or sink fails while a non-terminal query uses it.

## Relationship with the Model

The `migration` and `model` crates work with the same catalog at different levels.


`migration` defines how catalog data is stored and which rules the database enforces, while `model` provides the Rust types and operations for reading and changing that data. 

Changes to catalog tables and columns must be reflected in both crates.  
Adding an operation on data that the catalog already stores only requires a `model` change.  
For example, an operation that retrieves failed queries can use the existing query state and does not need a migration.

## Schema Changes

Changes to the catalog schema or its database rules are introduced through migrations.  
Once released, a migration remains unchanged.  
The catalog records each applied migration and does not run it again.  

A schema change accounts for:

- Preservation or deliberate transformation of existing catalog data.
- The same final schema whether the catalog is created from scratch or upgraded from an earlier version.
- Corresponding model changes when the stored representation changes.
- Rollback behavior when the change can be safely reversed.

For example, adding a required field involves defining its value for existing records, exposing it through the model, and defining how the schema change is reversed.

## Running Migrator CLI

- Generate a new migration file
    ```sh
    cargo run -- generate MIGRATION_NAME
    ```
- Apply all pending migrations
    ```sh
    cargo run
    ```
    ```sh
    cargo run -- up
    ```
- Apply first 10 pending migrations
    ```sh
    cargo run -- up -n 10
    ```
- Rollback last applied migrations
    ```sh
    cargo run -- down
    ```
- Rollback last 10 applied migrations
    ```sh
    cargo run -- down -n 10
    ```
- Drop all tables from the database, then reapply all migrations
    ```sh
    cargo run -- fresh
    ```
- Rollback all applied migrations, then reapply all migrations
    ```sh
    cargo run -- refresh
    ```
- Rollback all applied migrations
    ```sh
    cargo run -- reset
    ```
- Check the status of all migrations
    ```sh
    cargo run -- status
    ```

# Model

The `model` crate provides the Rust interface to the coordinator catalog.  
It represents catalog data as Rust types and defines the operations used to read and change that data.  
Other coordinator components use these types and operations instead of working with the database schema directly.

## Catalog Operations

Catalog operations cover creating and retrieving entries.  
Some drop operations delete matching entries immediately.  
Dropping a query or worker instead records a desired lifecycle state that the controller reconciles asynchronously.  
The query or worker record remains in the catalog after reaching its terminal state.

The model also defines the state changes available to workers and query fragments.

## Statements and Results

A statement represents one operation that can be applied to the catalog, such as creating a worker, retrieving queries, or removing a source.  
It contains the data required to perform that operation.

Executing a statement produces a corresponding result containing the affected catalog data.  
Statements provide the coordinator with a common way to execute different catalog operations and return their results.

## Transactions

Some catalog operations change several related entries.  
These changes are applied within a transaction, which treats them as one complete operation.

A transaction is committed when every change succeeds.  
If any change fails, the transaction is rolled back and none of its changes remain in the catalog.  
This prevents incomplete operations from leaving the catalog in an inconsistent state.

For example, creating a query also stores its fragments and its connections to sources and sinks.  
If any part of that operation fails, the complete change is rolled back.
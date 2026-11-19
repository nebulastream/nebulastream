# Model

The `model` crate provides the Rust interface to the coordinator catalog.  
It represents catalog data as Rust types and defines the operations that read and change that data.  
Other coordinator components use these types and operations instead of working with the database schema directly.

## Catalog Operations

Catalog operations cover creating and retrieving entries.  
Some drop operations delete matching entries immediately.  
Dropping a query or worker instead records a desired lifecycle state that the controller reconciles asynchronously.  
A dropped worker stays in the catalog.  
A dropped query is deleted once all of its fragments have terminated.

The model also defines the state changes available to workers and query fragments.

## Statements and Results

A statement represents one operation that can be applied to the catalog, such as creating a worker, retrieving queries, or removing a source.  
It contains the data required to perform that operation.

Executing a statement produces a result that contains the affected catalog data.

## Transactions

Some catalog operations change several related entries.  
Executing a statement applies them within a transaction, which treats them as one complete operation.

A transaction is committed when every change succeeds.  
If any change fails, the transaction is rolled back and none of its changes remain in the catalog.

For example, creating a query also stores its fragments and its connections to sources and sinks.  
If any part of that operation fails, the complete change is rolled back.

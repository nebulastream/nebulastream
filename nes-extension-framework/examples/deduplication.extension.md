---
type: operator
name: Deduplication
stateful: true
---

## Description

A stateful operator that removes duplicate records within a fixed-size time window based on
a configurable key field. The first occurrence of each key within the window passes through;
subsequent duplicates are dropped. State is stored in an `OperatorHandler` and reset when
the window expires.

## Behavior

`setup()`: Initialize the deduplication hash set in the OperatorHandler using the window size and key field name from the logical operator's configuration.

`execute()`: Extract the key field value from the current record. Look it up in the OperatorHandler's seen-keys set. If not seen: add it and pass the record to the child operator. If already seen: drop the record (do not call child).

`terminate()`: Clear the seen-keys set and release memory.

## Examples

- Records with keys [1, 2, 1, 3, 2] within window → only [1, 2, 3] pass through
- Window expires → seen-keys set resets; duplicate of key 1 from the previous window is treated as new

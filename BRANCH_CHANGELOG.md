# Branch Changelog: `distributed-poc-10-10`

## Query ID Architecture Changes

### Local Query IDs → UUIDs

**Breaking Change**: Local query IDs have been migrated from integer counters to UUIDs.

- **Previous**: `LocalQueryId(1)`, `LocalQueryId(2)`, etc.
- **New**: `LocalQueryId(generateUUID())` → e.g., `"741b827a-ec15-4b9f-a29e-661b12b74696"`

Local query IDs remain present in query plans and are now consistently represented as UUIDs throughout the system.

### Distributed Query IDs

Distributed query IDs are now string-based and random enough to eliminate the need for mappings:

- **Previous**: `nebucli` maintained a mapping from numeric distributed query IDs to temporary file names
- **New**: Distributed query IDs are self-contained strings (e.g., `"agile_warmblood0"`)

## SQL Parser Changes

### Query ID Literals and Syntax

Query\LogicalSource\PhysicalSource IDs are now consistently treated as **literals** throughout SQL:

| SQL Construct                    | Format                     | Example                                |
|----------------------------------|----------------------------|----------------------------------------|
| `WHERE id = <query_id>`          | String literal with quotes | `WHERE id = '741b827a-...'`            |
| `DROP ... WHERE id = <query_id>` | String literal with quotes | `DROP QUERY WHERE id = '741b827a-...'` |

**New DROP Syntax**: The `DROP QUERY` statement now uses a `WHERE` clause:

```sql
-- NEW syntax
DROP QUERY WHERE id = '741b827a-ec15-4b9f-a29e-661b12b74696';

-- OLD syntax (no longer supported)
DROP QUERY 1;
```

### SET Clause for Configuration

SQL statements now support a `SET` clause for inline configuration, including query IDs, source IDs, and other
parameters:

```sql
-- Named query
SELECT * FROM source INTO sink SET('my-query-id' AS `QUERY`.`ID`);
```

## Testing Infrastructure Changes

### New Interactive Expect Test

Created a comprehensive expect-based test (`test_query.exp`).

**Features:**

- Extracts actual query IDs from JSON responses
- Uses extracted IDs in subsequent SQL commands
- Silent on success, shows detailed errors on failure
- Helper procedures extracted to `helpers.exp` for reusability

**Test Coverage:**

1. CREATE LOGICAL SOURCE
2. CREATE PHYSICAL SOURCE
3. CREATE SINK
4. SHOW QUERIES (empty)
5. SELECT query with query ID extraction
6. SHOW QUERIES (with running query, validates ID matches)
7. SHOW QUERIES WHERE id = <extracted_id>
8. DROP QUERY <extracted_id>
9. SHOW QUERIES (after drop)

**Helper Procedures:**

- `wait_for_prompt` - Wraps the NES emoji prompt
- `fail` - Centralized failure reporting
- `expect_json` - Expects JSON response and returns it
- `expect_json_equal` - Expects JSON and asserts equality
- `clean_json` - Removes ANSI codes from terminal output
- `extract_json_from_buffer` - Extracts JSON array from expect buffer
- `assert_json_equal` - Asserts JSON equality using jq
- `assert_json_contains` - Asserts JSON contains expected subset

## Migration Guide

### For C++ Tests

```cpp
// OLD
LocalQueryId qid1(1);
LocalQueryId qid2(2);

// NEW
auto qid1 = LocalQueryId(generateUUID());
auto qid2 = LocalQueryId(generateUUID());
```

### For SQL Tests

Query IDs can no longer be hardcoded as sequential integers. Use one of:

1. **Named query syntax**: Specify query IDs explicitly using `SET('my-id' AS QUERY.ID)`
2. **Expect tests**: Extract query IDs from JSON responses and use them in subsequent commands

### For DistributedQueryId

```cpp
// OLD
DistributedQueryId(1)

// NEW
DistributedQueryId("test-distributed-query-1")  // Or any unique string
```

## Files Changed

### Test Infrastructure

- `nes-nebuli/apps/repl/tests/test_query.exp` - New expect test with query ID extraction
- `nes-nebuli/apps/repl/tests/helpers.exp` - Reusable expect helper procedures
- `nes-nebuli/apps/repl/tests/nebuli_invocation_test.bats` - Updated for UUID-based query IDs
- `nes-nebuli/tests/DistributedQueryTest.cpp` - Migrated to UUID/string-based query IDs
- `nes-logical-operators/tests/LogicalPlanTest.cpp` - Updated test infrastructure

### Core Changes

- `nes-nebuli/include/DistributedQuery.hpp` - Updated query ID types
- `nes-nebuli/src/DistributedQuery.cpp` - Query ID handling updates
- `nes-sql-parser/` - Added SET clause support for inline configuration
- `nes-sql-parser/tests/statement-binder-test.cpp` - Tests for named query syntax and SET clause binding
- Query plans now retain local query IDs as UUIDs throughout the system
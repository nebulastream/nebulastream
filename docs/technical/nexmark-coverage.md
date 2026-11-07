# Nexmark Coverage for NebulaStream

Goal: run all Nexmark queries (q0–q23 from [nexmark/nexmark](https://github.com/nexmark/nexmark/tree/master/nexmark-flink/src/main/resources/queries), pinned at `6b3646c3`) on NES with the same results as Flink.

Status (branch `claude/nexmark-query-coverage-2d7cfd`): **19 of 22 deterministic queries match Flink exactly** on a 10k-event dataset: q0–q5, q7–q10 and q13–q17 and q20–q23 (all but q11, q18, q19), all in `NexmarkFlinkReference.test`. q6 (unsupported in Flink) and q12 (processing time) are excluded. q15–q17 run as unbounded GROUP BYs (`WINDOW UNBOUNDED`) that emit a retract changelog like Flink's streaming mode; the check folds it into the final table. q4 and q9 match the final result on bounded input only: their aggregation over an updating stream and their Top-1 are still expressed with one window that spans all data. The other 3 need session windows (q11) or Top-N/deduplication (q18, q19), and streaming semantics for all unbounded queries need the changelog model (§7).

---

## 1. How Flink runs Nexmark

**Data.** Flink generates events in-process with a `nexmark` table connector: one physical stream `event_type, person, auction, bid`, split into three views. The mix is 1 person : 3 auctions : 46 bids. Event time is `dateTime TIMESTAMP(3)` with `WATERMARK FOR dateTime AS dateTime - INTERVAL '4' SECOND`.

| Stream | Columns |
|---|---|
| person | id BIGINT, name, emailAddress, creditCard, city, state VARCHAR, dateTime TIMESTAMP(3), extra VARCHAR |
| auction | id, itemName VARCHAR, description VARCHAR, initialBid, reserve BIGINT, dateTime, expires TIMESTAMP(3), seller, category BIGINT, extra VARCHAR |
| bid | auction, bidder, price BIGINT, channel VARCHAR, url VARCHAR, dateTime TIMESTAMP(3), extra VARCHAR |

**Sinks and measurement.** Every query writes to `blackhole` except q10 (partitioned filesystem sink). The harness reports throughput (events/s) and cores used.

**Semantics.** Many queries are not windowed: they are regular (unbounded) joins, unbounded GROUP BYs and Top-N/dedup operators over append streams. These keep state forever and emit a changelog (updates/retractions). This is still the biggest gap in NES.

---

## 2. Validation setup

All tooling is in `scripts/benchmarking/nexmark/`.

| Piece | What it does |
|---|---|
| `datagen/` | Standalone Rust tool (nexmark-rs 0.2.0). It writes headerless `person.csv`, `auction.csv`, `bid.csv` in Flink column order, plus the q13 `side_input.csv` (`i,i`, like upstream's `SideInputGenerator`). States are uppercased to match the Java generator. Deterministic: fixed base time, configurable rate. |
| `flink/run_reference.sh` | Runs the upstream query files on Flink 1.20 (Docker) over the same CSV files. It replaces only the blackhole sink with a CSV sink. `--mode batch` returns the final result of update-producing queries as plain rows. |
| `flink/ddl_files.sql` | CSV sources plus views with the exact upstream names and types. Timestamps are `TO_TIMESTAMP_LTZ(ms)` in UTC with the 4 s watermark. |
| `flink/queries/` | Documented local variants: **q10** (unpartitioned sink), **q13** (regular join instead of a PROCTIME lookup join; identical for a static table), **q14** (`CHAR_LENGTH - CHAR_LENGTH(REPLACE(...))` instead of the `CountChar` Java UDF). |
| `flink/to_nes_rows.py` | Converts Flink CSV into NES rows or the NES Checksum sink value. Timestamps become epoch ms, DECIMAL becomes FLOAT64 text, VARSIZED values are quoted. |
| `flink/update_expected.py` | Rewrites the expected block of every query in a `.test` file that has a `# reference: qN [options]` line. |
| `nes-systests/benchmark_small/NexmarkFlinkReference.test` | The suite. Data: `testdata/small/nexmark/flink_10k` (10k events, 100 events/s, 2 MB). |

Checks done on the reference itself:
- Batch and streaming Flink results are identical for q0, q1, q2, q5, q7 and q8.
- q11 (legacy session window syntax) only runs in streaming mode.
- At 1M events, NES matches Flink for q0, q1, q2, q5 and q8. q7 at 1M needs more memory than the default systest topology provides (see §4).

Flink reference results exist for **q0–q5, q7–q11, q13–q23** at 10k events.

---

## 3. Query by query

Legend: ✅ matches Flink in `NexmarkFlinkReference.test` · 🟡 runs as an approximation (different semantics, not Flink-comparable) · 🔴 needs a major feature.

| Q | Name | Flink execution | NES status | How NES runs it / what is missing |
|---|---|---|---|---|
| q0 | Pass-through | Calc | ✅ | |
| q1 | Currency conversion | Calc | ✅ | `price * 0.908` as FLOAT64 (Flink: DECIMAL) |
| q2 | Selection | Calc | ✅ | |
| q3 | Local item suggestion | regular join | ✅ | Unbounded inner hash join `auction ⋈ person WINDOW UNBOUNDED`. |
| q4 | Avg price per category | regular join → unbounded GROUP BY → AVG over retractions | ✅ (bounded) | Unbounded range join `auction ⋈ bid ON id = auction AND bTime BETWEEN aTime AND expires`, then MAX per auction and SUM/COUNT per category, each over one tumbling window spanning all data. Matches Flink's final result; streaming updates need the changelog model. |
| q5 | Hot items | HOP window, max, window join | ✅ | Rewrite: a 2 s tumbling join over window starts (slide = 2 s) |
| q6 | Avg selling price by seller | not supported by Flink | — | excluded |
| q7 | Highest bid | tumble MAX → interval join | ✅ | Real interval join: `INTERVAL (dateTime, maxEnd, 0 ms, 10000 ms)` |
| q8 | Monitor new users | window dedup + window join | ✅ | Keyed windowed dedup on both sides, then a window join |
| q9 | Winning bids | regular join → Top-1 (retracting) | ✅ (bounded) | No Top-N: the same range join, MAX(price) per auction, join back, MIN(bid time) among those bids, join back again (3 unbounded joins, 2 aggregations). Matches Flink's final result; a real Top-N and the changelog model are still missing. |
| q10 | Log to filesystem | Calc + DATE_FORMAT | ✅ | `DATE_FORMAT(dateTime, 'yyyy-MM-dd' / 'HH:mm')`. The partitioned sink is out of scope (the reference uses a plain sink). |
| q11 | User sessions | session window | 🔴 | Session windows |
| q12 | Processing-time windows | PROCTIME tumble | — | nondeterministic, excluded from the reference |
| q13 | Bounded side input join | lookup join on a static file | ✅ | Stream-table join `bid ⋈ TABLE side_input` without `TIME`. 40 s on 10k events (see §4). |
| q14 | Calculation | Calc, `HOUR`, `CASE`, UDF | ✅ | `CASE WHEN HOUR(...)`; the 'c' count is `CHAR_LENGTH(extra) - CHAR_LENGTH(REPLACE(extra, 'c', ''))` instead of the UDF |
| q15 | Bidding statistics | unbounded GROUP BY day, COUNT DISTINCT, FILTER | ✅ (streaming) | `GROUP BY … WINDOW UNBOUNDED(dateTime)` emits +I/-U/+U per 1 s slice; `COUNT(DISTINCT x)`; `agg FILTER (WHERE c)` as `COUNT(CASE WHEN c THEN 1 END)` / `COUNT(DISTINCT CASE WHEN c THEN x END)` |
| q16 | Channel statistics | as q15 by (channel, day) | ✅ (streaming) | as q15; `max(DATE_FORMAT(dateTime, 'HH:mm'))` as `DATE_FORMAT(MAX(dateTime), 'HH:mm')` |
| q17 | Auction statistics | unbounded GROUP BY (auction, day) | ✅ (streaming) | `WINDOW UNBOUNDED(dateTime)`; FILTER as `COUNT(CASE …)`; AVG as SUM / COUNT (Flink truncates) |
| q18 | Find last bid | dedup (keep last) | 🔴 | Dedup/Top-N |
| q19 | Auction Top-10 | Rank | 🔴 | Top-N |
| q20 | Expand bid with auction | regular join | ✅ | Unbounded inner hash join `bid ⋈ auction WINDOW UNBOUNDED` |
| q21 | Add channel id | Calc, LOWER, REGEXP_EXTRACT, CASE, IN | ✅ | `LOWER`, `REGEXP_EXTRACT` (std::regex), `CASE WHEN`, `IN` |
| q22 | URL directories | Calc, SPLIT_INDEX | ✅ | `SPLIT_INDEX` |
| q23 | bid × person × auction | two regular joins | ✅ | Two nested unbounded joins. The second uses the later of bid and person time as its event time, since the first join's output timestamps lag its watermark. |

Day-level aggregations (q15–q17) produce one final row per day in batch Flink and a changelog in streaming Flink. NES emits its changelog once per slice (`unbounded_slice_size_ms`, 1 s) instead of per event, like Flink's mini-batch aggregation, so the changelogs differ in granularity but fold into the same final table.

---

## 4. Problems found

| Problem | Where | Status |
|---|---|---|
| **Join condition with a range part falls back to NLJ.** `ON id = auction AND bTime >= aTime AND bTime <= expires` takes 271 s at 1M events vs. 1.2 s with the range in WHERE. `shallUseHashJoin` rejects any non-equality conjunct, and `join_strategy=HASH_JOIN` cannot override that. | `DecideJoinTypesRule` | Investigated separately: hash join with a residual predicate |
| **Stream-table join has no hash index.** It scans the whole table per stream tuple. q13 (10k stream × 10k table rows) takes 40 s. | `StreamTableJoinPhysicalOperator::probeStreamRecord` | open. The handler already mentions planned hash indexes. |
| **INLJ (from `ls/inlj`) returned wrong results whenever the compiled B-tree `lowerBound` was used** (RIGHT/FULL outer joins, inner joins with `>` or `<=`). The external comparator branched on a traced flag between two `recordComparator` calls with swapped arguments; clang merged the calls into one call site, so nautilus traced both arms as the same block. | `BTreeRef` | **Fixed** by compiling one comparator per argument order (cherry-picked from `claude/inlj-outer-join-bug-3ac696`) |
| **q7 at 1M events runs out of buffers** on the default two-node systest topology, even with PR #2145. It passes with an 8 GiB pool. The window join holds about 92k bids per 10 s window. | buffer pool sizing / spilling | open |
| Systest row comparison splits fields on spaces when lines differ, so correct results with free-text VARSIZED values (person names) can fail. | `ResultComparison.cpp` | worked around with Checksum sinks |
| `systest -t a.test b.test` runs only one of the files | systest CLI | open |
| **Several CASE expressions in one pipeline made tracing exponential.** Each WHEN branch returned from inside the unrolled loop, so the trace continued separately after every exit: 8 CASEs took over 2 minutes before the query ran, 10 about a minute in a plain projection. q15 has 7. | `ConditionalPhysicalFunction` (main) | fixed: one result variable assigned per branch, single exit; 10 CASEs now trace in under 0.5 s |
| Interval join emitted no end-of-stream watermark, so downstream windows never closed (after the "propagate source EOS" change on main) | ported interval join | fixed in `fix(IntervalJoin): adapt to current main` |
| Window build proxies `dynamic_cast` to `WindowBasedOperatorHandler` unchecked (PR #2145), which crashed the interval join | `WindowBuildPhysicalOperator.cpp` | fixed |

Flink-side setup issues (reference tooling only): in-memory checkpoint storage overflow (q7) caused endless restarts; the TaskManager ran out of memory with the default 1.7 GB; switching to batch mode after the DDL breaks rowtime types. All are fixed in `run_reference.sh`.

---

## 5. Performance snapshot

Release build, 10M events (9.2M bids, 600k auctions), one node with 8 worker threads and an 8 GiB pooled + 64 GiB unpooled memory budget; best of 3 runs; PR #2145 included.

| Query | Time | Throughput |
|---|---|---|
| q1 | 3.54 s | 2.60 M bids/s |
| q4, windowed approximation (range in WHERE) | 9.14 s | 1.07 M events/s |
| q5 | 4.32 s | 2.13 M bids/s |

These read CSV files, so they include parsing. Flink generates events in-process, so the numbers are not directly comparable (§7).

---

## 6. Ported features (this branch)

All picked onto current main as separate commits, with the original authorship kept.

| Feature | Source | Adaptation to main |
|---|---|---|
| Throttle sources that run ahead in multi-origin windows | PR #2145 | fast-forward, unchanged |
| Index nested-loop join + record-aware B-tree (+ keyed compiled-function cache, stack allocation) | `ls/inlj` | registry entries, probe `probeWindow`, schema headers, const buffers. The per-file optimizer systest config was not ported. Includes the lowerBound fix above. |
| `CASE WHEN … END` | demo `e53d092cf9` | unchanged |
| Interval join (`INTERVAL (tsL, tsR, lo, hi)`) | PR #1764 (9 commits) | slice-store rename redone over the tree; cache-miss ordering fix carried into the template; probe `probeWindow`; raw-value proxies; EOS watermark |
| Stream-table and ASOF joins (`JOIN TABLE`, `ASOF JOIN`) | demo `8f1df7a8a2` | grammar merged with INTERVAL; compiled terminate through `CompilationContext`; end-of-stream flush now carries an infinite watermark |
| Unbounded inner hash join (`WINDOW UNBOUNDED`) | `claude/nes-inner-join-nexmark-q3-5c6fd7` | window-type variant merged with `IntervalWindow`; slice-store rename and source-origin tracing from PR 2145 |
| Unbounded inner NLJ and INLJ (any predicate) | `ls/unbounded-nlj-inlj` | fast-forward, built on this branch |
| `COUNT(DISTINCT x)`; CASE without ELSE (NULL default) | new | – |
| HOUR/HOUR_OF, DATE_FORMAT, REPLACE, LOWER, SPLIT_INDEX, REGEXP_EXTRACT; `''` literal | new | follow Flink except edge cases: REPLACE with an empty search string, SPLIT_INDEX with an empty delimiter, ASCII-only LOWER, no text month names in DATE_FORMAT |

Not ported: UDFs (not ready), LAST/ARRAY_AGG (not needed yet), the demo's other commits.

---

## 7. Remaining work

### Small (in progress or next)
- **Hash join with a residual predicate** (in progress separately). It fixes the q4/q9 join form, and joins like q4 no longer need the WHERE rewrite.
- **Hash index for the stream-table join's equality keys** (q13).
- **`FILTER (WHERE …)` syntax** as sugar for the CASE rewrite. `COUNT(DISTINCT)` keeps a hash set per key with a fixed number of buckets (`count_distinct_number_of_buckets`, 64); a set that grows would suit keys with very many distinct values.
- **Session windows** (q11): a slice assigner with per-key merging.
- **Systest fixes:** row comparison with spaces; multiple `-t` paths.
- **1M-event reference run** of the whole suite (large test data via ExternalData, under `testdata/large/nexmark`).

### Substantially more effort
- **Changelog model:** a row kind (`+I/-U/+U/-D`) carried through operators and sinks, at least upsert-keyed. Every Flink query with unbounded state needs it for streaming semantics (q4, q9, q15–q19).
- **Retraction-aware aggregates** for unbounded aggregations over an updating stream (SUM/COUNT/AVG next; MIN/MAX over retractions need multiset state). Needed for streaming q4. Unbounded keyed aggregation over append-only input exists (`WINDOW UNBOUNDED`, q15–q17).
- **Top-N / ROW_NUMBER / deduplication** operators (q9, q18, q19).
- **State TTL for regular joins.** `WINDOW UNBOUNDED` (hash, NLJ, INLJ) covers q3, q4, q9, q20 and q23 but keeps all state forever and supports inner joins only. Equality plus a range still runs as an NLJ until the hash join has a residual predicate.
- **State beyond memory** (Flink uses RocksDB): spilling for unbounded keyed state and large window state (q7 at 1M already hits the buffer pool).
- **An in-process Nexmark generator source and a throughput/cores harness**, for numbers comparable to Flink's.
- A cheaper alternative for q4/q9 only: an aggregation that closes each key at a data-driven deadline (`expires`), which stays append-only.

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

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{

/// Owning byte payload of a single spilled slice (the value side of the store).
/// Distinct from the non-owning std::span<const std::byte> accepted by put().
using SpillRecord = std::vector<std::byte>;

/// E1-PR2: backend-level I/O statistics reported at query stop via getBackendStats().
/// Fields that are not applicable to a given backend should be zero (the default).
///
/// Write-amplification formula (RocksDB-specific):
///   writeAmplification = (bytesFlushed + bytesCompacted) / bytesFlushed
/// where:
///   bytesFlushed   = rocksdb::FLUSH_WRITE_BYTES  (bytes written by memtable flushes)
///   bytesCompacted = rocksdb::COMPACT_WRITE_BYTES (bytes written by compaction jobs)
///   bytesWritten   = rocksdb::BYTES_WRITTEN       (WAL-path bytes per Put/Write)
///
/// Rationale: flushed bytes represent the "useful" data written to L0; every additional
/// byte written by compaction is amplification relative to that first flush step. The
/// formula matches the write_amp column in the microbenchmark's parse_write_amp logic.
/// Guard: writeAmplification = 0.0 when bytesFlushed == 0 (no-flush sentinel — no flushes yet).
///
/// sstFootprintBytes = rocksdb.total-sst-files-size (includes all SST files, live + stale).
/// Cache counters (blockCacheHit / blockCacheMiss) are the BLOCK_CACHE_HIT / BLOCK_CACHE_MISS
/// tickers; both are 0 when no block cache is configured.
struct BackendStats
{
    std::uint64_t sstFootprintBytes{0};  ///< On-disk SST footprint (total, incl. stale)
    double writeAmplification{0.0};     ///< (bytesFlushed + bytesCompacted) / bytesFlushed; reported as 0.0 when bytesFlushed == 0 (no-flush sentinel)
    std::uint64_t bytesFlushed{0};      ///< Bytes written by memtable flush (FLUSH_WRITE_BYTES)
    std::uint64_t bytesCompacted{0};    ///< Bytes written by compaction (COMPACT_WRITE_BYTES)
    std::uint64_t bytesWritten{0};      ///< WAL-path bytes per Put/Write (includes key + RocksDB record overhead), not pure user payload
    std::uint64_t blockCacheHit{0};     ///< Block cache hits  (BLOCK_CACHE_HIT; 0 = no cache)
    std::uint64_t blockCacheMiss{0};    ///< Block cache misses (BLOCK_CACHE_MISS; 0 = no cache)
};

/// Partition index of a spilled blob within one (sliceEnd, workerThreadId) state.
/// Partition-at-spill (grace-hash) splits the per-worker map into P self-contained
/// blobs by `hash % P`; each is addressed by a distinct partition id. uint16_t
/// (P <= 65535) is ample for an edge device and keeps the RocksDB key narrow.
/// Defaults to 0 throughout, the single-partition (P==1) state, so non-partitioned
/// callers are source-compatible.
using PartitionId = std::uint16_t;

/// Disk-side dual of the in-memory slice store: a key/value store for spilled
/// window slices, keyed by (SliceEnd, WorkerThreadId, PartitionId).
///
/// A slice is uniquely identified by its end timestamp (SliceEnd = Timestamp);
/// there is no SliceId type in NebulaStream. Per-thread state is keyed by
/// WorkerThreadId (the uint32_t-backed thread identity), NOT a per-node id.
///
/// Phase 1 (skeleton): only the contract exists. The concrete RocksDB-backed
/// implementation lands in a later phase; NullSpillBackend is the no-op default.
class SpillBackend
{
public:
    SpillBackend() = default;
    virtual ~SpillBackend() = default;

    /// Polymorphic base: copy and move are deleted so the backend is only ever used
    /// through a pointer/reference (e.g. unique_ptr<SpillBackend>), preventing slicing.
    SpillBackend(const SpillBackend&) = delete;
    SpillBackend& operator=(const SpillBackend&) = delete;
    SpillBackend(SpillBackend&&) = delete;
    SpillBackend& operator=(SpillBackend&&) = delete;

    /// Persist the bytes of the slice ending at `sliceEnd` for (`workerThreadId`, `partition`).
    /// The key is (SliceEnd, WorkerThreadId, PartitionId); `partition` defaults to 0, the
    /// single-partition state used by every non-partitioned caller.
    /// TODO(future): an optional WriteBatch overload would let the P puts of one worker form a
    /// single atomic durable group (L5 future hardening); not required here.
    virtual void put(SliceEnd sliceEnd, WorkerThreadId workerThreadId, std::span<const std::byte> record, PartitionId partition = 0) = 0;

    /// Retrieve the bytes previously persisted for (sliceEnd, workerThreadId, partition),
    /// or std::nullopt if nothing is stored for that key. `partition` defaults to 0.
    /// TODO(Phase 3): the owning SpillRecord return forces a copy out of the backend's
    /// buffer (e.g. RocksDB pinned slice). Revisit for a move-only / pinned-view return
    /// before the RocksDB backend lands, since this signature is its public contract.
    virtual std::optional<SpillRecord> get(SliceEnd sliceEnd, WorkerThreadId workerThreadId, PartitionId partition = 0) = 0;

    /// Remove every per-thread record for the slice ending at `sliceEnd`.
    /// Idempotent: erasing a slice with no stored records is a well-defined no-op.
    virtual void erase(SliceEnd sliceEnd) = 0;

    /// E1-PR2: return backend-level I/O statistics accumulated since the DB was opened.
    /// Returns std::nullopt for backends that do not collect statistics (NullSpillBackend,
    /// InMemorySpillBackend).  RocksDBSpillBackend overrides this with real ticker data.
    /// The caller (SpillableTimeBasedSliceStore::logMetrics) emits the result as a
    /// stable `RocksDB stats:` NES_INFO line at query stop; nullopt → no extra line.
    [[nodiscard]] virtual std::optional<BackendStats> getBackendStats() const { return std::nullopt; }
};

}

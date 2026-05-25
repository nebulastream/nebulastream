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
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <Identifiers/Identifiers.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SpillBackend.hpp>

namespace rocksdb
{
class DB;
class Statistics;
}

namespace NES
{

/// Durable, RocksDB-backed SpillBackend (Phase 3).
///
/// Records are stored under a 14-byte big-endian key `<sliceEnd:8><workerThreadId:4><partition:2>`. The
/// big-endian layout makes all per-(worker, partition) records of one slice a contiguous key range, so
/// erase(SliceEnd) is a single prefix sweep on the unchanged 8-byte sliceEnd prefix. RocksDB owns block
/// compression (lz4 by default), matching the microbenchmark configuration.
///
/// Thread-safety: a single instance is safe for concurrent put/get/erase, because the underlying
/// rocksdb::DB is internally synchronised for concurrent reads and writes.
///
/// rocksdb::DB is forward-declared so the public header stays free of RocksDB includes; the
/// destructor is defined out-of-line where the type is complete.
class RocksDBSpillBackend final : public SpillBackend
{
public:
    /// Opens (creating if absent) a RocksDB instance at `path`. `compression` is one of
    /// "lz4", "zstd", or "none". `writeBufferSizeBytes` sets the RocksDB write-buffer (memtable) size and
    /// `blockCacheSizeBytes` the block cache; either being 0 leaves RocksDB's own default in place
    /// (behavior-preserving). Throws SpillStoreFailure if the database cannot be opened or the compression
    /// name is unknown.
    explicit RocksDBSpillBackend(
        const std::string& path,
        const std::string& compression = "lz4",
        std::size_t writeBufferSizeBytes = 0,
        std::size_t blockCacheSizeBytes = 0);
    ~RocksDBSpillBackend() override;

    void put(SliceEnd sliceEnd, WorkerThreadId workerThreadId, std::span<const std::byte> record, PartitionId partition = 0) override;
    std::optional<SpillRecord> get(SliceEnd sliceEnd, WorkerThreadId workerThreadId, PartitionId partition = 0) override;
    void erase(SliceEnd sliceEnd) override;

    /// E1-PR2: returns RocksDB internal I/O statistics accumulated since the DB was opened.
    /// Reads FLUSH_WRITE_BYTES, COMPACT_WRITE_BYTES, BYTES_WRITTEN, BLOCK_CACHE_HIT,
    /// BLOCK_CACHE_MISS tickers from the Statistics object, and rocksdb.total-sst-files-size
    /// via GetProperty.  The write-amplification formula is documented on BackendStats.
    /// Returns std::nullopt only if statistics collection was not initialised (should not
    /// happen in practice — the constructor always sets options.statistics).
    [[nodiscard]] std::optional<BackendStats> getBackendStats() const override;

private:
    /// E1-PR2: holds the Statistics object created at open time (shared with options.statistics).
    /// Kept as a separate member so tickers can be read after Open() returns the DB handle.
    /// rocksdb::Statistics is forward-declared above; the full type is only needed in the .cpp.
    // `statistics` declared before `db`: db must be destroyed first (its destructor may
    // write stats during background-work drain; destroying statistics first would be use-after-free).
    std::shared_ptr<rocksdb::Statistics> statistics;
    std::unique_ptr<rocksdb::DB> db;
};

}

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
}

namespace NES
{

/// Durable, RocksDB-backed SpillBackend (Phase 3).
///
/// Records are stored under a 12-byte big-endian key `<sliceEnd:8><workerThreadId:4>`. The
/// big-endian layout makes all per-thread records of one slice a contiguous key range, so
/// erase(SliceEnd) is a single prefix sweep. RocksDB owns block compression (lz4 by default),
/// matching the microbenchmark configuration.
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
    /// "lz4", "zstd", or "none". Throws SpillStoreFailure if the database cannot be opened
    /// or the compression name is unknown.
    explicit RocksDBSpillBackend(const std::string& path, const std::string& compression = "lz4");
    ~RocksDBSpillBackend() override;

    void put(SliceEnd sliceEnd, WorkerThreadId workerThreadId, std::span<const std::byte> record) override;
    std::optional<SpillRecord> get(SliceEnd sliceEnd, WorkerThreadId workerThreadId) override;
    void erase(SliceEnd sliceEnd) override;

private:
    std::unique_ptr<rocksdb::DB> db;
};

}

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

#include <SliceStore/RocksDBSpillBackend.hpp>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <Identifiers/Identifiers.hpp>
#include <ErrorHandling.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SpillBackend.hpp>
#include <Util/Logger/Logger.hpp>
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>

namespace NES
{

namespace
{
constexpr std::size_t SLICE_END_BYTES = sizeof(uint64_t);
constexpr std::size_t WORKER_BYTES = sizeof(uint32_t);
constexpr std::size_t PARTITION_BYTES = sizeof(PartitionId);
constexpr std::size_t KEY_BYTES = SLICE_END_BYTES + WORKER_BYTES + PARTITION_BYTES;

/// Appends `value` to `out` in big-endian order, so lexicographic key order matches numeric order.
template <std::unsigned_integral T>
void appendBigEndian(std::string& out, const T value)
{
    for (std::size_t i = 0; i < sizeof(T); ++i)
    {
        const auto shift = (sizeof(T) - 1 - i) * 8;
        out.push_back(static_cast<char>((value >> shift) & 0xFF));
    }
}

/// 8-byte big-endian slice-end prefix shared by every per-(worker, partition) record of one slice.
/// Deliberately the SLICE_END_BYTES prefix only (NOT extended by the partition suffix): erase(SliceEnd)
/// sweeps this prefix and so still collects every (worker, partition) record of the slice unchanged.
std::string slicePrefix(const SliceEnd sliceEnd)
{
    std::string prefix;
    prefix.reserve(SLICE_END_BYTES);
    appendBigEndian(prefix, sliceEnd.getRawValue());
    return prefix;
}

/// Full 14-byte key for a single (sliceEnd, workerThreadId, partition) record:
/// `<sliceEnd:8 BE><worker:4 BE><partition:2 BE>`. The 2-byte partition suffix goes AFTER the
/// worker bytes, keeping one worker's partitions key-contiguous and the slice prefix unchanged.
std::string recordKey(const SliceEnd sliceEnd, const WorkerThreadId workerThreadId, const PartitionId partition)
{
    std::string key;
    key.reserve(KEY_BYTES);
    appendBigEndian(key, sliceEnd.getRawValue());
    appendBigEndian(key, workerThreadId.getRawValue());
    appendBigEndian(key, partition);
    return key;
}

rocksdb::CompressionType toCompressionType(const std::string& compression)
{
    if (compression == "lz4")
    {
        return rocksdb::kLZ4Compression;
    }
    if (compression == "zstd")
    {
        return rocksdb::kZSTD;
    }
    if (compression == "none")
    {
        return rocksdb::kNoCompression;
    }
    throw SpillStoreFailure("unknown compression '{}', expected one of: lz4, zstd, none", compression);
}
}

RocksDBSpillBackend::RocksDBSpillBackend(
    const std::string& path,
    const std::string& compression,
    const std::size_t writeBufferSizeBytes,
    const std::size_t blockCacheSizeBytes)
{
    rocksdb::Options options;
    options.create_if_missing = true;
    options.compression = toCompressionType(compression);

    /// Phase 0a: cap RocksDB's memory tax under a tight cgroup budget. 0 = leave RocksDB's own default.
    constexpr std::size_t sanityCapBytes = 4ULL * 1024 * 1024 * 1024; /// 4 GiB — a misconfig guard, not a hard limit
    if (writeBufferSizeBytes > sanityCapBytes || blockCacheSizeBytes > sanityCapBytes)
    {
        NES_WARNING(
            "RocksDB spill memory budget unusually large (write_buffer={}B, block_cache={}B, sanity cap {}B) — check spill config",
            writeBufferSizeBytes,
            blockCacheSizeBytes,
            sanityCapBytes);
    }
    if (writeBufferSizeBytes > 0)
    {
        options.write_buffer_size = writeBufferSizeBytes;
    }
    if (blockCacheSizeBytes > 0)
    {
        rocksdb::BlockBasedTableOptions tableOptions;
        /// NewLRUCache is the supported cache factory in the pinned RocksDB build (its LRUCacheOptions has no
        /// single-arg capacity ctor); the "deprecated" comment in cache.h is not a -Werror trigger here.
        tableOptions.block_cache = rocksdb::NewLRUCache(blockCacheSizeBytes);
        /// NewBlockBasedTableFactory returns a raw owning pointer; table_factory (a shared_ptr) takes ownership.
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tableOptions));
    }

    rocksdb::DB* rawDb = nullptr;
    if (const auto status = rocksdb::DB::Open(options, path, &rawDb); !status.ok())
    {
        throw SpillStoreFailure("failed to open RocksDB at '{}': {}", path, status.ToString());
    }
    db.reset(rawDb);
    NES_DEBUG(
        "Opened RocksDB spill backend at '{}' (compression={}, write_buffer={}B, block_cache={}B; 0=RocksDB default)",
        path,
        compression,
        writeBufferSizeBytes,
        blockCacheSizeBytes);
}

RocksDBSpillBackend::~RocksDBSpillBackend() = default;

void RocksDBSpillBackend::put(
    const SliceEnd sliceEnd, const WorkerThreadId workerThreadId, std::span<const std::byte> record, const PartitionId partition)
{
    NES_TRACE(
        "RocksDB spill put: sliceEnd={}, worker={}, partition={}, bytes={}",
        sliceEnd.getRawValue(),
        workerThreadId.getRawValue(),
        partition,
        record.size());
    const std::string key = recordKey(sliceEnd, workerThreadId, partition);
    const rocksdb::Slice value(reinterpret_cast<const char*>(record.data()), record.size());
    if (const auto status = db->Put(rocksdb::WriteOptions(), key, value); !status.ok())
    {
        throw SpillStoreFailure("failed to write spill record: {}", status.ToString());
    }
}

std::optional<SpillRecord> RocksDBSpillBackend::get(const SliceEnd sliceEnd, const WorkerThreadId workerThreadId, const PartitionId partition)
{
    NES_TRACE(
        "RocksDB spill get: sliceEnd={}, worker={}, partition={}",
        sliceEnd.getRawValue(),
        workerThreadId.getRawValue(),
        partition);
    const std::string key = recordKey(sliceEnd, workerThreadId, partition);
    std::string value;
    const auto status = db->Get(rocksdb::ReadOptions(), key, &value);
    if (status.IsNotFound())
    {
        return std::nullopt;
    }
    if (!status.ok())
    {
        throw SpillStoreFailure("failed to read spill record: {}", status.ToString());
    }
    const auto* const bytes = reinterpret_cast<const std::byte*>(value.data());
    return SpillRecord(bytes, bytes + value.size());
}

void RocksDBSpillBackend::erase(const SliceEnd sliceEnd)
{
    const std::string prefix = slicePrefix(sliceEnd);
    rocksdb::WriteBatch batch;
    const std::unique_ptr<rocksdb::Iterator> iterator(db->NewIterator(rocksdb::ReadOptions()));
    for (iterator->Seek(prefix); iterator->Valid() && iterator->key().starts_with(prefix); iterator->Next())
    {
        batch.Delete(iterator->key());
    }
    if (const auto status = iterator->status(); !status.ok())
    {
        throw SpillStoreFailure("failed to scan spill records for erase: {}", status.ToString());
    }
    if (batch.Count() == 0)
    {
        return; /// nothing stored for this slice — avoid an empty WAL write
    }
    if (const auto status = db->Write(rocksdb::WriteOptions(), &batch); !status.ok())
    {
        throw SpillStoreFailure("failed to erase spill records: {}", status.ToString());
    }
}

}

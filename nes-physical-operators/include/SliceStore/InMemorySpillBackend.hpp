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
#include <map>
#include <optional>
#include <span>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SpillBackend.hpp>

namespace NES
{

/// In-memory SpillBackend that actually stores records, keyed by (SliceEnd, WorkerThreadId, PartitionId).
///
/// Unlike NullSpillBackend it round-trips data, so it backs serialization tests and serves
/// as a non-durable fallback when no on-disk backend (RocksDB) is configured. Records are
/// grouped by slice so that erase(SliceEnd) drops every per-(worker, partition) record of a
/// slice at once, matching the interface contract. The inner level is keyed by
/// (WorkerThreadId, PartitionId), mirroring the (sliceEnd, worker, partition) RocksDB key.
/// Not thread-safe.
class InMemorySpillBackend final : public SpillBackend
{
public:
    void put(const SliceEnd sliceEnd, const WorkerThreadId workerThreadId, std::span<const std::byte> record, const PartitionId partition = 0)
        override
    {
        records[sliceEnd][{workerThreadId, partition}] = SpillRecord(record.begin(), record.end());
    }

    std::optional<SpillRecord> get(const SliceEnd sliceEnd, const WorkerThreadId workerThreadId, const PartitionId partition = 0) override
    {
        if (const auto slice = records.find(sliceEnd); slice != records.end())
        {
            if (const auto entry = slice->second.find({workerThreadId, partition}); entry != slice->second.end())
            {
                return entry->second;
            }
        }
        return std::nullopt;
    }

    void erase(const SliceEnd sliceEnd) override { records.erase(sliceEnd); }

    /// Test/observability helpers.
    [[nodiscard]] bool contains(const SliceEnd sliceEnd, const WorkerThreadId workerThreadId, const PartitionId partition = 0) const
    {
        const auto slice = records.find(sliceEnd);
        return slice != records.end() && slice->second.contains({workerThreadId, partition});
    }

    [[nodiscard]] std::size_t sliceCount() const { return records.size(); }

private:
    /// Inner key (WorkerThreadId, PartitionId): one entry per partition blob of a worker's state.
    /// The std::pair key sorts correctly because NESStrongType provides operator<=> (and hence
    /// operator<) by default, so the lexicographic pair ordering makes this map well-ordered.
    std::map<SliceEnd, std::map<std::pair<WorkerThreadId, PartitionId>, SpillRecord>> records;
};

}

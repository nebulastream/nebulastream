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

#include <SliceStore/SliceSerializer.hpp>

#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <type_traits>
#include <vector>
#include <fmt/ranges.h>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <SliceStore/SpillBackend.hpp>

namespace NES
{

namespace
{
/// Bytes between an entry's start and its key/value payload (i.e. the next + hash header).
constexpr uint64_t ENTRY_HEADER_SIZE = sizeof(ChainedHashMapEntry);

template <typename T>
void appendPod(SpillRecord& out, const T& value)
{
    static_assert(std::is_trivially_copyable_v<T>);
    const auto* const bytes = reinterpret_cast<const std::byte*>(&value);
    out.insert(out.end(), bytes, bytes + sizeof(T));
}

/// Writes the fixed NSS1 record header into `record`. `entrySize`/`pageSize`/`numberOfBuckets`
/// describe the shared map layout; `numEntries` is this record's own entry count (differs per
/// partition). Mirrors the header bytes written by serialize() so every record stays a valid
/// NSS1 record indistinguishable from serialize() on a map with only those entries.
void writeHeader(
    SpillRecord& record,
    const uint64_t entrySize,
    const uint64_t pageSize,
    const uint64_t numberOfBuckets,
    const uint64_t numEntries)
{
    appendPod(record, SliceSerializer::MAGIC);
    appendPod(record, SliceSerializer::VERSION);
    appendPod(record, entrySize);
    appendPod(record, pageSize);
    appendPod(record, numberOfBuckets);
    appendPod(record, numEntries);
}

template <typename T>
T readPod(std::span<const std::byte> record, uint64_t& offset)
{
    static_assert(std::is_trivially_copyable_v<T>);
    /// Defensive bounds guard: callers validate sizes up front, so a violation here is a
    /// programming error, not malformed input.
    INVARIANT(offset + sizeof(T) <= record.size(), "readPod out of bounds: offset {} + {} > size {}", offset, sizeof(T), record.size());
    T value;
    std::memcpy(&value, record.data() + offset, sizeof(T));
    offset += sizeof(T);
    return value;
}
}

SpillRecord SliceSerializer::serialize(
    const ChainedHashMap& map, const uint64_t entrySize, const uint64_t pageSize, const uint64_t numberOfBuckets)
{
    if (entrySize <= ENTRY_HEADER_SIZE)
    {
        throw CannotSerialize("entrySize {} must exceed the entry header size {}", entrySize, ENTRY_HEADER_SIZE);
    }

    const uint64_t payloadSize = entrySize - ENTRY_HEADER_SIZE;
    const uint64_t numberOfChains = map.getNumberOfChains();
    const uint64_t numberOfEntries = map.getNumberOfTuples();

    SpillRecord record;
    record.reserve(HEADER_SIZE + (numberOfEntries * (sizeof(uint64_t) + payloadSize)));

    appendPod(record, MAGIC);
    appendPod(record, VERSION);
    appendPod(record, entrySize);
    appendPod(record, pageSize);
    /// Store the pre-rounding bucket count: deserialize passes it back to the constructor so
    /// calcCapacity reproduces the identical chain count (storing the rounded chain count would
    /// double the bucket array on every spill/unspill cycle).
    appendPod(record, numberOfBuckets);
    appendPod(record, numberOfEntries);

    /// Walk every chain. Each entry belongs to exactly one chain, so this visits all
    /// entries exactly once; chain order is irrelevant because it is rebuilt on the way in.
    /// The bucket array is lazily allocated on the first insert, so an empty map has no chains
    /// to walk — getStartOfChain would dereference an uninitialised array.
    uint64_t writtenEntries = 0;
    if (numberOfEntries > 0)
    {
        for (uint64_t chain = 0; chain < numberOfChains; ++chain)
        {
            for (const auto* entry = map.getStartOfChain(chain); entry != nullptr; entry = entry->next)
            {
                appendPod(record, entry->hash);
                const auto* const payload = reinterpret_cast<const std::byte*>(entry) + ENTRY_HEADER_SIZE;
                record.insert(record.end(), payload, payload + payloadSize);
                ++writtenEntries;
            }
        }
    }

    if (writtenEntries != numberOfEntries)
    {
        throw CannotSerialize(
            "chain walk visited {} entries but the map reports {} tuples", writtenEntries, numberOfEntries);
    }

    return record;
}

std::vector<SpillRecord> SliceSerializer::partitionedSerialize(
    const ChainedHashMap& map,
    const uint64_t entrySize,
    const uint64_t pageSize,
    const uint64_t numberOfBuckets,
    const uint64_t numberOfPartitions)
{
    /// Validation ordering is load-bearing: the numberOfPartitions and entrySize guards MUST stay above
    /// the L1 numberOfPartitions == 1 early-return so that every multi-partition path is validated up
    /// front (and a P > 1 caller with a bad entrySize fails here, not buried inside a delegated helper).
    /// Do not reorder these below the fast-path during future refactors.
    if (numberOfPartitions == 0)
    {
        throw CannotSerialize("numberOfPartitions must be at least 1");
    }
    if (entrySize <= ENTRY_HEADER_SIZE)
    {
        throw CannotSerialize("entrySize {} must exceed the entry header size {}", entrySize, ENTRY_HEADER_SIZE);
    }

    /// L1 fast-path: a single partition is byte-identical to serialize() (which also enforces
    /// the entrySize check above), so delegate verbatim and wrap the one record.
    if (numberOfPartitions == 1)
    {
        std::vector<SpillRecord> records;
        records.push_back(serialize(map, entrySize, pageSize, numberOfBuckets));
        return records;
    }

    const uint64_t payloadSize = entrySize - ENTRY_HEADER_SIZE;
    const uint64_t bytesPerEntry = sizeof(uint64_t) + payloadSize;
    const uint64_t numberOfChains = map.getNumberOfChains();
    const uint64_t numberOfEntries = map.getNumberOfTuples();

    /// Pass 1: count entries per partition by hash % P. The bucket array is lazily allocated on
    /// the first insert, so an empty map has no chains to walk (getStartOfChain would dereference
    /// an uninitialised array). Counts are zero for every partition in that case.
    std::vector<uint64_t> entriesPerPartition(numberOfPartitions, 0);
    if (numberOfEntries > 0)
    {
        for (uint64_t chain = 0; chain < numberOfChains; ++chain)
        {
            for (const auto* entry = map.getStartOfChain(chain); entry != nullptr; entry = entry->next)
            {
                ++entriesPerPartition[entry->hash % numberOfPartitions];
            }
        }
    }

    /// Allocate P records, each with an exact header (per-partition numEntries) and an exact
    /// reserve() so pass 2 never reallocates.
    std::vector<SpillRecord> records(numberOfPartitions);
    for (uint64_t p = 0; p < numberOfPartitions; ++p)
    {
        /// Overflow-safe form of `HEADER_SIZE + entriesPerPartition[p] * bytesPerEntry`, mirroring the
        /// guard in deserialize(). `bytesPerEntry >= sizeof(uint64_t) > 0`, so the division is well-defined.
        if (entriesPerPartition[p] > (std::numeric_limits<uint64_t>::max() - HEADER_SIZE) / bytesPerEntry)
        {
            throw CannotSerialize(
                "partition {} entry count {} overflows the {}-byte reserve size", p, entriesPerPartition[p], bytesPerEntry);
        }
        records[p].reserve(HEADER_SIZE + (entriesPerPartition[p] * bytesPerEntry));
        writeHeader(records[p], entrySize, pageSize, numberOfBuckets, entriesPerPartition[p]);
    }

    /// Pass 2: walk again, appending each entry's [hash][payload] to its partition's record.
    uint64_t writtenEntries = 0;
    if (numberOfEntries > 0)
    {
        for (uint64_t chain = 0; chain < numberOfChains; ++chain)
        {
            for (const auto* entry = map.getStartOfChain(chain); entry != nullptr; entry = entry->next)
            {
                SpillRecord& record = records[entry->hash % numberOfPartitions];
                appendPod(record, entry->hash);
                const auto* const payload = reinterpret_cast<const std::byte*>(entry) + ENTRY_HEADER_SIZE;
                record.insert(record.end(), payload, payload + payloadSize);
                ++writtenEntries;
            }
        }
    }

    if (writtenEntries != numberOfEntries)
    {
        throw CannotSerialize(
            "chain walk visited {} entries but the map reports {} tuples", writtenEntries, numberOfEntries);
    }

    /// Per-partition counts only (no payload bytes) for diagnosing skew. fmt::join is evaluated lazily
    /// inside the NES_TRACE format call, so the counts string is never materialised unless TRACE is active.
    NES_TRACE("partitionedSerialize: P={} entries split as [{}]", numberOfPartitions, fmt::join(entriesPerPartition, ","));

    return records;
}

std::unique_ptr<ChainedHashMap> SliceSerializer::deserialize(std::span<const std::byte> record, AbstractBufferProvider& bufferProvider)
{
    if (record.size() < HEADER_SIZE)
    {
        throw CannotDeserialize("record of {} bytes is smaller than the {}-byte header", record.size(), HEADER_SIZE);
    }

    uint64_t offset = 0;
    if (const auto magic = readPod<uint32_t>(record, offset); magic != MAGIC)
    {
        throw CannotDeserialize("bad magic {:#x}, expected {:#x}", magic, MAGIC);
    }
    if (const auto version = readPod<uint32_t>(record, offset); version != VERSION)
    {
        throw CannotDeserialize("unsupported record version {}, expected {}", version, VERSION);
    }

    const auto entrySize = readPod<uint64_t>(record, offset);
    const auto pageSize = readPod<uint64_t>(record, offset);
    const auto numberOfBuckets = readPod<uint64_t>(record, offset);
    const auto numberOfEntries = readPod<uint64_t>(record, offset);

    if (entrySize <= ENTRY_HEADER_SIZE)
    {
        throw CannotDeserialize("entrySize {} must exceed the entry header size {}", entrySize, ENTRY_HEADER_SIZE);
    }
    /// Validate the layout fields before constructing the map. ChainedHashMap enforces these via
    /// PRECONDITION, which calls std::terminate() (uncatchable) — so a malformed record must be
    /// rejected here, not handed to the constructor.
    if (numberOfBuckets == 0)
    {
        throw CannotDeserialize("numberOfBuckets must be greater than 0");
    }
    if (pageSize < entrySize)
    {
        throw CannotDeserialize("pageSize {} cannot hold a single entry of size {}", pageSize, entrySize);
    }

    const uint64_t payloadSize = entrySize - ENTRY_HEADER_SIZE;
    const uint64_t bytesPerEntry = sizeof(uint64_t) + payloadSize;
    /// Overflow-safe form of `record.size() == HEADER_SIZE + numberOfEntries * bytesPerEntry`.
    /// `record.size() >= HEADER_SIZE` is guaranteed by the truncation check above, and
    /// `bytesPerEntry >= sizeof(uint64_t) > 0`, so neither the subtraction nor the division underflows.
    const uint64_t bodyBytes = record.size() - HEADER_SIZE;
    if (numberOfEntries > bodyBytes / bytesPerEntry || bodyBytes != numberOfEntries * bytesPerEntry)
    {
        throw CannotDeserialize(
            "record body of {} bytes is inconsistent with {} entries of {} bytes each", bodyBytes, numberOfEntries, bytesPerEntry);
    }

    auto map = std::make_unique<ChainedHashMap>(entrySize, numberOfBuckets, pageSize);
    for (uint64_t i = 0; i < numberOfEntries; ++i)
    {
        const auto hash = readPod<uint64_t>(record, offset);
        auto* const entry = static_cast<ChainedHashMapEntry*>(map->insertEntry(hash, &bufferProvider));
        auto* const payload = reinterpret_cast<std::byte*>(entry) + ENTRY_HEADER_SIZE;
        std::memcpy(payload, record.data() + offset, payloadSize);
        offset += payloadSize;
    }

    return map;
}

}

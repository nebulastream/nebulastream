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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Runtime/BufferManager.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <SliceStore/InMemorySpillBackend.hpp>
#include <SliceStore/SliceSerializer.hpp>
#include <SliceStore/Slice.hpp>

namespace NES::Testing
{

using Entry = std::pair<uint64_t, std::vector<std::byte>>; /// (hash, key+value payload bytes)

class SliceSerializerTest : public BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SliceSerializerTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SliceSerializerTest test class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        bufferManager = BufferManager::create();
    }

protected:
    static constexpr uint64_t KEY_SIZE = 8;
    static constexpr uint64_t VALUE_SIZE = 8;
    static constexpr uint64_t NUMBER_OF_BUCKETS = 16;
    static constexpr uint64_t PAGE_SIZE = 256; /// small, so many entries span several pages

    std::shared_ptr<BufferManager> bufferManager;

    [[nodiscard]] static uint64_t entrySize() { return sizeof(ChainedHashMapEntry) + KEY_SIZE + VALUE_SIZE; }

    /// Inserts `count` deterministic entries directly (no Nautilus), writing key=i, value=7i+1.
    void populate(ChainedHashMap& map, const uint64_t count) const
    {
        for (uint64_t i = 0; i < count; ++i)
        {
            const uint64_t hash = (i + 1) * 2654435761ULL; /// spreads keys across buckets
            auto* const entry = static_cast<ChainedHashMapEntry*>(map.insertEntry(hash, bufferManager.get()));
            auto* const payload = reinterpret_cast<std::byte*>(entry) + sizeof(ChainedHashMapEntry);
            const uint64_t key = i;
            const uint64_t value = (i * 7) + 1;
            std::memcpy(payload, &key, sizeof(key));
            std::memcpy(payload + sizeof(key), &value, sizeof(value));
        }
    }

    /// Extracts (hash, payload) for every entry, sorted, so two maps can be compared
    /// independent of chain/bucket order.
    static std::vector<Entry> sortedEntries(const ChainedHashMap& map)
    {
        const uint64_t payloadSize = entrySize() - sizeof(ChainedHashMapEntry);
        std::vector<Entry> out;
        /// An empty map has no lazily-allocated bucket array, so skip the walk entirely.
        if (map.getNumberOfTuples() > 0)
        {
            for (uint64_t chain = 0; chain < map.getNumberOfChains(); ++chain)
            {
                for (const auto* entry = map.getStartOfChain(chain); entry != nullptr; entry = entry->next)
                {
                    const auto* const payload = reinterpret_cast<const std::byte*>(entry) + sizeof(ChainedHashMapEntry);
                    out.emplace_back(entry->hash, std::vector<std::byte>(payload, payload + payloadSize));
                }
            }
        }
        std::ranges::sort(out, [](const Entry& a, const Entry& b) { return a.first != b.first ? a.first < b.first : a.second < b.second; });
        return out;
    }

    void expectRoundTrip(const uint64_t count) const
    {
        ChainedHashMap original{KEY_SIZE, VALUE_SIZE, NUMBER_OF_BUCKETS, PAGE_SIZE};
        populate(original, count);
        ASSERT_EQ(original.getNumberOfTuples(), count);

        const SpillRecord record = SliceSerializer::serialize(original, entrySize(), PAGE_SIZE, NUMBER_OF_BUCKETS);
        const auto restored = SliceSerializer::deserialize(record, *bufferManager);

        EXPECT_EQ(restored->getNumberOfTuples(), count);
        EXPECT_EQ(restored->getNumberOfChains(), original.getNumberOfChains());
        EXPECT_EQ(sortedEntries(*restored), sortedEntries(original));
    }

    /// Builds a valid record from a freshly populated map (helper for adversarial tests).
    SpillRecord validRecord(const uint64_t count) const
    {
        ChainedHashMap original{KEY_SIZE, VALUE_SIZE, NUMBER_OF_BUCKETS, PAGE_SIZE};
        populate(original, count);
        return SliceSerializer::serialize(original, entrySize(), PAGE_SIZE, NUMBER_OF_BUCKETS);
    }

    /// Overwrites a little-endian field in a record so we can forge malformed inputs.
    template <typename T>
    static void patchField(SpillRecord& record, const std::size_t offset, const T value)
    {
        std::memcpy(record.data() + offset, &value, sizeof(T));
    }

    /// Header field byte offsets (magic, version, entrySize, pageSize, numberOfBuckets, numEntries).
    static constexpr std::size_t OFFSET_VERSION = sizeof(uint32_t);
    static constexpr std::size_t OFFSET_PAGE_SIZE = (sizeof(uint32_t) * 2) + sizeof(uint64_t);
    static constexpr std::size_t OFFSET_NUMBER_OF_BUCKETS = (sizeof(uint32_t) * 2) + (sizeof(uint64_t) * 2);
    static constexpr std::size_t OFFSET_NUMBER_OF_ENTRIES = (sizeof(uint32_t) * 2) + (sizeof(uint64_t) * 3);
};

TEST_F(SliceSerializerTest, roundTripEmptyMap)
{
    expectRoundTrip(0);
}

TEST_F(SliceSerializerTest, roundTripSinglePage)
{
    expectRoundTrip(5);
}

TEST_F(SliceSerializerTest, roundTripMultiPage)
{
    /// entriesPerPage = PAGE_SIZE / entrySize() = 256/32 = 8, so 50 entries spans ~7 pages.
    expectRoundTrip(50);
}

TEST_F(SliceSerializerTest, roundTripThroughInMemoryBackend)
{
    ChainedHashMap original{KEY_SIZE, VALUE_SIZE, NUMBER_OF_BUCKETS, PAGE_SIZE};
    populate(original, 20);

    InMemorySpillBackend backend;
    const SliceEnd sliceEnd{Timestamp(42)};
    const WorkerThreadId workerThreadId{3};

    backend.put(sliceEnd, workerThreadId, SliceSerializer::serialize(original, entrySize(), PAGE_SIZE, NUMBER_OF_BUCKETS));
    const auto record = backend.get(sliceEnd, workerThreadId);
    ASSERT_TRUE(record.has_value());

    const auto restored = SliceSerializer::deserialize(*record, *bufferManager);
    EXPECT_EQ(sortedEntries(*restored), sortedEntries(original));
}

TEST_F(SliceSerializerTest, rejectsCorruptMagic)
{
    ChainedHashMap original{KEY_SIZE, VALUE_SIZE, NUMBER_OF_BUCKETS, PAGE_SIZE};
    populate(original, 3);
    SpillRecord record = SliceSerializer::serialize(original, entrySize(), PAGE_SIZE, NUMBER_OF_BUCKETS);
    record[0] = std::byte{0xFF}; /// clobber the magic marker

    ASSERT_EXCEPTION_ERRORCODE(SliceSerializer::deserialize(record, *bufferManager), ErrorCode::CannotDeserialize);
}

TEST_F(SliceSerializerTest, rejectsTruncatedRecord)
{
    ChainedHashMap original{KEY_SIZE, VALUE_SIZE, NUMBER_OF_BUCKETS, PAGE_SIZE};
    populate(original, 3);
    SpillRecord record = SliceSerializer::serialize(original, entrySize(), PAGE_SIZE, NUMBER_OF_BUCKETS);
    record.pop_back(); /// size no longer matches the header's entry count

    ASSERT_EXCEPTION_ERRORCODE(SliceSerializer::deserialize(record, *bufferManager), ErrorCode::CannotDeserialize);
}

TEST_F(SliceSerializerTest, rejectsTooSmallEntrySize)
{
    ChainedHashMap original{KEY_SIZE, VALUE_SIZE, NUMBER_OF_BUCKETS, PAGE_SIZE};
    ASSERT_EXCEPTION_ERRORCODE(SliceSerializer::serialize(original, sizeof(ChainedHashMapEntry), PAGE_SIZE, NUMBER_OF_BUCKETS), ErrorCode::CannotSerialize);
}

TEST_F(SliceSerializerTest, rejectsUnsupportedVersion)
{
    SpillRecord record = validRecord(3);
    patchField<uint32_t>(record, OFFSET_VERSION, 999);

    ASSERT_EXCEPTION_ERRORCODE(SliceSerializer::deserialize(record, *bufferManager), ErrorCode::CannotDeserialize);
}

TEST_F(SliceSerializerTest, rejectsZeroNumberOfBuckets)
{
    /// numberOfBuckets == 0 would trip a ChainedHashMap PRECONDITION (std::terminate) if not caught first.
    SpillRecord record = validRecord(3);
    patchField<uint64_t>(record, OFFSET_NUMBER_OF_BUCKETS, 0);

    ASSERT_EXCEPTION_ERRORCODE(SliceSerializer::deserialize(record, *bufferManager), ErrorCode::CannotDeserialize);
}

TEST_F(SliceSerializerTest, rejectsPageSizeSmallerThanEntry)
{
    /// pageSize < entrySize would trip the entriesPerPage > 0 PRECONDITION (std::terminate).
    SpillRecord record = validRecord(3);
    patchField<uint64_t>(record, OFFSET_PAGE_SIZE, 8);

    ASSERT_EXCEPTION_ERRORCODE(SliceSerializer::deserialize(record, *bufferManager), ErrorCode::CannotDeserialize);
}

TEST_F(SliceSerializerTest, rejectsEntryCountOverflow)
{
    /// A huge numberOfEntries must be rejected by the overflow-safe size check, not multiplied modulo 2^64.
    SpillRecord record = validRecord(3);
    patchField<uint64_t>(record, OFFSET_NUMBER_OF_ENTRIES, 1ULL << 60);

    ASSERT_EXCEPTION_ERRORCODE(SliceSerializer::deserialize(record, *bufferManager), ErrorCode::CannotDeserialize);
}

/// 2a (i): the P partitions are disjoint AND complete — every entry of the full map appears in
/// exactly one partition, with identical (hash, payload). Round-tripped through deserialize.
TEST_F(SliceSerializerTest, partitionedSplitIsDisjointAndComplete)
{
    constexpr uint64_t COUNT = 50;
    constexpr uint64_t PARTITIONS = 4;

    ChainedHashMap original{KEY_SIZE, VALUE_SIZE, NUMBER_OF_BUCKETS, PAGE_SIZE};
    populate(original, COUNT);

    const auto records = SliceSerializer::partitionedSerialize(original, entrySize(), PAGE_SIZE, NUMBER_OF_BUCKETS, PARTITIONS);
    ASSERT_EQ(records.size(), PARTITIONS);

    /// Collect the union of all partitions' entries (and confirm no key appears twice).
    std::vector<Entry> unionEntries;
    uint64_t totalTuples = 0;
    for (const auto& record : records)
    {
        const auto restored = SliceSerializer::deserialize(record, *bufferManager);
        totalTuples += restored->getNumberOfTuples();
        const auto partitionEntries = sortedEntries(*restored);
        unionEntries.insert(unionEntries.end(), partitionEntries.begin(), partitionEntries.end());
    }

    /// Completeness: union size equals the original tuple count (no drops).
    EXPECT_EQ(totalTuples, COUNT);

    std::ranges::sort(
        unionEntries, [](const Entry& a, const Entry& b) { return a.first != b.first ? a.first < b.first : a.second < b.second; });

    /// Disjoint + complete: the sorted union equals the full serialize entry set exactly,
    /// which (with equal sizes) also proves no entry is duplicated across partitions.
    const auto expected = sortedEntries(original);
    EXPECT_EQ(unionEntries, expected);
}

/// 2a (ii): partitions with zero entries are tolerated — a sparse map across many partitions
/// leaves some record[p] with numEntries == 0, which deserialize must read back as an empty map.
TEST_F(SliceSerializerTest, partitionedEmptyPartitionsTolerated)
{
    constexpr uint64_t COUNT = 3; /// fewer keys than partitions, so some partitions are guaranteed empty
    constexpr uint64_t PARTITIONS = 8;

    ChainedHashMap original{KEY_SIZE, VALUE_SIZE, NUMBER_OF_BUCKETS, PAGE_SIZE};
    populate(original, COUNT);

    const auto records = SliceSerializer::partitionedSerialize(original, entrySize(), PAGE_SIZE, NUMBER_OF_BUCKETS, PARTITIONS);
    ASSERT_EQ(records.size(), PARTITIONS);

    uint64_t emptyPartitions = 0;
    uint64_t totalTuples = 0;
    for (const auto& record : records)
    {
        const auto restored = SliceSerializer::deserialize(record, *bufferManager);
        const auto tuples = restored->getNumberOfTuples();
        totalTuples += tuples;
        if (tuples == 0)
        {
            ++emptyPartitions;
            EXPECT_EQ(sortedEntries(*restored).size(), 0U);
        }
    }

    EXPECT_EQ(totalTuples, COUNT);
    EXPECT_GT(emptyPartitions, 0U); /// at least one partition must be empty with COUNT < PARTITIONS
}

/// 2a (iii): numberOfPartitions == 1 takes the fast-path and returns a single record that is
/// byte-for-byte equal to serialize() (L1).
TEST_F(SliceSerializerTest, partitionedSingleIsByteIdenticalToSerialize)
{
    ChainedHashMap original{KEY_SIZE, VALUE_SIZE, NUMBER_OF_BUCKETS, PAGE_SIZE};
    populate(original, 20);

    const SpillRecord expected = SliceSerializer::serialize(original, entrySize(), PAGE_SIZE, NUMBER_OF_BUCKETS);
    const auto records = SliceSerializer::partitionedSerialize(original, entrySize(), PAGE_SIZE, NUMBER_OF_BUCKETS, 1);

    ASSERT_EQ(records.size(), 1U);
    EXPECT_EQ(records[0], expected);
}

/// 2a (iv): numberOfPartitions == 0 is invalid and throws CannotSerialize.
TEST_F(SliceSerializerTest, partitionedRejectsZeroPartitions)
{
    ChainedHashMap original{KEY_SIZE, VALUE_SIZE, NUMBER_OF_BUCKETS, PAGE_SIZE};
    populate(original, 5);

    ASSERT_EXCEPTION_ERRORCODE(
        SliceSerializer::partitionedSerialize(original, entrySize(), PAGE_SIZE, NUMBER_OF_BUCKETS, 0), ErrorCode::CannotSerialize);
}

/// 2a (v): a too-small entrySize on the multi-partition path (P > 1) is rejected directly by
/// partitionedSerialize's own guard, not only via the delegated serialize() fast-path.
TEST_F(SliceSerializerTest, partitionedRejectsTooSmallEntrySize)
{
    constexpr uint64_t PARTITIONS = 4;
    ChainedHashMap original{KEY_SIZE, VALUE_SIZE, NUMBER_OF_BUCKETS, PAGE_SIZE};
    populate(original, 5);

    ASSERT_EXCEPTION_ERRORCODE(
        SliceSerializer::partitionedSerialize(original, sizeof(ChainedHashMapEntry), PAGE_SIZE, NUMBER_OF_BUCKETS, PARTITIONS),
        ErrorCode::CannotSerialize);
}

}

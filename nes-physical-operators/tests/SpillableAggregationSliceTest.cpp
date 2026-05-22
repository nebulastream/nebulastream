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
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/BufferManager.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/SpillableAggregationSlice.hpp>
#include <Engine.hpp>
#include <HashMapSlice.hpp>
#include <SliceStore/InMemorySpillBackend.hpp>
#include <SliceStore/Slice.hpp>

namespace NES::Testing
{

using Entry = std::pair<uint64_t, std::vector<std::byte>>; /// (hash, key+value payload bytes)

class SpillableAggregationSliceTest : public BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SpillableAggregationSliceTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SpillableAggregationSliceTest test class.");
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
    static constexpr uint64_t PAGE_SIZE = 256;
    static constexpr uint64_t NUMBER_OF_HASH_MAPS = 4;

    std::shared_ptr<BufferManager> bufferManager;

    /// Args with a single no-op cleanup function (HashMapSlice's dtor requires one per input stream).
    static CreateNewHashMapSliceArgs makeArgs()
    {
        auto noopCleanup = std::make_shared<CreateNewHashMapSliceArgs::NautilusCleanupExec>(
            std::function<void(nautilus::val<HashMap*>)>([](nautilus::val<HashMap*>) { }));
        return CreateNewHashMapSliceArgs({std::move(noopCleanup)}, KEY_SIZE, VALUE_SIZE, PAGE_SIZE, NUMBER_OF_BUCKETS);
    }

    [[nodiscard]] static uint64_t entrySize() { return sizeof(ChainedHashMapEntry) + KEY_SIZE + VALUE_SIZE; }

    /// Inserts `count` deterministic entries (key=offset+i, value=7i+1) into a worker's map.
    void populate(SpillableAggregationSlice& slice, const WorkerThreadId workerThreadId, const uint64_t count, const uint64_t offset) const
    {
        auto* const map = dynamic_cast<ChainedHashMap*>(slice.getHashMapPtrOrCreate(workerThreadId));
        for (uint64_t i = 0; i < count; ++i)
        {
            const uint64_t hash = (offset + i + 1) * 2654435761ULL;
            auto* const entry = static_cast<ChainedHashMapEntry*>(map->insertEntry(hash, bufferManager.get()));
            auto* const payload = reinterpret_cast<std::byte*>(entry) + sizeof(ChainedHashMapEntry);
            const uint64_t key = offset + i;
            const uint64_t value = (i * 7) + 1;
            std::memcpy(payload, &key, sizeof(key));
            std::memcpy(payload + sizeof(key), &value, sizeof(value));
        }
    }

    /// Sorted (hash, payload) of a worker's resident map, for order-independent comparison.
    static std::vector<Entry> sortedEntries(const SpillableAggregationSlice& slice, const WorkerThreadId workerThreadId)
    {
        const auto* const map = dynamic_cast<const ChainedHashMap*>(slice.getHashMapPtr(workerThreadId));
        std::vector<Entry> out;
        if (map != nullptr && map->getNumberOfTuples() > 0)
        {
            const uint64_t payloadSize = entrySize() - sizeof(ChainedHashMapEntry);
            for (uint64_t chain = 0; chain < map->getNumberOfChains(); ++chain)
            {
                for (const auto* entry = map->getStartOfChain(chain); entry != nullptr; entry = entry->next)
                {
                    const auto* const payload = reinterpret_cast<const std::byte*>(entry) + sizeof(ChainedHashMapEntry);
                    out.emplace_back(entry->hash, std::vector<std::byte>(payload, payload + payloadSize));
                }
            }
        }
        std::ranges::sort(out, [](const Entry& a, const Entry& b) { return a.first != b.first ? a.first < b.first : a.second < b.second; });
        return out;
    }
};

TEST_F(SpillableAggregationSliceTest, isResidentInitiallyTrue)
{
    SpillableAggregationSlice slice(Timestamp(0), Timestamp(100), makeArgs(), NUMBER_OF_HASH_MAPS);
    EXPECT_TRUE(slice.isResident());
}

TEST_F(SpillableAggregationSliceTest, spillThenUnspillRestoresEntries)
{
    SpillableAggregationSlice slice(Timestamp(0), Timestamp(100), makeArgs(), NUMBER_OF_HASH_MAPS);
    populate(slice, WorkerThreadId(0), 30, 0);
    const auto expected = sortedEntries(slice, WorkerThreadId(0));
    ASSERT_FALSE(expected.empty());

    InMemorySpillBackend backend;
    slice.spill(backend);

    EXPECT_FALSE(slice.isResident());
    EXPECT_TRUE(backend.contains(Timestamp(100), WorkerThreadId(0)));

    slice.unspill(backend, *bufferManager);

    EXPECT_TRUE(slice.isResident());
    EXPECT_EQ(sortedEntries(slice, WorkerThreadId(0)), expected);
}

TEST_F(SpillableAggregationSliceTest, spillUnspillIsPerWorkerAndSkipsEmptyWorkers)
{
    SpillableAggregationSlice slice(Timestamp(0), Timestamp(100), makeArgs(), NUMBER_OF_HASH_MAPS);
    populate(slice, WorkerThreadId(0), 10, 0);
    populate(slice, WorkerThreadId(1), 20, 1000);
    /// worker 2 left empty; worker 3 populated
    populate(slice, WorkerThreadId(3), 5, 5000);
    const auto expected0 = sortedEntries(slice, WorkerThreadId(0));
    const auto expected1 = sortedEntries(slice, WorkerThreadId(1));
    const auto expected3 = sortedEntries(slice, WorkerThreadId(3));

    InMemorySpillBackend backend;
    slice.spill(backend);
    EXPECT_TRUE(backend.contains(Timestamp(100), WorkerThreadId(0)));
    EXPECT_TRUE(backend.contains(Timestamp(100), WorkerThreadId(1)));
    EXPECT_FALSE(backend.contains(Timestamp(100), WorkerThreadId(2))); /// never populated
    EXPECT_TRUE(backend.contains(Timestamp(100), WorkerThreadId(3)));

    slice.unspill(backend, *bufferManager);

    EXPECT_EQ(sortedEntries(slice, WorkerThreadId(0)), expected0);
    EXPECT_EQ(sortedEntries(slice, WorkerThreadId(1)), expected1);
    EXPECT_EQ(sortedEntries(slice, WorkerThreadId(3)), expected3);
    EXPECT_EQ(slice.getHashMapPtr(WorkerThreadId(2)), nullptr); /// still absent
}

TEST_F(SpillableAggregationSliceTest, doubleSpillIsNoOp)
{
    SpillableAggregationSlice slice(Timestamp(0), Timestamp(100), makeArgs(), NUMBER_OF_HASH_MAPS);
    populate(slice, WorkerThreadId(0), 12, 0);
    const auto expected = sortedEntries(slice, WorkerThreadId(0));

    InMemorySpillBackend backend;
    slice.spill(backend);
    EXPECT_NO_THROW(slice.spill(backend)); /// second spill: already non-resident, no-op
    EXPECT_FALSE(slice.isResident());

    slice.unspill(backend, *bufferManager);
    EXPECT_EQ(sortedEntries(slice, WorkerThreadId(0)), expected);
}

TEST_F(SpillableAggregationSliceTest, unspillWhileResidentIsNoOp)
{
    SpillableAggregationSlice slice(Timestamp(0), Timestamp(100), makeArgs(), NUMBER_OF_HASH_MAPS);
    populate(slice, WorkerThreadId(0), 8, 0);
    const auto expected = sortedEntries(slice, WorkerThreadId(0));

    InMemorySpillBackend backend;
    slice.unspill(backend, *bufferManager); /// resident -> no-op, no backend access

    EXPECT_TRUE(slice.isResident());
    EXPECT_EQ(sortedEntries(slice, WorkerThreadId(0)), expected);
    EXPECT_EQ(backend.sliceCount(), 0U);
}

TEST_F(SpillableAggregationSliceTest, reInsertAfterUnspillSucceeds)
{
    SpillableAggregationSlice slice(Timestamp(0), Timestamp(100), makeArgs(), NUMBER_OF_HASH_MAPS);
    populate(slice, WorkerThreadId(0), 10, 0);

    InMemorySpillBackend backend;
    slice.spill(backend);
    slice.unspill(backend, *bufferManager);

    /// The rebuilt map must accept further inserts (late-arriving tuples after unspill).
    populate(slice, WorkerThreadId(0), 5, 1000);
    EXPECT_EQ(sortedEntries(slice, WorkerThreadId(0)).size(), 15U);
}

TEST_F(SpillableAggregationSliceTest, tupleCountSurvivesSpill)
{
    SpillableAggregationSlice slice(Timestamp(0), Timestamp(100), makeArgs(), NUMBER_OF_HASH_MAPS);
    populate(slice, WorkerThreadId(0), 10, 0);
    populate(slice, WorkerThreadId(1), 7, 1000);
    const auto residentCount = slice.getNumberOfTuples();
    EXPECT_EQ(residentCount, 17U);

    InMemorySpillBackend backend;
    slice.spill(backend);
    /// getNumberOfTuples must not dereference the freed maps; it returns the snapshot.
    EXPECT_EQ(slice.getNumberOfTuples(), residentCount);
}

TEST_F(SpillableAggregationSliceTest, spillAndUnspillEmptySliceIsNoOp)
{
    SpillableAggregationSlice slice(Timestamp(0), Timestamp(100), makeArgs(), NUMBER_OF_HASH_MAPS);
    InMemorySpillBackend backend;

    slice.spill(backend);
    EXPECT_FALSE(slice.isResident());
    EXPECT_EQ(backend.sliceCount(), 0U); /// nothing populated -> no records
    EXPECT_EQ(slice.getNumberOfTuples(), 0U);

    slice.unspill(backend, *bufferManager);
    EXPECT_TRUE(slice.isResident());
    EXPECT_EQ(slice.getNumberOfTuples(), 0U);
}

/// --- D1 seam (Route B): getHashMapPtr / getHashMapPtrOrCreate / getNumberOfTuples are virtual on HashMapSlice
/// and dispatch polymorphically through a HashMapSlice& regardless of the concrete slice type. ---

TEST_F(SpillableAggregationSliceTest, seamDispatchSpillableViaBaseRef)
{
    SpillableAggregationSlice slice(Timestamp(0), Timestamp(100), makeArgs(), NUMBER_OF_HASH_MAPS);
    populate(slice, WorkerThreadId(0), 12, 0);
    HashMapSlice& base = slice;
    /// Through the base ref we reach the spillable override (resident → returns the worker map).
    EXPECT_NE(base.getHashMapPtr(WorkerThreadId(0)), nullptr);
    EXPECT_EQ(base.getHashMapPtr(WorkerThreadId(0)), slice.getHashMapPtr(WorkerThreadId(0)));
    EXPECT_EQ(base.getNumberOfTuples(), 12U);
}

TEST_F(SpillableAggregationSliceTest, seamDispatchGetNumberOfTuplesSpilledViaBaseRef)
{
    SpillableAggregationSlice slice(Timestamp(0), Timestamp(100), makeArgs(), NUMBER_OF_HASH_MAPS);
    populate(slice, WorkerThreadId(0), 9, 0);
    HashMapSlice& base = slice;
    InMemorySpillBackend backend;
    slice.spill(backend);
    /// The virtual override must return the spill-time snapshot through HashMapSlice& — the base accumulate
    /// would return 0 once the maps are freed. This is the load-bearing reason getNumberOfTuples is virtual.
    EXPECT_EQ(base.getNumberOfTuples(), 9U);
}

TEST_F(SpillableAggregationSliceTest, seamDispatchAggregationSliceInheritsBase)
{
    AggregationSlice slice(Timestamp(0), Timestamp(100), makeArgs(), NUMBER_OF_HASH_MAPS);
    HashMapSlice& base = slice;
    /// AggregationSlice dropped its overrides; the inherited base impl lazily creates and returns the worker map.
    auto* const created = base.getHashMapPtrOrCreate(WorkerThreadId(0));
    EXPECT_NE(created, nullptr);
    EXPECT_EQ(base.getHashMapPtr(WorkerThreadId(0)), created);
}

}

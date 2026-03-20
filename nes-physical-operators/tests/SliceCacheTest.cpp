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
#include <cstdint>
#include <functional>
#include <memory>
#include <random>
#include <sstream>
#include <tuple>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <SliceCache/SliceCache.hpp>
#include <SliceCache/SliceCacheNone.hpp>
#include <SliceCache/SliceCacheSecondChance.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SliceAssigner.hpp>
#include <Time/Timestamp.hpp>
#include <Util/ExecutionMode.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>
#include <BaseUnitTest.hpp>
#include <Engine.hpp>
#include <function.hpp>
#include <options.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Reference implementation of a second-chance (clock) cache for verifying the Nautilus
/// cache implementation. This uses standard C++ data structures and prioritizes simplicity
/// and correctness over performance.
class SecondChanceCache
{
public:
    /// Represents a single cache entry with slice boundaries, a data pointer, and the second-chance bit
    struct CacheEntry
    {
        SliceStart sliceStart{Timestamp(0)};
        SliceEnd sliceEnd{Timestamp(0)};
        int8_t* dataStructure = nullptr;
        bool secondChanceBit = false;
    };

    /// Stores the result of a cache lookup, indicating whether it was a hit and the associated data pointer
    struct LookupResult
    {
        bool hit;
        int8_t* dataStructure;
    };

    /// Constructs a cache with the given number of entries, all initially empty.
    /// Empty entries have sliceStart == sliceEnd == 0, so no timestamp can match them.
    explicit SecondChanceCache(const uint64_t numberOfEntries) : entries(numberOfEntries) { }

    /// Looks up the slice containing the given timestamp using the clock (second-chance) algorithm.
    /// On a cache hit, sets the entry's second-chance bit and returns the cached data pointer.
    /// On a cache miss, scans from the clock hand position to find a victim entry: entries with
    /// their second-chance bit set get a reprieve (bit cleared, clock advances), while the first
    /// entry with a cleared bit is evicted and replaced with the new slice data.
    /// @param timestamp The event timestamp to look up
    /// @param sliceStart The start of the slice that contains the timestamp (used on miss)
    /// @param sliceEnd The end of the slice that contains the timestamp (used on miss)
    /// @param newDataStructure The data pointer to store in the cache on a miss
    /// @return A LookupResult with the hit/miss indicator and the data pointer
    LookupResult lookup(const Timestamp timestamp, const SliceStart sliceStart, const SliceEnd sliceEnd, int8_t* newDataStructure)
    {
        /// Linear search through all entries for a slice containing the timestamp, i.e., sliceStart <= timestamp < sliceEnd
        for (auto& entry : entries)
        {
            if (entry.sliceStart <= timestamp && timestamp < entry.sliceEnd)
            {
                entry.secondChanceBit = true;
                return {.hit = true, .dataStructure = entry.dataStructure};
            }
        }

        /// Cache miss: scan from the clock hand to find a victim with secondChanceBit == false.
        /// Entries with secondChanceBit == true get a second chance: their bit is cleared and the
        /// clock hand advances past them.
        while (entries[clockHand].secondChanceBit)
        {
            entries[clockHand].secondChanceBit = false;
            clockHand = (clockHand + 1) % entries.size();
        }

        /// Replace the victim entry with the new slice data and mark it as recently used
        auto& victim = entries[clockHand];
        victim.sliceStart = sliceStart;
        victim.sliceEnd = sliceEnd;
        victim.dataStructure = newDataStructure;
        victim.secondChanceBit = true;

        /// The clock hand stays at the just-replaced position, matching the behavior of
        /// SliceCacheSecondChance which does not advance replacementIndex after replacement
        return {.hit = false, .dataStructure = newDataStructure};
    }

private:
    std::vector<CacheEntry> entries;
    uint64_t clockHand{0};
};

/// Shared base class containing members and helpers used by both SliceCacheNoneTest and SliceCacheSecondChanceTest.
class SliceCacheTestBase : public Testing::BaseUnitTest
{
public:
    struct SliceCacheTestOperation
    {
        Timestamp timestamp;
        SliceStart sliceStart;
        SliceEnd sliceEnd;
        int8_t* expectedResult; /// Points to expectedResultHelper
        std::unique_ptr<uint64_t> expectedResultHelper;
    };

    static constexpr bool mlirEnableMultithreading = false;
    static constexpr uint64_t minNumberOfOperations = 10'000;
    static constexpr uint64_t maxNumberOfOperations = 100'000;
    std::unique_ptr<nautilus::engine::NautilusEngine> nautilusEngine;
    ExecutionMode backend = ExecutionMode::INTERPRETER;
    std::unique_ptr<SliceCache> sliceCache;
    uint64_t numberOfEntries = 1;
    uint64_t sliceSize = 1;
    std::vector<SliceCacheTestOperation> operations;

    static void SetUpTestSuite()
    {
        Logger::setupLogging("SliceCacheTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup SliceCacheTest class.");
    }

    /// Initializes the Nautilus engine with the current backend setting.
    void initEngine()
    {
        nautilus::engine::Options options;
        const bool compilation = (backend == ExecutionMode::COMPILER);
        NES_INFO("Backend: {} and compilation: {}", magic_enum::enum_name(backend), compilation);
        options.setOption("engine.Compilation", compilation);
        options.setOption("mlir.enableMultithreading", mlirEnableMultithreading);
        nautilusEngine = std::make_unique<nautilus::engine::NautilusEngine>(options);
    }

    /// Creates random SliceCacheTestOperations and stores them in the operations vector.
    /// Uses a random seed (logged for reproducibility) to generate timestamps, computes
    /// slice boundaries via the SliceAssigner, and shuffles the resulting operations.
    void createRandomSliceCacheTestOperation()
    {
        const SliceAssigner sliceAssigner{sliceSize, sliceSize};

        /// Use a random seed and log it so that test failures can be reproduced
        std::random_device rd;
        const auto seed = rd();
        NES_INFO("SliceCacheTest random seed: {}", seed);
        std::mt19937 gen{seed};

        /// Generate timestamps in a range that produces more distinct slices than the cache
        /// can hold, ensuring both cache hits and evictions will occur
        const uint64_t maxTimestamp = sliceSize * numberOfEntries * 4;
        std::uniform_int_distribution<uint64_t> dist{0, maxTimestamp > 0 ? maxTimestamp - 1 : 0};

        /// Create a random number of operations to thoroughly exercise the cache
        std::uniform_int_distribution opCountDist(minNumberOfOperations, maxNumberOfOperations);
        const uint64_t numOperations = opCountDist(gen);
        for (uint64_t i = 0; i < numOperations; ++i)
        {
            const auto timestamp = dist(gen);
            const auto sliceStart = sliceAssigner.getSliceStartTs(Timestamp{timestamp});
            const auto sliceEnd = sliceAssigner.getSliceEndTs(Timestamp{timestamp});

            /// Each operation gets a unique data pointer backed by expectedResultHelper
            auto expectedResultHelper = std::make_unique<uint64_t>(i);
            auto* expectedResult = reinterpret_cast<int8_t*>(expectedResultHelper.get());
            operations.push_back({Timestamp{timestamp}, sliceStart, sliceEnd, expectedResult, std::move(expectedResultHelper)});
        }

        /// Shuffle operations to simulate random access patterns
        std::ranges::shuffle(operations, gen);
    }

    static void TearDownTestSuite() { NES_INFO("Tear down SliceCacheTest class."); }
};

/// Fixture for SliceCacheNone tests, parameterized only by ExecutionMode.
class SliceCacheNoneTest : public SliceCacheTestBase, public testing::WithParamInterface<ExecutionMode>
{
public:
    void SetUp() override
    {
        BaseUnitTest::SetUp();
        backend = GetParam();
        initEngine();
    }
};

/// Fixture for SliceCacheSecondChance tests, parameterized by (ExecutionMode, numberOfEntries, sliceSize).
class SliceCacheSecondChanceTest : public SliceCacheTestBase,
                                   public testing::WithParamInterface<std::tuple<ExecutionMode, uint64_t, uint64_t>>
{
public:
    void SetUp() override
    {
        BaseUnitTest::SetUp();
        backend = std::get<0>(GetParam());
        numberOfEntries = std::get<1>(GetParam());
        sliceSize = std::get<2>(GetParam());
        initEngine();
    }
};

/// Proxy function for nautilus::invoke that sets the callback flag and the dataStructure field.
/// Must be a free function (not a lambda) so that nautilus::invoke can obtain a valid function pointer.
static void replaceCacheEntryNoneProxy(bool* callbackFlag, SliceCacheEntry* entry, int8_t* dataStructure)
{
    *callbackFlag = true;
    entry->dataStructure = dataStructure;
}

/// Proxy for SecondChance: sets the callback flag, slice boundaries, and dataStructure.
static void replaceCacheEntrySecondChanceProxy(
    bool* callbackFlag, SliceCacheEntry* entry, Timestamp::Underlying sliceStart, Timestamp::Underlying sliceEnd, int8_t* dataStructure)
{
    *callbackFlag = true;
    entry->sliceStart = sliceStart;
    entry->sliceEnd = sliceEnd;
    entry->dataStructure = dataStructure;
}

TEST_P(SliceCacheNoneTest, testSliceCacheNone)
{
    sliceCache = std::make_unique<SliceCacheNone>();
    createRandomSliceCacheTestOperation();

    /// Allocate memory for the single dummy entry used by SliceCacheNone
    std::vector<uint8_t> noneCacheMemory(sliceCache->getCacheMemorySize(), 0);
    auto* const noneCacheMemoryPtr = reinterpret_cast<int8_t*>(noneCacheMemory.data());
    sliceCache->setStartOfEntries(reinterpret_cast<SliceCacheEntry*>(noneCacheMemoryPtr));

    /// SliceCacheNone never caches anything, so every lookup must invoke the replacement
    /// callback and return exactly what the callback provides
    bool callbackCalled = false;
    using CompiledCacheFunction
        = std::function<nautilus::val<int8_t*>(nautilus::val<uint64_t>, nautilus::val<int8_t*>, nautilus::val<bool*>)>;
    auto sliceCacheCallableFunction = nautilusEngine->registerFunction(CompiledCacheFunction(
        [&](const nautilus::val<uint64_t>& timestampRaw,
            const nautilus::val<int8_t*>& newDataStructurePtr,
            const nautilus::val<bool*>& callbackCalledPtr) -> nautilus::val<int8_t*>
        {
            const nautilus::val<Timestamp> timestamp{timestampRaw};
            return sliceCache->getDataStructureRef(
                timestamp,
                nautilus::val<WorkerThreadId>{0},
                [&](const nautilus::val<SliceCacheEntry*>& entryToReplace)
                {
                    /// Use nautilus::invoke with a proxy function to avoid copying the val
                    /// (which would trigger traceCopy on a null-state val, creating a free variable).
                    nautilus::invoke(replaceCacheEntryNoneProxy, callbackCalledPtr, entryToReplace, newDataStructurePtr);
                });
        }));

    for (const auto& op : operations)
    {
        callbackCalled = false;
        auto* const rawResult = sliceCacheCallableFunction(op.timestamp.getRawValue(), op.expectedResult, &callbackCalled);

        EXPECT_TRUE(callbackCalled) << "SliceCacheNone must always call the replacement callback";

        /// Verify the returned pointer matches what the callback provided
        EXPECT_EQ(rawResult, op.expectedResult) << "SliceCacheNone must return the callback's result";
    }
}

TEST_P(SliceCacheSecondChanceTest, testSliceCacheSecondChance)
{
    sliceCache = std::make_unique<SliceCacheSecondChance>(numberOfEntries, sizeof(SliceCacheEntrySecondChance));
    createRandomSliceCacheTestOperation();

    /// Allocate and zero-initialize the cache memory buffer that the Nautilus cache operates on.
    /// Zero-initialized entries have sliceStart == sliceEnd == 0, so no timestamp will match them.
    /// getCacheMemorySize() includes extra space for the replacement index stored after the entries.
    const auto cacheMemorySize = sliceCache->getCacheMemorySize();
    std::vector<uint8_t> cacheMemory(cacheMemorySize, 0);
    auto* const cacheMemoryPtr = reinterpret_cast<int8_t*>(cacheMemory.data());
    sliceCache->setStartOfEntries(reinterpret_cast<SliceCacheEntry*>(cacheMemoryPtr));

    bool callbackCalled = false;
    using CompiledCacheFunction = std::function<nautilus::val<int8_t*>(
        nautilus::val<Timestamp::Underlying>,
        nautilus::val<Timestamp::Underlying>,
        nautilus::val<Timestamp::Underlying>,
        nautilus::val<int8_t*>,
        nautilus::val<bool*>)>;
    auto sliceCacheCallableFunction = nautilusEngine->registerFunction(CompiledCacheFunction(
        [&](const nautilus::val<Timestamp::Underlying>& timestampRaw,
            const nautilus::val<Timestamp::Underlying>& sliceStartRaw,
            const nautilus::val<Timestamp::Underlying>& sliceEndRaw,
            const nautilus::val<int8_t*>& newDataStructurePtr,
            const nautilus::val<bool*>& callbackCalledPtr) -> nautilus::val<int8_t*>
        {
            const nautilus::val<Timestamp> timestamp{timestampRaw};
            return sliceCache->getDataStructureRef(
                timestamp,
                nautilus::val<WorkerThreadId>{0},
                [&](const nautilus::val<SliceCacheEntry*>& entryToReplace)
                {
                    /// Use nautilus::invoke with a proxy function to avoid copying the val
                    /// (which would trigger traceCopy on a null-state val, creating a free variable).
                    nautilus::invoke(
                        replaceCacheEntrySecondChanceProxy,
                        callbackCalledPtr,
                        entryToReplace,
                        sliceStartRaw,
                        sliceEndRaw,
                        newDataStructurePtr);
                });
        }));
    /// Create a reference cache to independently verify the Nautilus implementation
    SecondChanceCache referenceCache(numberOfEntries);

    for (const auto& op : operations)
    {
        /// Track whether the replacement callback was invoked to distinguish hits from misses
        callbackCalled = false;
        auto* const rawResult = sliceCacheCallableFunction(
            op.timestamp.getRawValue(), op.sliceStart.getRawValue(), op.sliceEnd.getRawValue(), op.expectedResult, &callbackCalled);

        /// Perform the same lookup in the reference cache
        const auto [refHit, refPtr] = referenceCache.lookup(Timestamp{op.timestamp}, op.sliceStart, op.sliceEnd, op.expectedResult);

        /// Verify hit/miss agreement between the Nautilus implementation and the reference cache
        if (refHit)
        {
            EXPECT_FALSE(callbackCalled) << "Expected cache hit for timestamp " << op.timestamp
                                         << " but the replacement callback was called";
        }
        else
        {
            EXPECT_TRUE(callbackCalled) << "Expected cache miss for timestamp " << op.timestamp
                                        << " but the replacement callback was not called";
        }

        /// Verify the returned data pointer matches the reference cache's result
        EXPECT_EQ(rawResult, refPtr) << "Returned pointer does not match reference cache for timestamp " << op.timestamp;
    }
}

INSTANTIATE_TEST_CASE_P(
    SliceCacheNoneTest,
    SliceCacheNoneTest,
    ::testing::Values(ExecutionMode::INTERPRETER, ExecutionMode::COMPILER),
    [](const testing::TestParamInfo<SliceCacheNoneTest::ParamType>& info) { return std::string{magic_enum::enum_name(info.param)}; });

INSTANTIATE_TEST_CASE_P(
    SliceCacheSecondChanceTest,
    SliceCacheSecondChanceTest,
    ::testing::Combine(
        ::testing::Values(ExecutionMode::INTERPRETER, ExecutionMode::COMPILER), /// Nautilus execution backend
        ::testing::Values(1, 5, 10, 15, 50, 100), /// Number of cache entries
        ::testing::Values(1, 10, 100, 1000, 100'000) /// Size of slice
        ),
    [](const testing::TestParamInfo<SliceCacheSecondChanceTest::ParamType>& info)
    {
        std::stringstream ss;
        ss << magic_enum::enum_name(std::get<0>(info.param)) << "_Entries" << std::get<1>(info.param) << "_SliceSize"
           << std::get<2>(info.param);
        return ss.str();
    });
}

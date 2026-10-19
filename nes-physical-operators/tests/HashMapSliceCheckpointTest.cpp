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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <tuple>
#include <utility>
#include <vector>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/HashMap/ChainedHashMap/FieldOffsets.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <SliceStore/Slice.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>
#include <HashMapSliceCheckpoint.hpp>
#include <PipelineExecutionContext.hpp>
#include <SliceCacheConfiguration.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{
namespace
{
constexpr uint64_t NumberOfWorkerThreads = 2;
constexpr uint64_t WindowSize = 10;
constexpr uint64_t KeyOffset = sizeof(ChainedHashMapEntry);
constexpr uint64_t ValueOffset = KeyOffset + sizeof(uint64_t);
constexpr uint64_t EntrySize = ValueOffset + sizeof(uint64_t);

class TestPipelineExecutionContext final : public PipelineExecutionContext
{
public:
    explicit TestPipelineExecutionContext(std::shared_ptr<AbstractBufferProvider> bufferProvider)
        : bufferProvider(std::move(bufferProvider))
    {
    }

    bool emitBuffer(const TupleBuffer&, ContinuationPolicy) override { return true; }

    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { }

    TupleBuffer allocateTupleBuffer() override { return bufferProvider->getBufferBlocking(); }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return WorkerThreadId(0); }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return NumberOfWorkerThreads; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferProvider; }

    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

private:
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
};

ChainedHashMapConfig createConfig()
{
    const DataType uint64Type{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};
    return ChainedHashMapConfig{
        .entrySize = EntrySize,
        .numberOfBuckets = 4,
        .pageSize = EntrySize * 3,
        .bloomFilterParams = std::nullopt,
        .fieldKeys = {FieldOffsets{.fieldIdentifier = Identifier::parse("key"), .type = uint64Type, .fieldOffset = KeyOffset}},
        .fieldValues = {FieldOffsets{.fieldIdentifier = Identifier::parse("value"), .type = uint64Type, .fieldOffset = ValueOffset}},
        .hashFunction = std::make_shared<MurMur3HashFunction>()};
}

HashMapSliceCheckpoint createCheckpoint(const ChainedHashMapConfig& config)
{
    return HashMapSliceCheckpoint{
        {config},
        [config](WindowBasedOperatorHandler& handler, AbstractBufferProvider& bufferProvider)
        { return handler.getCreateNewSlicesFunction(CreateNewHashMapSliceArgs{config, bufferProvider}); }};
}

/// Keys are their own hashes, which places several of them in the same chain of a map with four buckets.
void insert(const TupleBuffer& hashMapBuffer, AbstractBufferProvider& bufferProvider, const uint64_t key, const uint64_t value)
{
    const auto config = createConfig();
    auto hashMap = ChainedHashMap::load(hashMapBuffer);
    auto* entry = reinterpret_cast<std::byte*>(hashMap.insertEntry( /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        key,
        &bufferProvider,
        config.entrySize,
        config.entriesPerPage(),
        config.pageSize,
        ChainedHashMap::calculateMask(config.numberOfBuckets)));
    std::memcpy(entry + KeyOffset, &key, sizeof(key));
    std::memcpy(entry + ValueOffset, &value, sizeof(value));
}

/// Walks every chain, so that the result also depends on the rebuilt chain pointers.
std::map<uint64_t, uint64_t> readAll(const TupleBuffer& hashMapBuffer)
{
    const auto config = createConfig();
    auto hashMap = ChainedHashMap::load(hashMapBuffer);
    std::map<uint64_t, uint64_t> entries;
    for (uint64_t chain = 0; chain < ChainedHashMap::calculateNumberOfChains(config.numberOfBuckets); ++chain)
    {
        for (const auto* entry = hashMap.getChain(chain); entry != nullptr; entry = entry->next)
        {
            EXPECT_EQ(entry->hash & ChainedHashMap::calculateMask(config.numberOfBuckets), chain);
            const auto* entryMemory = reinterpret_cast<const std::byte*>(entry); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            uint64_t key = 0;
            uint64_t value = 0;
            std::memcpy(&key, entryMemory + KeyOffset, sizeof(key));
            std::memcpy(&value, entryMemory + ValueOffset, sizeof(value));
            entries.emplace(key, value);
        }
    }
    EXPECT_EQ(entries.size(), hashMap.getTotalNumberOfRecords());
    return entries;
}

using SliceBounds = std::pair<uint64_t, uint64_t>;
using SliceContents = std::map<SliceBounds, std::map<uint64_t, std::map<uint64_t, uint64_t>>>;

SliceContents readSlices(AggregationOperatorHandler& handler)
{
    SliceContents contents;
    for (const auto& slice : handler.getSliceAndWindowStore().getActiveSlices())
    {
        const auto& aggregationSlice = dynamic_cast<const AggregationSlice&>(*slice);
        auto& sliceContents = contents[{slice->getSliceStart().getRawValue(), slice->getSliceEnd().getRawValue()}];
        for (uint64_t worker = 0; worker < NumberOfWorkerThreads; ++worker)
        {
            if (const auto* hashMapBuffer = aggregationSlice.getHashMapBufferRefForWorker(WorkerThreadId(worker)))
            {
                sliceContents.emplace(worker, readAll(*hashMapBuffer));
            }
        }
    }
    return contents;
}
}

class HashMapSliceCheckpointTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("HashMapSliceCheckpointTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        bufferManager
            = BufferManager::create(64 * 1024 * 1024, 0.5, BufferAlignment{64}, 4096, std::make_shared<NesDefaultMemoryAllocator>());
        stateDirectory = std::filesystem::temp_directory_path()
            / ("hash-map-slice-checkpoint-test-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    }

    void TearDown() override
    {
        std::error_code errorCode;
        std::filesystem::remove_all(stateDirectory, errorCode);
        BaseUnitTest::TearDown();
    }

    [[nodiscard]] static std::shared_ptr<AggregationOperatorHandler> createHandler()
    {
        auto handler = std::make_shared<AggregationOperatorHandler>(
            std::vector{OriginId(1)},
            OriginId(2),
            std::make_unique<DefaultTimeBasedSliceStore>(WindowSize, WindowSize, SliceCacheConfiguration{}));
        handler->enableCheckpointing(createCheckpoint(createConfig()));
        return handler;
    }

    [[nodiscard]] AggregationSlice& getOrCreateSlice(AggregationOperatorHandler& handler, const uint64_t timestamp) const
    {
        const auto slices = handler.getSliceAndWindowStore().getSlicesOrCreate(
            Timestamp(timestamp), handler.getCreateNewSlicesFunction(CreateNewHashMapSliceArgs{createConfig(), *bufferManager}));
        return dynamic_cast<AggregationSlice&>(*slices.front());
    }

    [[nodiscard]] const TupleBuffer&
    getOrCreateHashMap(AggregationOperatorHandler& handler, const uint64_t timestamp, const uint64_t worker) const
    {
        return *getOrCreateSlice(handler, timestamp).getOrCreateHashMapBufferRefForWorker(*bufferManager, WorkerThreadId(worker));
    }

    std::shared_ptr<BufferManager> bufferManager;
    std::filesystem::path stateDirectory;
};

TEST_F(HashMapSliceCheckpointTest, ActiveSlicesAreOrderedBySliceEnd)
{
    DefaultTimeBasedSliceStore sliceStore(10, 5, SliceCacheConfiguration{});
    const auto createSlice = [](const SliceStart sliceStart, const SliceEnd sliceEnd)
    { return std::vector<std::shared_ptr<Slice>>{std::make_shared<Slice>(sliceStart, sliceEnd)}; };

    std::ignore = sliceStore.getSlicesOrCreate(Timestamp(14), createSlice);
    std::ignore = sliceStore.getSlicesOrCreate(Timestamp(1), createSlice);
    std::ignore = sliceStore.getSlicesOrCreate(Timestamp(8), createSlice);
    std::ignore = sliceStore.getSlicesOrCreate(Timestamp(9), createSlice);

    const auto activeSlices = sliceStore.getActiveSlices();
    ASSERT_EQ(activeSlices.size(), 3U);
    EXPECT_EQ(activeSlices[0]->getSliceEnd(), Timestamp(5));
    EXPECT_EQ(activeSlices[1]->getSliceEnd(), Timestamp(10));
    EXPECT_EQ(activeSlices[2]->getSliceEnd(), Timestamp(15));
}

TEST_F(HashMapSliceCheckpointTest, RestoresSlicesAndHashMapsIntoFreshHandler)
{
    TestPipelineExecutionContext pipelineContext(bufferManager);
    const auto checkpointedHandler = createHandler();
    checkpointedHandler->start(pipelineContext);

    const auto& firstWorkerMap = getOrCreateHashMap(*checkpointedHandler, 3, 0);
    for (uint64_t key = 0; key < 20; ++key)
    {
        insert(firstWorkerMap, *bufferManager, key, key * 10);
    }
    insert(getOrCreateHashMap(*checkpointedHandler, 25, 1), *bufferManager, 42, 4242);
    const auto expected = readSlices(*checkpointedHandler);
    ASSERT_EQ(expected.size(), 2U);

    checkpointedHandler->serializeState(stateDirectory);

    const auto recoveredHandler = createHandler();
    recoveredHandler->setCheckpointed(stateDirectory);
    recoveredHandler->start(pipelineContext);
    EXPECT_EQ(readSlices(*recoveredHandler), expected);

    insert(getOrCreateHashMap(*recoveredHandler, 25, 1), *bufferManager, 43, 4343);
    EXPECT_EQ(readSlices(*recoveredHandler).at(SliceBounds{20, 30}).at(1), (std::map<uint64_t, uint64_t>{{42, 4242}, {43, 4343}}));
}

TEST_F(HashMapSliceCheckpointTest, RestoredWindowsThatWereEmittedBeforeTheCheckpointAreNotEmittedAgain)
{
    TestPipelineExecutionContext pipelineContext(bufferManager);
    const auto checkpointedHandler = createHandler();
    checkpointedHandler->start(pipelineContext);
    insert(getOrCreateHashMap(*checkpointedHandler, 3, 0), *bufferManager, 1, 1);
    insert(getOrCreateHashMap(*checkpointedHandler, 25, 0), *bufferManager, 2, 2);
    ASSERT_EQ(checkpointedHandler->getSliceAndWindowStore().getTriggerableWindowSlices(Timestamp(11)).size(), 1U);
    checkpointedHandler->serializeState(stateDirectory);

    const auto recoveredHandler = createHandler();
    recoveredHandler->setCheckpointed(stateDirectory);
    recoveredHandler->start(pipelineContext);
    auto& recoveredSliceStore = recoveredHandler->getSliceAndWindowStore();
    EXPECT_EQ(recoveredSliceStore.getEmittedWindowsEnd(), Timestamp(WindowSize));
    const auto triggeredWindows = recoveredSliceStore.getTriggerableWindowSlices(Timestamp(31));
    ASSERT_EQ(triggeredWindows.size(), 1U);
    EXPECT_EQ(triggeredWindows.begin()->first.windowInfo.windowEnd, Timestamp(30));
}

TEST_F(HashMapSliceCheckpointTest, CorruptCheckpointFailsRecovery)
{
    TestPipelineExecutionContext pipelineContext(bufferManager);
    const auto checkpointedHandler = createHandler();
    checkpointedHandler->start(pipelineContext);
    insert(getOrCreateHashMap(*checkpointedHandler, 3, 0), *bufferManager, 1, 1);
    checkpointedHandler->serializeState(stateDirectory);

    const auto checkpointFile = stateDirectory / "hash_map_slices.bin";
    std::filesystem::resize_file(checkpointFile, std::filesystem::file_size(checkpointFile) / 2);

    const auto recoveredHandler = createHandler();
    recoveredHandler->setCheckpointed(stateDirectory);
    ASSERT_EXCEPTION_ERRORCODE(recoveredHandler->start(pipelineContext), ErrorCode::CheckpointError);
    EXPECT_TRUE(recoveredHandler->getSliceAndWindowStore().getActiveSlices().empty());
}

TEST_F(HashMapSliceCheckpointTest, HandlerWithoutRecoveryDirectoryStartsEmpty)
{
    TestPipelineExecutionContext pipelineContext(bufferManager);
    const auto checkpointedHandler = createHandler();
    checkpointedHandler->start(pipelineContext);
    insert(getOrCreateHashMap(*checkpointedHandler, 3, 0), *bufferManager, 1, 1);
    checkpointedHandler->serializeState(stateDirectory);

    const auto freshHandler = createHandler();
    freshHandler->setCheckpointed(std::nullopt);
    freshHandler->start(pipelineContext);
    EXPECT_TRUE(freshHandler->getSliceAndWindowStore().getActiveSlices().empty());
}

}

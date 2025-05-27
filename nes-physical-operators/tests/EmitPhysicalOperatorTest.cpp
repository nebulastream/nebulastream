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

#include <EmitOperatorHandler.hpp>

#include <algorithm>
#include <barrier>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <memory>
#include <random>
#include <ranges>
#include <set>
#include <source_location>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <folly/Synchronized.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ExecutionContext.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

class EmitPhysicalOperatorTest : public Testing::BaseUnitTest
{
    struct MockedPipelineContext final : PipelineExecutionContext
    {
        bool emitBuffer(const Memory::TupleBuffer& buffer, ContinuationPolicy) override
        {
            buffers.wlock()->emplace_back(buffer);
            return true;
        }

        Memory::TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }
        [[nodiscard]] WorkerThreadId getId() const override { return INITIAL<WorkerThreadId>; }
        [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }
        [[nodiscard]] std::shared_ptr<Memory::AbstractBufferProvider> getBufferManager() const override { return bufferManager; }
        [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }
        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override
        {
            return *operatorHandlers;
        }
        void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& opHandlers) override
        {
            operatorHandlers = &opHandlers;
        }

        MockedPipelineContext(
            folly::Synchronized<std::vector<Memory::TupleBuffer>>& buffers, std::shared_ptr<Memory::BufferManager> bufferManager)
            : buffers(buffers), bufferManager(std::move(bufferManager))
        {
        }

        ///NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members) lifetime is ensured by the `run` method.
        folly::Synchronized<std::vector<Memory::TupleBuffer>>& buffers;
        std::shared_ptr<Memory::BufferManager> bufferManager;
        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>* operatorHandlers = nullptr;
    };

public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("EmitPhysicalOperatorTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup EmitPhysicalOperatorTest test class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        reset();
    }

    EmitPhysicalOperator createUUT()
    {
        auto schema = Schema{}.addField("A_FIELD", DataType::Type::UINT32);
        auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(512, schema);
        EmitPhysicalOperator emit{OperatorHandlerId(0), std::make_shared<Interface::MemoryProvider::RowTupleBufferMemoryProvider>(layout)};
        handlers.emplace(OperatorHandlerId(0), std::make_shared<EmitOperatorHandler>());
        return emit;
    }

    void run(const std::function<void(ExecutionContext&, RecordBuffer&)>& test, Memory::TupleBuffer buffer)
    {
        MockedPipelineContext pec{buffers, bm};
        pec.setOperatorHandlers(handlers);
        Arena arena(bm);

        ExecutionContext executionContext{&pec, &arena};
        executionContext.chunkNumber = buffer.getChunkNumber();
        executionContext.sequenceNumber = buffer.getSequenceNumber(), executionContext.lastChunk = buffer.isLastChunk();
        executionContext.originId = buffer.getOriginId();

        RecordBuffer recordBuffer(std::addressof(buffer));
        test(executionContext, recordBuffer);
    }

    ///NOLINTBEGIN(fuchsia-default-arguments-declarations)

    void checkNumberOfBuffers(size_t numberOfBuffers, std::source_location location = std::source_location::current())
    {
        const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "checkNumberOfBuffers");
        EXPECT_EQ(buffers.rlock()->size(), numberOfBuffers) << fmt::format("expects {} buffers to be emitted", numberOfBuffers);
    }

    void checkBufferAt(
        size_t index,
        SequenceNumber::Underlying sequence,
        ChunkNumber::Underlying chunkNumber,
        bool isLastChunk = false,
        OriginId originId = INITIAL<OriginId>,
        size_t numberOfTuples = 0,
        std::source_location location = std::source_location::current())
    {
        const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "checkBufferAt");
        ASSERT_GE(buffers.rlock()->size(), index) << fmt::format("Index out of bound when checking buffer at {}", index);
        EXPECT_EQ(buffers.rlock()->at(index).getNumberOfTuples(), numberOfTuples) << fmt::format("Expected {} tuples", numberOfTuples);
        EXPECT_EQ(buffers.rlock()->at(index).getSequenceNumber(), SequenceNumber(sequence))
            << fmt::format("Expected Sequence Number {}", sequence);
        EXPECT_EQ(buffers.rlock()->at(index).getChunkNumber(), ChunkNumber(chunkNumber))
            << fmt::format("Expected Chunk Number {}", sequence);
        EXPECT_EQ(buffers.rlock()->at(index).getOriginId(), OriginId(originId)) << fmt::format("Expected Chunk Number {}", sequence);
        EXPECT_EQ(buffers.rlock()->at(index).isLastChunk(), isLastChunk);
    }

    void checkForDups(std::source_location location = std::source_location::current())
    {
        const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "checkForDups");
        auto uniqueSequences = (*buffers.rlock())
            | std::views::transform([](const auto& buffer)
                                    { return SequenceData(buffer.getSequenceNumber(), buffer.getChunkNumber(), buffer.isLastChunk()); })
            | std::ranges::to<std::set>();

        EXPECT_EQ(buffers.rlock()->size(), uniqueSequences.size()) << "Received duplicate sequences";
    }

    void checkLastChunks(std::source_location location = std::source_location::current())
    {
        const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "checkForLastChunks");
        std::unordered_map<SequenceNumber, std::tuple<size_t, ChunkNumber::Underlying, bool>> sequenceNumberTerminations;

        for (const auto& buffer : *buffers.rlock())
        {
            auto& [seen, maxChunkNumber, termination] = sequenceNumberTerminations[buffer.getSequenceNumber()];
            seen++;
            maxChunkNumber = std::max(maxChunkNumber, buffer.getChunkNumber().getRawValue());
            EXPECT_FALSE(termination && buffer.isLastChunk())
                << fmt::format("Sequence {} has multiple last chunks", buffer.getSequenceNumber());
            termination |= buffer.isLastChunk();
        }

        for (const auto& [seq, t] : sequenceNumberTerminations)
        {
            const auto& [seen, max, terminated] = t;
            EXPECT_TRUE(terminated) << fmt::format("Sequence {} is not terminated", seq);
            EXPECT_EQ(seen, max) << fmt::format("Sequence {}: The maximum chunk number is {}, but we only saw {} chunks", seq, max, seen);
        }
    }

    Memory::TupleBuffer createBuffer(
        SequenceNumber::Underlying sequence,
        ChunkNumber::Underlying chunkNumber,
        bool isLastChunk = false,
        OriginId originId = INITIAL<OriginId>,
        size_t numberOfTuples = 0)
    {
        auto buffer = bm->getBufferBlocking();
        buffer.setNumberOfTuples(numberOfTuples);
        buffer.setLastChunk(isLastChunk);
        buffer.setChunkNumber(ChunkNumber(chunkNumber));
        buffer.setSequenceNumber(SequenceNumber(sequence));
        buffer.setOriginId(originId);

        return buffer;
    }

    ///NOLINTEND(fuchsia-default-arguments-declarations)

    void reset() { buffers.wlock()->clear(); }


    folly::Synchronized<std::vector<Memory::TupleBuffer>> buffers;
    std::shared_ptr<Memory::BufferManager> bm = Memory::BufferManager::create(512, 100000);
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;

    std::random_device rd;
};


TEST_F(EmitPhysicalOperatorTest, BasicTest)
{
    auto buffer = createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL, true);
    EmitPhysicalOperator emit = createUUT();

    run(
        [&](auto& executionContext, auto& recordBuffer)
        {
            emit.open(executionContext, recordBuffer);
            emit.close(executionContext, recordBuffer);
        },
        buffer);

    checkBufferAt(0, SequenceNumber::INITIAL, ChunkNumber::INITIAL, true);
    checkForDups();
    checkLastChunks();
}

TEST_F(EmitPhysicalOperatorTest, ChunkNumberTest)
{
    std::vector<Memory::TupleBuffer> inputBuffers;
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL, false));
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL + 1, false));
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL + 2, false));
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL + 3, false));
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL + 4, true));


    bool hasMorePermutations = true;
    while (hasMorePermutations)
    {
        reset();
        EmitPhysicalOperator emit = createUUT();
        for (auto& buffer : inputBuffers)
        {
            run(
                [&](auto& executionContext, auto& recordBuffer)
                {
                    emit.open(executionContext, recordBuffer);
                    emit.close(executionContext, recordBuffer);
                },
                buffer);
        }
        checkNumberOfBuffers(5);
        checkBufferAt(4, SequenceNumber::INITIAL, ChunkNumber::INITIAL + 4, true);
        checkForDups();
        checkLastChunks();

        hasMorePermutations = std::ranges::next_permutation(
                                  inputBuffers,
                                  std::less{},
                                  [](const Memory::TupleBuffer& buffer)
                                  { return SequenceData(buffer.getSequenceNumber(), buffer.getChunkNumber(), buffer.isLastChunk()); })
                                  .found;
    }
}

/// Tests if all permutations result in a sane ordering of chunk numbers.
/// This means every sequence number should have the same number of chunks as inserted (albeit in different order) with exactly one chunk
/// marked as the last chunk, and no duplicates.
TEST_F(EmitPhysicalOperatorTest, SequenceChunkNumberTest)
{
    std::vector<Memory::TupleBuffer> inputBuffers;
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL, false));
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL + 1, false));
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL + 2, true));

    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL + 1, ChunkNumber::INITIAL, false));
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL + 1, ChunkNumber::INITIAL + 1, true));

    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL + 2, ChunkNumber::INITIAL, false));
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL + 2, ChunkNumber::INITIAL + 1, false));
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL + 2, ChunkNumber::INITIAL + 2, true));

    bool hasMorePermutations = true;
    while (hasMorePermutations)
    {
        reset();
        EmitPhysicalOperator emit = createUUT();
        for (auto& buffer : inputBuffers)
        {
            run(
                [&](auto& executionContext, auto& recordBuffer)
                {
                    emit.open(executionContext, recordBuffer);
                    emit.close(executionContext, recordBuffer);
                },
                buffer);
        }
        checkNumberOfBuffers(8);
        checkForDups();
        checkLastChunks();
        hasMorePermutations = std::ranges::next_permutation(
                                  inputBuffers,
                                  std::less{},
                                  [](const Memory::TupleBuffer& buffer)
                                  { return SequenceData(buffer.getSequenceNumber(), buffer.getChunkNumber(), buffer.isLastChunk()); })
                                  .found;
    };
}

TEST_F(EmitPhysicalOperatorTest, ConcurrentSequenceChunkNumberTest)
{
    for (auto [numberOfSequences, maxChunksPerSequence, numberOfThreads] :
         std::initializer_list<std::tuple<size_t, size_t, size_t>>{{2, 10, 2}, {1000, 2, 4}, {10, 100, 4}, {1000, 20, 10}})
    {
        reset();
        std::vector<Memory::TupleBuffer> inputBuffers;
        for (size_t seq = 0; seq < numberOfSequences; seq++)
        {
            std::uniform_int_distribution chunkNumbers(ChunkNumber::INITIAL + 1, maxChunksPerSequence);
            auto maxChunkForThisSequence = chunkNumbers(rd);
            for (size_t chunk = 0; chunk < maxChunkForThisSequence - 1; chunk++)
            {
                inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL + seq, ChunkNumber::INITIAL + chunk, false));
            }
            inputBuffers.emplace_back(
                createBuffer(SequenceNumber::INITIAL + seq, ChunkNumber::INITIAL + maxChunkForThisSequence - 1, true));
        }

        EmitPhysicalOperator emit = createUUT();
        std::ranges::shuffle(inputBuffers, rd);
        std::barrier<> barrier(static_cast<int>(numberOfThreads) + 1);
        std::vector<std::jthread> threads;
        threads.reserve(numberOfThreads);
        for (size_t threadId = 0; threadId < numberOfThreads; threadId++)
        {
            threads.emplace_back(
                [threadId, &inputBuffers, this, &emit, &barrier, numberOfThreads]()
                {
                    barrier.arrive_and_wait();
                    for (size_t index = threadId; index < inputBuffers.size(); index += numberOfThreads)
                    {
                        run(
                            [&](auto& executionContext, auto& recordBuffer)
                            {
                                emit.open(executionContext, recordBuffer);
                                emit.close(executionContext, recordBuffer);
                            },
                            inputBuffers.at(index));
                    }
                });
        }
        barrier.arrive_and_wait();
        threads.clear();

        checkNumberOfBuffers(inputBuffers.size());
        checkForDups();
        checkLastChunks();
    }
}
}

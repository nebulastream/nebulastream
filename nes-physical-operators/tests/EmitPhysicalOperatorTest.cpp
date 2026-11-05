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
#include <chrono>
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
#include <DataTypes/VarVal.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/BufferRef/RowTupleBufferRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
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
#include <nautilus/Engine.hpp>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Arena.hpp>
#include <BaseUnitTest.hpp>
#include <CoalescingEmitOperatorHandler.hpp>
#include <CompilationContext.hpp>
#include <DataStructureTestUtils.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PipelineExecutionContext.hpp>
#include <TestTupleBuffer.hpp>
#include <val_arith.hpp>

namespace NES
{

namespace
{
constexpr uint32_t POOLED_BUFFER_SIZE = 512;
constexpr uint32_t NUMBER_OF_POOLED_BUFFERS = 100000;
constexpr NES::BufferAlignment BUFFER_ALIGNMENT{64};
constexpr double UNPOOLED_MEMORY_FRACTION = 0.9;
constexpr size_t TOTAL_MEMORY_IN_BYTES = 10 * static_cast<size_t>(NUMBER_OF_POOLED_BUFFERS) * POOLED_BUFFER_SIZE;
constexpr size_t MAX_HELD_RUNS = 64;
}

class EmitPhysicalOperatorTest : public Testing::BaseUnitTest
{
    struct MockedPipelineContext final : PipelineExecutionContext
    {
        bool emitBuffer(const TupleBuffer& buffer, ContinuationPolicy) override
        {
            buffers.wlock()->emplace_back(buffer);
            return true;
        }

        TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }

        [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return INITIAL<WorkerThreadId>; }

        [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }

        [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }

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
            folly::Synchronized<std::vector<TupleBuffer>>& buffers,
            folly::Synchronized<std::vector<std::function<void(PipelineExecutionContext&)>>>& callbacks,
            std::shared_ptr<BufferManager> bufferManager)
            : buffers(buffers), callbacks(callbacks), bufferManager(std::move(bufferManager))
        {
        }

        void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { INVARIANT(false, "This function should not be called"); }

        void scheduleCallback(std::chrono::microseconds, std::function<void(PipelineExecutionContext&)> callback) override
        {
            callbacks.wlock()->emplace_back(std::move(callback));
        }

        ///NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members) lifetime is ensured by the `run` method.
        folly::Synchronized<std::vector<TupleBuffer>>& buffers;
        folly::Synchronized<std::vector<std::function<void(PipelineExecutionContext&)>>>& callbacks;
        ///NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
        std::shared_ptr<BufferManager> bufferManager;
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
        auto schema = Schema<QualifiedUnboundField, Ordered>{QualifiedUnboundField{Identifier::parse("A_FIELD"), DataType::Type::UINT32}};
        auto bufferRef = LowerSchemaProvider::lowerSchema(512, schema, MemoryLayoutType::ROW_LAYOUT);
        EmitPhysicalOperator emit{OperatorHandlerId(0), std::move(bufferRef)};
        handlers.insert_or_assign(OperatorHandlerId(0), std::make_shared<EmitOperatorHandler>());
        return emit;
    }

    /// An emit whose handler coalesces, set up with a compiled merge as the pipeline stage does.
    EmitPhysicalOperator createCoalescingUUT(const std::chrono::microseconds maxDelay)
    {
        auto emit = createUUT();
        handlers.insert_or_assign(
            emit.getOperatorHandlerId(), std::make_shared<CoalescingEmitOperatorHandler>(emit.getBufferRef(), maxDelay, MAX_HELD_RUNS));
        const auto engine = TestUtils::makeEngine(TestUtils::EngineMode::Compiler);
        auto module = engine.createModule();
        CompilationContext compilationContext{module};
        withContext([&](ExecutionContext& executionContext) { emit.setup(executionContext, compilationContext); });
        auto compiledModule = module.compile();
        compilationContext.resolveAfterCompilation(compiledModule);
        return emit;
    }

    void withContext(const std::function<void(ExecutionContext&)>& test)
    {
        MockedPipelineContext pec{buffers, callbacks, bm};
        pec.setOperatorHandlers(handlers);
        Arena arena(bm);
        ExecutionContext executionContext{&pec, &arena};
        test(executionContext);
    }

    /// Runs the callbacks scheduled so far, ignoring their delays.
    void runCallbacks()
    {
        std::vector<std::function<void(PipelineExecutionContext&)>> due;
        callbacks.wlock()->swap(due);
        MockedPipelineContext pec{buffers, callbacks, bm};
        for (auto& callback : due)
        {
            callback(pec);
        }
    }

    void run(const std::function<void(ExecutionContext&, RecordBuffer&)>& test, TupleBuffer buffer)
    {
        MockedPipelineContext pec{buffers, callbacks, bm};
        pec.setOperatorHandlers(handlers);
        Arena arena(bm);

        ExecutionContext executionContext{&pec, &arena};
        executionContext.chunkNumber = buffer.getChunkNumber();
        executionContext.sequenceNumber = buffer.getSequenceNumber(), executionContext.lastChunk = buffer.isLastChunk();
        executionContext.sequenceRangeOffset = buffer.getSequenceRangeOffset();
        executionContext.originId = buffer.getOriginId();

        RecordBuffer recordBuffer{BorrowedNautilusBuffer::from(std::addressof(buffer))};
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
        auto uniqueSequences
            = (*buffers.rlock())
            | std::views::transform(
                  [](const auto& buffer)
                  {
                      return SequenceData(
                          buffer.getSequenceNumber(), buffer.getSequenceRangeOffset(), buffer.getChunkNumber(), buffer.isLastChunk());
                  })
            | std::ranges::to<std::set>();

        EXPECT_EQ(buffers.rlock()->size(), uniqueSequences.size()) << "Received duplicate sequences";
    }

    void checkLastChunks(std::source_location location = std::source_location::current())
    {
        const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "checkForLastChunks");
        std::unordered_map<SequenceNumber, std::tuple<size_t, ChunkNumber::Underlying, bool, ChunkNumber::Underlying>>
            sequenceNumberTerminations;

        for (const auto& buffer : *buffers.rlock())
        {
            auto& [seen, maxChunkNumber, termination, terminationAt] = sequenceNumberTerminations[buffer.getSequenceNumber()];
            seen++;
            maxChunkNumber = std::max(maxChunkNumber, buffer.getChunkNumber().getRawValue());
            if (buffer.isLastChunk())
            {
                terminationAt = buffer.getChunkNumber().getRawValue();
            }
            EXPECT_FALSE(termination && buffer.isLastChunk())
                << fmt::format("Sequence {} has multiple last chunks", buffer.getSequenceNumber());
            termination |= buffer.isLastChunk();
        }

        for (const auto& [seq, t] : sequenceNumberTerminations)
        {
            const auto& [seen, max, terminated, terminationAt] = t;
            EXPECT_TRUE(terminated) << fmt::format("Sequence {} is not terminated", seq);
            EXPECT_EQ(terminationAt, max) << fmt::format("Sequence {} Non Max Chunk Number has Last Flag", seq);
            EXPECT_EQ(seen, max) << fmt::format("Sequence {}: The maximum chunk number is {}, but we only saw {} chunks", seq, max, seen);
        }
    }

    TupleBuffer createBuffer(
        SequenceNumber::Underlying sequence,
        ChunkNumber::Underlying chunkNumber,
        bool isLastChunk = false,
        OriginId originId = INITIAL<OriginId>,
        size_t numberOfTuples = 0) const
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

    void reset()
    {
        buffers.wlock()->clear();
        callbacks.wlock()->clear();
    }

    void runTask(const EmitPhysicalOperator& emit, TupleBuffer buffer, const uint32_t numberOfRecords)
    {
        run(
            [&](auto& executionContext, auto& recordBuffer)
            {
                emit.open(executionContext, recordBuffer);
                for (uint32_t value = 0; value < numberOfRecords; ++value)
                {
                    Record record{std::unordered_map<Record::RecordFieldIdentifier, VarVal>{
                        {Identifier::parse("A_FIELD"), VarVal{nautilus::val<uint32_t>{value}}}}};
                    emit.execute(executionContext, record);
                }
                emit.close(executionContext, recordBuffer);
            },
            std::move(buffer));
    }

    /// Runs an input covering 5 sequence numbers whose 300 records spill over several output buffers, and checks that every output
    /// keeps its range.
    void expectSpilledOutputKeepsRange(const EmitPhysicalOperator& emit)
    {
        constexpr uint32_t numberOfRecords = 300;
        auto buffer = createBuffer(SequenceNumber::INITIAL + 9, ChunkNumber::INITIAL, true);
        buffer.setSequenceRange(SequenceNumber(SequenceNumber::INITIAL + 9), 4);
        runTask(emit, buffer, numberOfRecords);

        ASSERT_GT(buffers.rlock()->size(), 1);
        checkForDups();
        checkLastChunks();
        uint64_t numberOfTuples = 0;
        for (const auto& output : *buffers.rlock())
        {
            EXPECT_EQ(output.getSequenceNumber(), SequenceNumber(SequenceNumber::INITIAL + 9));
            EXPECT_EQ(output.getSequenceRangeOffset(), 4);
            numberOfTuples += output.getNumberOfTuples();
        }
        EXPECT_EQ(numberOfTuples, numberOfRecords);
    }

    folly::Synchronized<std::vector<TupleBuffer>> buffers;
    folly::Synchronized<std::vector<std::function<void(PipelineExecutionContext&)>>> callbacks;
    std::shared_ptr<BufferManager> bm = BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>());
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;
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
    std::vector<TupleBuffer> inputBuffers;
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

        hasMorePermutations
            = std::ranges::next_permutation(
                  inputBuffers,
                  std::less{},
                  [](const TupleBuffer& buffer)
                  {
                      return SequenceData(
                          buffer.getSequenceNumber(), buffer.getSequenceRangeOffset(), buffer.getChunkNumber(), buffer.isLastChunk());
                  })
                  .found;
    }
}

/// Tests if all permutations result in a sane ordering of chunk numbers.
/// This means every sequence number should have the same number of chunks as inserted (albeit in different order) with exactly one chunk
/// marked as the last chunk, and no duplicates.
TEST_F(EmitPhysicalOperatorTest, SequenceChunkNumberTest)
{
    std::vector<TupleBuffer> inputBuffers;
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
        hasMorePermutations
            = std::ranges::next_permutation(
                  inputBuffers,
                  std::less{},
                  [](const TupleBuffer& buffer)
                  {
                      return SequenceData(
                          buffer.getSequenceNumber(), buffer.getSequenceRangeOffset(), buffer.getChunkNumber(), buffer.isLastChunk());
                  })
                  .found;
    };
}

/// Concurrent inputs of sequences with one or more chunks, some of which spill over several output buffers, keep the chunk numbering of
/// every sequence and all their records.
TEST_F(EmitPhysicalOperatorTest, ConcurrentSequenceChunkNumberTest)
{
    /// Spills up to two full buffers of 128 records.
    constexpr uint32_t maxRecordsPerInput = 300;
    const auto seed = std::random_device{}();
    SCOPED_TRACE(fmt::format("seed {}", seed));
    std::mt19937 random{seed};
    std::uniform_int_distribution<uint32_t> recordsPerInput(0, maxRecordsPerInput);
    for (auto [numberOfSequences, maxChunksPerSequence, numberOfThreads] :
         std::initializer_list<std::tuple<size_t, size_t, size_t>>{{2, 10, 2}, {1000, 2, 4}, {10, 100, 4}, {1000, 20, 10}})
    {
        reset();
        std::vector<std::pair<TupleBuffer, uint32_t>> inputBuffers;
        uint64_t numberOfRecords = 0;
        for (size_t seq = 0; seq < numberOfSequences; seq++)
        {
            std::uniform_int_distribution chunkNumbers(ChunkNumber::INITIAL, maxChunksPerSequence);
            auto maxChunkForThisSequence = chunkNumbers(random);
            for (size_t chunk = 0; chunk < maxChunkForThisSequence; chunk++)
            {
                const auto records = recordsPerInput(random);
                numberOfRecords += records;
                inputBuffers.emplace_back(
                    createBuffer(SequenceNumber::INITIAL + seq, ChunkNumber::INITIAL + chunk, chunk + 1 == maxChunkForThisSequence),
                    records);
            }
        }

        EmitPhysicalOperator emit = createUUT();
        std::ranges::shuffle(inputBuffers, random);
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
                        runTask(emit, inputBuffers.at(index).first, inputBuffers.at(index).second);
                    }
                });
        }
        barrier.arrive_and_wait();
        threads.clear();

        checkForDups();
        checkLastChunks();
        uint64_t emittedRecords = 0;
        for (const auto& output : *buffers.rlock())
        {
            emittedRecords += output.getNumberOfTuples();
        }
        EXPECT_EQ(emittedRecords, numberOfRecords);
    }
}

TEST_F(EmitPhysicalOperatorTest, SpilledOutputIsChunkedAndKeepsRange)
{
    expectSpilledOutputKeepsRange(createUUT());
}

TEST_F(EmitPhysicalOperatorTest, CoalescingEmitMergesCompleteOutputsUntilTerminate)
{
    const auto emit = createCoalescingUUT(std::chrono::hours(1));
    for (SequenceNumber::Underlying sequence = SequenceNumber::INITIAL; sequence < SequenceNumber::INITIAL + 3; ++sequence)
    {
        runTask(emit, createBuffer(sequence, ChunkNumber::INITIAL, true), 3);
    }
    checkNumberOfBuffers(0);

    withContext([&](ExecutionContext& executionContext) { emit.terminate(executionContext); });
    checkNumberOfBuffers(1);
    checkBufferAt(0, SequenceNumber::INITIAL + 2, ChunkNumber::INITIAL, true, INITIAL<OriginId>, 9);
    auto merged = buffers.rlock()->at(0);
    EXPECT_EQ(merged.getSequenceRangeOffset(), 2);
    const Testing::TestSchema schema{UnqualifiedUnboundField{Identifier::parse("A_FIELD"), DataType::Type::UINT32}};
    auto view = Testing::TestTupleBuffer{schema}.open(merged);
    std::vector<uint32_t> values;
    values.reserve(view.getNumberOfTuples());
    for (size_t i = 0; i < view.getNumberOfTuples(); ++i)
    {
        values.push_back(view[i]["A_FIELD"].as<uint32_t>());
    }
    std::ranges::sort(values);
    EXPECT_EQ(values, (std::vector<uint32_t>{0, 0, 0, 1, 1, 1, 2, 2, 2}));
}

TEST_F(EmitPhysicalOperatorTest, CoalescingEmitPassesChunkedInputsThrough)
{
    const auto emit = createCoalescingUUT(std::chrono::hours(1));
    runTask(emit, createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL, false), 1);
    runTask(emit, createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL + 1, true), 1);
    checkNumberOfBuffers(2);
    checkForDups();
    checkLastChunks();

    withContext([&](ExecutionContext& executionContext) { emit.terminate(executionContext); });
    checkNumberOfBuffers(2);
}

TEST_F(EmitPhysicalOperatorTest, CoalescingEmitPassesSpilledOutputThrough)
{
    expectSpilledOutputKeepsRange(createCoalescingUUT(std::chrono::hours(1)));
    EXPECT_TRUE(callbacks.rlock()->empty());
}

/// A sequence that is closed twice as complete fails debug builds, with and without coalescing.
TEST_F(EmitPhysicalOperatorTest, CompletingASequenceTwiceFails)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const auto complete = [this] { return createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL, true); };

    const auto emit = createUUT();
    runTask(emit, complete(), 1);
    EXPECT_DEATH_DEBUG(runTask(emit, complete(), 1), "");

    /// More than half full, so the first output leaves the coalescer at once and only the emit can catch the second.
    constexpr uint32_t moreThanHalfFull = 100;
    const auto coalescing = createCoalescingUUT(std::chrono::hours(1));
    runTask(coalescing, complete(), moreThanHalfFull);
    EXPECT_DEATH_DEBUG(runTask(coalescing, complete(), moreThanHalfFull), "");
}

TEST_F(EmitPhysicalOperatorTest, CoalescingEmitFlushesHeldOutputsFromItsTimer)
{
    const auto emit = createCoalescingUUT(std::chrono::microseconds(1));
    runTask(emit, createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL, true), 3);
    checkNumberOfBuffers(0);
    ASSERT_EQ(callbacks.rlock()->size(), 1);

    /// The callback arms itself again until the deadline has passed.
    while (buffers.rlock()->empty())
    {
        ASSERT_EQ(callbacks.rlock()->size(), 1);
        runCallbacks();
    }
    checkNumberOfBuffers(1);
    checkBufferAt(0, SequenceNumber::INITIAL, ChunkNumber::INITIAL, true, INITIAL<OriginId>, 3);
    EXPECT_TRUE(callbacks.rlock()->empty());
}

}

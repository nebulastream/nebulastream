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
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/BufferRef/RowTupleBufferRef.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/FracturedNumber.hpp>
#include <Sequencing/SequenceRange.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <folly/Synchronized.h>
#include <gtest/gtest.h>

#include <BaseUnitTest.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

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

        TupleBuffer& pinBuffer(TupleBuffer&& tupleBuffer) override
        {
            pinnedBuffers.emplace_back(std::make_unique<TupleBuffer>(tupleBuffer));
            return *pinnedBuffers.back();
        }

        [[nodiscard]] WorkerThreadId getId() const override { return INITIAL<WorkerThreadId>; }

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

        MockedPipelineContext(folly::Synchronized<std::vector<TupleBuffer>>& buffers, std::shared_ptr<BufferManager> bufferManager)
            : buffers(buffers), bufferManager(std::move(bufferManager))
        {
        }

        void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { INVARIANT(false, "This function should not be called"); }

        ///NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members) lifetime is ensured by the `run` method.
        folly::Synchronized<std::vector<TupleBuffer>>& buffers;
        std::shared_ptr<BufferManager> bufferManager;
        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>* operatorHandlers = nullptr;
        /// We want to ensure that the address of the TupleBuffer is always the same. If we would simply store the object directly in the vector,
        /// the address might change as the vector might be resized and thus, the object have a different address.
        std::vector<std::unique_ptr<TupleBuffer>> pinnedBuffers;
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
        auto bufferRef = LowerSchemaProvider::lowerSchema(512, schema, MemoryLayoutType::ROW_LAYOUT);
        EmitPhysicalOperator emit{OperatorHandlerId(0), std::move(bufferRef)};
        handlers.insert_or_assign(OperatorHandlerId(0), std::make_shared<EmitOperatorHandler>());
        return emit;
    }

    void run(const std::function<void(ExecutionContext&, RecordBuffer&)>& test, TupleBuffer buffer)
    {
        MockedPipelineContext pec{buffers, bm};
        pec.setOperatorHandlers(handlers);
        Arena arena(bm);

        ExecutionContext executionContext{&pec, &arena};
        executionContext.originId = buffer.getOriginId();

        /// Set up the sequenceRange in the ExecutionContext from the buffer's SequenceRange
        auto& range = buffer.getSequenceRange();
        executionContext.sequenceRange = RecordBuffer::SequenceRangeRef::from(&range);

        RecordBuffer recordBuffer(std::addressof(buffer));
        test(executionContext, recordBuffer);
    }

    ///NOLINTBEGIN(fuchsia-default-arguments-declarations)

    void checkNumberOfBuffers(size_t numberOfBuffers, std::source_location location = std::source_location::current())
    {
        const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "checkNumberOfBuffers");
        EXPECT_EQ(buffers.rlock()->size(), numberOfBuffers) << fmt::format("expects {} buffers to be emitted", numberOfBuffers);
    }

    void checkAllBuffersHaveValidRange(std::source_location location = std::source_location::current())
    {
        const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "checkAllBuffersHaveValidRange");
        for (const auto& buffer : *buffers.rlock())
        {
            EXPECT_TRUE(buffer.getSequenceRange().isValid()) << "Output buffer should have a valid SequenceRange";
        }
    }

    void checkForDups(std::source_location location = std::source_location::current())
    {
        const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "checkForDups");
        auto uniqueRanges = (*buffers.rlock()) | std::views::transform([](const auto& buffer) { return buffer.getSequenceRange(); })
            | std::ranges::to<std::set>();

        EXPECT_EQ(buffers.rlock()->size(), uniqueRanges.size()) << "Received duplicate sequence ranges";
    }

    TupleBuffer createBuffer(uint64_t sequence, OriginId originId = INITIAL<OriginId>, size_t numberOfTuples = 0) const
    {
        auto buffer = bm->getBufferBlocking();
        buffer.setNumberOfTuples(numberOfTuples);
        buffer.setSequenceRange(SequenceRange(FracturedNumber(sequence), FracturedNumber(sequence + 1)));
        buffer.setOriginId(originId);
        return buffer;
    }

    ///NOLINTEND(fuchsia-default-arguments-declarations)

    void reset() { buffers.wlock()->clear(); }

    folly::Synchronized<std::vector<TupleBuffer>> buffers;
    std::shared_ptr<BufferManager> bm = BufferManager::create(512, 100000);
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;

    std::random_device rd;
};

TEST_F(EmitPhysicalOperatorTest, BasicTest)
{
    auto buffer = createBuffer(1);
    EmitPhysicalOperator emit = createUUT();

    run(
        [&](auto& executionContext, auto& recordBuffer)
        {
            emit.open(executionContext, recordBuffer);
            emit.close(executionContext, recordBuffer);
        },
        buffer);

    checkNumberOfBuffers(1);
    checkAllBuffersHaveValidRange();
    checkForDups();
}

TEST_F(EmitPhysicalOperatorTest, MultipleInputBuffersTest)
{
    std::vector<TupleBuffer> inputBuffers;
    for (size_t i = 0; i < 5; ++i)
    {
        inputBuffers.emplace_back(createBuffer(1 + i));
    }

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
    checkAllBuffersHaveValidRange();
    checkForDups();
}

TEST_F(EmitPhysicalOperatorTest, FracturesOnBufferFull)
{
    /// Buffer is 512 bytes, UINT32 is 4 bytes → 128 records per buffer.
    /// Writing 300 records from a single input should produce 3 output buffers
    /// with fractured depth-2 ranges: [{1,0},{1,1}), [{1,1},{1,2}), [{1,2},{2,0})
    constexpr uint64_t recordsPerBuffer = 512 / sizeof(uint32_t);
    constexpr uint64_t totalRecords = recordsPerBuffer * 2 + recordsPerBuffer / 2;
    auto buffer = createBuffer(1);
    EmitPhysicalOperator emit = createUUT();

    run(
        [&](auto& executionContext, auto& recordBuffer)
        {
            emit.open(executionContext, recordBuffer);
            for (uint64_t i = 0; i < totalRecords; ++i)
            {
                Record dummyRecord;
                dummyRecord.write("A_FIELD", nautilus::val<uint32_t>(42));
                emit.execute(executionContext, dummyRecord);
            }
            emit.close(executionContext, recordBuffer);
        },
        buffer);

    checkNumberOfBuffers(3);
    checkAllBuffersHaveValidRange();
    checkForDups();

    /// Verify the output ranges form a contiguous cover of depth-2
    auto locked = buffers.rlock();
    std::vector<SequenceRange> outputRanges;
    for (const auto& buf : *locked)
    {
        outputRanges.push_back(buf.getSequenceRange());
    }
    std::ranges::sort(outputRanges);

    /// All ranges should be depth-2
    for (const auto& r : outputRanges)
    {
        EXPECT_EQ(r.start.depth(), 2) << "Expected depth-2 output range, got " << r;
    }
    /// First should start at {1,0}, last should end at {2,0}
    EXPECT_EQ(outputRanges.front().start, FracturedNumber(std::vector<uint64_t>{1, 0}));
    EXPECT_EQ(outputRanges.back().end, FracturedNumber(std::vector<uint64_t>{2, 0}));
    /// Adjacent ranges should be contiguous
    for (size_t i = 1; i < outputRanges.size(); ++i)
    {
        EXPECT_EQ(outputRanges[i - 1].end, outputRanges[i].start) << "Gap between " << outputRanges[i - 1] << " and " << outputRanges[i];
    }
}

TEST_F(EmitPhysicalOperatorTest, SingleOutputNoFracture)
{
    /// A single input producing a single output should still get a depth-2 range: [{1,0},{2,0})
    auto buffer = createBuffer(1);
    EmitPhysicalOperator emit = createUUT();

    run(
        [&](auto& executionContext, auto& recordBuffer)
        {
            emit.open(executionContext, recordBuffer);
            emit.close(executionContext, recordBuffer);
        },
        buffer);

    checkNumberOfBuffers(1);
    checkAllBuffersHaveValidRange();

    auto locked = buffers.rlock();
    const auto& range = locked->front().getSequenceRange();
    EXPECT_EQ(range.start.depth(), 2);
    EXPECT_EQ(range.start, FracturedNumber(std::vector<uint64_t>{1, 0}));
    EXPECT_EQ(range.end, FracturedNumber(std::vector<uint64_t>{2, 0}));
}
}

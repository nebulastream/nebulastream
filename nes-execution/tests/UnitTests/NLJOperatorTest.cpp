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


#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalStreamJoinBuildOperator.hpp>
#include <QueryCompiler/Phases/Translations/DefaultPhysicalOperatorProvider.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Util/TestTupleBuffer.hpp>


#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>
namespace NES
{
class NLJTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("NLJTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup NLJTest test class.");
    }

    constexpr static size_t PAGE_SIZE = 8192;
    constexpr static size_t WINDOW_SIZE_IN_MS = 1000;
    constexpr static size_t WINDOW_SLIDE_IN_MS = 1000;

    Runtime::Execution::PipelineExecutionContextPtr noOpPipelineExecutionContext()
    {
        auto bm = Memory::BufferManager::create();
        return std::make_shared<Runtime::Execution::PipelineExecutionContext>(
            PipelineId(1), QueryId(1), bm, 1, [](auto, auto) {}, [](auto) {}, std::vector<Runtime::Execution::OperatorHandlerPtr>{});
    }

    template <typename... Ts>
    void insertIntoRightSlice(Runtime::Execution::StreamJoinSlice& slice, SchemaPtr schema, Ts... args)
    {
        auto& nljSlice = dynamic_cast<Runtime::Execution::NLJSlice&>(slice);
        auto pvector = nljSlice.getPagedVectorRefRight(WorkerThreadId(1));
        auto buffer = pvector->getLastPage();
        Memory::MemoryLayouts::TestTupleBuffer testBuffer(Memory::MemoryLayouts::RowLayout::create(schema, PAGE_SIZE), buffer);
        testBuffer.pushRecordToBuffer(std::tuple<Ts...>{args...});
        auto index = pvector->getBufferPosForEntry(0);
    }
    template <typename... Ts>
    void insertIntoLeftSlice(Runtime::Execution::StreamJoinSlice& slice, SchemaPtr schema, Ts... args)
    {
        auto& nljSlice = dynamic_cast<Runtime::Execution::NLJSlice&>(slice);
        auto pvector = nljSlice.getPagedVectorRefLeft(WorkerThreadId(1));
        auto buffer = pvector->getLastPage();
        Memory::MemoryLayouts::TestTupleBuffer testBuffer(Memory::MemoryLayouts::RowLayout::create(schema, PAGE_SIZE), buffer);
        testBuffer.pushRecordToBuffer(std::tuple<Ts...>{args...});
        auto index = pvector->getBufferPosForEntry(0);
    }
};


TEST_F(NLJTest, Test1)
{
    auto schemaLeft = Schema::create()->addField("id", BasicType::UINT64)->addField("timestamp", BasicType::UINT64);
    auto schemaRight = Schema::create()->addField("id", BasicType::UINT64)->addField("timestamp", BasicType::UINT64);

    auto leftMemoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(PAGE_SIZE, schemaLeft);
    leftMemoryProvider->getMemoryLayoutPtr()->setKeyFieldNames({"id"});
    auto rightMemoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(PAGE_SIZE, schemaRight);
    rightMemoryProvider->getMemoryLayoutPtr()->setKeyFieldNames({"id"});

    NES_DEBUG(
        "Created left and right memory provider for StreamJoin with page size {}--{}",
        leftMemoryProvider->getMemoryLayoutPtr()->getBufferSize(),
        rightMemoryProvider->getMemoryLayoutPtr()->getBufferSize());

    auto handler = std::make_shared<Runtime::Execution::Operators::NLJOperatorHandler>(
        std::vector{OriginId(1), OriginId(2)}, OriginId(3), WINDOW_SIZE_IN_MS, WINDOW_SLIDE_IN_MS, leftMemoryProvider, rightMemoryProvider);


    auto pec = noOpPipelineExecutionContext();
    handler->setBufferProvider(pec->getBufferManager());
    handler->setWorkerThreads(pec->getNumberOfWorkerThreads());
    handler->start(pec, 0);

    auto slice = handler->getSliceByTimestampOrCreateIt(Timestamp(500));
    insertIntoLeftSlice<uint64_t, uint64_t>(*slice, schemaLeft, 32, 3);
    insertIntoRightSlice<uint64_t, uint64_t>(*slice, schemaLeft, 32, 3);

    handler->checkAndTriggerWindows(
        Runtime::Execution::BufferMetaData(500, SequenceData(SequenceNumber(1), ChunkNumber(1), true), OriginId(1)), pec.get());
    handler->checkAndTriggerWindows(
        Runtime::Execution::BufferMetaData(600, SequenceData(SequenceNumber(1), ChunkNumber(1), true), OriginId(2)), pec.get());
    handler->checkAndTriggerWindows(
        Runtime::Execution::BufferMetaData(1000, SequenceData(SequenceNumber(2), ChunkNumber(1), true), OriginId(1)), pec.get());
    handler->checkAndTriggerWindows(
        Runtime::Execution::BufferMetaData(1010, SequenceData(SequenceNumber(2), ChunkNumber(1), true), OriginId(2)), pec.get());

    handler->garbageCollectSlicesAndWindows(
        Runtime::Execution::BufferMetaData(1010, SequenceData(SequenceNumber(2), ChunkNumber(1), true), OriginId(2)));
    handler->stop(Runtime::QueryTerminationType::HardStop, pec);
}

}

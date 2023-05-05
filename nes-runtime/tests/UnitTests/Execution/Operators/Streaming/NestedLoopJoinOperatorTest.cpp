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
#include <API/Schema.hpp>
#include <API/AttributeField.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJBuild.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJSink.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <NesBaseTest.hpp>
#include <Execution/Operators/ExecutionContext.hpp>


namespace NES::Runtime::Execution {


class MockPiplineExecutionContext : public PipelineExecutionContext {
  public:
    MockPiplineExecutionContext(OperatorHandlerPtr nljOperatorHandler) :
                                    PipelineExecutionContext(
                                        -1,// mock pipeline id
                                        0, // mock query id
                                        nullptr,
                                        1, {}, {},
                                        {nljOperatorHandler}) {}
};

class NestedLoopJoinOperatorTest : public Testing::NESBaseTest {
  public:
    std::shared_ptr<Runtime::BufferManager> bm;
    std::vector<TupleBuffer> emittedBuffers;
    WorkerContextPtr workerContext;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NestedLoopJoinOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NestedLoopJoinOperatorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NESBaseTest::SetUp();
        NES_INFO("Setup NestedLoopJoinOperatorTest test case.");
        leftSchema = Schema::create()
                              ->addField("id", BasicType::UINT64)
                              ->addField("value_left", BasicType::UINT64)
                              ->addField("ts", BasicType::UINT64);
        rightSchema = Schema::create()
                               ->addField("id", BasicType::UINT64)
                               ->addField("value_right", BasicType::UINT64)
                               ->addField("ts", BasicType::UINT64);

        windowSize = 1000;
        handlerIndex = 0;
        windowIdentifier = windowSize - 1;
    }

    std::vector<Record> createRandomRecords(uint64_t numberOfRecords, bool isLeftSide) {
        std::vector<Record> retVector;
        auto schema = isLeftSide ? leftSchema : rightSchema;

        hier weiter machen mit dem schrieben des tests
        for (auto i = 0UL; i < numberOfRecords; ++i) {
            retVector.emplace_back(Record({{schema->get(0)->getName(), Value<UInt64>(0UL)},
                                           {schema->get(1)->getName(), Value<UInt64>()},
                                           {schema->get(2)->getName(), Value<UInt64>(i)}}));
        }

        return retVector;
    }

    SchemaPtr leftSchema;
    SchemaPtr rightSchema;
    uint64_t windowSize;
    uint64_t handlerIndex;
    uint64_t noWorkerThreads;
    uint64_t windowIdentifier;
};

TEST_F(NestedLoopJoinOperatorTest, joinBuildSimpleTestOneRecord) {
//    Each test does it for the left and right side

    auto joinFieldnameLeft = leftSchema->get(1)->getName();
    auto joinFieldnameRight = rightSchema->get(1)->getName();
    auto timestampFieldLeft = leftSchema->get(2)->getName();
    auto timestampFieldRight = leftSchema->get(2)->getName();

    auto nljOperatorHandler = std::make_shared<Operators::NLJOperatorHandler>(windowSize, leftSchema, rightSchema,
                                                                              joinFieldnameLeft, joinFieldnameRight);

    auto nljBuildLeft = std::make_shared<Operators::NLJBuild>(handlerIndex, leftSchema, joinFieldnameLeft, timestampFieldLeft,
                                                              /*isLeftSide*/ true);
    auto nljBuildRight = std::make_shared<Operators::NLJBuild>(handlerIndex, rightSchema, joinFieldnameRight, timestampFieldRight,
                                                               /*isLeftSide*/ false);

    MockPiplineExecutionContext pipelineContext(nljOperatorHandler);
    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));

    nljBuildLeft->setup(executionContext);
    nljBuildRight->setup(executionContext);

    Record leftRecord({{leftSchema->get(0)->getName(), Value<UInt64>(0UL)},
                       {leftSchema->get(1)->getName(), Value<UInt64>(10UL)},
                       {leftSchema->get(2)->getName(), Value<UInt64>(0UL)}});
    Record rightRecord({{rightSchema->get(0)->getName(), Value<UInt64>(0UL)},
                        {rightSchema->get(1)->getName(), Value<UInt64>(42UL)},
                        {rightSchema->get(2)->getName(), Value<UInt64>(0UL)}});

    nljBuildLeft->execute(executionContext, leftRecord);
    nljBuildRight->execute(executionContext, rightRecord);

    ASSERT_EQ(nljOperatorHandler->getNumberOfTuplesInWindow(windowIdentifier, /*isLeftSide*/ true), 1);
    ASSERT_EQ(nljOperatorHandler->getNumberOfTuplesInWindow(windowIdentifier, /*isLeftSide*/ false), 1);

    auto memoryProviderLeft = MemoryProvider::MemoryProvider::createMemoryProvider(leftSchema->getSchemaSizeInBytes(),
                                                                                   leftSchema);
    auto memoryProviderRight = MemoryProvider::MemoryProvider::createMemoryProvider(rightSchema->getSchemaSizeInBytes(),
                                                                                    rightSchema);
    auto startOfTupleLeft = Value<MemRef>((int8_t*) nljOperatorHandler->getFirstTuple(windowIdentifier, /*isLeftSide*/ true));
    auto startOfTupleRight = Value<MemRef>((int8_t*) nljOperatorHandler->getFirstTuple(windowIdentifier, /*isLeftSide*/ false));
    Value<UInt64> zeroValue(0UL);

    auto readRecordLeft = memoryProviderLeft->read({}, startOfTupleLeft, zeroValue);
    auto readRecordRight = memoryProviderRight->read({}, startOfTupleRight, zeroValue);

    for (auto& field : leftSchema->fields) {
        ASSERT_EQ(readRecordLeft.read(field->getName()), leftRecord.read(field->getName()));
    }

    for (auto& field : rightSchema->fields) {
        ASSERT_EQ(readRecordRight.read(field->getName()), rightRecord.read(field->getName()));
    }
}

TEST_F(NestedLoopJoinOperatorTest, joinBuildSimpleTestMultipleRecords) {

}

TEST_F(NestedLoopJoinOperatorTest, joinBuildSimpleTestMultipleWindows) {

}

TEST_F(NestedLoopJoinOperatorTest, joinSinkSimpleTestOneRecord) {

}

TEST_F(NestedLoopJoinOperatorTest, joinSinkSimpleTestMultipleRecords) {

}

TEST_F(NestedLoopJoinOperatorTest, joinSinkSimpleTestMultipleWindows) {

}

} // namespace NES::Runtime::Execution
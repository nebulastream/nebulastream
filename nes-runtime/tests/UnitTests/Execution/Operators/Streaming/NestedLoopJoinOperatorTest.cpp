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
#include <Common/DataTypes/BasicTypes.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJBuild.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJSink.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <NesBaseTest.hpp>
#include <Execution/Operators/ExecutionContext.hpp>


namespace NES::Runtime::Execution {

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
        bm = std::make_shared<Runtime::BufferManager>();
        workerContext = std::make_shared<Runtime::WorkerContext>(/*workerId*/ 0, bm, /*numberOfBuffersPerWorker*/ 64);
    }
};

TEST_F(NestedLoopJoinOperatorTest, joinBuildSimpleTestOneRecord) {
    Each test does it for the left and right side
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
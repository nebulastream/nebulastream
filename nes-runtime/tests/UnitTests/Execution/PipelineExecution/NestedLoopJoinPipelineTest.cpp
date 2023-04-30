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
#include <Exceptions/ErrorListener.hpp>
#include <Execution/Pipelines/ExecutablePipelineProvider.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJBuild.hpp>
#include <Execution/RecordBuffer.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <string>

namespace NES::Runtime::Execution {

class NestedLoopJoinMockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    NestedLoopJoinMockedPipelineExecutionContext(BufferManagerPtr bufferManager,
                                                 uint64_t noWorkerThreads,
                                                 OperatorHandlerPtr nljOpHandler,
                                                 uint64_t pipelineId)
        : PipelineExecutionContext(
            pipelineId,// mock pipeline id
            1,         // mock query id
            bufferManager,
            noWorkerThreads,
            [this](TupleBuffer& buffer, Runtime::WorkerContextRef) {
                this->emittedBuffers.emplace_back(std::move(buffer));
            },
            [this](TupleBuffer& buffer) {
                this->emittedBuffers.emplace_back(std::move(buffer));
            },
            {nljOpHandler}){};

    std::vector<Runtime::TupleBuffer> emittedBuffers;
};

class NestedLoopJoinPipelineTest : public Testing::NESBaseTest, public AbstractPipelineExecutionTest {

  public:
    ExecutablePipelineProvider* provider;
    BufferManagerPtr bufferManager;
    WorkerContextPtr workerContext;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NestedLoopJoinPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NestedLoopJoinPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NESBaseTest::SetUp();
        NES_INFO("Setup NestedLoopJoinPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bufferManager = std::make_shared<Runtime::BufferManager>();
        workerContext = std::make_shared<WorkerContext>(0, bufferManager, 100);
    }
};


TEST_P(NestedLoopJoinPipelineTest, hashJoinPipeline) {

}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        NestedLoopJoinPipelineTest,
                        ::testing::Values("PipelineInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<NestedLoopJoinPipelineTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution
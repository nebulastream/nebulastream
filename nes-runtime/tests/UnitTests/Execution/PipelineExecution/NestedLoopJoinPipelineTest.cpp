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
#include <Exceptions/ErrorListener.hpp>
#include <Execution/Pipelines/ExecutablePipelineProvider.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJBuild.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJSink.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
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
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Scan.hpp>
#include <fstream>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJSink.hpp>


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
        Nautilus::CompilationOptions options;

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

        bool checkIfNLJWorks(const std::string& fileNameBuffersLeft, const std::string& fileNameBuffersRight,
                             const std::string& fileNameBuffersSink, uint64_t windowSize,
                             const SchemaPtr leftSchema, const SchemaPtr rightSchema, const SchemaPtr joinSchema,
                             const std::string& joinFieldNameLeft, const std::string& joinFieldNameRight,
                             const std::string& timeStampField) {

            bool nljWorks = true;

            // Creating the input left and right buffers and the expected output buffer
            auto leftBuffers = Util::createBuffersFromCSVFile(fileNameBuffersLeft, leftSchema, bufferManager);
            auto rightBuffers = Util::createBuffersFromCSVFile(fileNameBuffersRight, rightSchema, bufferManager);
            auto expectedSinkBuffers = Util::createBuffersFromCSVFile(fileNameBuffersSink, joinSchema, bufferManager);

            // Creating the scan operator
            auto memoryLayoutLeft = Runtime::MemoryLayouts::RowLayout::create(leftSchema, bufferManager->getBufferSize());
            auto memoryLayoutRight = Runtime::MemoryLayouts::RowLayout::create(rightSchema, bufferManager->getBufferSize());
            auto memoryLayoutJoined = Runtime::MemoryLayouts::RowLayout::create(joinSchema, bufferManager->getBufferSize());

            auto scanMemoryProviderLeft = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayoutLeft);
            auto scanMemoryProviderRight = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayoutRight);

            auto scanOperatorLeft = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderLeft));
            auto scanOperatorRight = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderRight));

            // Creating the left, right and sink NLJ operator
            auto handlerIndex = 0;
            auto nljBuildLeft = std::make_shared<Operators::NLJBuild>(handlerIndex, leftSchema, joinFieldNameLeft,
                                                                      timeStampField, /*isLeftSide*/ true);
            auto nljBuildRight = std::make_shared<Operators::NLJBuild>(handlerIndex, rightSchema, joinFieldNameRight,
                                                                       timeStampField, /*isLeftSide*/ false);
            auto nljSink = std::make_shared<Operators::NLJSink>(handlerIndex, leftSchema, rightSchema, joinSchema,
                                                                joinFieldNameLeft, joinFieldNameRight);

            // Creating the NLJ operator handler
            auto nljOpHandler = std::make_shared<Operators::NLJOperatorHandler>(windowSize, leftSchema, rightSchema,
                                                                                joinFieldNameLeft, joinFieldNameRight);

            // Building the pipeline
            auto pipelineBuildLeft = std::make_shared<PhysicalOperatorPipeline>();
            auto pipelineBuildRight = std::make_shared<PhysicalOperatorPipeline>();
            auto pipelineSink = std::make_shared<PhysicalOperatorPipeline>();

            scanOperatorLeft->setChild(nljBuildLeft);
            scanOperatorRight->setChild(nljBuildRight);
            pipelineBuildLeft->setRootOperator(scanOperatorLeft);
            pipelineBuildRight->setRootOperator(scanOperatorRight);
            pipelineSink->setRootOperator(nljSink);

            auto curPipelineId = 0;
            auto noWorkerThreads = 1;
            auto pipelineExecCtxLeft =
                    NestedLoopJoinMockedPipelineExecutionContext(bufferManager, noWorkerThreads, nljOpHandler, curPipelineId++);
            auto pipelineExecCtxRight =
                    NestedLoopJoinMockedPipelineExecutionContext(bufferManager, noWorkerThreads, nljOpHandler, curPipelineId++);
            auto pipelineExecCtxSink =
                    NestedLoopJoinMockedPipelineExecutionContext(bufferManager, noWorkerThreads, nljOpHandler, curPipelineId++);

            auto executablePipelineLeft = provider->create(pipelineBuildLeft, options);
            auto executablePipelineRight = provider->create(pipelineBuildRight, options);
            auto executablePipelineSink = provider->create(pipelineSink, options);

            nljWorks = nljWorks && (executablePipelineLeft->setup(pipelineExecCtxLeft) == 0);
            nljWorks = nljWorks && (executablePipelineRight->setup(pipelineExecCtxRight) == 0);
            nljWorks = nljWorks && (executablePipelineSink->setup(pipelineExecCtxSink) == 0);

            // Executing left and right buffers
            for (auto buffer : leftBuffers) {
                executablePipelineLeft->execute(buffer, pipelineExecCtxLeft, *workerContext);
            }
            for (auto buffer : rightBuffers) {
                executablePipelineRight->execute(buffer, pipelineExecCtxRight, *workerContext);
            }

            // Assure that at least one buffer has been emitted
            nljWorks = nljWorks && (pipelineExecCtxLeft.emittedBuffers.size() > 0 ||
                                    pipelineExecCtxRight.emittedBuffers.size() > 0);

            // Executing sink buffers
            std::vector<Runtime::TupleBuffer> buildEmittedBuffers(pipelineExecCtxLeft.emittedBuffers);
            buildEmittedBuffers.insert(buildEmittedBuffers.end(),
                                       pipelineExecCtxRight.emittedBuffers.begin(),
                                       pipelineExecCtxRight.emittedBuffers.end());
            for (auto buf : buildEmittedBuffers) {
                executablePipelineSink->execute(buf, pipelineExecCtxSink, *workerContext);
            }

            nljWorks = nljWorks && (pipelineExecCtxSink.emittedBuffers.size() == 20);
            auto resultBuffer = Util::mergeBuffers(pipelineExecCtxSink.emittedBuffers, joinSchema, bufferManager);

            NES_DEBUG("resultBuffer: \n" << Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
            NES_DEBUG("expectedSinkBuffer: \n" << Util::printTupleBufferAsCSV(expectedSinkBuffers[0], joinSchema));


            nljWorks = nljWorks && (resultBuffer.getNumberOfTuples() == expectedSinkBuffers[0].getNumberOfTuples());
            nljWorks = nljWorks && (Util::checkIfBuffersAreEqual(resultBuffer,
                                                                 expectedSinkBuffers[0],
                                                                 joinSchema->getSchemaSizeInBytes()));
            return nljWorks;
        }
    };



    TEST_P(NestedLoopJoinPipelineTest, nljSimplePipeline) {
        const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                    ->addField("id", BasicType::UINT64)
                                    ->addField("value", BasicType::UINT64)
                                    ->addField("timestamp", BasicType::UINT64);

        const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                    ->addField("id", BasicType::UINT64)
                                    ->addField("value", BasicType::UINT64)
                                    ->addField("timestamp", BasicType::UINT64);

        const auto joinFieldNameRight = rightSchema->get(1)->getName();
        const auto joinFieldNameLeft = leftSchema->get(1)->getName();


        // This is necessary as, the timestamp field has to be the same for the left and right #3407
        EXPECT_EQ(leftSchema->get(2)->getName(), rightSchema->get(2)->getName());
        auto timeStampField = leftSchema->get(2)->getName();

        EXPECT_EQ(leftSchema->getLayoutType(), rightSchema->getLayoutType());
        const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);

        // read values from csv file into one buffer for each join side and for one window
        const auto windowSize = 1000UL;
        const std::string fileNameBuffersLeft(std::string(TEST_DATA_DIRECTORY) + "window.csv");
        const std::string fileNameBuffersRight(std::string(TEST_DATA_DIRECTORY) + "window.csv");
        const std::string fileNameBuffersSink(std::string(TEST_DATA_DIRECTORY) + "window_sink.csv");

        ASSERT_TRUE(checkIfNLJWorks(fileNameBuffersLeft, fileNameBuffersRight, fileNameBuffersSink, windowSize,
                                    leftSchema, rightSchema, joinSchema, joinFieldNameLeft, joinFieldNameRight,
                                    timeStampField));
    }

    TEST_P(NestedLoopJoinPipelineTest, nljSimplePipelineDifferentInput) {
        const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                    ->addField("id", BasicType::UINT64)
                                    ->addField("value", BasicType::UINT64)
                                    ->addField("timestamp", BasicType::UINT64);

        const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                    ->addField("id", BasicType::UINT64)
                                    ->addField("value", BasicType::UINT64)
                                    ->addField("timestamp", BasicType::UINT64);

        const auto joinFieldNameRight = rightSchema->get(1)->getName();
        const auto joinFieldNameLeft = leftSchema->get(1)->getName();


        // This is necessary as, the timestamp field has to be the same for the left and right #3407
        EXPECT_EQ(leftSchema->get(2)->getName(), rightSchema->get(2)->getName());
        auto timeStampField = leftSchema->get(2)->getName();

        EXPECT_EQ(leftSchema->getLayoutType(), rightSchema->getLayoutType());
        const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);

        // read values from csv file into one buffer for each join side and for one window
        const auto windowSize = 1000UL;
        const std::string fileNameBuffersLeft(std::string(TEST_DATA_DIRECTORY) + "window.csv");
        const std::string fileNameBuffersRight(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
        const std::string fileNameBuffersSink(std::string(TEST_DATA_DIRECTORY) + "window_sink2.csv");

        ASSERT_TRUE(checkIfNLJWorks(fileNameBuffersLeft, fileNameBuffersRight, fileNameBuffersSink, windowSize,
                                    leftSchema, rightSchema, joinSchema, joinFieldNameLeft, joinFieldNameRight,
                                    timeStampField));
    }

    TEST_P(NestedLoopJoinPipelineTest, nljSimplePipelineDifferentNumberOfAttributes) {
        const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                ->addField("id", BasicType::UINT64)
                ->addField("value", BasicType::UINT64)
                ->addField("timestamp", BasicType::UINT64);

        const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                ->addField("id", BasicType::UINT64)
                ->addField("timestamp", BasicType::UINT64);

        const auto joinFieldNameLeft = "value";
        const auto joinFieldNameRight = "id";

        // This is necessary as, the timestamp field has to be the same for the left and right #3407
        EXPECT_EQ(leftSchema->get(2)->getName(), rightSchema->get(1)->getName());
        auto timeStampField = leftSchema->get(2)->getName();

        EXPECT_EQ(leftSchema->getLayoutType(), rightSchema->getLayoutType());
        const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);

        // read values from csv file into one buffer for each join side and for one window
        const auto windowSize = 1000UL;
        const std::string fileNameBuffersLeft(std::string(TEST_DATA_DIRECTORY) + "window.csv");
        const std::string fileNameBuffersRight(std::string(TEST_DATA_DIRECTORY) + "window3.csv");
        const std::string fileNameBuffersSink(std::string(TEST_DATA_DIRECTORY) + "window_sink3.csv");

        ASSERT_TRUE(checkIfNLJWorks(fileNameBuffersLeft, fileNameBuffersRight, fileNameBuffersSink, windowSize,
                                    leftSchema, rightSchema, joinSchema, joinFieldNameLeft, joinFieldNameRight,
                                    timeStampField));
    }

    INSTANTIATE_TEST_CASE_P(nestedLoopJoinPipelineTest,
                            NestedLoopJoinPipelineTest,
                            ::testing::Values("PipelineInterpreter", "PipelineCompiler"),
                            [](const testing::TestParamInfo<NestedLoopJoinPipelineTest::ParamType>& info) {
                                return info.param;
                            });

}// namespace NES::Runtime::Execution
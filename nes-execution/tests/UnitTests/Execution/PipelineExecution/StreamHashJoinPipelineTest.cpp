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
#include <BaseIntegrationTest.hpp>
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJProbe.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/Slicing/HJBuildSlicing.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/Slicing/HJOperatorHandlerSlicing.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/Pipelines/ExecutablePipelineProvider.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/UtilityFunctions.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <cstring>
#include <gtest/gtest.h>
#include <string>

namespace NES::Runtime::Execution {

class HashJoinMockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    HashJoinMockedPipelineExecutionContext(BufferManagerPtr bufferManager,
                                           uint64_t noWorkerThreads,
                                           OperatorHandlerPtr hashJoinOpHandler,
                                           PipelineId pipelineId)
        : PipelineExecutionContext(
              pipelineId,              // mock pipeline id
              DecomposedQueryPlanId(1),// mock query id
              bufferManager,
              noWorkerThreads,
              [this](TupleBuffer& buffer, Runtime::WorkerContextRef) {
                  this->emittedBuffers.emplace_back(std::move(buffer));
              },
              [this](TupleBuffer& buffer) {
                  this->emittedBuffers.emplace_back(std::move(buffer));
              },
              {hashJoinOpHandler}){};

    std::vector<Runtime::TupleBuffer> emittedBuffers;
};

class HashJoinPipelineTest : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest {

  public:
    ExecutablePipelineProvider* provider;
    BufferManagerPtr bufferManager;
    WorkerContextPtr workerContext;
    Nautilus::CompilationOptions options;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("HashJoinPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup HashJoinPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        BaseUnitTest::SetUp();
        NES_INFO("Setup HashJoinPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bufferManager = std::make_shared<Runtime::BufferManager>();
        workerContext = std::make_shared<WorkerContext>(0, bufferManager, 100);
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        NES_INFO("Tear down HashJoinPipelineTest test case.");
        BaseUnitTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down HashJoinPipelineTest test class."); }

    bool checkIfHashJoinWorks(const std::string& fileNameBuffersLeft,
                              const std::string& fileNameBuffersRight,
                              const std::string& fileNameBuffersSink,
                              const uint64_t windowSize,
                              const uint64_t windowSlide,
                              const SchemaPtr leftSchema,
                              const SchemaPtr rightSchema,
                              const SchemaPtr joinSchema,
                              const std::string& joinFieldNameLeft,
                              const std::string& joinFieldNameRight,
                              const std::string& timeStampFieldLeft,
                              const std::string& timeStampFieldRight,
                              const std::string& windowStartFieldName,
                              const std::string& windowEndFieldName,
                              const std::string& windowKeyFieldName) {
        bool hashJoinWorks = true;

        // Creating the input left and right buffers and the expected output buffer
        auto originId = 0UL;
        auto leftBuffers =
            Util::createBuffersFromCSVFile(fileNameBuffersLeft, leftSchema, bufferManager, originId++, timeStampFieldLeft);
        auto rightBuffers =
            Util::createBuffersFromCSVFile(fileNameBuffersRight, rightSchema, bufferManager, originId++, timeStampFieldRight);
        auto expectedSinkBuffers = Util::createBuffersFromCSVFile(fileNameBuffersSink, joinSchema, bufferManager, originId++);
        NES_DEBUG("read file={}", fileNameBuffersSink);

        NES_DEBUG("leftBuffer: \n{}", Util::printTupleBufferAsCSV(leftBuffers[0], leftSchema));
        NES_DEBUG("rightBuffers: \n{}", Util::printTupleBufferAsCSV(rightBuffers[0], rightSchema));

        // Creating the scan (for build) and emit operator (for sink)
        auto memoryLayoutLeft = Runtime::MemoryLayouts::RowLayout::create(leftSchema, bufferManager->getBufferSize());
        auto memoryLayoutRight = Runtime::MemoryLayouts::RowLayout::create(rightSchema, bufferManager->getBufferSize());
        auto memoryLayoutJoined = Runtime::MemoryLayouts::RowLayout::create(joinSchema, bufferManager->getBufferSize());

        auto scanMemoryProviderLeft = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayoutLeft);
        auto scanMemoryProviderRight = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayoutRight);
        auto emitMemoryProviderSink = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayoutJoined);

        auto scanOperatorLeft = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderLeft));
        auto scanOperatorRight = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderRight));
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderSink));

        // Creating the left, right and sink hash join operator
        const auto handlerIndex = 0;
        const auto readTsFieldLeft = std::make_shared<Expressions::ReadFieldExpression>(timeStampFieldLeft);
        const auto readTsFieldRight = std::make_shared<Expressions::ReadFieldExpression>(timeStampFieldRight);
        const auto leftEntrySize = leftSchema->getSchemaSizeInBytes();
        const auto rightEntrySize = rightSchema->getSchemaSizeInBytes();

        auto joinBuildLeft = std::make_shared<Operators::HJBuildSlicing>(
            handlerIndex,
            leftSchema,
            joinFieldNameLeft,
            QueryCompilation::JoinBuildSideType::Left,
            leftSchema->getSchemaSizeInBytes(),
            std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldLeft),
            QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL,
            QueryCompilation::WindowingStrategy::SLICING);
        auto joinBuildRight = std::make_shared<Operators::HJBuildSlicing>(
            handlerIndex,
            rightSchema,
            joinFieldNameRight,
            QueryCompilation::JoinBuildSideType::Right,
            rightSchema->getSchemaSizeInBytes(),
            std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldRight),
            QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL,
            QueryCompilation::WindowingStrategy::SLICING);

        Operators::JoinSchema joinSchemaStruct(leftSchema,
                                               rightSchema,
                                               Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft));
        Operators::WindowMetaData windowMetaData(windowStartFieldName, windowEndFieldName, windowKeyFieldName);

        auto joinProbe = std::make_shared<Operators::HJProbe>(handlerIndex,
                                                              joinSchemaStruct,
                                                              joinFieldNameLeft,
                                                              joinFieldNameRight,
                                                              windowMetaData,
                                                              QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL,
                                                              QueryCompilation::WindowingStrategy::SLICING);

        // Creating the hash join operator
        std::vector<OriginId> originIds{INVALID_ORIGIN_ID, OriginId(1)};
        OriginId outputOriginId = OriginId(2);
        auto hashJoinOpHandler =
            Operators::HJOperatorHandlerSlicing::create(originIds,
                                                        outputOriginId,
                                                        windowSize,
                                                        windowSlide,
                                                        leftSchema,
                                                        rightSchema,
                                                        QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL,
                                                        NES::Configurations::DEFAULT_HASH_TOTAL_HASH_TABLE_SIZE,
                                                        NES::Configurations::DEFAULT_HASH_PREALLOC_PAGE_COUNT,
                                                        NES::Configurations::DEFAULT_HASH_PAGE_SIZE,
                                                        NES::Configurations::DEFAULT_HASH_NUM_PARTITIONS);

        // Building the pipeline
        auto pipelineBuildLeft = std::make_shared<PhysicalOperatorPipeline>();
        auto pipelineBuildRight = std::make_shared<PhysicalOperatorPipeline>();
        auto pipelineProbe = std::make_shared<PhysicalOperatorPipeline>();

        scanOperatorLeft->setChild(joinBuildLeft);
        scanOperatorRight->setChild(joinBuildRight);
        joinProbe->setChild(emitOperator);

        pipelineBuildLeft->setRootOperator(scanOperatorLeft);
        pipelineBuildRight->setRootOperator(scanOperatorRight);
        pipelineProbe->setRootOperator(joinProbe);

        auto curPipelineId = 0;
        auto noWorkerThreads = 1;
        auto pipelineExecCtxLeft = HashJoinMockedPipelineExecutionContext(bufferManager,
                                                                          noWorkerThreads,
                                                                          hashJoinOpHandler,
                                                                          PipelineId(curPipelineId++));
        auto pipelineExecCtxRight = HashJoinMockedPipelineExecutionContext(bufferManager,
                                                                           noWorkerThreads,
                                                                           hashJoinOpHandler,
                                                                           PipelineId(curPipelineId++));
        auto pipelineExecCtxSink = HashJoinMockedPipelineExecutionContext(bufferManager,
                                                                          noWorkerThreads,
                                                                          hashJoinOpHandler,
                                                                          PipelineId(curPipelineId++));

        auto executablePipelineLeft = provider->create(pipelineBuildLeft, options);
        auto executablePipelineRight = provider->create(pipelineBuildRight, options);
        auto executablePipelineSink = provider->create(pipelineProbe, options);

        hashJoinWorks = (executablePipelineLeft->setup(pipelineExecCtxLeft) == 0);
        hashJoinWorks = hashJoinWorks && (executablePipelineRight->setup(pipelineExecCtxRight) == 0);
        hashJoinWorks = hashJoinWorks && (executablePipelineSink->setup(pipelineExecCtxSink) == 0);

        // Executing left and right buffers
        for (auto buffer : leftBuffers) {
            executablePipelineLeft->execute(buffer, pipelineExecCtxLeft, *workerContext);
        }
        for (auto buffer : rightBuffers) {
            executablePipelineRight->execute(buffer, pipelineExecCtxRight, *workerContext);
        }
        hashJoinWorks = hashJoinWorks && (executablePipelineLeft->stop(pipelineExecCtxLeft) == 0);
        hashJoinWorks = hashJoinWorks && (executablePipelineRight->stop(pipelineExecCtxRight) == 0);

        // Assure that at least one buffer has been emitted
        hashJoinWorks =
            hashJoinWorks && (!pipelineExecCtxLeft.emittedBuffers.empty() || !pipelineExecCtxRight.emittedBuffers.empty());

        // Executing sink buffers
        std::vector<Runtime::TupleBuffer> buildEmittedBuffers(pipelineExecCtxLeft.emittedBuffers);
        buildEmittedBuffers.insert(buildEmittedBuffers.end(),
                                   pipelineExecCtxRight.emittedBuffers.begin(),
                                   pipelineExecCtxRight.emittedBuffers.end());
        for (auto buf : buildEmittedBuffers) {
            executablePipelineSink->execute(buf, pipelineExecCtxSink, *workerContext);
        }
        hashJoinWorks = hashJoinWorks && (executablePipelineSink->stop(pipelineExecCtxSink) == 0);

        auto resultBuffer = Util::mergeBuffers(pipelineExecCtxSink.emittedBuffers, joinSchema, bufferManager);
        NES_DEBUG("resultBuffer: \n{}", Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
        NES_DEBUG("expectedSinkBuffer: \n{}", Util::printTupleBufferAsCSV(expectedSinkBuffers[0], joinSchema));

        hashJoinWorks = hashJoinWorks && (resultBuffer.getNumberOfTuples() == expectedSinkBuffers[0].getNumberOfTuples());
        hashJoinWorks = hashJoinWorks
            && (Util::checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffers[0], joinSchema->getSchemaSizeInBytes()));
        return hashJoinWorks;
    }
};

TEST_P(HashJoinPipelineTest, simpleHashJoinPipeline) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("left$id", BasicType::UINT64)
                                ->addField("left$value", BasicType::UINT64)
                                ->addField("left$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("right$id", BasicType::UINT64)
                                 ->addField("right$value", BasicType::UINT64)
                                 ->addField("right$timestamp", BasicType::UINT64);

    const auto joinFieldNameRight = rightSchema->get(1)->getName();
    const auto joinFieldNameLeft = leftSchema->get(1)->getName();
    const auto timeStampFieldRight = rightSchema->get(2)->getName();
    const auto timeStampFieldLeft = leftSchema->get(2)->getName();

    EXPECT_EQ(leftSchema->getLayoutType(), rightSchema->getLayoutType());
    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);
    const auto windowStartFieldName = joinSchema->get(0)->getName();
    const auto windowEndFieldName = joinSchema->get(1)->getName();
    const auto windowKeyFieldName = joinSchema->get(2)->getName();

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffersLeft(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    const std::string fileNameBuffersRight(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    const std::string fileNameBuffersSink(std::string(TEST_DATA_DIRECTORY) + "window_sink2.csv");

    ASSERT_TRUE(checkIfHashJoinWorks(fileNameBuffersLeft,
                                     fileNameBuffersRight,
                                     fileNameBuffersSink,
                                     windowSize,
                                     windowSize,
                                     leftSchema,
                                     rightSchema,
                                     joinSchema,
                                     joinFieldNameLeft,
                                     joinFieldNameRight,
                                     timeStampFieldLeft,
                                     timeStampFieldRight,
                                     windowStartFieldName,
                                     windowEndFieldName,
                                     windowKeyFieldName));
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        HashJoinPipelineTest,
                        ::testing::Values("PipelineInterpreter",
                                          "PipelineCompiler"),//CPPPipelineCompiler is currently not working
                        [](const testing::TestParamInfo<HashJoinPipelineTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution
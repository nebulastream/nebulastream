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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/ExecutablePipelineProvider.hpp>
#include <Experimental/Benchmarking/MicroBenchmarkSchemas.hpp>
#include <Experimental/Operators/SynopsesOperator.hpp>
#include <Experimental/Synopses/AbstractSynopsis.hpp>
#include <Experimental/Synopses/Samples/RandomSampleWithoutReplacementOperatorHandler.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Common.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <vector>

namespace NES::Runtime::Execution::Operators {

class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    MockedPipelineExecutionContext(BufferManagerPtr bufferManager,
                                   uint64_t pipelineId,
                                   std::vector<Runtime::Execution::OperatorHandlerPtr> handlers = {})
        : PipelineExecutionContext(
            pipelineId,// mock pipeline id
            1,         // mock query id
            bufferManager,
            1, // noWorkerThreads
            [this](TupleBuffer& buffer, Runtime::WorkerContextRef) {
                this->emittedBuffers.emplace_back(std::move(buffer));
            },
            [this](TupleBuffer& buffer) {
                this->emittedBuffers.emplace_back(std::move(buffer));
            },
            handlers){};

    std::vector<Runtime::TupleBuffer> emittedBuffers;
};

class SynopsisPipelineTest : public Testing::BaseIntegrationTest, public ::testing::WithParamInterface<std::string> {
  public:
    ExecutablePipelineProvider* provider;
    BufferManagerPtr bufferManager;
    WorkerContextPtr workerContext;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SynopsisPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SynopsisPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        BaseIntegrationTest::SetUp();
        NES_INFO("Setup SynopsisPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bufferManager = std::make_shared<Runtime::BufferManager>();
        workerContext = std::make_shared<WorkerContext>(0, bufferManager, 256);
    }
};

Runtime::Execution::OperatorHandlerPtr createOperatorHandler(ASP::Parsing::Synopsis_Type type) {
    switch (type) {

        case ASP::Parsing::Synopsis_Type::SRSWoR: return std::make_shared<ASP::RandomSampleWithoutReplacementOperatorHandler>();
        case ASP::Parsing::Synopsis_Type::SRSWR:
        case ASP::Parsing::Synopsis_Type::Poisson:
        case ASP::Parsing::Synopsis_Type::Stratified:
        case ASP::Parsing::Synopsis_Type::ResSamp:
        case ASP::Parsing::Synopsis_Type::EqWdHist:
        case ASP::Parsing::Synopsis_Type::EqDpHist:
        case ASP::Parsing::Synopsis_Type::vOptHist:
        case ASP::Parsing::Synopsis_Type::HaarWave:
        case ASP::Parsing::Synopsis_Type::CM:
        case ASP::Parsing::Synopsis_Type::ECM:
        case ASP::Parsing::Synopsis_Type::HLL:
        case ASP::Parsing::Synopsis_Type::NONE: NES_NOT_IMPLEMENTED();
    }
}

std::pair<std::shared_ptr<Runtime::Execution::PhysicalOperatorPipeline>, std::shared_ptr<MockedPipelineExecutionContext>>
createExecutableSynopsisPipeline(ASP::AbstractSynopsesPtr synopsis, ASP::Parsing::Synopsis_Type type,
                                 SchemaPtr inputSchema, BufferManagerPtr bufferManager) {
    using namespace Runtime::Execution;

    // Scan Operator
    auto scanMemoryProvider = MemoryProvider::MemoryProvider::createMemoryProvider(bufferManager->getBufferSize(),inputSchema);
    auto scan = std::make_shared<Operators::Scan>(std::move(scanMemoryProvider));

    // Synopses Operator
    auto opHandlers = {createOperatorHandler(type)};
    auto synopsesOperator = std::make_shared<Operators::SynopsesOperator>(/*handlerIndex*/ 0, synopsis);
    auto pipelineExecutionContext = std::make_shared<MockedPipelineExecutionContext>(bufferManager, -1, opHandlers);

    // Build Pipeline
    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    scan->setChild(synopsesOperator);
    pipeline->setRootOperator(scan);

    return std::make_pair(pipeline, pipelineExecutionContext);
}

std::vector<Runtime::TupleBuffer> fillBuffer(BufferManager& bufferManager, SchemaPtr schema, size_t numberOfTuplesToProduce) {
    std::vector<Runtime::TupleBuffer> allBuffers;
    auto buffer = bufferManager.getBufferBlocking();
    auto tuplesPerBuffer = bufferManager.getBufferSize() / schema->getSchemaSizeInBytes();
    NES_ASSERT(tuplesPerBuffer > 0, "There has to fit at least tuple in the buffer.");

    for (auto i = 0UL; i < numberOfTuplesToProduce; ++i) {
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(buffer, schema);

        auto pos = dynamicBuffer.getNumberOfTuples();
        dynamicBuffer[pos][schema->get(0)->getName()].write((int64_t) (i % 10));
        dynamicBuffer[pos][schema->get(1)->getName()].write((int64_t) ((i % 10) + 100));
        dynamicBuffer[pos][schema->get(2)->getName()].write((uint64_t)(i));
        dynamicBuffer.setNumberOfTuples(pos + 1);

        if (dynamicBuffer.getNumberOfTuples() >= tuplesPerBuffer) {
            allBuffers.emplace_back(buffer);
            buffer = bufferManager.getBufferBlocking();
        }
    }

    if (buffer.getNumberOfTuples() > 0) {
        allBuffers.emplace_back(buffer);
    }

    return allBuffers;
}

/**
 * @brief Tests if the pipeline works by creating a sample with the same size as the number of tuples and then
 * checking if the correct result is returned
 */
TEST_P(SynopsisPipelineTest, simpleSynopsisPipelineTest) {
    auto fieldNameKey = "id";
    auto fieldNameAggregation = "value";
    auto fieldNameApproximate = "aggregation";
    auto timestampFieldName = "ts";
    auto numberOfTuplesToProduce = 1000;
    auto aggregationType = ASP::Parsing::Aggregation_Type::MIN;

    auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField(fieldNameKey, BasicType::INT64)
                                ->addField(fieldNameAggregation, BasicType::INT64)
                                ->addField(timestampFieldName, BasicType::UINT64);

    auto outputSchema = ASP::Benchmarking::getOutputSchemaFromTypeAndInputSchema(aggregationType, *inputSchema,
                                                                                 fieldNameKey, fieldNameAggregation,
                                                                                 fieldNameKey, fieldNameApproximate);

    auto allBuffers  = fillBuffer(*bufferManager, inputSchema, numberOfTuplesToProduce);
    auto synopsisType = ASP::Parsing::Synopsis_Type::SRSWoR;
    auto synopsisConfig = ASP::Parsing::SynopsisConfiguration::create(synopsisType, numberOfTuplesToProduce);
    auto aggregationConfig = ASP::Parsing::SynopsisAggregationConfig::create(aggregationType,
                                                                             fieldNameKey,
                                                                             fieldNameAggregation,
                                                                             fieldNameApproximate,
                                                                             timestampFieldName,
                                                                             inputSchema,
                                                                             outputSchema);
    auto synopsis = ASP::AbstractSynopsis::create(*synopsisConfig, aggregationConfig);

    auto [pipeline, pipelineContext] = createExecutableSynopsisPipeline(synopsis, synopsisType, inputSchema, bufferManager);
    auto workerContext = std::make_shared<Runtime::WorkerContext>(0, bufferManager, 100);
    auto executablePipeline = provider->create(pipeline, CompilationOptions());
    Runtime::Execution::ExecutionContext executionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                                          Nautilus::Value<Nautilus::MemRef>((int8_t*) pipelineContext.get()));

    auto handlerIndex = 0UL;
    executablePipeline->setup(*pipelineContext);
    for (auto& buffer : allBuffers) {
        executablePipeline->execute(buffer, *pipelineContext, *workerContext);
    }

    std::vector<Nautilus::Value<>> queryKeys = {Nautilus::Value<>(0_s64), Nautilus::Value<>(1_s64),
        Nautilus::Value<>(2_s64), Nautilus::Value<>(3_s64),
        Nautilus::Value<>(4_s64),
    };

    auto approximateBuffers = synopsis->getApproximateForKeys(handlerIndex, executionContext, queryKeys, bufferManager);
    ASSERT_EQ(approximateBuffers.size(), 1);
    ASSERT_EQ(approximateBuffers[0].getNumberOfTuples(), queryKeys.size());
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(approximateBuffers[0],
                                                                                              outputSchema);

    EXPECT_EQ(dynamicBuffer[0][fieldNameKey].read<int64_t>(), 0);
    EXPECT_EQ(dynamicBuffer[0][fieldNameApproximate].read<int64_t>(), 100);

    EXPECT_EQ(dynamicBuffer[1][fieldNameKey].read<int64_t>(), 1);
    EXPECT_EQ(dynamicBuffer[1][fieldNameApproximate].read<int64_t>(), 101);

    EXPECT_EQ(dynamicBuffer[2][fieldNameKey].read<int64_t>(), 2);
    EXPECT_EQ(dynamicBuffer[2][fieldNameApproximate].read<int64_t>(), 102);

    EXPECT_EQ(dynamicBuffer[3][fieldNameKey].read<int64_t>(), 3);
    EXPECT_EQ(dynamicBuffer[3][fieldNameApproximate].read<int64_t>(), 103);

    EXPECT_EQ(dynamicBuffer[4][fieldNameKey].read<int64_t>(), 4);
    EXPECT_EQ(dynamicBuffer[4][fieldNameApproximate].read<int64_t>(), 104);
}

INSTANTIATE_TEST_CASE_P(testSynopsisPipeline,
                        SynopsisPipelineTest,
                        ::testing::Values("PipelineInterpreter", "PipelineCompiler", "CPPPipelineCompiler"),
                        [](const testing::TestParamInfo<SynopsisPipelineTest::ParamType>& info) {
                            return info.param;
                        });

} // namespace NES::Runtime::Execution::Operators
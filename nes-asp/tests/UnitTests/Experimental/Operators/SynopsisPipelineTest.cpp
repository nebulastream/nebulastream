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
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/ExecutablePipelineProvider.hpp>
#include <Experimental/Operators/SynopsesOperator.hpp>
#include <Experimental/Synopses/AbstractSynopsis.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <vector>

namespace NES::Runtime::Execution::Operators {

class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    MockedPipelineExecutionContext(BufferManagerPtr bufferManager,
                                   uint64_t pipelineId)
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
            {}){};

    std::vector<Runtime::TupleBuffer> emittedBuffers;
};

class SynopsisPipelineTest : public Testing::NESBaseTest, public ::testing::WithParamInterface<std::string> {
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
        NESBaseTest::SetUp();
        NES_INFO("Setup SynopsisPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bufferManager = std::make_shared<Runtime::BufferManager>();
        workerContext = std::make_shared<WorkerContext>(0, bufferManager, 100);
    }
};

std::pair<std::shared_ptr<Runtime::Execution::PhysicalOperatorPipeline>, std::shared_ptr<MockedPipelineExecutionContext>>
createExecutableSynopsisPipeline(ASP::AbstractSynopsesPtr synopsis, SchemaPtr inputSchema, BufferManagerPtr bufferManager) {
    using namespace Runtime::Execution;

    // Scan Operator
    auto scanMemoryProvider = MemoryProvider::MemoryProvider::createMemoryProvider(bufferManager->getBufferSize(),inputSchema);
    auto scan = std::make_shared<Operators::Scan>(std::move(scanMemoryProvider));

    // Synopses Operator
    auto synopsesOperator = std::make_shared<Operators::SynopsesOperator>(synopsis);
    auto pipelineExecutionContext = std::make_shared<MockedPipelineExecutionContext>(bufferManager, -1);

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
        dynamicBuffer[pos][schema->get(0)->getName()].write((int64_t) (i + 1000));
        dynamicBuffer[pos][schema->get(1)->getName()].write((int64_t) (numberOfTuplesToProduce - i + 100));
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
 * @brief Tests if the pipeline works by creating a sample with the same size as the number of tuples and then checking if the
 * correct result is returned
 */
TEST_P(SynopsisPipelineTest, simpleSynopsisPipelineTest) {
    auto fieldNameAggregation = "value";
    auto fieldNameApproximate = "aggregation";
    auto timestampFieldName = "ts";
    auto numberOfTuplesToProduce = 1000;

    auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("id", BasicType::INT64)
                                ->addField(fieldNameAggregation, BasicType::INT64)
                                ->addField(timestampFieldName, BasicType::UINT64);
    auto outputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField(fieldNameApproximate, BasicType::INT64);

    auto allBuffers  = fillBuffer(*bufferManager, inputSchema, numberOfTuplesToProduce);
    auto expectedBuffer = bufferManager->getBufferBlocking();
    auto dynamicExpectedBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(expectedBuffer, outputSchema);
    dynamicExpectedBuffer[0][outputSchema->get(0)->getName()].write(101L);
    dynamicExpectedBuffer.setNumberOfTuples(1);

    auto synopsisConfig = ASP::Parsing::SynopsisConfiguration::create(ASP::Parsing::SYNOPSIS_TYPE::SRSWoR, numberOfTuplesToProduce);
    auto aggregationConfig = ASP::Parsing::SynopsisAggregationConfig::create(ASP::Parsing::AGGREGATION_TYPE::MIN,
                                                                             fieldNameAggregation,
                                                                             fieldNameApproximate,
                                                                             timestampFieldName,
                                                                             inputSchema,
                                                                             outputSchema);
    auto synopsis = ASP::AbstractSynopsis::create(*synopsisConfig, aggregationConfig);
    synopsis->setBufferManager(bufferManager);

    auto [pipeline, pipelineContext] = createExecutableSynopsisPipeline(synopsis, inputSchema, bufferManager);
    auto workerContext = std::make_shared<Runtime::WorkerContext>(0, bufferManager, 100);
    auto executablePipeline = provider->create(pipeline);

    executablePipeline->setup(*pipelineContext);
    for (auto& buffer : allBuffers) {
        executablePipeline->execute(buffer, *pipelineContext, *workerContext);
    }

    auto approximateBuffer = synopsis->getApproximate(bufferManager);

    ASSERT_EQ(approximateBuffer.size(), 1);
    NES_INFO("approximateBuffer: " << Util::printTupleBufferAsCSV(approximateBuffer[0], outputSchema));
    NES_INFO("expectedBuffer: " << Util::printTupleBufferAsCSV(expectedBuffer, outputSchema));

    auto dynamicApproxBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(approximateBuffer[0], outputSchema);
    ASSERT_EQ(dynamicApproxBuffer.getNumberOfTuples(), 1);
    ASSERT_TRUE(dynamicApproxBuffer[0][fieldNameApproximate].equal(dynamicExpectedBuffer[0][fieldNameApproximate]));
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        SynopsisPipelineTest,
                        ::testing::Values("PipelineInterpreter"), //, "PipelineCompiler"), This will be enabled in #3677
                        [](const testing::TestParamInfo<SynopsisPipelineTest::ParamType>& info) {
                            return info.param;
                        });

} // namespace NES::Runtime::Execution::Operators
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

#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/ExecutablePipelineProvider.hpp>
#include <Experimental/Operators/SynopsesOperator.hpp>
#include <Experimental/Synopses/AbstractSynopsis.hpp>
#include <Experimental/Synopses/Samples/SimpleRandomSampleWithoutReplacement.hpp>
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
createExecutablePipeline(ASP::AbstractSynopsesPtr synopsis, SchemaPtr inputSchema,
                         BufferManagerPtr bufferManager, size_t bufferSize) {
    using namespace Runtime::Execution;

    // Scan Operator
    auto scanMemoryProvider = Runtime::Execution::MemoryProvider::MemoryProvider::createMemoryProvider(bufferSize, inputSchema);
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

TEST_P(SynopsisPipelineTest, synopsisPipeline) {
    auto synopsisConfig = ASP::SynopsisConfiguration::create(ASP::SYNOPSIS_TYPE::SRSWoR, 1000);
    auto synopsis = ASP::AbstractSynopsis::create(*synopsisConfig);
    auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                           ->addField("id", BasicType::INT64)
                           ->addField("value", BasicType::INT64);
    auto outputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField("aggregation", BasicType::INT64);

    synopsis->setAggregationFunction(aggregationFunction);
    synopsis->setAggregationValue(std::move(aggregationValue));
    synopsis->setFieldNameAggregation(fieldNameAggregation);
    synopsis->setFieldNameApproximate(fieldNameApproximate);
    synopsis->setInputSchema(inputSchema);
    synopsis->setOutputSchema(outputSchema);
    synopsis->setBufferManager(bufferManager);

    auto [pipeline, pipelineContext] = createExecutablePipeline(synopsis);
    auto workerContext = std::make_shared<Runtime::WorkerContext>(0, bufferManager, 100);

}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        SynopsisPipelineTest,
                        ::testing::Values("PipelineInterpreter"), //, "PipelineCompiler"), This will be enabled in #3677
                        [](const testing::TestParamInfo<SynopsisPipelineTest::ParamType>& info) {
                            return info.param;
                        });

} // namespace NES::Runtime::Execution::Operators
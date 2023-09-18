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

#include <BaseIntegrationTest.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Experimental/Benchmarking/MicroBenchmarkSchemas.hpp>
#include <Experimental/Parsing/SynopsisAggregationConfig.hpp>
#include <Experimental/Synopses/Samples/RandomSampleWithoutReplacement.hpp>
#include <Experimental/Synopses/Samples/RandomSampleWithoutReplacementOperatorHandler.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <gtest/gtest.h>

namespace NES::ASP {

/**
 * @brief MockedPipelineExecutionContext for our executable pipeline
 */
class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    MockedPipelineExecutionContext(std::vector<Runtime::Execution::OperatorHandlerPtr> handlers = {})
        : PipelineExecutionContext(
        -1,// mock pipeline id
        1,         // mock query id
        nullptr,
        1, //noWorkerThreads
        {},
        {},
        handlers){};
};

class RandomSampleWithoutReplacementTest : public Testing::BaseIntegrationTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("RandomSampleWithoutReplacementTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup RandomSampleWithoutReplacementTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        BaseIntegrationTest::SetUp();
        NES_INFO("Setup RandomSampleWithoutReplacementTest test case.");
        bufferManager = std::make_shared<Runtime::BufferManager>();

        // Creating the worker context and the pipeline necessary for testing the sampling
        opHandler = std::make_shared<RandomSampleWithoutReplacementOperatorHandler>();
        std::vector<Runtime::Execution::OperatorHandlerPtr> opHandlers = {opHandler};
        workerContext = std::make_shared<Runtime::WorkerContext>(0, bufferManager, 100);
        pipelineContext = std::make_shared<MockedPipelineExecutionContext>(opHandlers);
        executionContext = std::make_unique<Runtime::Execution::ExecutionContext>(
            Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
            Nautilus::Value<Nautilus::MemRef>((int8_t*) pipelineContext.get()));
        inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
            ->addField(idString, BasicType::INT64)
            ->addField(aggregationString, BasicType::FLOAT64)
            ->addField(timestampFieldName, BasicType::UINT64);

        entrySize = inputSchema->getSchemaSizeInBytes();
    }

    std::pair<std::vector<Runtime::TupleBuffer>, SchemaPtr> fillRandomSample(const Parsing::Aggregation_Type& aggregationType,
                                                                             const std::vector<Nautilus::Record>& inputRecords,
                                                                             const std::vector<Nautilus::Value<>>& queryKeys) {
        auto outputSchema = Benchmarking::getOutputSchemaFromTypeAndInputSchema(aggregationType, *inputSchema,
                                                                                idString, aggregationString,
                                                                                idString, approximateString);

        // Creating aggregation config and the histogram
        auto aggregationConfig = Parsing::SynopsisAggregationConfig::create(aggregationType, idString, aggregationString,
                                                                            approximateString, timestampFieldName,
                                                                            inputSchema, outputSchema);
        RandomSampleWithoutReplacement randomSample(aggregationConfig, numberOfSamples, entrySize, pageSize);

        // Setting up the synopsis and creating the local operator state
        randomSample.setup(handlerIndex, *executionContext);
        auto sampleMemRef = Nautilus::Value<Nautilus::MemRef>((int8_t*) opHandler->getPagedVectorRef());
        auto samples = Nautilus::Interface::PagedVectorRef(sampleMemRef, entrySize);
        auto opState = std::make_unique<RandomSampleWithoutReplacement::LocalRandomSampleOperatorState>(samples);

        // Inserting records
        for (auto& record : inputRecords) {
            randomSample.addToSynopsis(handlerIndex, *executionContext, record, opState.get());
        }
        
        return {randomSample.getApproximateForKeys(handlerIndex, *executionContext, queryKeys, bufferManager), outputSchema};
    }

    Runtime::BufferManagerPtr bufferManager;

    // Input and output variables
    const std::string idString = "id";
    const std::string aggregationString = "value";
    const std::string approximateString = "aggregation";
    const std::string timestampFieldName = "ts";
    const uint64_t numberOfSamples = 3;
    const uint64_t minValue = 0;
    const uint64_t handlerIndex = 0;
    const uint64_t maxValue = 5;
    const uint64_t pageSize = 4096;

    uint64_t entrySize;
    SchemaPtr inputSchema;
    std::shared_ptr<RandomSampleWithoutReplacementOperatorHandler> opHandler;
    std::unique_ptr<Runtime::Execution::ExecutionContext> executionContext;
    Runtime::WorkerContextPtr workerContext;
    std::shared_ptr<MockedPipelineExecutionContext> pipelineContext;
};


std::vector<Nautilus::Record> getInputData(Schema& inputSchema) {
    return {
        Nautilus::Record({
            {inputSchema.get(0)->getName(), Nautilus::Value<Nautilus::UInt64>(0_u64)},
            {inputSchema.get(1)->getName(), Nautilus::Value<Nautilus::Double>((double_t) 42)},
            {inputSchema.get(2)->getName(), Nautilus::Value<Nautilus::UInt64>(0_u64)}
        }),

        Nautilus::Record({
            {inputSchema.get(0)->getName(), Nautilus::Value<Nautilus::UInt64>(2_u64)},
            {inputSchema.get(1)->getName(), Nautilus::Value<Nautilus::Double>((double_t) 1234)},
            {inputSchema.get(2)->getName(), Nautilus::Value<Nautilus::UInt64>(1_u64)}
        }),

        Nautilus::Record({
            {inputSchema.get(0)->getName(), Nautilus::Value<Nautilus::UInt64>(4_u64)},
            {inputSchema.get(1)->getName(), Nautilus::Value<Nautilus::Double>((double_t) 404)},
            {inputSchema.get(2)->getName(), Nautilus::Value<Nautilus::UInt64>(1_u64)}
        }),

        Nautilus::Record({
            {inputSchema.get(0)->getName(), Nautilus::Value<Nautilus::UInt64>(4_u64)},
            {inputSchema.get(1)->getName(), Nautilus::Value<Nautilus::Double>((double_t) 101)},
            {inputSchema.get(2)->getName(), Nautilus::Value<Nautilus::UInt64>(1_u64)}
        })
    };
}

TEST_F(RandomSampleWithoutReplacementTest, sampleTestCount) {
    auto aggregationType = Parsing::Aggregation_Type::COUNT;
    // Creating query keys for all histograms
    std::vector<Nautilus::Value<>> queryKeys = {Nautilus::Value<>(0_s64), Nautilus::Value<>(1_s64),
        Nautilus::Value<>(2_s64), Nautilus::Value<>(3_s64),
        Nautilus::Value<>(4_s64),
    };
    
    auto [approximateBuffers, outputSchema] = fillRandomSample(aggregationType, getInputData(*inputSchema), queryKeys);
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(approximateBuffers[0],
                                                                                              outputSchema);

    // Checking for correctness
    EXPECT_EQ(dynamicBuffer[0][idString].read<int64_t>(), 0);
    EXPECT_EQ(dynamicBuffer[0][approximateString].read<double_t>(), 1 + 1.0/3);

    EXPECT_EQ(dynamicBuffer[1][idString].read<int64_t>(), 1);
    EXPECT_EQ(dynamicBuffer[1][approximateString].read<double_t>(), 0);

    EXPECT_EQ(dynamicBuffer[2][idString].read<int64_t>(), 2);
    EXPECT_EQ(dynamicBuffer[2][approximateString].read<double_t>(), 1 + 1.0/3);

    EXPECT_EQ(dynamicBuffer[3][idString].read<int64_t>(), 3);
    EXPECT_EQ(dynamicBuffer[3][approximateString].read<double_t>(), 0);

    EXPECT_EQ(dynamicBuffer[4][idString].read<int64_t>(), 4);
    EXPECT_EQ(dynamicBuffer[4][approximateString].read<double_t>(), 1 + 1.0/3);
}

TEST_F(RandomSampleWithoutReplacementTest, sampleTestMin) {
    auto aggregationType = Parsing::Aggregation_Type::MIN;
    // Creating query keys for all histograms
    std::vector<Nautilus::Value<>> queryKeys = {Nautilus::Value<>(0_s64), Nautilus::Value<>(1_s64),
        Nautilus::Value<>(2_s64), Nautilus::Value<>(3_s64),
        Nautilus::Value<>(4_s64),
    };
    
    auto [approximateBuffers, outputSchema] = fillRandomSample(aggregationType, getInputData(*inputSchema), queryKeys);
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(approximateBuffers[0],
                                                                                              outputSchema);

    // Checking for correctness
    EXPECT_EQ(dynamicBuffer[0][idString].read<int64_t>(), 0);
    EXPECT_EQ(dynamicBuffer[0][approximateString].read<double_t>(), 42.0);

    EXPECT_EQ(dynamicBuffer[1][idString].read<int64_t>(), 1);
    EXPECT_EQ(dynamicBuffer[1][approximateString].read<double_t>(), std::numeric_limits<double_t>::max());

    EXPECT_EQ(dynamicBuffer[2][idString].read<int64_t>(), 2);
    EXPECT_EQ(dynamicBuffer[2][approximateString].read<double_t>(), 1234.0);

    EXPECT_EQ(dynamicBuffer[3][idString].read<int64_t>(), 3);
    EXPECT_EQ(dynamicBuffer[3][approximateString].read<double_t>(), std::numeric_limits<double_t>::max());

    EXPECT_EQ(dynamicBuffer[4][idString].read<int64_t>(), 4);
    EXPECT_EQ(dynamicBuffer[4][approximateString].read<double_t>(), 101.0);
}

TEST_F(RandomSampleWithoutReplacementTest, sampleTestMax) {
    auto aggregationType = Parsing::Aggregation_Type::MAX;
    // Creating query keys for all histograms
    std::vector<Nautilus::Value<>> queryKeys = {Nautilus::Value<>(0_s64), Nautilus::Value<>(1_s64),
        Nautilus::Value<>(2_s64), Nautilus::Value<>(3_s64),
        Nautilus::Value<>(4_s64),
    };
    
    auto [approximateBuffers, outputSchema] = fillRandomSample(aggregationType, getInputData(*inputSchema), queryKeys);
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(approximateBuffers[0],
                                                                                              outputSchema);

    // Checking for correctness
    EXPECT_EQ(dynamicBuffer[0][idString].read<int64_t>(), 0);
    EXPECT_EQ(dynamicBuffer[0][approximateString].read<double_t>(), 42.0);

    EXPECT_EQ(dynamicBuffer[1][idString].read<int64_t>(), 1);
    EXPECT_EQ(dynamicBuffer[1][approximateString].read<double_t>(), std::numeric_limits<double_t>::min());

    EXPECT_EQ(dynamicBuffer[2][idString].read<int64_t>(), 2);
    EXPECT_EQ(dynamicBuffer[2][approximateString].read<double_t>(), 1234.0);

    EXPECT_EQ(dynamicBuffer[3][idString].read<int64_t>(), 3);
    EXPECT_EQ(dynamicBuffer[3][approximateString].read<double_t>(), std::numeric_limits<double_t>::min());

    EXPECT_EQ(dynamicBuffer[4][idString].read<int64_t>(), 4);
    EXPECT_EQ(dynamicBuffer[4][approximateString].read<double_t>(), 101.0);
}

TEST_F(RandomSampleWithoutReplacementTest, sampleTestSum) {
    auto aggregationType = Parsing::Aggregation_Type::SUM;
    // Creating query keys for all histograms
    std::vector<Nautilus::Value<>> queryKeys = {Nautilus::Value<>(0_s64), Nautilus::Value<>(1_s64),
        Nautilus::Value<>(2_s64), Nautilus::Value<>(3_s64),
        Nautilus::Value<>(4_s64),
    };
    
    auto [approximateBuffers, outputSchema] = fillRandomSample(aggregationType, getInputData(*inputSchema), queryKeys);
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(approximateBuffers[0],
                                                                                              outputSchema);

    // Checking for correctness
    EXPECT_EQ(dynamicBuffer[0][idString].read<int64_t>(), 0);
    EXPECT_EQ(dynamicBuffer[0][approximateString].read<double_t>(), 56.0);

    EXPECT_EQ(dynamicBuffer[1][idString].read<int64_t>(), 1);
    EXPECT_EQ(dynamicBuffer[1][approximateString].read<double_t>(), 0);

    EXPECT_EQ(dynamicBuffer[2][idString].read<int64_t>(), 2);
    EXPECT_EQ(dynamicBuffer[2][approximateString].read<double_t>(), 1645 + 1.0/3);

    EXPECT_EQ(dynamicBuffer[3][idString].read<int64_t>(), 3);
    EXPECT_EQ(dynamicBuffer[3][approximateString].read<double_t>(), 0);

    EXPECT_EQ(dynamicBuffer[4][idString].read<int64_t>(), 4);
    EXPECT_EQ(dynamicBuffer[4][approximateString].read<double_t>(), 134 + 2.0/3);
}

TEST_F(RandomSampleWithoutReplacementTest, sampleTestAverage) {
    auto aggregationType = Parsing::Aggregation_Type::AVERAGE;
    // Creating query keys for all histograms
    std::vector<Nautilus::Value<>> queryKeys = {Nautilus::Value<>(0_s64), Nautilus::Value<>(1_s64),
        Nautilus::Value<>(2_s64), Nautilus::Value<>(3_s64),
        Nautilus::Value<>(4_s64),
    };
    
    auto [approximateBuffers, outputSchema] = fillRandomSample(aggregationType, getInputData(*inputSchema), queryKeys);
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(approximateBuffers[0],
                                                                                              outputSchema);

    // Checking for correctness
    EXPECT_EQ(dynamicBuffer[0][idString].read<int64_t>(), 0);
    EXPECT_EQ(dynamicBuffer[0][approximateString].read<double_t>(), 42.0);

    EXPECT_EQ(dynamicBuffer[1][idString].read<int64_t>(), 1);
    EXPECT_TRUE(std::isinf(dynamicBuffer[1][approximateString].read<double_t>()) ||
                std::isnan(dynamicBuffer[1][approximateString].read<double_t>()));

    EXPECT_EQ(dynamicBuffer[2][idString].read<int64_t>(), 2);
    EXPECT_EQ(dynamicBuffer[2][approximateString].read<double_t>(), 1234.0);

    EXPECT_EQ(dynamicBuffer[3][idString].read<int64_t>(), 3);
    EXPECT_TRUE(std::isinf(dynamicBuffer[3][approximateString].read<double_t>()) ||
                std::isnan(dynamicBuffer[3][approximateString].read<double_t>()));

    EXPECT_EQ(dynamicBuffer[4][idString].read<int64_t>(), 4);
    EXPECT_EQ(dynamicBuffer[4][approximateString].read<double_t>(), 101.0);
}


} // namespace NES::ASP
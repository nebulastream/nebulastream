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

#include <Util/Logger/Logger.hpp>
#include <Experimental/Parsing/SynopsisAggregationConfig.hpp>
#include <gtest/gtest.h>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Experimental/Benchmarking/MicroBenchmarkSchemas.hpp>
#include <Experimental/Synopses/Histograms/EquiWidth1DHist.hpp>
#include <Experimental/Synopses/Histograms/EquiWidth1DHistOperatorHandler.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>


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

class EquiWidth1DHistTest : public Testing::NESBaseTest {
public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("EquiWidth1DHistTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup EquiWidth1DHistTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NESBaseTest::SetUp();
        NES_INFO("Setup EquiWidth1DHistTest test case.");
        bufferManager = std::make_shared<Runtime::BufferManager>();
        readBinDimension = std::make_unique<Runtime::Execution::Expressions::ReadFieldExpression>(idString);

        // Creating the worker context and the pipeline necessary for testing the sampling
        opHandler = std::make_shared<EquiWidth1DHistOperatorHandler>();
        std::vector<Runtime::Execution::OperatorHandlerPtr> opHandlers = {opHandler};
        workerContext = std::make_shared<Runtime::WorkerContext>(0, bufferManager, 100);
        pipelineContext = std::make_shared<MockedPipelineExecutionContext>(opHandlers);
        executionContext = std::make_unique<Runtime::Execution::ExecutionContext>(
                Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                Nautilus::Value<Nautilus::MemRef>((int8_t*) pipelineContext.get()));
    }

    Runtime::BufferManagerPtr bufferManager;

    // Input and output variables
    const std::string idString = "id";
    const std::string aggregationString = "value";
    const std::string approximateString = "aggregation";
    const std::string timestampFieldName = "ts";
    const std::string lowerBoundBinName = "lowerBinValue";
    const std::string upperBoundBinName = "upperBinValue";
    const uint64_t numberOfBins = 5;
    const uint64_t minValue = 0;
    const uint64_t maxValue = 5;
    std::unique_ptr<Runtime::Execution::Expressions::ReadFieldExpression> readBinDimension;

    const uint64_t handlerIndex = 0;
    std::shared_ptr<EquiWidth1DHistOperatorHandler> opHandler;
    std::unique_ptr<Runtime::Execution::ExecutionContext> executionContext;
    Runtime::WorkerContextPtr workerContext;
    std::shared_ptr<MockedPipelineExecutionContext> pipelineContext;
};


TEST_F(EquiWidth1DHistTest, simpleHistTestCount) {
    auto aggregationType = Parsing::Aggregation_Type::COUNT;
    const auto entrySize = sizeof(uint64_t);

    const auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                            ->addField(idString, BasicType::UINT64)
                            ->addField(aggregationString, BasicType::INT64)
                            ->addField(timestampFieldName, BasicType::UINT64);
    auto outputSchema = Benchmarking::getOutputSchemaFromTypeAndInputSchema(aggregationType, *inputSchema, aggregationString);
    outputSchema->addField(lowerBoundBinName, BasicType::INT64);
    outputSchema->addField(upperBoundBinName, BasicType::INT64);

    // Creating aggregation config and the histogram
    auto aggregationConfig = Parsing::SynopsisAggregationConfig::create(aggregationType, aggregationString, approximateString,
                                                                        timestampFieldName, inputSchema, outputSchema);
    EquiWidth1DHist histSynopsis(aggregationConfig, entrySize, minValue, maxValue, numberOfBins,
                                 lowerBoundBinName, upperBoundBinName, std::move(readBinDimension));


    // Setting up the synopsis and creating the local operator state
    histSynopsis.setup(handlerIndex, *executionContext);
    auto binMemRef = Nautilus::Value<Nautilus::MemRef>((int8_t*) opHandler->getBinsRef());
    auto bins = Nautilus::Interface::Fixed2DArrayRef(binMemRef, entrySize, numberOfBins);
    auto opState = std::make_unique<EquiWidth1DHist::LocalBinsOperatorState>(bins);


    // Creating records
    Nautilus::Record record1({
        {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 0)},
        {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Int64>((int64_t) 0)},
        {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 0)}
    });

    Nautilus::Record record2({
        {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 2)},
        {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Int64>((int64_t) 1)},
        {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 1)}
    });

    Nautilus::Record record3({
        {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 4)},
        {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Int64>((int64_t) 1)},
        {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 1)}
    });

    Nautilus::Record record4({
        {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 4)},
        {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Int64>((int64_t) 1)},
        {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 1)}
    });

    // Inserting records
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record1, opState.get());
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record2, opState.get());
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record3, opState.get());
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record4, opState.get());


    auto approximateBuffers = histSynopsis.getApproximate(handlerIndex, *executionContext, bufferManager);
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(approximateBuffers[0],
                                                                                              outputSchema);

    EXPECT_EQ(dynamicBuffer.getNumberOfTuples(), numberOfBins);
    EXPECT_EQ(dynamicBuffer[0][approximateString].read<int64_t>(), 1);
    EXPECT_EQ(dynamicBuffer[0][lowerBoundBinName].read<int64_t>(), 0);
    EXPECT_EQ(dynamicBuffer[0][upperBoundBinName].read<int64_t>(), 1);

    EXPECT_EQ(dynamicBuffer[1][approximateString].read<int64_t>(), 0);
    EXPECT_EQ(dynamicBuffer[1][lowerBoundBinName].read<int64_t>(), 1);
    EXPECT_EQ(dynamicBuffer[1][upperBoundBinName].read<int64_t>(), 2);

    EXPECT_EQ(dynamicBuffer[2][approximateString].read<int64_t>(), 1);
    EXPECT_EQ(dynamicBuffer[2][lowerBoundBinName].read<int64_t>(), 2);
    EXPECT_EQ(dynamicBuffer[2][upperBoundBinName].read<int64_t>(), 3);

    EXPECT_EQ(dynamicBuffer[3][approximateString].read<int64_t>(), 0);
    EXPECT_EQ(dynamicBuffer[3][lowerBoundBinName].read<int64_t>(), 3);
    EXPECT_EQ(dynamicBuffer[3][upperBoundBinName].read<int64_t>(), 4);

    EXPECT_EQ(dynamicBuffer[4][approximateString].read<int64_t>(), 2);
    EXPECT_EQ(dynamicBuffer[4][lowerBoundBinName].read<int64_t>(), 4);
    EXPECT_EQ(dynamicBuffer[4][upperBoundBinName].read<int64_t>(), 5);
}

TEST_F(EquiWidth1DHistTest, simpleHistTestSum) {
    auto aggregationType = Parsing::Aggregation_Type::SUM;
    const auto entrySize = sizeof(uint64_t);

    const auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
            ->addField(idString, BasicType::UINT64)
            ->addField(aggregationString, BasicType::INT64)
            ->addField(timestampFieldName, BasicType::UINT64);
    auto outputSchema = Benchmarking::getOutputSchemaFromTypeAndInputSchema(aggregationType, *inputSchema, aggregationString);
    outputSchema->addField(lowerBoundBinName, BasicType::INT64);
    outputSchema->addField(upperBoundBinName, BasicType::INT64);

    // Creating aggregation config and the histogram
    auto aggregationConfig = Parsing::SynopsisAggregationConfig::create(aggregationType, aggregationString, approximateString,
                                                                        timestampFieldName, inputSchema, outputSchema);
    EquiWidth1DHist histSynopsis(aggregationConfig, entrySize, minValue, maxValue, numberOfBins,
                                 lowerBoundBinName, upperBoundBinName, std::move(readBinDimension));

    // Setting up the synopsis and creating the local operator state
    histSynopsis.setup(handlerIndex, *executionContext);
    auto binMemRef = Nautilus::Value<Nautilus::MemRef>((int8_t*) opHandler->getBinsRef());
    auto bins = Nautilus::Interface::Fixed2DArrayRef(binMemRef, entrySize, numberOfBins);
    auto opState = std::make_unique<EquiWidth1DHist::LocalBinsOperatorState>(bins);


    // Creating records
    Nautilus::Record record1({
        {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 0)},
        {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Int64>((int64_t) 42)},
        {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 0)}
    });

    Nautilus::Record record2({
        {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 2)},
        {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Int64>((int64_t) 1234)},
        {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 1)}
    });

    Nautilus::Record record3({
        {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 4)},
        {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Int64>((int64_t) 404)},
        {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 1)}
    });

    Nautilus::Record record4({
        {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 4)},
        {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Int64>((int64_t) 100)},
        {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 1)}
    });

    // Inserting records
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record1, opState.get());
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record2, opState.get());
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record3, opState.get());
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record4, opState.get());


    auto approximateBuffers = histSynopsis.getApproximate(handlerIndex, *executionContext, bufferManager);
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(approximateBuffers[0],
                                                                                              outputSchema);

    EXPECT_EQ(dynamicBuffer.getNumberOfTuples(), numberOfBins);
    EXPECT_EQ(dynamicBuffer[0][approximateString].read<int64_t>(), 42);
    EXPECT_EQ(dynamicBuffer[0][lowerBoundBinName].read<int64_t>(), 0);
    EXPECT_EQ(dynamicBuffer[0][upperBoundBinName].read<int64_t>(), 1);

    EXPECT_EQ(dynamicBuffer[1][approximateString].read<int64_t>(), 0);
    EXPECT_EQ(dynamicBuffer[1][lowerBoundBinName].read<int64_t>(), 1);
    EXPECT_EQ(dynamicBuffer[1][upperBoundBinName].read<int64_t>(), 2);

    EXPECT_EQ(dynamicBuffer[2][approximateString].read<int64_t>(), 1234);
    EXPECT_EQ(dynamicBuffer[2][lowerBoundBinName].read<int64_t>(), 2);
    EXPECT_EQ(dynamicBuffer[2][upperBoundBinName].read<int64_t>(), 3);

    EXPECT_EQ(dynamicBuffer[3][approximateString].read<int64_t>(), 0);
    EXPECT_EQ(dynamicBuffer[3][lowerBoundBinName].read<int64_t>(), 3);
    EXPECT_EQ(dynamicBuffer[3][upperBoundBinName].read<int64_t>(), 4);

    EXPECT_EQ(dynamicBuffer[4][approximateString].read<int64_t>(), 504);
    EXPECT_EQ(dynamicBuffer[4][lowerBoundBinName].read<int64_t>(), 4);
    EXPECT_EQ(dynamicBuffer[4][upperBoundBinName].read<int64_t>(), 5);
}

TEST_F(EquiWidth1DHistTest, simpleHistTestMin) {
    auto aggregationType = Parsing::Aggregation_Type::MIN;
    const auto entrySize = sizeof(uint64_t);

    const auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
            ->addField(idString, BasicType::UINT64)
            ->addField(aggregationString, BasicType::INT64)
            ->addField(timestampFieldName, BasicType::UINT64);
    auto outputSchema = Benchmarking::getOutputSchemaFromTypeAndInputSchema(aggregationType, *inputSchema, aggregationString);
    outputSchema->addField(lowerBoundBinName, BasicType::INT64);
    outputSchema->addField(upperBoundBinName, BasicType::INT64);

    // Creating aggregation config and the histogram
    auto aggregationConfig = Parsing::SynopsisAggregationConfig::create(aggregationType, aggregationString, approximateString,
                                                                        timestampFieldName, inputSchema, outputSchema);
    EquiWidth1DHist histSynopsis(aggregationConfig, entrySize, minValue, maxValue, numberOfBins,
                                 lowerBoundBinName, upperBoundBinName, std::move(readBinDimension));


    // Setting up the synopsis and creating the local operator state
    histSynopsis.setup(handlerIndex, *executionContext);
    auto binMemRef = Nautilus::Value<Nautilus::MemRef>((int8_t*) opHandler->getBinsRef());
    auto bins = Nautilus::Interface::Fixed2DArrayRef(binMemRef, entrySize, numberOfBins);
    auto opState = std::make_unique<EquiWidth1DHist::LocalBinsOperatorState>(bins);


    // Creating records
    Nautilus::Record record1({
         {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 0)},
         {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Int64>((int64_t) 42)},
         {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 0)}
    });

    Nautilus::Record record2({
        {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 2)},
        {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Int64>((int64_t) 1234)},
        {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 1)}
    });

    Nautilus::Record record3({
        {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 4)},
        {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Int64>((int64_t) 404)},
        {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 1)}
    });

    Nautilus::Record record4({
        {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 4)},
        {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Int64>((int64_t) 100)},
        {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 1)}
    });

    // Inserting records
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record1, opState.get());
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record2, opState.get());
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record3, opState.get());
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record4, opState.get());


    auto approximateBuffers = histSynopsis.getApproximate(handlerIndex, *executionContext, bufferManager);
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(approximateBuffers[0],
                                                                                              outputSchema);

    EXPECT_EQ(dynamicBuffer.getNumberOfTuples(), numberOfBins);
    EXPECT_EQ(dynamicBuffer[0][approximateString].read<int64_t>(), 42);
    EXPECT_EQ(dynamicBuffer[0][lowerBoundBinName].read<int64_t>(), 0);
    EXPECT_EQ(dynamicBuffer[0][upperBoundBinName].read<int64_t>(), 1);

    EXPECT_EQ(dynamicBuffer[1][approximateString].read<int64_t>(), std::numeric_limits<int64_t>::max());
    EXPECT_EQ(dynamicBuffer[1][lowerBoundBinName].read<int64_t>(), 1);
    EXPECT_EQ(dynamicBuffer[1][upperBoundBinName].read<int64_t>(), 2);

    EXPECT_EQ(dynamicBuffer[2][approximateString].read<int64_t>(), 1234);
    EXPECT_EQ(dynamicBuffer[2][lowerBoundBinName].read<int64_t>(), 2);
    EXPECT_EQ(dynamicBuffer[2][upperBoundBinName].read<int64_t>(), 3);

    EXPECT_EQ(dynamicBuffer[3][approximateString].read<int64_t>(), std::numeric_limits<int64_t>::max());
    EXPECT_EQ(dynamicBuffer[3][lowerBoundBinName].read<int64_t>(), 3);
    EXPECT_EQ(dynamicBuffer[3][upperBoundBinName].read<int64_t>(), 4);

    EXPECT_EQ(dynamicBuffer[4][approximateString].read<int64_t>(), 100);
    EXPECT_EQ(dynamicBuffer[4][lowerBoundBinName].read<int64_t>(), 4);
    EXPECT_EQ(dynamicBuffer[4][upperBoundBinName].read<int64_t>(), 5);
}

TEST_F(EquiWidth1DHistTest, simpleHistTestMax) {
    auto aggregationType = Parsing::Aggregation_Type::MAX;
    const auto entrySize = sizeof(uint64_t);

    const auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
            ->addField(idString, BasicType::UINT64)
            ->addField(aggregationString, BasicType::INT64)
            ->addField(timestampFieldName, BasicType::UINT64);
    auto outputSchema = Benchmarking::getOutputSchemaFromTypeAndInputSchema(aggregationType, *inputSchema, aggregationString);
    outputSchema->addField(lowerBoundBinName, BasicType::INT64);
    outputSchema->addField(upperBoundBinName, BasicType::INT64);

    // Creating aggregation config and the histogram
    auto aggregationConfig = Parsing::SynopsisAggregationConfig::create(aggregationType, aggregationString, approximateString,
                                                                        timestampFieldName, inputSchema, outputSchema);
    EquiWidth1DHist histSynopsis(aggregationConfig, entrySize, minValue, maxValue, numberOfBins,
                                 lowerBoundBinName, upperBoundBinName, std::move(readBinDimension));

    // Setting up the synopsis and creating the local operator state
    histSynopsis.setup(handlerIndex, *executionContext);
    auto binMemRef = Nautilus::Value<Nautilus::MemRef>((int8_t*) opHandler->getBinsRef());
    auto bins = Nautilus::Interface::Fixed2DArrayRef(binMemRef, entrySize, numberOfBins);
    auto opState = std::make_unique<EquiWidth1DHist::LocalBinsOperatorState>(bins);


    // Creating records
    Nautilus::Record record1({
        {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 0)},
        {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Int64>((int64_t) 42)},
        {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 0)}
    });

    Nautilus::Record record2({
        {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 2)},
        {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Int64>((int64_t) 1234)},
        {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 1)}
    });

    Nautilus::Record record3({
        {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 4)},
        {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Int64>((int64_t) 404)},
        {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 1)}
    });

    Nautilus::Record record4({
        {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 4)},
        {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Int64>((int64_t) 100)},
        {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 1)}
    });

    // Inserting records
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record1, opState.get());
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record2, opState.get());
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record3, opState.get());
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record4, opState.get());


    auto approximateBuffers = histSynopsis.getApproximate(handlerIndex, *executionContext, bufferManager);
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(approximateBuffers[0],
                                                                                              outputSchema);

    EXPECT_EQ(dynamicBuffer.getNumberOfTuples(), numberOfBins);
    EXPECT_EQ(dynamicBuffer[0][approximateString].read<int64_t>(), 42);
    EXPECT_EQ(dynamicBuffer[0][lowerBoundBinName].read<int64_t>(), 0);
    EXPECT_EQ(dynamicBuffer[0][upperBoundBinName].read<int64_t>(), 1);

    EXPECT_EQ(dynamicBuffer[1][approximateString].read<int64_t>(), std::numeric_limits<int64_t>::min());
    EXPECT_EQ(dynamicBuffer[1][lowerBoundBinName].read<int64_t>(), 1);
    EXPECT_EQ(dynamicBuffer[1][upperBoundBinName].read<int64_t>(), 2);

    EXPECT_EQ(dynamicBuffer[2][approximateString].read<int64_t>(), 1234);
    EXPECT_EQ(dynamicBuffer[2][lowerBoundBinName].read<int64_t>(), 2);
    EXPECT_EQ(dynamicBuffer[2][upperBoundBinName].read<int64_t>(), 3);

    EXPECT_EQ(dynamicBuffer[3][approximateString].read<int64_t>(), std::numeric_limits<int64_t>::min());
    EXPECT_EQ(dynamicBuffer[3][lowerBoundBinName].read<int64_t>(), 3);
    EXPECT_EQ(dynamicBuffer[3][upperBoundBinName].read<int64_t>(), 4);

    EXPECT_EQ(dynamicBuffer[4][approximateString].read<int64_t>(), 404);
    EXPECT_EQ(dynamicBuffer[4][lowerBoundBinName].read<int64_t>(), 4);
    EXPECT_EQ(dynamicBuffer[4][upperBoundBinName].read<int64_t>(), 5);
}

TEST_F(EquiWidth1DHistTest, simpleHistTestAverage) {
    auto aggregationType = Parsing::Aggregation_Type::AVERAGE;
    const auto entrySize = sizeof(uint64_t) * 2; // one for the count and one for the sum

    const auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
            ->addField(idString, BasicType::UINT64)
            ->addField(aggregationString, BasicType::FLOAT64)
            ->addField(timestampFieldName, BasicType::UINT64);
    auto outputSchema = Benchmarking::getOutputSchemaFromTypeAndInputSchema(aggregationType, *inputSchema, aggregationString);
    outputSchema->addField(lowerBoundBinName, BasicType::INT64);
    outputSchema->addField(upperBoundBinName, BasicType::INT64);

    // Creating aggregation config and the histogram
    auto aggregationConfig = Parsing::SynopsisAggregationConfig::create(aggregationType, aggregationString, approximateString,
                                                                        timestampFieldName, inputSchema, outputSchema);
    EquiWidth1DHist histSynopsis(aggregationConfig, entrySize, minValue, maxValue, numberOfBins,
                                 lowerBoundBinName, upperBoundBinName, std::move(readBinDimension));


    // Setting up the synopsis and creating the local operator state
    histSynopsis.setup(handlerIndex, *executionContext);
    auto binMemRef = Nautilus::Value<Nautilus::MemRef>((int8_t*) opHandler->getBinsRef());
    auto bins = Nautilus::Interface::Fixed2DArrayRef(binMemRef, entrySize, numberOfBins);
    auto opState = std::make_unique<EquiWidth1DHist::LocalBinsOperatorState>(bins);


    // Creating records
    Nautilus::Record record1({
        {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 0)},
        {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Double>((double_t) 42)},
        {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 0)}
    });

    Nautilus::Record record2({
        {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 2)},
        {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Double>((double_t) 1234)},
        {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 1)}
    });

    Nautilus::Record record3({
        {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 4)},
        {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Double>((double_t) 404)},
        {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 1)}
    });

    Nautilus::Record record4({
        {inputSchema->get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 4)},
        {inputSchema->get(1)->getName(), Nautilus::Value<Nautilus::Double>((double_t) 101)},
        {inputSchema->get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 1)}
    });

    // Inserting records
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record1, opState.get());
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record2, opState.get());
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record3, opState.get());
    histSynopsis.addToSynopsis(handlerIndex, *executionContext, record4, opState.get());


    auto approximateBuffers = histSynopsis.getApproximate(handlerIndex, *executionContext, bufferManager);
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(approximateBuffers[0],
                                                                                              outputSchema);

    EXPECT_EQ(dynamicBuffer.getNumberOfTuples(), numberOfBins);
    EXPECT_EQ(dynamicBuffer[0][approximateString].read<double_t>(), 42.0);
    EXPECT_EQ(dynamicBuffer[0][lowerBoundBinName].read<int64_t>(), 0);
    EXPECT_EQ(dynamicBuffer[0][upperBoundBinName].read<int64_t>(), 1);

    EXPECT_EQ(dynamicBuffer[1][approximateString].read<double_t>(), 0);
    EXPECT_EQ(dynamicBuffer[1][lowerBoundBinName].read<int64_t>(), 1);
    EXPECT_EQ(dynamicBuffer[1][upperBoundBinName].read<int64_t>(), 2);

    EXPECT_EQ(dynamicBuffer[2][approximateString].read<double_t>(), 1234.0);
    EXPECT_EQ(dynamicBuffer[2][lowerBoundBinName].read<int64_t>(), 2);
    EXPECT_EQ(dynamicBuffer[2][upperBoundBinName].read<int64_t>(), 3);

    EXPECT_EQ(dynamicBuffer[3][approximateString].read<double_t>(), 0);
    EXPECT_EQ(dynamicBuffer[3][lowerBoundBinName].read<int64_t>(), 3);
    EXPECT_EQ(dynamicBuffer[3][upperBoundBinName].read<int64_t>(), 4);

    EXPECT_EQ(dynamicBuffer[4][approximateString].read<double_t>(), 252.5);
    EXPECT_EQ(dynamicBuffer[4][lowerBoundBinName].read<int64_t>(), 4);
    EXPECT_EQ(dynamicBuffer[4][upperBoundBinName].read<int64_t>(), 5);
}

} // namespace NES::ASP
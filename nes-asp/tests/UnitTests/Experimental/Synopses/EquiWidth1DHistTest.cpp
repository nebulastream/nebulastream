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
    }

    Runtime::BufferManagerPtr bufferManager;
};


TEST_F(EquiWidth1DHistTest, simpleHistTestCount) {
    auto aggregationType = Parsing::Aggregation_Type::COUNT;

    // Input and output variables
    const auto idString = "id";
    const auto aggregationString = "value";
    const auto approximateString = "aggregation";
    const auto timestampFieldName = "ts";
    const auto lowerBoundBinName = "lowerBoundBin";
    const auto upperBoundBinName = "upperBoundBin";
    const auto entrySize = sizeof(uint64_t);
    const auto numberOfBins = 5;
    const auto minValue = 0;
    const auto maxValue = 4;
    auto readBinDimension = std::make_unique<Runtime::Execution::Expressions::ReadFieldExpression>(idString);

    const auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
            ->addField(idString, BasicType::UINT64)
            ->addField(aggregationString, BasicType::INT64)
            ->addField(timestampFieldName, BasicType::UINT64);
    auto outputSchema = Benchmarking::getOutputSchemaFromTypeAndInputSchema(aggregationType, *inputSchema, aggregationString);

    outputSchema->addField(lowerBoundBinName, BasicType::UINT64);
    outputSchema->addField(upperBoundBinName, BasicType::UINT64);

    // Creating aggregation config and the histogram
    auto aggregationConfig = Parsing::SynopsisAggregationConfig::create(aggregationType, aggregationString, approximateString,
                                                                        timestampFieldName, inputSchema, outputSchema);
    EquiWidth1DHist histSynopsis(aggregationConfig, entrySize, minValue, maxValue, numberOfBins, std::move(readBinDimension));

    // Creating the worker context and the pipeline necessary for testing the sampling
    auto workerContext = std::make_shared<Runtime::WorkerContext>(0, bufferManager, 100);
    auto handlerIndex = 0;
    auto opHandler = std::make_shared<EquiWidth1DHistOperatorHandler>();
    std::vector<Runtime::Execution::OperatorHandlerPtr> opHandlers = {opHandler};
    auto pipelineContext = std::make_shared<MockedPipelineExecutionContext>(opHandlers);
    Runtime::Execution::ExecutionContext executionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
    Nautilus::Value<Nautilus::MemRef>((int8_t*) pipelineContext.get()));


    // Setting up the synopsis and creating the local operator state
    histSynopsis.setup(handlerIndex, executionContext);
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
    histSynopsis.addToSynopsis(handlerIndex, executionContext, record1, opState.get());
    histSynopsis.addToSynopsis(handlerIndex, executionContext, record2, opState.get());
    histSynopsis.addToSynopsis(handlerIndex, executionContext, record3, opState.get());
    histSynopsis.addToSynopsis(handlerIndex, executionContext, record4, opState.get());


    auto approximateBuffers = histSynopsis.getApproximate(handlerIndex, executionContext, bufferManager);
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(approximateBuffers[0],
                                                                                              outputSchema);

    EXPECT_EQ(dynamicBuffer.getNumberOfTuples(), numberOfBins);
    EXPECT_EQ(dynamicBuffer[0][approximateString].read<uint64_t>(), 1);
    EXPECT_EQ(dynamicBuffer[0][lowerBoundBinName].read<uint64_t>(), 0);
    EXPECT_EQ(dynamicBuffer[0][upperBoundBinName].read<uint64_t>(), 1);

    EXPECT_EQ(dynamicBuffer[1][approximateString].read<uint64_t>(), 0);
    EXPECT_EQ(dynamicBuffer[1][lowerBoundBinName].read<uint64_t>(), 1);
    EXPECT_EQ(dynamicBuffer[1][upperBoundBinName].read<uint64_t>(), 2);

    EXPECT_EQ(dynamicBuffer[2][approximateString].read<uint64_t>(), 1);
    EXPECT_EQ(dynamicBuffer[2][lowerBoundBinName].read<uint64_t>(), 2);
    EXPECT_EQ(dynamicBuffer[2][upperBoundBinName].read<uint64_t>(), 3);

    EXPECT_EQ(dynamicBuffer[3][approximateString].read<uint64_t>(), 0);
    EXPECT_EQ(dynamicBuffer[3][lowerBoundBinName].read<uint64_t>(), 3);
    EXPECT_EQ(dynamicBuffer[3][upperBoundBinName].read<uint64_t>(), 4);

    EXPECT_EQ(dynamicBuffer[4][approximateString].read<uint64_t>(), 2);
    EXPECT_EQ(dynamicBuffer[4][lowerBoundBinName].read<uint64_t>(), 4);
    EXPECT_EQ(dynamicBuffer[4][upperBoundBinName].read<uint64_t>(), 5);

}

} // namespace NES::ASP
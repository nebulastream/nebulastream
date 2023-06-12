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
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Experimental/Synopses/Sketches/CountMinOperatorHandler.hpp>
#include <Experimental/Synopses/Sketches/CountMin.hpp>

#include <memory>
#include <Experimental/Benchmarking/MicroBenchmarkSchemas.hpp>

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

class CountMinTest : public Testing::NESBaseTest {
 public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("CountMinTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO2("Setup CountMinTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NESBaseTest::SetUp();
        NES_INFO2("Setup CountMinTest test case.");
        bufferManager = std::make_shared<Runtime::BufferManager>();
        readKeyExpression = std::make_unique<Runtime::Execution::Expressions::ReadFieldExpression>(idString);

        // Creating the worker context and the pipeline necessary for testing the sampling
        opHandler = std::make_shared<CountMinOperatorHandler>();
        std::vector<Runtime::Execution::OperatorHandlerPtr> opHandlers = {opHandler};
        workerContext = std::make_shared<Runtime::WorkerContext>(0, bufferManager, 100);
        pipelineContext = std::make_shared<MockedPipelineExecutionContext>(opHandlers);
        executionContext = std::make_unique<Runtime::Execution::ExecutionContext>(
        Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
        Nautilus::Value<Nautilus::MemRef>((int8_t*) pipelineContext.get()));


        inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
            ->addField(idString, BasicType::UINT64)
            ->addField(aggregationString, BasicType::FLOAT64)
            ->addField(timestampFieldName, BasicType::UINT64);
    }

    Runtime::BufferManagerPtr bufferManager;

    // Input and output variables
    const std::string idString = "id";
    const std::string aggregationString = "value";
    const std::string approximateString = "aggregation";
    const std::string timestampFieldName = "ts";
    const std::string keyFieldName = "key";
    const uint64_t numberOfRows = 3;
    const uint64_t numberOfCols = 5;
    const uint64_t handlerIndex = 0;

    SchemaPtr inputSchema;
    std::unique_ptr<Runtime::Execution::Expressions::ReadFieldExpression> readKeyExpression;
    std::shared_ptr<CountMinOperatorHandler> opHandler;
    std::unique_ptr<Runtime::Execution::ExecutionContext> executionContext;
    Runtime::WorkerContextPtr workerContext;
    std::shared_ptr<MockedPipelineExecutionContext> pipelineContext;

};


std::vector<Nautilus::Record> getInputData(Schema& inputSchema) {
    return {
        Nautilus::Record({
            {inputSchema.get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 0)},
            {inputSchema.get(1)->getName(), Nautilus::Value<Nautilus::Double>((double_t) 42)},
            {inputSchema.get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 0)}
        }),

        Nautilus::Record({
            {inputSchema.get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 2)},
            {inputSchema.get(1)->getName(), Nautilus::Value<Nautilus::Double>((double_t) 1234)},
            {inputSchema.get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 1)}
        }),

        Nautilus::Record({
            {inputSchema.get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 4)},
            {inputSchema.get(1)->getName(), Nautilus::Value<Nautilus::Double>((double_t) 404)},
            {inputSchema.get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 1)}
        }),

        Nautilus::Record({
            {inputSchema.get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 4)},
            {inputSchema.get(1)->getName(), Nautilus::Value<Nautilus::Double>((double_t) 101)},
            {inputSchema.get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) 1)}
        })
    };
}

TEST_F(CountMinTest, countMinTestCount) {
    auto aggregationType = Parsing::Aggregation_Type::COUNT;
    const auto entrySize = sizeof(uint64_t);
    auto outputSchema = Benchmarking::getOutputSchemaFromTypeAndInputSchema(aggregationType, *inputSchema,
                                                                            idString, aggregationString,
                                                                            idString, approximateString);

    // Creating aggregation config and the histogram
    auto aggregationConfig = Parsing::SynopsisAggregationConfig::create(aggregationType, idString,  aggregationString,
                                                                        approximateString, timestampFieldName,
                                                                        inputSchema, outputSchema);
    CountMin countMin(aggregationConfig, numberOfRows, numberOfCols, entrySize);


    // Setting up the synopsis and creating the local operator state
    countMin.setup(handlerIndex, *executionContext);
    auto h3SeedsMemRef = Nautilus::Value<Nautilus::MemRef>((int8_t*) opHandler->getH3SeedsRef());
    auto sketchMemRef = Nautilus::Value<Nautilus::MemRef>((int8_t*) opHandler->getSketchRef());
    auto sketch = Nautilus::Interface::Fixed2DArrayRef(sketchMemRef, entrySize, numberOfCols);
    auto opState = std::make_unique<CountMin::LocalCountMinState>(sketch, h3SeedsMemRef);

    // Inserting records
    for (auto& record : getInputData(*inputSchema)) {
        countMin.addToSynopsis(handlerIndex, *executionContext, record, opState.get());
    }

    // Creating query keys for all histograms
    std::vector<Nautilus::Value<>> queryKeys = {Nautilus::Value<>((int64_t)0), Nautilus::Value<>((int64_t)1),
                                                Nautilus::Value<>((int64_t)2), Nautilus::Value<>((int64_t)3),
                                                Nautilus::Value<>((int64_t)4),
    };

    auto approximateBuffers = countMin.getApproximate(handlerIndex, *executionContext, queryKeys, bufferManager);
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(approximateBuffers[0],
                                                                                              outputSchema);


    // Checking for correctness
    EXPECT_EQ(dynamicBuffer[0][idString].read<int64_t>(), 0);
    EXPECT_EQ(dynamicBuffer[0][approximateString].read<double_t>(), 1);

    EXPECT_EQ(dynamicBuffer[1][idString].read<int64_t>(), 1);
    EXPECT_EQ(dynamicBuffer[1][approximateString].read<double_t>(), 0);

    EXPECT_EQ(dynamicBuffer[2][idString].read<int64_t>(), 2);
    EXPECT_EQ(dynamicBuffer[2][approximateString].read<double_t>(), 1);

    EXPECT_EQ(dynamicBuffer[3][idString].read<int64_t>(), 3);
    EXPECT_EQ(dynamicBuffer[3][approximateString].read<double_t>(), 0);

    EXPECT_EQ(dynamicBuffer[4][idString].read<int64_t>(), 4);
    EXPECT_EQ(dynamicBuffer[4][approximateString].read<double_t>(), 2);
}

} // namespace NES::ASP
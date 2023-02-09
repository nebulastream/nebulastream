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

#ifdef USE_BABELFISH
#include <Experimental/Babelfish/BabelfishPipelineCompilerBackend.hpp>
#endif
#include <API/Schema.hpp>
#include <Experimental/ExecutionEngine/CompilationBasedPipelineExecutionEngine.hpp>
#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/InterpretationBasedPipelineExecutionEngine.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Interpreter/Expressions/ArithmeticalExpression/AddExpression.hpp>
#include <Experimental/Interpreter/Expressions/ArithmeticalExpression/MulExpression.hpp>
#include <Experimental/Interpreter/Expressions/ArithmeticalExpression/SubExpression.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Experimental/Interpreter/Operators/Aggregation/AvgFunction.hpp>
#include <Experimental/Interpreter/Operators/GroupedAggregation.hpp>
#include <Experimental/Utility/TPCHUtil.hpp>
#include <Util/Timer.hpp>
#include <Util/UtilityFunctions.hpp>
#ifdef USE_FLOUNDER
#include <Experimental/Flounder/FlounderPipelineCompilerBackend.hpp>
#endif
#include <Execution/Expressions/ConstantIntegerExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/UDFCallExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Operators/Aggregation.hpp>
#include <Experimental/Interpreter/Operators/Aggregation/AggregationFunction.hpp>
#include <Experimental/Interpreter/Operators/Emit.hpp>
#include <Experimental/Interpreter/Operators/Join/JoinBuild.hpp>
#include <Experimental/Interpreter/Operators/Join/JoinProbe.hpp>
#include <Experimental/Interpreter/Operators/Map.hpp>
#include <Experimental/Interpreter/Operators/Scan.hpp>
#include <Experimental/Interpreter/Operators/Selection.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#ifdef USE_MLIR
#include <Nautilus/Backends/MLIR/MLIRPipelineCompilerBackend.hpp>
#include <Nautilus/Backends/MLIR/MLIRUtility.hpp>
#endif
#include <Experimental/Interpreter/Operators/Streaming/WindowAggregation.hpp>
#include <Experimental/Runtime/RuntimeExecutionContext.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <algorithm>
#include <execinfo.h>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>

using namespace NES::Nautilus;
namespace NES::Nautilus {

/**
 * @brief This test tests query execution using th mlir backend
 */
class UDFTest : public Testing::NESBaseTest,
                public ::testing::WithParamInterface<std::tuple<std::string, Schema::MemoryLayoutType>> {
  public:
    Tracing::SSACreationPhase ssaCreationPhase;
    Tracing::TraceToIRConversionPhase irCreationPhase;
    std::shared_ptr<ExecutionEngine::Experimental::PipelineExecutionEngine> executionEngine;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("UDFExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup UDFExecutionTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        auto param = this->GetParam();
        auto compiler = std::get<0>(param);
        NES_INFO("Setup Query6Test test case." << compiler);
        if (compiler == "INTERPRETER") {
            executionEngine = std::make_shared<InterpretationBasedPipelineExecutionEngine>();
        } else if (compiler == "MLIR") {
#ifdef USE_MLIR
            auto backend = std::make_shared<Nautilus::Backends::MLIR::MLIRPipelineCompilerBackend>();
            executionEngine = std::make_shared<CompilationBasedPipelineExecutionEngine>(backend);
#endif
        } else if (compiler == "FLOUNDER") {
#ifdef USE_FLOUNDER
            auto backend = std::make_shared<FlounderPipelineCompilerBackend>();
            executionEngine = std::make_shared<CompilationBasedPipelineExecutionEngine>(backend);
#endif
        } else if (compiler == "BABELFISH") {
#ifdef USE_BABELFISH
            auto backend = std::make_shared<BabelfishPipelineCompilerBackend>();
            executionEngine = std::make_shared<CompilationBasedPipelineExecutionEngine>(backend);
#endif
        }
        if (executionEngine == nullptr) {
            GTEST_SKIP_("No backend found");
        }
    }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Tear down UDFExecutionTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down UDFExecutionTest test class."); }
};

SchemaPtr getSchema() {
    return Schema::create()
        ->addField("user_id", INT64)
        ->addField("page_id", INT64)
        ->addField("campaign_id", INT64)
        ->addField("ad_type", INT64)
        ->addField("event_type", INT64)
        ->addField("current_ms", INT64)
        ->addField("ip", INT64)
        ->addField("d1", INT64)
        ->addField("d2", INT64)
        ->addField("d3", INT32)
        ->addField("d4", INT16);
}

std::vector<Runtime::TupleBuffer>
createData(uint64_t numberOfBuffers, Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout, Runtime::BufferManagerPtr bm) {

    std::vector<Runtime::TupleBuffer> buffers;
    for (uint64_t currentBuffer = 0; currentBuffer < numberOfBuffers; currentBuffer++) {
        auto buffer = bm->getUnpooledBuffer(memoryLayout->getBufferSize()).value();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (uint64_t currentRecord = 0; currentRecord < dynamicBuffer.getCapacity(); currentRecord++) {
            auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::high_resolution_clock::now().time_since_epoch())
                          .count();
            auto campaign_id = rand() % 100;
            auto event_type = currentRecord % 3;
            dynamicBuffer[currentRecord]["user_id"].write<int64_t>(1);
            dynamicBuffer[currentRecord]["page_id"].write<int64_t>(0);
            dynamicBuffer[currentRecord]["campaign_id"].write<int64_t>(campaign_id);
            dynamicBuffer[currentRecord]["ad_type"].write<int64_t>(0);
            dynamicBuffer[currentRecord]["event_type"].write<int64_t>(event_type);
            dynamicBuffer[currentRecord]["current_ms"].write<int64_t>(100);
            dynamicBuffer[currentRecord]["ip"].write<int64_t>(0x01020304);
            dynamicBuffer[currentRecord]["d1"].write<int64_t>(1);
            dynamicBuffer[currentRecord]["d2"].write<int64_t>(1);
            dynamicBuffer[currentRecord]["d3"].write<int32_t>(1);
            dynamicBuffer[currentRecord]["d4"].write<int16_t>(1);
        }
        dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
        buffers.emplace_back(buffer);
    }
    return buffers;
}

TEST_P(UDFTest, distanceUDF) {
    uint64_t bufferSize = 1000000;
    uint64_t warmup = 100000;
    uint64_t iterations = 100;
    auto bm = std::make_shared<Runtime::BufferManager>(bufferSize);

    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(getSchema(), bufferSize);
    auto data = createData(1, memoryLayout, bm);

    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);

    Scan scan = Scan(memoryLayout, {4});
    auto readCampaignId = std::make_shared<ReadFieldExpression>(0);
    std::vector<ExpressionPtr> arguments = {readCampaignId, readCampaignId, readCampaignId, readCampaignId};
    auto udfCallExpression = std::make_shared<UDFCallExpression>(arguments, "Distance", "distance", "(JJJJ)J");
    auto writeExpression = std::make_shared<WriteFieldExpression>(0, udfCallExpression);
    auto mapOperator = std::make_shared<Map>(writeExpression);
    scan.setChild(mapOperator);

    auto resultSchema = Schema::create()->addField("user_id", INT64);
    auto resMem = Runtime::MemoryLayouts::RowLayout::create(resultSchema, bm->getBufferSize());
    auto aggField = std::make_shared<ReadFieldExpression>(0);
    auto sumAggFunction = std::make_shared<SumFunction>(aggField, IR::Types::StampFactory::createInt64Stamp());
    std::vector<std::shared_ptr<AggregationFunction>> aggfunctions = {sumAggFunction};
    auto aggregation = std::make_shared<Aggregation>(aggfunctions);
    scan.setChild(aggregation);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(&scan);

    auto executablePipeline = executionEngine->compile(pipeline);

    executablePipeline->setup();

#ifdef USE_BABELFISH
    for (auto i = 0ull; i < warmup; i++) {
        for (auto& buffer : data) {
            executablePipeline->execute(*runtimeWorkerContext, buffer);
        }
    }
#endif

    Timer timer("QueryExecutionTime");
    timer.start();
    for (auto i = 0ull; i < iterations; i++) {
        for (auto& buffer : data) {
            executablePipeline->execute(*runtimeWorkerContext, buffer);
        }
    }
    timer.snapshot("Execute");
    timer.pause();

    NES_INFO(timer);
    auto processedTuples = data.size() * memoryLayout->getCapacity() * iterations;
    double recordsPerMs = (double) processedTuples / timer.getPrintTime();
    NES_INFO("ProcessedTuple: " << processedTuples << " recordsPerMs: " << recordsPerMs
                                << " Throughput: " << (recordsPerMs * 1000));
}

TEST_P(UDFExecutionTest, longAggregationUDFQueryTest) {

    auto bm = std::make_shared<Runtime::BufferManager>();

    // use 100 mb buffer
    auto buffer = bm->getUnpooledBuffer(1024 * 1024 * 100).value();
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, buffer.getBufferSize());
    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);

    Scan scan = Scan(memoryLayout);

    // map
    auto field_0 = std::make_shared<ReadFieldExpression>(0);
    auto udfCallExpression = std::make_shared<UDFCallExpression>(field_0);
    auto writeExpression = std::make_shared<WriteFieldExpression>(0, udfCallExpression);
    auto mapOperator = std::make_shared<Map>(writeExpression);
    scan.setChild(mapOperator);
    auto aggField = std::make_shared<ReadFieldExpression>(0);
    auto sumAggFunction = std::make_shared<SumFunction>(aggField, IR::Types::StampFactory::createInt64Stamp());
    std::vector<std::shared_ptr<AggregationFunction>> functions = {sumAggFunction};
    auto aggregation = std::make_shared<Aggregation>(functions);
    mapOperator->setChild(aggregation);
    // auto backend = std::make_shared<Nautilus::Backends::MLIR::MLIRPipelineCompilerBackend>();
    //auto backend = std::make_shared<FlounderPipelineCompilerBackend>();
    auto executionEngine = CompilationBasedPipelineExecutionEngine(backend);
    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(&scan);

    auto executablePipeline = executionEngine.compile(pipeline);

    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (auto i = 0ull; i < dynamicBuffer.getCapacity() - 1; i++) {
        dynamicBuffer[i]["f1"].write((int64_t) 1);
    }
    dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity() - 1);

    for (auto i = 0; i < 100; i++) {
        Timer timer("QueryExecutionTime");
        timer.start();
        executablePipeline->setup();
        executablePipeline->execute(*runtimeWorkerContext, buffer);
        auto globalState = (GlobalAggregationState*) executablePipeline->getExecutionContext()->getGlobalOperatorState(0);
        auto sumState = (GlobalSumState*) globalState->threadLocalAggregationSlots[0].get();
        NES_INFO2("Result {}", sumState->sum);
        timer.snapshot("QueryExecutionTime");
        timer.pause();
        NES_INFO("QueryExecutionTime: " << timer);
    }
}

TEST_P(UDFTest, crimeIndexUDF) {
    uint64_t bufferSize = 1000000;
    uint64_t iterations = 100;
    auto bm = std::make_shared<Runtime::BufferManager>(bufferSize);

    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(getSchema(), bufferSize);
    auto data = createData(1, memoryLayout, bm);

    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);

    Scan scan = Scan(memoryLayout, {4});
    auto readCampaignId = std::make_shared<ReadFieldExpression>(0);
    std::vector<ExpressionPtr> arguments = {readCampaignId, readCampaignId, readCampaignId};
    auto udfCallExpression = std::make_shared<UDFCallExpression>(arguments, "CrimeIndex", "crimeIndex", "(JJJ)J");
    auto writeExpression = std::make_shared<WriteFieldExpression>(0, udfCallExpression);
    auto mapOperator = std::make_shared<Map>(writeExpression);
    scan.setChild(mapOperator);

    auto resultSchema = Schema::create()->addField("crimeIndex", INT64);
    auto resMem = Runtime::MemoryLayouts::RowLayout::create(resultSchema, bm->getBufferSize());

    auto emit = std::make_shared<Emit>(resMem);
    mapOperator->setChild(emit);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(&scan);

    auto executablePipeline = executionEngine->compile(pipeline);

    executablePipeline->setup();

    Timer timer("QueryExecutionTime");
    timer.start();
    for (auto i = 0ull; i < iterations; i++) {
        for (auto& buffer : data) {
            executablePipeline->execute(*runtimeWorkerContext, buffer);
        }
    }
    timer.snapshot("Execute");
    timer.pause();

    NES_INFO(timer);
    auto processedTuples = data.size() * memoryLayout->getCapacity() * iterations;
    double recordsPerMs = (double) processedTuples / timer.getPrintTime();
    NES_INFO("ProcessedTuple: " << processedTuples << " recordsPerMs: " << recordsPerMs
                                << " Throughput: " << (recordsPerMs * 1000));
}
#ifdef USE_BABELFISH
INSTANTIATE_TEST_CASE_P(testUDF,
                        UDFTest,
                        ::testing::Combine(::testing::Values("BABELFISH"),
                                           ::testing::Values(Schema::MemoryLayoutType::ROW_LAYOUT)),
                        [](const testing::TestParamInfo<UDFTest::ParamType>& info) {
                            auto layout = std::get<1>(info.param);
                            if (layout == Schema::ROW_LAYOUT) {
                                return std::get<0>(info.param) + "_ROW";
                            } else {
                                return std::get<0>(info.param) + "_COLUMNAR";
                            }
                        });

#else
INSTANTIATE_TEST_CASE_P(testUDF,
                        UDFTest,
                        ::testing::Combine(::testing::Values("INTERPRETER", "MLIR", "FLOUNDER"),
                                           ::testing::Values(Schema::MemoryLayoutType::ROW_LAYOUT)),
                        [](const testing::TestParamInfo<UDFTest::ParamType>& info) {
                            auto layout = std::get<1>(info.param);
                            if (layout == Schema::ROW_LAYOUT) {
                                return std::get<0>(info.param) + "_ROW";
                            } else {
                                return std::get<0>(info.param) + "_COLUMNAR";
                            }
                        });

#endif

}// namespace NES::Nautilus
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

#include "Experimental/ExecutionEngine/CompilationBasedPipelineExecutionEngine.hpp"
#include "Experimental/ExecutionEngine/ExecutablePipeline.hpp"
#include "Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp"
#include "Runtime/MemoryLayout/DynamicTupleBuffer.hpp"
#include "Util/Timer.hpp"
#include "Util/UtilityFunctions.hpp"
#include <API/Schema.hpp>
#include <Experimental/Interpreter/DataValue/MemRef.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Experimental/Interpreter/Expressions/ReadFieldExpression.hpp>
#include <Experimental/Interpreter/FunctionCall.hpp>
#include <Experimental/Interpreter/Operators/Aggregation.hpp>
#include <Experimental/Interpreter/Operators/AggregationFunction.hpp>
#include <Experimental/Interpreter/Operators/Emit.hpp>
#include <Experimental/Interpreter/Operators/Scan.hpp>
#include <Experimental/Interpreter/Operators/CustomScan.hpp>
#include <Experimental/Interpreter/Operators/Selection.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Experimental/MLIR/MLIRUtility.hpp>
#include <Experimental/NESIR/Phases/LoopInferencePhase.hpp>
#include <Experimental/Trace/ExecutionTrace.hpp>
#include <Experimental/Trace/Phases/SSACreationPhase.hpp>
#include <Experimental/Trace/Phases/TraceToIRConversionPhase.hpp>
#include <Experimental/Trace/TraceContext.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <cstdint>
#include <execinfo.h>
#include <gtest/gtest.h>
#include <memory>
#include <fstream>

#include <Experimental/Utility/TestUtility.hpp>
#include <Experimental/Interpreter/ProxyFunctions.hpp>
#include <numeric>
#include <string>


namespace NES::ExecutionEngine::Experimental::Interpreter {
class InliningBenchmark : public testing::Test {
  public:
    Trace::SSACreationPhase ssaCreationPhase;
    Trace::TraceToIRConversionPhase irCreationPhase;
    IR::LoopInferencePhase loopInferencePhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TraceTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup TraceTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup TraceTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down TraceTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down TraceTest test class." << std::endl; }


};

auto loadLineItemTable(std::shared_ptr<Runtime::BufferManager> bm) {
    std::ifstream inFile("/home/pgrulich/projects/tpch-dbgen/lineitem.tbl");
    uint64_t linecount = 0;
    std::string line;
    while (std::getline(inFile, line)) {
        // using printf() in all tests for consistency
        linecount++;
    }
    NES_DEBUG("LOAD lineitem with " << linecount << " lines");
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);

    schema->addField("l_quantity", BasicType::INT64);
    auto targetBufferSize = schema->getSchemaSizeInBytes() * linecount;
    auto buffer = bm->getUnpooledBuffer(targetBufferSize).value();
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, buffer.getBufferSize());
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);

    inFile.clear();// clear fail and eof bits
    inFile.seekg(0, std::ios::beg);

    int currentLineCount = 0;
    printf("Current LineCount: %ld\n", linecount);
    while (std::getline(inFile, line)) {
        if(!(currentLineCount % 60000)) {
            printf("Current LineCount: %f\n", currentLineCount/60012.150);
        }
        ++currentLineCount;
        // using printf() in all tests for consistency
        auto index = dynamicBuffer.getNumberOfTuples();
        auto strings = NES::Util::splitWithStringDelimiter<std::string>(line, "|");

        auto l_quantityString = strings[4];
        int64_t l_quantity = std::stoi(l_quantityString);
        dynamicBuffer[index][0].write(l_quantity);

        dynamicBuffer.setNumberOfTuples(index + 1);
    }
    inFile.close();
    NES_DEBUG("Loading of Lineitem done");
    return std::make_pair(memoryLayout, dynamicBuffer);
}

/*
BUGS:
Value<UInt64> sumLoop(int upperLimit) {
    Value<UInt64> runningValue((uint64_t) 0);
    for (Value<UInt64> i = (uint64_t)0; i < (uint64_t)upperLimit; i = i + (uint64_t)1) {
        for (Value<UInt64> start = (uint64_t)0; start < (uint64_t)upperLimit; start = start + (uint64_t)1) {
            runningValue = runningValue + (uint64_t)1;
        }
    }
    return runningValue;
}
*/

//Currently we are printing the hash results -> no constant folding.
Value<UInt64> sumLoop(int upperLimit) {
    Value<UInt64> test((uint64_t) 1);
    // for (; test < (uint64_t)upperLimit;) {
    for (Value<UInt64> start = (uint64_t)0; start < (uint64_t)upperLimit; start = start + (uint64_t)1) {
        // test = test + FunctionCall<>("getHash", Runtime::ProxyFunctions::getHash, test);
    }

    return test;
}


TEST_F(InliningBenchmark, DISABLED_sumLoopTestSCF) {
    auto execution = Trace::traceFunctionSymbolicallyWithReturn([]() {
        return sumLoop(100);
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << *execution.get() << std::endl;
    auto ir = irCreationPhase.apply(execution);
    std::cout << ir->toString() << std::endl;
    ir = loopInferencePhase.apply(ir);
    std::cout << ir->toString() << std::endl;

    // create and print MLIR
    auto mlirUtility = new MLIR::MLIRUtility("", false);
    MLIR::MLIRUtility::DebugFlags df = {false, false, false};
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, true);
    auto engine = mlirUtility->prepareEngine(false);
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 101);
}

// void memScanOnly(Value<MemRef> ptr, Value<Int64> size) {
//     for (auto i = Value(0l); i < size; i = i + 1l) {
//         auto address = ptr + i * 8l;
//         auto value = address.as<MemRef>().load<Int64>();
//         FunctionCall<>("printInt64", NES::Runtime::ProxyFunctions::printInt64, value);
//     }
// }

Value<Int64> memScanAgg(Value<MemRef> ptr, Value<Int64> size) {
    Value<Int64> sum = 0l;
    for (auto i = Value(0l); i < size; i = i + 1l) {
        auto address = ptr + i * 8l;
        auto value = address.as<MemRef>().load<Int64>();
        auto hashResult = FunctionCall<>("getHash", NES::Runtime::ProxyFunctions::getHash, value);
        sum = sum + hashResult;
    }
    return sum;
}

TEST_F(InliningBenchmark, memScanFunctionTest) {
    // Setup test for proxy inlining with reduced and non-reduced proxy file.
    auto testUtility = std::make_unique<NES::ExecutionEngine::Experimental::TestUtility>();
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto lineitemBuffer = loadLineItemTable(bm);
    auto mlirUtility = new MLIR::MLIRUtility("", false);
    auto buffer = lineitemBuffer.second.getBuffer();

    //Setup timing, and results logging.
    const int NUM_ITERATIONS = 10;
    const int NUM_SNAPSHOTS = 7;
    const std::string RESULTS_FILE_NAME = "inlining/proxyFunctionsSize/results.csv";
    const std::vector<std::string> snapshotNames {
        "Symbolic Execution Trace     ", 
        "SSA Phase                    ", 
        "IR Created                   ", 
        "MLIR Created                 ", 
        "MLIR Compiled to Function Ptr", 
        "Executed                     ",
        "Overall Time                 "
    };
    std::vector<std::vector<double>> runningSnapshotVectors(NUM_SNAPSHOTS);

    // Execute workload NUM_ITERATIONS number of times.
    for(int i = 0; i < NUM_ITERATIONS; ++i) {
        Timer timer("Hash Result Aggregation Timer Nr." + std::to_string(i));
        // Set up empty values for symbolic execution
        timer.start();
        auto memPtr = Value<MemRef>(nullptr);
        memPtr.ref = Trace::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createAddressStamp());
        auto size = Value<Int64>(0l);
        size.ref = Trace::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createInt64Stamp());
        auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([&memPtr, &size]() {
            return memScanAgg(memPtr, size);
        });
        timer.snapshot(snapshotNames.at(0));

        executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
        timer.snapshot(snapshotNames.at(1));
        auto ir = irCreationPhase.apply(executionTrace);
        timer.snapshot(snapshotNames.at(2));

        // create and print MLIR
        int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, false);
        timer.snapshot(snapshotNames.at(3));
        auto engine = mlirUtility->prepareEngine(false);
        auto function = (int64_t(*)(int, void*)) engine->lookup("execute").get();
        timer.snapshot(snapshotNames.at(4));

        function(buffer.getNumberOfTuples(), buffer.getBuffer());

        // Wrap up timing
        timer.snapshot(snapshotNames.at(5));
        timer.pause();
        auto snapshots = timer.getSnapshots();
        NES_INFO(timer);
        std::cout << "Overall time: " << timer.getPrintTime();
        NES_DEBUG("Snapshot length: "<< snapshots.size());
        for(int snapShotIndex = 0; snapShotIndex < NUM_SNAPSHOTS-1; ++snapShotIndex) {
            runningSnapshotVectors.at(snapShotIndex).emplace_back(snapshots[snapShotIndex].getPrintTime());
        }
        runningSnapshotVectors.at(NUM_SNAPSHOTS-1).emplace_back(timer.getPrintTime());
    }
    testUtility->produceResults(runningSnapshotVectors, snapshotNames, RESULTS_FILE_NAME);
}

// Scan -> Map //-> Emit
TEST_F(InliningBenchmark, DISABLED_aggregateHashes) {
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto lineitemBuffer = loadLineItemTable(bm);

    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);
    CustomScan scan = CustomScan(lineitemBuffer.first);

    // auto aggField = std::make_shared<ReadFieldExpression>(0);
    // auto sumAggFunction = std::make_shared<SumFunction>(aggField, IR::Types::StampFactory::createInt64Stamp());
    // std::vector<std::shared_ptr<AggregationFunction>> functions = {sumAggFunction};
    // auto aggregation = std::make_shared<Aggregation>(functions);
    // scan.setChild(aggregation);

    auto readShipdate = std::make_shared<ReadFieldExpression>(0);
    auto selection1 = std::make_shared<Selection>(readShipdate);
    scan.setChild(selection1);
    // selection1->setChild(aggregation);

    auto executionEngine = CompilationBasedPipelineExecutionEngine();
    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(&scan);

    auto executablePipeline = executionEngine.compile(pipeline);

    executablePipeline->setup();

    auto buffer = lineitemBuffer.second.getBuffer();
    Timer timer("QueryExecutionTime");
    timer.start();
    executablePipeline->execute(*runtimeWorkerContext, buffer);
    timer.snapshot("QueryExecutionTime");
    timer.pause();
    NES_INFO("QueryExecutionTime: " << timer);

    auto globalState = (GlobalAggregationState*) executablePipeline->getExecutionContext()->getGlobalOperatorState(0);
    auto sumState = (GlobalSumState*) globalState->threadLocalAggregationSlots[0].get();
    ASSERT_EQ(sumState->sum, (int64_t) 1995906217); //Scale 0.01
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter
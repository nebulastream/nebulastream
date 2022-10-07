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

#include "Experimental/ExecutionEngine/InterpretationBasedPipelineExecutionEngine.hpp"
#include "Experimental/Interpreter/Expressions/ArithmeticalExpression/AddExpression.hpp"
#include "Experimental/Interpreter/Expressions/ArithmeticalExpression/MulExpression.hpp"
#include "Experimental/Interpreter/Expressions/ArithmeticalExpression/SubExpression.hpp"
#include "Experimental/Interpreter/Expressions/LogicalExpressions/AndExpression.hpp"
#include "Experimental/Interpreter/Operators/Aggregation/AvgFunction.hpp"
#include "Experimental/Interpreter/Operators/GroupedAggregation.hpp"
#include "Experimental/Utility/TestUtility.hpp"
#include "Nautilus/Backends/MLIR/LLVMIROptimizer.hpp"
#include "Util/Timer.hpp"
#include "Util/UtilityFunctions.hpp"
#include <API/Schema.hpp>
#include <Experimental/ExecutionEngine/CompilationBasedPipelineExecutionEngine.hpp>
#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Utility/TPCHUtil.hpp>
#include <Experimental/IR/Phases/LoopInferencePhase.hpp>

#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Expressions/ConstantIntegerExpression.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Experimental/Interpreter/Expressions/ReadFieldExpression.hpp>
#include <Experimental/Interpreter/Expressions/UDFCallExpression.hpp>
#include <Experimental/Interpreter/Expressions/WriteFieldExpression.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
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
#include <mlir/IR/MLIRContext.h>
#ifdef USE_MLIR
#include <Nautilus/Backends/MLIR/MLIRPipelineCompilerBackend.hpp>
#include <Nautilus/Backends/MLIR/MLIRUtility.hpp>
#endif
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
#include <algorithm>
#include <execinfo.h>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>

// #include <Experimental/Utility/TestUtility.hpp>
#include <Experimental/Interpreter/ProxyFunctions.hpp>
#include <numeric>
// #include <string>


using namespace NES::Nautilus;
namespace NES::ExecutionEngine::Experimental::Interpreter {
class ScanQuery : public testing::Test {
  public:
    Tracing::SSACreationPhase ssaCreationPhase;
    Tracing::TraceToIRConversionPhase irCreationPhase;
    Nautilus::IR::LoopInferencePhase loopInferencePhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ScanQuery.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup ScanQuery test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup ScanQuery test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down ScanQuery test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down ScanQuery test class." << std::endl; }


};

//Todo allow loading specific column via TestUtility
auto loadLineItemTable(std::shared_ptr<Runtime::BufferManager> bm) {
    // Todo handle filepaths
    std::ifstream inFile("/home/alepping/tpch/dbgen/lineitem.tbl");
    uint64_t linecount = 0;
    std::string line;
    while (std::getline(inFile, line)) {
        // using printf() in all tests for consistency
        linecount++;
    }
    NES_DEBUG("LOAD lineitem with " << linecount << " lines");

    // schema->addField("l_comment", DataTypeFactory::createFixedChar(8));
    // schema->addField("l_comment", BasicType::UINT64);
    // schema->addField("l_quantity", BasicType::FLOAT64);
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

    int runningSum = 0;
    while (std::getline(inFile, line)) {
        if(!(currentLineCount % 60000)) {
            printf("Current LineCount: %f\n", currentLineCount/60012.150);
        }
        ++currentLineCount;
        // using printf() in all tests for consistency
        auto index = dynamicBuffer.getNumberOfTuples();
        auto strings = NES::Util::splitWithStringDelimiter<std::string>(line, "|");

        auto l_quantityString = strings[4];
        // printf("Comment string: %s\n", l_quantityString.c_str());
        int64_t l_quantity = std::stoi(l_quantityString);
        dynamicBuffer[index][0].write(l_quantity);
        // double l_quantityAsDouble = (double) l_quantity;
        // dynamicBuffer[index][0].write(l_quantityAsDouble);
        runningSum += l_quantity;

        dynamicBuffer.setNumberOfTuples(index + 1);
    }
    std::cout << "Loaded Buffer Num Tuples: " << dynamicBuffer.getNumberOfTuples() << '\n';
    std::cout << "Final Running Sum is: " << runningSum << '\n';
    inFile.close();
    NES_DEBUG("Loading of Lineitem done");
    return std::make_pair(memoryLayout, dynamicBuffer);
}

void scan(Value<Int64> size, Value<MemRef> inPtr, Value<MemRef> outPtr) {
    auto inputPtr = inPtr;
    auto outputPtr = outPtr;
    // Value<Int64> result = 0l;
    for (Value<Int64> i = 0l; i < size; i = i + 1l) {
        auto inAddress = inputPtr + i * 8l;
        auto outAddress = outputPtr + i * 8l;
        Value<Int64> value = inAddress.as<MemRef>().load<Int64>();
        outAddress.as<MemRef>().store<Int64>(value);
    }
}

// Struct to convert TupleBufferPtrs to Memrefs in MLIR. 'N' is the dimension of the Memref.
// template<typename T, size_t N>
// struct MemRefDescriptor {
//     T *allocated;
//     T *aligned;
//     intptr_t offset;
//     intptr_t sizes[N];
//     intptr_t strides[N];
// };

//==-------------------------------------------------------------==//
//==-------------- MLIR OPTIMIZATION BENCHMARKS ---------------==//
//==-----------------------------------------------------------==//

TEST_F(ScanQuery, scanBenchmark) {
    // Setup test for proxy inlining with reduced and non-reduced proxy file.
    auto testUtility = std::make_unique<NES::ExecutionEngine::Experimental::TestUtility>();
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto lineitemBuffer = loadLineItemTable(bm);
    auto buffer = lineitemBuffer.second.getBuffer();
    auto outBuffer = bm->getUnpooledBuffer(buffer.getBufferSize()).value();

    //Setup timing, and results logging.
    auto CONF = testUtility->getTestParamaterConfig("scan.csv");
    std::vector<std::vector<double>> runningSnapshotVectors(CONF->NUM_SNAPSHOTS);

    // Execute workload NUM_ITERATIONS number of times.
    for(int i = 0; i < CONF->NUM_ITERATIONS; ++i) {
        auto timer = std::make_shared<Timer<>>("Scan Map Benchmark Timer Nr." + std::to_string(i));

        // Set up empty values for symbolic execution
        timer->start();
        auto inPtr = Value<MemRef>(nullptr);
        auto outPtr = Value<MemRef>(nullptr);
        inPtr.ref = Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createAddressStamp());
        outPtr.ref = Tracing::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createAddressStamp());
        auto size = Value<Int64>(0l);
        size.ref = Tracing::ValueRef(INT32_MAX, 2, IR::Types::StampFactory::createInt64Stamp());
        std::shared_ptr<Tracing::ExecutionTrace> executionTrace;
        std::cout << "Creating Execution Trace.\n";
        // executionTrace = Tracing::traceFunctionSymbolicallyWithReturn([]() {
        executionTrace = Tracing::traceFunctionSymbolically([&size, &inPtr, &outPtr]() {
            // return standardDeviationAggregation(memPtr, size);
            scan(size, inPtr, outPtr);
            // return standardDeviationAggregation();
        });
        std::cout << "Created Execution Trace.\n";
        timer->snapshot(CONF->snapshotNames.at(0));

        // Create SSA from trace.
        executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
        timer->snapshot(CONF->snapshotNames.at(1));

        // Create Nautilus IR from SSA trace.
        auto ir = irCreationPhase.apply(executionTrace);
        timer->snapshot(CONF->snapshotNames.at(2));

        // Create MLIR
        mlir::MLIRContext context;
        auto engine = Backends::MLIR::MLIRUtility::jitCompileNESIR(ir, context, CONF->OPT_LEVEL, CONF->PERFORM_INLINING, timer);
        // auto module = Backends::MLIR::MLIRUtility::loadMLIRModuleFromNESIR(ir, context);
        // timer->snapshot(CONF->snapshotNames.at(3));

        // Compile MLIR -> return function pointer
        // auto engine = Backends::MLIR::MLIRUtility::lowerAndCompileMLIRModuleToMachineCode(module, CONF->OPT_LEVEL, CONF->PERFORM_INLINING, timer);
        auto function = (void(*)(int, void*, void*)) engine->lookup("execute").get();
        timer->snapshot(CONF->snapshotNames.at(6)); // LLVM Compilation

        // Execute function
        int64_t scanMapResult = 0;
        function(buffer.getNumberOfTuples(), outBuffer.getBuffer(), buffer.getBuffer()); // Wrong order on purpose.
        timer->snapshot(CONF->snapshotNames.at(7)); // Execute

        // Print aggregation result to force execution.
        std::cout << "Output Buffer at n-1: " << outBuffer.getBuffer<int64_t>()[buffer.getNumberOfTuples()-1] << '\n';

        // Wrap up timing
        timer->pause();
        NES_DEBUG("Overall time: " << timer->getPrintTime());
        if(!CONF->IS_PERFORMANCE_BENCHMARK || i >= (CONF->NUM_ITERATIONS / 3)) {
            auto snapshots = timer->getSnapshots();
            std::cout << "num snapshots: " << snapshots.size() << '\n';
            for(int snapShotIndex = 0; snapShotIndex < CONF->NUM_SNAPSHOTS-1; ++snapShotIndex) {
                runningSnapshotVectors.at(snapShotIndex).emplace_back(snapshots[snapShotIndex].getPrintTime());
            }
            runningSnapshotVectors.at(CONF->NUM_SNAPSHOTS-1).emplace_back(timer->getPrintTime());
        }
    }
    testUtility->produceResults(runningSnapshotVectors, CONF->snapshotNames, CONF->RESULTS_FILE_NAME, CONF->IS_PERFORMANCE_BENCHMARK);
}


}// namespace NES::ExecutionEngine::Experimental::Interpreter

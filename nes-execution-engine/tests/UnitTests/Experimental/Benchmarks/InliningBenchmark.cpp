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

#include "Common/DataTypes/DataTypeFactory.hpp"
#include "Experimental/ExecutionEngine/CompilationBasedPipelineExecutionEngine.hpp"
#include "Experimental/ExecutionEngine/ExecutablePipeline.hpp"
#include "Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp"
#include "Experimental/Interpreter/DataValue/Float/Double.hpp"
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
#include <cstdio>
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

//Todo allow loading specific column via TestUtility
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

    // schema->addField("l_comment", DataTypeFactory::createFixedChar(8));
    // schema->addField("l_comment", BasicType::UINT64);
    // schema->addField("l_quantity", BasicType::FLOAT64);
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

        
        //Todo enable loading comments into TupleBuffer
        // auto l_commentString = strings[15];
        // std::string test = std::to_string(index);
        // char charArray[8];
	    // std::strcpy(charArray, test.c_str());
        // dynamicBuffer[index][0].write(charArray);

        dynamicBuffer.setNumberOfTuples(index + 1);
    }
    std::cout << "Loaded Buffer Num Tuples: " << dynamicBuffer.getNumberOfTuples() << '\n';
    std::cout << "Final Running Sum is: " << runningSum << '\n';
    inFile.close();
    NES_DEBUG("Loading of Lineitem done");
    return std::make_pair(memoryLayout, dynamicBuffer);
}

Value<Double> standardDeviationAggregation(Value<MemRef> ptr, Value<Int64> size) {
    auto meanPtr = ptr;
    // 1. calculate mean via a single proxy call
    Value<Double> mean  = FunctionCall<>("standardDeviationGetMean", NES::Runtime::ProxyFunctions::standardDeviationGetMean, size, meanPtr);

    // 2. Aggregate squared difference between mean and values
    auto deviationPtr = ptr;
    Value<Double> runningDeviationSum = 0.0;
    for (Value<Int64> i = 0l; i < size; i = i + 1l) {
        auto address = deviationPtr + i * 8l;
        auto value = address.as<MemRef>().load<Int64>();
        runningDeviationSum = FunctionCall<>("standardDeviationGetVariance", NES::Runtime::ProxyFunctions::standardDeviationGetVariance, runningDeviationSum, mean, value);
    }

    // //3. get root of aggregated squared values and divide by num elements
    return FunctionCall<>("standardDeviationGetStdDev", NES::Runtime::ProxyFunctions::standardDeviationGetStdDev, runningDeviationSum, size);
}

// Struct to convert TupleBufferPtrs to Memrefs in MLIR. 'N' is the dimension of the Memref.
template<typename T, size_t N>
struct MemRefDescriptor {
    T *allocated;
    T *aligned;
    intptr_t offset;
    intptr_t sizes[N];
    intptr_t strides[N];
};

//==-------------------------------------------------------------==//
//==-------------- MLIR OPTIMIZATION BENCHMARKS ---------------==//
//==-----------------------------------------------------------==//
TEST_F(InliningBenchmark, mlirOptimizationMemrefApproach) {
    //Todo can we actually skip adding symbols for functions
    auto testUtility = std::make_unique<NES::ExecutionEngine::Experimental::TestUtility>();
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto lineitemBuffer = loadLineItemTable(bm);
    auto mlirUtility = new MLIR::MLIRUtility("/home/rudi/dima/nebulastream/cmake-build-debug/nes-execution-engine/tests/UnitTests/Experimental/Benchmarks/mlirCode/memrefLoad.mlir", true);
    auto buffer = lineitemBuffer.second.getBuffer();

    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nullptr, nullptr, false);
     // Compile MLIR -> return function pointer
    auto engine = mlirUtility->prepareEngine(false);
    auto function = (double(*)(int64_t, NES::ExecutionEngine::Experimental::Interpreter::MemRefDescriptor<long, 1> *)) engine->lookup("_mlir_ciface_execute").get();
    
    const int NUM_ITERATIONS = 1;

    //Populate MemrefDescriptor
    auto int64BufferPtr = buffer.getBuffer<int64_t>();
    //x1 Num Tuples: 6001215, x1 Sum: 153078795 -- x001 Num Tuples: 60175, x001 Sum: 
    auto resultMemref = new MemRefDescriptor<int64_t, 1>{int64BufferPtr, int64BufferPtr, 0, {153078795}, {1}};
    std::cout << "Strides: " << resultMemref->strides[0] << '\n';

    // Execute function
    double executionTimeSum = 0.0;
    for(int i = 0; i < NUM_ITERATIONS; ++i) {
        Timer timer("Hash Result Aggregation Timer Nr." + std::to_string(i));
        timer.start();
        double stdDeviationResult = function(buffer.getNumberOfTuples(), resultMemref);
        timer.pause();  
        // Print aggregation result to force execution.
        // std::cout << "Standard Deviation Result: " << stdDeviationResult << '\n';
        executionTimeSum += timer.getPrintTime();
    }
    std::cout << "Memref Execution Time Average: " << executionTimeSum / (double)NUM_ITERATIONS << '\n';
}

TEST_F(InliningBenchmark, DISABLED_mlirOptimizationStandardDev) {
    //Todo can we actually skip adding symbols for functions
    auto testUtility = std::make_unique<NES::ExecutionEngine::Experimental::TestUtility>();
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto lineitemBuffer = loadLineItemTable(bm);
    // auto mlirUtility = new MLIR::MLIRUtility("/home/rudi/dima/nebulastream/cmake-build-debug/nes-execution-engine/tests/UnitTests/Experimental/Benchmarks/mlirCode/simpleSum.mlir", true);
    // auto mlirUtility = new MLIR::MLIRUtility("/home/rudi/dima/nebulastream/cmake-build-debug/nes-execution-engine/tests/UnitTests/Experimental/Benchmarks/mlirCode/deviationLoop.mlir", true);
    // auto mlirUtility = new MLIR::MLIRUtility("/home/rudi/dima/nebulastream/cmake-build-debug/nes-execution-engine/tests/UnitTests/Experimental/Benchmarks/mlirCode/deviationLoop_backup.mlir", true);
    auto mlirUtility = new MLIR::MLIRUtility("/home/rudi/dima/nebulastream/cmake-build-debug/nes-execution-engine/tests/UnitTests/Experimental/Benchmarks/mlirCode/deviationLoop_loops_unoptimized.mlir", true);
    auto buffer = lineitemBuffer.second.getBuffer();

    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nullptr, nullptr, false);
     // Compile MLIR -> return function pointer
    auto engine = mlirUtility->prepareEngine(false);
    auto function = (double(*)(int64_t, void*)) engine->lookup("execute").get();
    // auto function = (double(*)(int, void*)) engine->lookup("execute").get();
    
    const int NUM_ITERATIONS = 100;

    //Populate MemrefDescriptor
    auto bufferPtr = buffer.getBuffer<void*>();

    // Execute function
    double executionTimeSum = 0.0;
    for(int i = 0; i < NUM_ITERATIONS; ++i) {
        Timer timer("Hash Result Aggregation Timer Nr." + std::to_string(i));
        timer.start();
        // double stdDeviationResult = function(resultBuffer);
        double stdDeviationResult = function(buffer.getNumberOfTuples(), bufferPtr);
        // double stdDeviationResult = function(buffer.getNumberOfTuples(), buffer.getBuffer());
        timer.pause();  
        // Print aggregation result to force execution.
        // std::cout << "Standard Deviation Result: " << stdDeviationResult << '\n';
        // std::cout << "Final Timer: " << stdDeviationResult << '\n';
        executionTimeSum += timer.getPrintTime();
    }
    // Affine Optimized For approach: 9.76605, 1000: 9.64763
    // SCF Unoptimized: 11.0659, 1000: 9.83526
    // Control Flow (not inlined) approach: 22.8029, 25.2842
    // Control Flow (inlined) approach: 10.1949
    std::cout << "No Memref Execution Time Average: " << executionTimeSum / (double)NUM_ITERATIONS << '\n';
}

//==-------------------------------------------------------------==//
//==-------------- ALGEBRAIC FUNCTION BENCHMARKS ---------------==//
//==-----------------------------------------------------------==//
TEST_F(InliningBenchmark, DISABLED_algebraicFunctionBenchmark) {
    // Setup test for proxy inlining with reduced and non-reduced proxy file.
    auto testUtility = std::make_unique<NES::ExecutionEngine::Experimental::TestUtility>();
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto lineitemBuffer = loadLineItemTable(bm);
    auto mlirUtility = new MLIR::MLIRUtility("", false);
    auto buffer = lineitemBuffer.second.getBuffer();

    //Setup timing, and results logging.
    const bool PERFORM_INLINING = true;
    const int NUM_ITERATIONS = 10;
    const int NUM_SNAPSHOTS = 7;
    const std::string RESULTS_FILE_NAME = "algebraicFunctionBenchmark.csv";
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
        std::shared_ptr<NES::ExecutionEngine::Experimental::Trace::ExecutionTrace> executionTrace;
        executionTrace = Trace::traceFunctionSymbolicallyWithReturn([&memPtr, &size]() {
            return standardDeviationAggregation(memPtr, size);
        });
        timer.snapshot(snapshotNames.at(0));

        // Create SSA from trace.
        executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
        timer.snapshot(snapshotNames.at(1));

        // Create Nautilus IR from SSA trace.
        auto ir = irCreationPhase.apply(executionTrace);
        timer.snapshot(snapshotNames.at(2));

        // Create MLIR
        int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, false);
        timer.snapshot(snapshotNames.at(3));

        // Compile MLIR -> return function pointer
        auto engine = mlirUtility->prepareEngine(PERFORM_INLINING);
        auto function = (double(*)(int, void*)) engine->lookup("execute").get();
        timer.snapshot(snapshotNames.at(4));

        // Execute function
        double stdDeviationResult = function(buffer.getNumberOfTuples(), buffer.getBuffer());
        timer.snapshot(snapshotNames.at(5));

        // Print aggregation result to force execution.
        std::cout << "Standard Deviation Result: " << stdDeviationResult << '\n';

        // Wrap up timing
        timer.pause();
        NES_DEBUG("Overall time: " << timer.getPrintTime());
        auto snapshots = timer.getSnapshots();
        for(int snapShotIndex = 0; snapShotIndex < NUM_SNAPSHOTS-1; ++snapShotIndex) {
            runningSnapshotVectors.at(snapShotIndex).emplace_back(snapshots[snapShotIndex].getPrintTime());
        }
        runningSnapshotVectors.at(NUM_SNAPSHOTS-1).emplace_back(timer.getPrintTime());
    }
    testUtility->produceResults(runningSnapshotVectors, snapshotNames, RESULTS_FILE_NAME);
}


//==--------------------------------------------------------------==//
//==-------------- STRING MANIPULATION BENCHMARKS ---------------==//
//==------------------------------------------------------------==//
void stringManipulationConstSize(Value<MemRef> ptr, Value<Int64> size) {
    Value<Int64> stringSize = 10l;
    for (Value<Int64> i = 0l; i < size; i = i + 1l) {
        FunctionCall<>("stringToUpperCaseConstSize", Runtime::ProxyFunctions::stringToUpperCaseConstSize, i, ptr, stringSize);
    }
}
void stringManipulation(Value<MemRef> ptr, Value<Int64> size) {
    for (Value<Int64> i = 0l; i < size; i = i + 1l) {
        FunctionCall<>("stringToUpperCase", Runtime::ProxyFunctions::stringToUpperCase, i, ptr);
    }
}
TEST_F(InliningBenchmark, DISABLED_stringManipulationBenchmark) {
    auto mlirUtility = new MLIR::MLIRUtility("", false);
    auto testUtility = std::make_unique<NES::ExecutionEngine::Experimental::TestUtility>();
    // Get comment strings from Lineitem table and fill array of char pointers with it.
    auto lineitemStrings = testUtility->loadStringsFromLineitemTable();
    // avoid loading array on the small stack -> heap instead
//    char *langStrings  = new char[lineitemStrings.size()];
    char **langStrings  = new char*[lineitemStrings.size()];
    // malloc/alloc/new char
    for(size_t i = 0; i < lineitemStrings.size(); ++i) {
        langStrings[i] = lineitemStrings.at(i).data();
    }

    //Setup timing, and results logging.
    const bool PERFORM_INLINING = false;
    const int NUM_ITERATIONS = 100;
    const int NUM_SNAPSHOTS = 7;
    const std::string RESULTS_FILE_NAME = "stringManipulationBenchmark.csv";
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
        auto executionTrace = Trace::traceFunctionSymbolically([&memPtr, &size]() {
            stringManipulation(memPtr, size);
        });
        timer.snapshot(snapshotNames.at(0));

        executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
        timer.snapshot(snapshotNames.at(1));

        auto ir = irCreationPhase.apply(executionTrace);
        timer.snapshot(snapshotNames.at(2));

        // create and print MLIR
        int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, false);
        timer.snapshot(snapshotNames.at(3));

        auto engine = mlirUtility->prepareEngine(PERFORM_INLINING);
        auto function = (int64_t(*)(int, void*)) engine->lookup("execute").get(); //reverse argument order
        timer.snapshot(snapshotNames.at(4));

        // Execute to uppercase benchmark function.
        function(lineitemStrings.size(), langStrings);
        timer.snapshot(snapshotNames.at(5));

        timer.pause();
        NES_DEBUG("Overall time: " << timer.getPrintTime());
        auto snapshots = timer.getSnapshots();
        for(int snapShotIndex = 0; snapShotIndex < NUM_SNAPSHOTS-1; ++snapShotIndex) {
            runningSnapshotVectors.at(snapShotIndex).emplace_back(snapshots[snapShotIndex].getPrintTime());
        }
        runningSnapshotVectors.at(NUM_SNAPSHOTS-1).emplace_back(timer.getPrintTime());

    }
    testUtility->produceResults(runningSnapshotVectors, snapshotNames, RESULTS_FILE_NAME);


    // Print random manipulated string to force execution.
    srand((unsigned) time(0));
    int result = (rand() % lineitemStrings.size()-1);
    std::cout << "Random Uppercase String: " << langStrings[result] << '\n';
}


//==----------------------------------------------------------==//
//==-------------- HASH AGGREGATION BENCHMARKS --------------==//
//==--------------------------------------------------------==//
Value<Int64> murmurHashAggregation(Value<MemRef> ptr, Value<Int64> size) {
    Value<Int64> sum = 0l;
    for (auto i = Value(0l); i < size; i = i + 1l) {
        auto address = ptr + i * 8l;
        auto value = address.as<MemRef>().load<Int64>();
       auto hashResult = FunctionCall<>("getMurMurHash", NES::Runtime::ProxyFunctions::getMurMurHash, value);
//        auto secondHashResult = hashResult + FunctionCall<>("getCRC32Hash", NES::Runtime::ProxyFunctions::getCRC32Hash, value, value);
        // auto hashResult = FunctionCall<>("getCRC32Hash", NES::Runtime::ProxyFunctions::getCRC32Hash, value, value);
//        hashResult = secondHashResult - hashResult;
        sum = sum + hashResult;
    }
    return sum;
}

Value<Int64> crc32HashAggregation(Value<MemRef> ptr, Value<Int64> size) {
    Value<Int64> sum = 0l;
    for (auto i = Value(0l); i < size; i = i + 1l) {
        auto address = ptr + i * 8l;
        auto value = address.as<MemRef>().load<Int64>();
        auto hashResult = FunctionCall<>("getCRC32Hash", NES::Runtime::ProxyFunctions::getCRC32Hash, value, value);
        sum = sum + hashResult;
    }
    return sum;
}

TEST_F(InliningBenchmark, DISABLED_crc32HashAggregationBenchmark) {
    // Setup test for proxy inlining with reduced and non-reduced proxy file.
    auto testUtility = std::make_unique<NES::ExecutionEngine::Experimental::TestUtility>();
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto lineitemBuffer = loadLineItemTable(bm);
    auto mlirUtility = new MLIR::MLIRUtility("", false);
    auto buffer = lineitemBuffer.second.getBuffer();

    //Setup timing, and results logging.
    const bool PERFORM_INLINING = false;
    const bool USE_CRC32_HASH_FUNCTION = false; //ELSE: We are using the MurMur hash function.
    const int NUM_ITERATIONS = 100;
    const int NUM_SNAPSHOTS = 7;
    std::string RESULTS_FILE_NAME;
    if(USE_CRC32_HASH_FUNCTION) {
        RESULTS_FILE_NAME = "crc32HashAggregationBenchmark.csv";
    } else {
        RESULTS_FILE_NAME = "murmurHashAggregationBenchmark.csv";
    }
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
        std::shared_ptr<NES::ExecutionEngine::Experimental::Trace::ExecutionTrace> executionTrace;
        if(USE_CRC32_HASH_FUNCTION) {
            executionTrace = Trace::traceFunctionSymbolicallyWithReturn([&memPtr, &size]() {
                return crc32HashAggregation(memPtr, size);
            });
        } else {
            executionTrace = Trace::traceFunctionSymbolicallyWithReturn([&memPtr, &size]() {
                return murmurHashAggregation(memPtr, size);
            });
        }
        timer.snapshot(snapshotNames.at(0));

        // Create SSA from trace.
        executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
        timer.snapshot(snapshotNames.at(1));

        // Create Nautilus IR from SSA trace.
        auto ir = irCreationPhase.apply(executionTrace);
        timer.snapshot(snapshotNames.at(2));

        // Create MLIR
        int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, false);
        timer.snapshot(snapshotNames.at(3));

        // Compile MLIR -> return function pointer
        auto engine = mlirUtility->prepareEngine(PERFORM_INLINING);
        auto function = (int64_t(*)(int, void*)) engine->lookup("execute").get();
        timer.snapshot(snapshotNames.at(4));

        // Execute function
        int64_t hashAggregationResult = function(buffer.getNumberOfTuples(), buffer.getBuffer());
        timer.snapshot(snapshotNames.at(5));

        // Print aggregation result to force execution.
        std::cout << "Result: " << hashAggregationResult << '\n';

        // Wrap up timing
        timer.pause();
        NES_DEBUG("Overall time: " << timer.getPrintTime());
        auto snapshots = timer.getSnapshots();
        for(int snapShotIndex = 0; snapShotIndex < NUM_SNAPSHOTS-1; ++snapShotIndex) {
            runningSnapshotVectors.at(snapShotIndex).emplace_back(snapshots[snapShotIndex].getPrintTime());
        }
        runningSnapshotVectors.at(NUM_SNAPSHOTS-1).emplace_back(timer.getPrintTime());
    }
    testUtility->produceResults(runningSnapshotVectors, snapshotNames, RESULTS_FILE_NAME);
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter

/*
BUGS:
- Cannot call functions that take doubles as args or return doubles
- Cannot run two consecutive loops
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

/*
BACKUP:
    Value<Int64> runningSum = 0l; //1536127 (x0.01)
    for (Value<Int64> i = 0l; i < size; i = i + 1l) {
        auto address = sumPtr + i * 8l;
        auto value = address.as<MemRef>().load<Int64>();
        // runningSum = FunctionCall<>("standardDeviationOne", NES::Runtime::ProxyFunctions::standardDeviationOne, runningSum, value);
        runningSum = FunctionCall<>("standardDeviationOne", NES::Runtime::ProxyFunctions::standardDeviationOne, runningSum, value);
    }
*/
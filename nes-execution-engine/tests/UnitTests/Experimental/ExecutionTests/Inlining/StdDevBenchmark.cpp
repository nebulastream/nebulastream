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
class StdDevBenchmark : public testing::Test {
  public:
    Tracing::SSACreationPhase ssaCreationPhase;
    Tracing::TraceToIRConversionPhase irCreationPhase;
    Nautilus::IR::LoopInferencePhase loopInferencePhase;
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
// auto loadLineItemTable(std::shared_ptr<Runtime::BufferManager> bm) {
//     // Todo handle filepaths
//     std::ifstream inFile("/home/alepping/tpch/dbgen/lineitem.tbl");
//     uint64_t linecount = 0;
//     std::string line;
//     while (std::getline(inFile, line)) {
//         // using printf() in all tests for consistency
//         linecount++;
//     }
//     NES_DEBUG("LOAD lineitem with " << linecount << " lines");

//     // schema->addField("l_comment", DataTypeFactory::createFixedChar(8));
//     // schema->addField("l_comment", BasicType::UINT64);
//     // schema->addField("l_quantity", BasicType::FLOAT64);
//     auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
//     schema->addField("l_quantity", BasicType::INT64);
//     auto targetBufferSize = schema->getSchemaSizeInBytes() * linecount;
//     auto buffer = bm->getUnpooledBuffer(targetBufferSize).value();
//     auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, buffer.getBufferSize());
//     auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);


//     inFile.clear();// clear fail and eof bits
//     inFile.seekg(0, std::ios::beg);

//     int currentLineCount = 0;
//     printf("Current LineCount: %ld\n", linecount);

//     int runningSum = 0;
//     while (std::getline(inFile, line)) {
//         if(!(currentLineCount % 60000)) {
//             printf("Current LineCount: %f\n", currentLineCount/60012.150);
//         }
//         ++currentLineCount;
//         // using printf() in all tests for consistency
//         auto index = dynamicBuffer.getNumberOfTuples();
//         auto strings = NES::Util::splitWithStringDelimiter<std::string>(line, "|");

//         auto l_quantityString = strings[4];
//         // printf("Comment string: %s\n", l_quantityString.c_str());
//         int64_t l_quantity = std::stoi(l_quantityString);
//         dynamicBuffer[index][0].write(l_quantity);
//         // double l_quantityAsDouble = (double) l_quantity;
//         // dynamicBuffer[index][0].write(l_quantityAsDouble);
//         runningSum += l_quantity;


//         //Todo enable loading comments into TupleBuffer
//         // auto l_commentString = strings[15];
//         // std::string test = std::to_string(index);
//         // char charArray[8];
// 	    // std::strcpy(charArray, test.c_str());
//         // dynamicBuffer[index][0].write(charArray);

//         dynamicBuffer.setNumberOfTuples(index + 1);
//     }
//     std::cout << "Loaded Buffer Num Tuples: " << dynamicBuffer.getNumberOfTuples() << '\n';
//     std::cout << "Final Running Sum is: " << runningSum << '\n';
//     inFile.close();
//     NES_DEBUG("Loading of Lineitem done");
//     return std::make_pair(memoryLayout, dynamicBuffer);
// }

template<typename T, size_t N>
struct MemRefDescriptor {
    T *allocated;
    T *aligned;
    intptr_t offset;
    intptr_t sizes[N];
    intptr_t strides[N];
};

TEST_F(StdDevBenchmark, mlirDoubleColumnAggregation) {
    auto testUtility = std::make_unique<NES::ExecutionEngine::Experimental::TestUtility>();
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto lineitemBuffer = testUtility->loadLineItemTable(bm);
    auto buffer = lineitemBuffer.second.getBuffer();
    auto outBuffer = bm->getUnpooledBuffer(buffer.getBufferSize()).value();

    // std::string modulePath ="/home/alepping/MLIRCode/loadAddStore.mlir";
    // std::string modulePath ="/home/alepping/MLIRCode/loadAddStoreOptimized.mlir"; //6.9231
    // std::string modulePath ="/home/alepping/MLIRCode/autoVecUnrollScan.mlir"; //10.2724


    const int NUM_ITERATIONS = 1000; //Todo

    // //Populate MemrefDescriptor
    auto int64BufferPtr = buffer.getBuffer<int64_t>();
    auto int64OutBufferPtr = outBuffer.getBuffer<int64_t>();
    auto bufferMemref = new MemRefDescriptor<int64_t, 1>{int64BufferPtr, int64BufferPtr, 0, {static_cast<intptr_t>(buffer.getNumberOfTuples())}, {1}};
    //==-------------------------------------------------------==//
    //==-------------- MemRef MLIR Unoptimized ----------------==//
    //==-------------------------------------------------------==//
    // Todo: note in Evaluation: -affine-loop-unroll -scf-parallel-loop-tiling --scf-for-loop-canonicalization --scf-for-loop-peeling
    mlir::MLIRContext context;
    std::string modulePath ="/home/alepping/MLIRCode/stdDevMemrefUnoptimized.mlir";
    mlir::OwningOpRef<mlir::ModuleOp> module = Backends::MLIR::MLIRUtility::loadMLIRModuleFromFilePath(modulePath, context);
    auto engine = Backends::MLIR::MLIRUtility::lowerAndCompileMLIRModuleToMachineCode(module, Nautilus::Backends::MLIR::LLVMIROptimizer::None);
    auto function = (double(*)(int64_t, NES::ExecutionEngine::Experimental::Interpreter::MemRefDescriptor<long, 1> *)) engine->lookup("_mlir_ciface_execute").get();

    // Execute function
    double executionTimeSum = 0.0;
    double resultSum = 0.0;
    for(int i = 0; i < NUM_ITERATIONS; ++i) {
        Timer timer("MLIR Benchmark Timer." + std::to_string(i));
        timer.start();
        double result = function(buffer.getNumberOfTuples(), bufferMemref);
        timer.pause();
        executionTimeSum += timer.getPrintTime();
        resultSum += result;
    }
    std::cout << "\n\n--------------------------------------\n";
    std::cout << "Standard Deviation Result: " << resultSum << '\n';
    std::cout << "MemRef MLIR Optimized Total Time Sum: " << executionTimeSum << '\n';
    std::cout << "MemRef MLIR Optimized Execution Time Average: " << executionTimeSum / (double)NUM_ITERATIONS << '\n';


    //==-----------------------------------------==//
    //==------------ MemRef LLVM O3 -------------==//
    //==-----------------------------------------==//
    mlir::MLIRContext contextO3;
    modulePath ="/home/alepping/MLIRCode/stdDevMemrefUnoptimized.mlir";
    module = Backends::MLIR::MLIRUtility::loadMLIRModuleFromFilePath(modulePath, contextO3);
    engine = Backends::MLIR::MLIRUtility::lowerAndCompileMLIRModuleToMachineCode(module, Nautilus::Backends::MLIR::LLVMIROptimizer::O3);
    auto functionO3 = (double(*)(int64_t, NES::ExecutionEngine::Experimental::Interpreter::MemRefDescriptor<long, 1> *)) engine->lookup("_mlir_ciface_execute").get();
    // Execute function
    executionTimeSum = 0.0;
    resultSum = 0.0;
    for(int i = 0; i < NUM_ITERATIONS; ++i) {
        Timer timer("MLIR Benchmark Timer." + std::to_string(i));
        timer.start();
        double result = functionO3(buffer.getNumberOfTuples(), bufferMemref);
        timer.pause();
        executionTimeSum += timer.getPrintTime();
        resultSum += result;
    }
    std::cout << "\n\n--------------------------------------\n";
    std::cout << "Standard Deviation Result: " << resultSum << '\n';
    std::cout << "STDev LLVM O3 Total Time Sum: " << executionTimeSum << '\n';
    std::cout << "STDev LLVM O3 Execution Time Average: " << executionTimeSum / (double)NUM_ITERATIONS << '\n';


    //==------------------------------------------------==//
    //==------------ MemRef MLIR Optimized -------------==//
    //==------------------------------------------------==//
    // auto function2 = (double(*)(int64_t, void*)) engine->lookup("_mlir_ciface_execute").get();
    // modulePath ="/home/alepping/MLIRCode/deviationLoop_loops_unoptimized.mlir";
    mlir::MLIRContext context2;
    modulePath ="/home/alepping/MLIRCode/stdDevMemrefOptimized.mlir";
    module = Backends::MLIR::MLIRUtility::loadMLIRModuleFromFilePath(modulePath, context2);
    engine = Backends::MLIR::MLIRUtility::lowerAndCompileMLIRModuleToMachineCode(module, Nautilus::Backends::MLIR::LLVMIROptimizer::None);
    auto function2 = (double(*)(int64_t, NES::ExecutionEngine::Experimental::Interpreter::MemRefDescriptor<long, 1> *)) engine->lookup("_mlir_ciface_execute").get();
    // Execute function
    executionTimeSum = 0.0;
    resultSum = 0.0;
    for(int i = 0; i < NUM_ITERATIONS; ++i) {
        Timer timer("MLIR Benchmark Timer." + std::to_string(i));
        timer.start();
        double result = function2(buffer.getNumberOfTuples(), bufferMemref);
        timer.pause();
        executionTimeSum += timer.getPrintTime();
        resultSum += result;
    }
    std::cout << "\n\n--------------------------------------\n";
    std::cout << "Standard Deviation Result: " << resultSum << '\n';
    std::cout << "NES BufferPtr Total Time Sum: " << executionTimeSum << '\n';
    std::cout << "NES BufferPtr Execution Time Average: " << executionTimeSum / (double)NUM_ITERATIONS << '\n';


    //==------------------------------------------------------==//
    //==------------ Manually Vectorized POINTER -------------==//
    //==------------------------------------------------------==//
    mlir::MLIRContext context3;
    modulePath ="/home/alepping/MLIRCode/stdDevMemrefVectorized.mlir";
    module = Backends::MLIR::MLIRUtility::loadMLIRModuleFromFilePath(modulePath, context3);
    engine = Backends::MLIR::MLIRUtility::lowerAndCompileMLIRModuleToMachineCode(module, Nautilus::Backends::MLIR::LLVMIROptimizer::None);
    auto function3 = (double(*)(int64_t, NES::ExecutionEngine::Experimental::Interpreter::MemRefDescriptor<long, 1> *)) engine->lookup("_mlir_ciface_execute").get();

    // Execute function
    executionTimeSum = 0.0;
    resultSum = 0.0;
    for(int i = 0; i < NUM_ITERATIONS; ++i) {
        Timer timer("MLIR Benchmark Timer." + std::to_string(i));
        timer.start();
        double result = function3(buffer.getNumberOfTuples(), bufferMemref);
        timer.pause();
        executionTimeSum += timer.getPrintTime();
        resultSum += result;
    }
    std::cout << "\n\n--------------------------------------\n";
    std::cout << "Standard Deviation Result: " << resultSum << '\n';
    std::cout << "MemRef Manually Vectorized Total Time Sum: " << executionTimeSum << '\n';
    std::cout << "MemRef Manually Vectorized Execution Time Average: " << executionTimeSum / (double)NUM_ITERATIONS << '\n';
}
}
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

#include <API/Schema.hpp>
#include <Experimental/Interpreter/DataValue/Integer.hpp>
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
#include <execinfo.h>
#include <gtest/gtest.h>
#include <memory>

namespace NES::ExecutionEngine::Experimental::Interpreter {
class MemoryAccessExecutionTest : public testing::Test {
  public:
    Trace::SSACreationPhase ssaCreationPhase;
    Trace::TraceToIRConversionPhase irCreationPhase;
    IR::LoopInferencePhase loopInferencePhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MemoryAccessExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup MemoryAccessExecutionTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup MemoryAccessExecutionTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down MemoryAccessExecutionTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down MemoryAccessExecutionTest test class." << std::endl; }
};

Value<> loadFunction(Value<MemRef> ptr) { return ptr.load<Integer>(); }

TEST_F(MemoryAccessExecutionTest, loadFunctionTest) {
    int64_t valI = 42;
    Value<> tempPara = Value<MemRef>(std::make_unique<MemRef>((int8_t*) &valI));
    // create fake ref TODO improve handling of parameters
    tempPara.ref = Trace::ValueRef(INT32_MAX, 0, IR::Operations::INT8PTR);
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([&tempPara]() {
        return loadFunction(tempPara);
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

    // create and print MLIR
    auto mlirUtility = new MLIR::MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, false);
    auto engine = mlirUtility->prepareEngine();
    auto function = (int64_t(*)(void*)) engine->lookup("execute").get();
    ASSERT_EQ(function(&valI), 42);
}

void storeFunction(Value<MemRef> ptr) {
    auto value = ptr.load<Integer>();
    auto tmp = value + 1;
    ptr.store(tmp);
}

TEST_F(MemoryAccessExecutionTest, storeFunctionTest) {
    int64_t valI = 42;
    auto tempPara = Value<MemRef>((int8_t*) &valI);
    tempPara.load<Integer>();
    // create fake ref TODO improve handling of parameters
    tempPara.ref = Trace::ValueRef(INT32_MAX, 0, IR::Operations::INT8PTR);
    auto executionTrace = Trace::traceFunctionSymbolically([&tempPara]() {
        storeFunction(tempPara);
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

    // create and print MLIR
    auto mlirUtility = new MLIR::MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, false);
    auto engine = mlirUtility->prepareEngine();
    auto function = (int64_t(*)(void*)) engine->lookup("execute").get();
    function(&valI);
    ASSERT_EQ(valI, 43);
}

Value<Integer> memScan(Value<MemRef> ptr, Value<Integer> size) {
    Value<Integer> sum = 0;
    for (auto i = Value(0); i < size; i = i + 1) {
        auto address = ptr + i * 8;
        auto value = address.as<MemRef>().load<Integer>();
        sum = sum + value;
    }
    return sum;
}

TEST_F(MemoryAccessExecutionTest, memScanFunctionTest) {
    auto memPtr = Value<MemRef>(nullptr);
    memPtr.ref = Trace::ValueRef(INT32_MAX, 0, IR::Operations::INT8PTR);
    auto size = Value<Integer>(0);
    size.ref = Trace::ValueRef(INT32_MAX, 1, IR::Operations::INT64);
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([&memPtr, &size]() {
        return memScan(memPtr, size);
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

    // create and print MLIR
    auto mlirUtility = new MLIR::MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, false);
    auto engine = mlirUtility->prepareEngine();
    auto function = (int64_t(*)(int, void*)) engine->lookup("execute").get();

    auto array = new int64_t[]{1, 2, 3, 4, 5, 6, 7};
    ASSERT_EQ(function(7, array), 28);
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter
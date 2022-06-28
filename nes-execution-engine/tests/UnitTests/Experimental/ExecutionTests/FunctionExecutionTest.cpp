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
#include <Experimental/Interpreter/FunctionCall.hpp>
#include <Experimental/NESIR/ProxyFunctions.hpp>
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
#include <Experimental/Interpreter/ProxyFunctions.hpp>
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
class FunctionExecutionTest : public testing::Test {
  public:
    Trace::SSACreationPhase ssaCreationPhase;
    Trace::TraceToIRConversionPhase irCreationPhase;
    IR::LoopInferencePhase loopInferencePhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("FunctionExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup FunctionExecutionTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup TraceTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down TraceTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down TraceTest test class." << std::endl; }
};

int64_t addInt(int64_t x, int64_t y){
    return x + y;
};

Value<> addIntFunction() {
    auto x = Value<Integer>(2);
    auto y = Value<Integer>(3);
    Value<Integer> res = FunctionCall<>("add", addInt, x, y);
    return res;
}

TEST_F(FunctionExecutionTest, addIntFunctionTest) {

    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([]() {
        return addIntFunction();
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
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 5);
}

int64_t returnConst(){
    return 42;
};

Value<> returnConstFunction() {
    Value<Integer> res = FunctionCall<>("returnConst", returnConst);
    return res;
}

TEST_F(FunctionExecutionTest, returnConstFunctionTest) {

    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([]() {
        return returnConstFunction();
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
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_EQ(function(), 42);
}

void voidException(){
    NES_THROW_RUNTIME_ERROR("An expected exception");
};


void voidExceptionFunction() {
    FunctionCall<>("voidException", voidException);
}

TEST_F(FunctionExecutionTest, voidExceptionFunctionTest) {

    auto executionTrace = Trace::traceFunctionSymbolically([]() {
        voidExceptionFunction();
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
    auto function = (int64_t(*)()) engine->lookup("execute").get();
    ASSERT_ANY_THROW(function());
}

int64_t multiplyArgument(int64_t x){
    return x * 10;
};

Value<> multiplyArgumentFunction(Value<Integer> x) {
    Value<Integer> res = FunctionCall<>("multiplyArgument", multiplyArgument, x);
    return res;
}

TEST_F(FunctionExecutionTest, multiplyArgumentTest) {
    Value<> tempPara = Value<Integer>(0);
    // create fake ref TODO improve handling of parameters
    tempPara.ref = Trace::ValueRef(INT32_MAX, 0, IR::Operations::INT64);
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([&tempPara]() {
        return multiplyArgumentFunction(tempPara);
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
    auto function = (int64_t(*)(int64_t)) engine->lookup("execute").get();
    ASSERT_EQ(function(10), 100);
    ASSERT_EQ(function(42), 420);
}




}// namespace NES::ExecutionEngine::Experimental::Interpreter
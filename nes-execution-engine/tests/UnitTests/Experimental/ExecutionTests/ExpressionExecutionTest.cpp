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
/**
 * @brief This test tests execution of scala expression
 */
class ExpressionExecutionTest : public testing::Test {
  public:
    Trace::SSACreationPhase ssaCreationPhase;
    Trace::TraceToIRConversionPhase irCreationPhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ExpressionExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup ExpressionExecutionTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup TraceTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down ExpressionExecutionTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down ExpressionExecutionTest test class." << std::endl; }

    auto prepate(std::shared_ptr<Trace::ExecutionTrace> executionTrace) {
        executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
        std::cout << *executionTrace.get() << std::endl;
        auto ir = irCreationPhase.apply(executionTrace);
        std::cout << ir->toString() << std::endl;

        // create and print MLIR
        auto mlirUtility = new MLIR::MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
        int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir);
        assert(loadedModuleSuccess == 0);
        return mlirUtility->prepareEngine();
    }
};

Value<> int8AddExpression(Value<Int8> x) {
    Value<Int8> y = (int8_t) 2;
    return x + y;
}

TEST_F(ExpressionExecutionTest, addI8Test) {
    Value<Int8> tempx = (int8_t) 0;
    tempx.ref.blockId = -1;
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([tempx]() {
        return int8AddExpression(tempx);
    });
    auto engine = prepate(executionTrace);
    auto function = (int8_t(*)(int8_t)) engine->lookup("execute").get();
    ASSERT_EQ(function(1), 3.0);
}

Value<> int16AddExpression(Value<Int16> x) {
    Value<Int16> y = (int16_t) 5;
    return x + y;
}

TEST_F(ExpressionExecutionTest, addI16Test) {
    Value<Int16> tempx = (int16_t) 0;
    tempx.ref.blockId = -1;
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([tempx]() {
        return int16AddExpression(tempx);
    });
    auto engine = prepate(executionTrace);
    auto function = (int16_t(*)(int16_t)) engine->lookup("execute").get();
    ASSERT_EQ(function(8), 13);
}

Value<> int32AddExpression(Value<Int32> x) {
    Value<Int32> y = 5;
    return x + y;
}

TEST_F(ExpressionExecutionTest, addI32Test) {
    Value<Int32> tempx = 0;
    tempx.ref.blockId = -1;
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([tempx]() {
        return int32AddExpression(tempx);
    });
    auto engine = prepate(executionTrace);
    auto function = (int32_t(*)(int32_t)) engine->lookup("execute").get();
    ASSERT_EQ(function(8), 13);
}

Value<> int64AddExpression(Value<Int64> x) {
    Value<Int64> y = 7l;
    return x + y;
}

TEST_F(ExpressionExecutionTest, addI64Test) {
    Value<Int64> tempx = 0l;
    tempx.ref.blockId = -1;
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([tempx]() {
        return int64AddExpression(tempx);
    });
    auto engine = prepate(executionTrace);
    auto function = (int64_t(*)(int64_t)) engine->lookup("execute").get();
    ASSERT_EQ(function(7), 14);
    ASSERT_EQ(function(-7), 0);
    ASSERT_EQ(function(-14), -7);
}

Value<> floatAddExpression(Value<Float> x) {
    Value<Float> y = 7.0f;
    return x + y;
}

TEST_F(ExpressionExecutionTest, addFloatTest) {
    Value<Float> tempx = 0.0f;
    tempx.ref.blockId = -1;
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([tempx]() {
        return floatAddExpression(tempx);
    });
    auto engine = prepate(executionTrace);
    auto function = (float (*)(float)) engine->lookup("execute").get();
    ASSERT_EQ(function(7.0), 14.0);
    ASSERT_EQ(function(-7.0), 0.0);
    ASSERT_EQ(function(-14.0), -7.0);
}

Value<> doubleAddExpression(Value<Double> x) {
    Value<Double> y = 7.0;
    return x + y;
}

TEST_F(ExpressionExecutionTest, addDobleTest) {
    Value<Double> tempx = 0.0;
    tempx.ref.blockId = -1;
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([tempx]() {
        return doubleAddExpression(tempx);
    });
    auto engine = prepate(executionTrace);
    auto function = (double (*)(double)) engine->lookup("execute").get();
    ASSERT_EQ(function(7.0), 14.0);
    ASSERT_EQ(function(-7.0), 0.0);
    ASSERT_EQ(function(-14.0), -7.0);
}

Value<> castFloatToDoubleAddExpression(Value<Float> x) {
    Value<Double> y = 7.0;
    return x + y;
}

TEST_F(ExpressionExecutionTest, castFloatToDoubleTest) {
    Value<Float> tempx = 0.0f;
    tempx.ref.blockId = -1;
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([tempx]() {
        return castFloatToDoubleAddExpression(tempx);
    });
    auto engine = prepate(executionTrace);
    auto function = (double (*)(float)) engine->lookup("execute").get();
    ASSERT_EQ(function(7.0), 14.0);
    ASSERT_EQ(function(-7.0), 0.0);
    ASSERT_EQ(function(-14.0), -7.0);
}

Value<> castInt8ToInt64AddExpression(Value<Int8> x) {
    Value<Int64> y = 7l;
    return x + y;
}

TEST_F(ExpressionExecutionTest, castInt8ToInt64Test) {
    Value<Int8> tempx = (int8_t) 0;
    tempx.ref.blockId = -1;
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([tempx]() {
        return castInt8ToInt64AddExpression(tempx);
    });
    auto engine = prepate(executionTrace);
    auto function = (int64_t(*)(int8_t)) engine->lookup("execute").get();
    ASSERT_EQ(function(7), 14);
    ASSERT_EQ(function(-7), 0);
    ASSERT_EQ(function(-14), -7);
}

Value<> castInt8ToInt64AddExpression2(Value<> x) {
    Value<> y = 42l;
    return y + x;
}

TEST_F(ExpressionExecutionTest, castInt8ToInt64Test2) {
    Value<Int8> tempx = (int8_t) 0 ;
    tempx.ref.blockId = -1;
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([tempx]() {
        return castInt8ToInt64AddExpression2(tempx);
    });
    auto engine = prepate(executionTrace);
    auto function = (int64_t(*)(int8_t)) engine->lookup("execute").get();
    ASSERT_EQ(function(8), 50);
    ASSERT_EQ(function(-2), 40);
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter
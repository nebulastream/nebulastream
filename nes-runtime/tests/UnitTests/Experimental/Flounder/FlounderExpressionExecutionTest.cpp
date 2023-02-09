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

#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Experimental/Flounder/FlounderLoweringProvider.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Operators/Aggregation.hpp>
#include <Experimental/Interpreter/Operators/Aggregation/AggregationFunction.hpp>
#include <Experimental/Interpreter/Operators/Emit.hpp>
#include <Experimental/Interpreter/Operators/Scan.hpp>
#include <Experimental/Interpreter/Operators/Selection.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <gtest/gtest.h>

#include <Experimental/Babelfish/IRSerialization.hpp>
#include <Util/Logger/Logger.hpp>
#include <babelfish.h>
#include <execinfo.h>
#include <memory>

namespace NES::Nautilus {
/**
 * @brief This test tests execution of scala expression
 */
class FlounderExpressionExecutionTest : public Testing::NESBaseTest {
  public:
    Trace::SSACreationPhase ssaCreationPhase;
    Trace::TraceToIRConversionPhase irCreationPhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("FlounderExpressionExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup FlounderExpressionExecutionTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override { NES_INFO("Setup TraceTest test case."); }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Tear down FlounderExpressionExecutionTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down FlounderExpressionExecutionTest test class."); }
};

Value<> addExpression(Value<Int64> x) {
    Value<Int64> y = 2l;
    return x + y;
}

TEST_F(FlounderExpressionExecutionTest, addI8Test) {
    Value<Int64> tempx = (int64_t) 0;
    tempx.ref.blockId = -1;
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([tempx]() {
        return addExpression(tempx);
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace.get());
    auto ir = irCreationPhase.apply(executionTrace);
    NES_INFO(ir->toString());

    auto lp = Flounder::FlounderLoweringProvider();
    auto ex = lp.lower(ir);
    constexpr std::int64_t argument = 11;
    NES_INFO(" == Execute == ");
    ASSERT_EQ(ex->execute<std::uint64_t>(argument), 13);
}

Value<> addExpressionConst() {
    Value<Int64> y = (int64_t) 58;
    Value<Int64> x = (int64_t) 42;
    auto r = x + y;
    if (r == (int64_t) 42) {
        r = r + (int64_t) 1;
    }
    return r + (int64_t) 42;
}

int64_t test(int64_t) { return 10; };

Value<> sumLoop(Value<MemRef> ptr) {
    auto agg = ptr.load<Int64>();
    //agg = FunctionCall<>("callUDFProxyFunction", test, agg);
    for (Value start = (int64_t) 0; start < (int64_t) 10; start = start + (int64_t) 1) {

        for (Value start2 = (int64_t) 0; start2 < (int64_t) 10; start2 = start2 + (int64_t) 1) {
            agg = agg + (int64_t) 10;
        }
        agg = agg + (int64_t) 10;
    }
    ptr.store(agg);
    return agg;
}

TEST_F(FlounderExpressionExecutionTest, bftest) {
    int64_t valI = 42;
    auto tempPara = Value<MemRef>(std::make_unique<MemRef>((int8_t*) &valI));
    tempPara.ref = Trace::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createAddressStamp());
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([tempPara]() {
        return sumLoop(tempPara);
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace.get());
    auto ir = irCreationPhase.apply(executionTrace);
    NES_INFO(ir->toString());

    auto serializedIr = IRSerialization().serialize(ir);
    NES_INFO(serializedIr);

    graal_isolatethread_t* thread = NULL;
    ASSERT_TRUE(graal_create_isolate(NULL, NULL, &thread) == 0);
    auto* pipeline = initializePipeline(thread, serializedIr.data());
    for (auto i = 0; i < 100000; i++) {
        executePipeline(thread, pipeline, &valI, nullptr);
    }
    for (auto i = 0; i < 100000; i++) {
        executePipeline(thread, pipeline, &valI, nullptr);
    }
}

Value<> longExpression(Value<Int64> i1) {
    auto i2 = i1 + (int64_t) 1;
    auto i3 = i1 + (int64_t) 1;
    auto i4 = i1 + (int64_t) 1;
    auto i5 = i1 + (int64_t) 1;
    auto i6 = i1 + (int64_t) 1;
    auto i7 = i1 + (int64_t) 1;
    auto i8 = i1 + (int64_t) 1;
    auto i9 = i1 + (int64_t) 1;
    return i2 + i3 + i4 + i5 + i6 + i7 + i8 + i9;
}

TEST_F(FlounderExpressionExecutionTest, longExpressionTest) {
    Value<Int64> tempx = (int64_t) 0;
    tempx.ref.blockId = -1;
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([tempx]() {
        return longExpression(tempx);
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace.get());
    auto ir = irCreationPhase.apply(executionTrace);
    NES_INFO(ir->toString());

    auto lp = Flounder::FlounderLoweringProvider();
    auto ex = lp.lower(ir);
    constexpr std::int64_t argument = 1;
    NES_INFO(" == Execute == ");
    ASSERT_EQ(ex->execute<std::int64_t>(argument), 16);
}

Value<> ifThenCondition() {
    Value value = 1;
    Value iw = 1;
    if (value == 42) {
        iw = iw + 1;
    }
    return iw + 42;
}

TEST_F(FlounderExpressionExecutionTest, ifConditionTest) {
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([]() {
        return ifThenCondition();
    });
    NES_INFO(*executionTrace.get());
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace.get());
    auto ir = irCreationPhase.apply(executionTrace);
    NES_INFO(ir->toString());

    auto lp = Flounder::FlounderLoweringProvider();
    auto ex = lp.lower(ir);
    constexpr std::int64_t argument = 11;
    NES_INFO(" == Execute == ");
    ASSERT_EQ(ex->execute<std::int64_t>(argument), 43);
}

Value<> ifThenElseCondition() {
    Value value = Value(1);
    Value iw = Value(1);
    if (value == 42) {
        iw = iw + 1;
    } else {
        iw = iw + 42;
    }
    return iw + 42;
}

TEST_F(FlounderExpressionExecutionTest, ifThenElseConditionTest) {
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([]() {
        return ifThenElseCondition();
    });
    NES_INFO(*executionTrace.get());
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace.get());
    auto ir = irCreationPhase.apply(executionTrace);
    NES_INFO(ir->toString());

    // create and print MLIR
    auto lp = Flounder::FlounderLoweringProvider();
    auto ex = lp.lower(ir);
    constexpr std::int64_t argument = 11;
    NES_INFO(" == Execute == ");
    ASSERT_EQ(ex->execute<std::int64_t>(argument), 85);
}

Value<> ifThenElseConditionParameter(Value<> x) {
    Value iw = x * 2;
    if (iw == 8) {
        iw = iw * 2;
    } else {
        iw = 42;
    }
    return iw;
}

TEST_F(FlounderExpressionExecutionTest, ifThenElseConditionParameterTests) {
    Value<Int32> tempx = (int32_t) 0;
    tempx.ref.blockId = -1;
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([tempx]() {
        return ifThenElseConditionParameter(tempx);
    });
    NES_INFO(*executionTrace.get());
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace.get());
    auto ir = irCreationPhase.apply(executionTrace);
    NES_INFO(ir->toString());

    // create and print MLIR
    auto lp = Flounder::FlounderLoweringProvider();
    auto ex = lp.lower(ir);
    NES_INFO(" == Execute == ");
    ASSERT_EQ(ex->execute<std::int64_t>(1), 42);
    ASSERT_EQ(ex->execute<std::int64_t>(4), 16);
}

Value<> nestedIfThenElseCondition() {
    Value value = Value(1);
    Value iw = Value(1);
    if (value == 42) {
    } else {
        if (iw == 8) {
        } else {
            iw = iw + 2;
        }
    }
    return iw = iw + 2;
}

TEST_F(FlounderExpressionExecutionTest, nestedIFThenElseConditionTest) {
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([]() {
        return nestedIfThenElseCondition();
    });
    NES_INFO(*executionTrace.get());
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace.get());
    auto ir = irCreationPhase.apply(executionTrace);
    NES_INFO(ir->toString());

    // create and print MLIR
    auto lp = Flounder::FlounderLoweringProvider();
    auto ex = lp.lower(ir);
    NES_INFO(" == Execute == ");
    NES_INFO(ex->code().value());
    ASSERT_EQ(ex->execute<std::int64_t>(), 5);
}

/*
TEST_F(FlounderExpressionExecutionTest, sumLoopTest) {
    auto execution = Trace::traceFunctionSymbolicallyWithReturn([]() {
        return sumLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    NES_INFO(*execution.get() );
    auto ir = irCreationPhase.apply(execution);

    // create and print MLIR
    auto lp = Flounder::FlounderLoweringProvider();
    auto ex = lp.lower(ir);
    constexpr std::int64_t argument = 11;
    NES_INFO(" == Execute == " );
    NES_INFO(ex->code().value() );
    ASSERT_EQ(ex->execute<std::int64_t>(argument), 101);
}*/

Value<> ifSumLoop() {
    Value agg = Value(1);
    for (Value start = 0; start < 10; start = start + 1) {
        if (agg < 50) {
            agg = agg + 10;
        }
    }
    return agg;
}

TEST_F(FlounderExpressionExecutionTest, ifSumLoopTest) {
    auto execution = Trace::traceFunctionSymbolicallyWithReturn([]() {
        return ifSumLoop();
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    NES_INFO(*execution.get());
    auto ir = irCreationPhase.apply(execution);
    NES_INFO(ir->toString());

    // create and print MLIR
    auto lp = Flounder::FlounderLoweringProvider();
    auto ex = lp.lower(ir);
    constexpr std::int64_t argument = 11;
    NES_INFO(" == Execute == ");
    ASSERT_EQ(ex->execute<std::int64_t>(), 51);
}

Value<> loadFunction(Value<MemRef> ptr) { return ptr.load<Int64>(); }

TEST_F(FlounderExpressionExecutionTest, loadFunctionTest) {
    int64_t valI = 42;
    auto tempPara = Value<MemRef>(std::make_unique<MemRef>((int8_t*) &valI));
    // create fake ref TODO improve handling of parameters
    tempPara.ref = Trace::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createAddressStamp());
    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([&tempPara]() {
        return loadFunction(tempPara);
    });
    NES_INFO(*executionTrace.get());
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace.get());
    auto ir = irCreationPhase.apply(executionTrace);
    NES_INFO(ir->toString());

    // create and print MLIR
    auto lp = Flounder::FlounderLoweringProvider();
    auto ex = lp.lower(ir);
    NES_INFO(" == Execute == ");
    ASSERT_EQ(ex->execute<std::int64_t>(&valI), 42);
}

void storeFunction(Value<MemRef> ptr) {
    auto value = ptr.load<Int64>();
    auto tmp = value + (int64_t) 1;
    ptr.store(tmp);
}

TEST_F(FlounderExpressionExecutionTest, storeFunctionTest) {

    int64_t valI = 42;
    auto tempPara = Value<MemRef>((int8_t*) &valI);
    tempPara.load<Int64>();
    // create fake ref TODO improve handling of parameters
    tempPara.ref = Trace::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createAddressStamp());
    auto executionTrace = Trace::traceFunctionSymbolically([&tempPara]() {
        storeFunction(tempPara);
    });
    NES_INFO(*executionTrace.get());
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace.get());
    auto ir = irCreationPhase.apply(executionTrace);
    NES_INFO(ir->toString());

    auto lp = Flounder::FlounderLoweringProvider();
    auto ex = lp.lower(ir);
    constexpr std::int64_t argument = 11;
    NES_INFO(" == Execute == ");

    ex->execute<>(&valI);
    ASSERT_EQ(valI, 43);
    ex.release();
}

int64_t addInt(int64_t x, int64_t y) { return x + y; };

Value<> addIntFunction() {
    auto x = Value<Int64>((int64_t) 2);
    auto y = Value<Int64>((int64_t) 3);
    Value<Int64> res = FunctionCall<>("add", addInt, x, y);
    return res;
}

TEST_F(FlounderExpressionExecutionTest, addIntFunctionTest) {

    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([]() {
        return addIntFunction();
    });
    NES_INFO(*executionTrace.get());
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace.get());
    auto ir = irCreationPhase.apply(executionTrace);
    NES_INFO(ir->toString());

    // create and print MLIR
    auto lp = Flounder::FlounderLoweringProvider();
    auto ex = lp.lower(ir);
    constexpr std::int64_t argument = 11;
    NES_INFO(" == Execute == ");
    ASSERT_EQ(ex->execute<int64_t>(), 5);
}

Value<> andIfFunction() {
    auto x = Value<Int64>(2l);
    auto y = Value<Int64>(3l);
    if (x == 42l && y == 42l) {
        x = x + y;
    }
    return x;
}

TEST_F(FlounderExpressionExecutionTest, andIfFunctionTest) {

    auto executionTrace = Trace::traceFunctionSymbolicallyWithReturn([]() {
        return andIfFunction();
    });
    NES_INFO(*executionTrace.get());
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_INFO(*executionTrace.get());
    auto ir = irCreationPhase.apply(executionTrace);
    NES_INFO(ir->toString());

    // create and print MLIR
    auto lp = Flounder::FlounderLoweringProvider();
    auto ex = lp.lower(ir);
    constexpr std::int64_t argument = 11;
    NES_INFO(" == Execute == ");
    ASSERT_EQ(ex->execute<int64_t>(), 5);
}

/*
Value<> int16AddExpression(Value<Int16> x) {
    Value<Int16> y = (int16_t) 5;
    return x + y;
}

TEST_F(FlounderExpressionExecutionTest, addI16Test) {
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

TEST_F(FlounderExpressionExecutionTest, addI32Test) {
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

TEST_F(FlounderExpressionExecutionTest, addI64Test) {
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

TEST_F(FlounderExpressionExecutionTest, addFloatTest) {
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

TEST_F(FlounderExpressionExecutionTest, addDobleTest) {
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

TEST_F(FlounderExpressionExecutionTest, castFloatToDoubleTest) {
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

TEST_F(FlounderExpressionExecutionTest, castInt8ToInt64Test) {
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

TEST_F(FlounderExpressionExecutionTest, castInt8ToInt64Test2) {
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
 */

}// namespace NES::Nautilus
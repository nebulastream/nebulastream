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

#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <TestUtils/AbstractCompilationBackendTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <functional>
#include <memory>
#include <mir.h>
#include <mir-gen.h>

namespace NES::Nautilus {

/**
 * @brief This test tests execution of scala expression
 */
class ExpressionExecutionTest : public Testing::NESBaseTest, public AbstractCompilationBackendTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ExpressionExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup ExpressionExecutionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ExpressionExecutionTest test class."); }
};

MIR_item_t create_mir_func_with_loop(MIR_context_t ctx, MIR_module_t* m) {
    MIR_item_t func;
    MIR_label_t fin, cont;
    MIR_reg_t ARG1, R2;
    MIR_type_t res_type;

    if (m != NULL)
        *m = MIR_new_module(ctx, "m");
    res_type = MIR_T_I64;
    func = MIR_new_func(ctx, "loop", 1, &res_type, 1, MIR_T_I64, "arg1");
    R2 = MIR_new_func_reg(ctx, func->u.func, MIR_T_I64, "count");
    ARG1 = MIR_reg(ctx, "arg1", func->u.func);
    fin = MIR_new_label(ctx);
    cont = MIR_new_label(ctx);
    MIR_append_insn(ctx, func, MIR_new_insn(ctx, MIR_MOV, MIR_new_reg_op(ctx, R2), MIR_new_int_op(ctx, 0)));
    MIR_append_insn(ctx,
                    func,
                    MIR_new_insn(ctx, MIR_BGE, MIR_new_label_op(ctx, fin), MIR_new_reg_op(ctx, R2), MIR_new_reg_op(ctx, ARG1)));
    MIR_append_insn(ctx, func, cont);
    MIR_append_insn(ctx,
                    func,
                    MIR_new_insn(ctx, MIR_ADD, MIR_new_reg_op(ctx, R2), MIR_new_reg_op(ctx, R2), MIR_new_int_op(ctx, 1)));
    MIR_append_insn(ctx,
                    func,
                    MIR_new_insn(ctx, MIR_BLT, MIR_new_label_op(ctx, cont), MIR_new_reg_op(ctx, R2), MIR_new_reg_op(ctx, ARG1)));
    MIR_append_insn(ctx, func, fin);
    MIR_append_insn(ctx, func, MIR_new_ret_insn(ctx, 1, MIR_new_reg_op(ctx, R2)));
    auto x = MIR_new_func_reg(ctx, func->u.func, MIR_T_I64, "count2");
    MIR_finish_func(ctx);

    if (m != NULL)
        MIR_finish_module(ctx);
    return func;
}

TEST_P(ExpressionExecutionTest, interpretMIR) {
    MIR_module_t m;
    MIR_context_t ctx = MIR_init();
    auto func = create_mir_func_with_loop(ctx, &m);
    MIR_output (ctx, stderr);
    MIR_load_module (ctx, m);
    MIR_link (ctx, MIR_set_interp_interface, NULL);
    MIR_output (ctx, stderr);
    MIR_set_interp_interface (ctx, func);
    typedef int64_t (*loop_func) (int64_t);
    //  auto fun_addr = (loop_func) MIR_gen (ctx, 0, func);
    int64_t res = ((loop_func) func->addr) (10);
    //MIR_link (ctx, MIR_set_gen_interface, NULL);
    //int64_t res = fun_addr(10);
    NES_DEBUG(res);
    MIR_finish (ctx);
}


TEST_P(ExpressionExecutionTest, compileMIR) {
    MIR_module_t m;
    MIR_context_t ctx = MIR_init();
    auto func = create_mir_func_with_loop(ctx, &m);
    MIR_output (ctx, stderr);
    MIR_load_module (ctx, m);
    MIR_gen_init (ctx, 1);
    MIR_gen_set_optimize_level (ctx, 0, 3);
    MIR_gen_set_debug_file (ctx, 0, stderr);
    MIR_link (ctx, MIR_set_gen_interface, NULL);
    MIR_output (ctx, stderr);

    typedef int64_t (*loop_func) (int64_t);
    auto fun_addr = (loop_func) MIR_gen (ctx, 0, func);
    int64_t res = fun_addr(10);
    NES_DEBUG(res);
    MIR_finish (ctx);
}

Value<> int8AddExpression(Value<Int8> x) {
    Value<Int8> y = (int8_t) 2;
    return x + y;
}

TEST_P(ExpressionExecutionTest, addI8Test) {
    Value<Int8> tempx = (int8_t) 0;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt8Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return int8AddExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int8_t, int8_t>("execute");

    ASSERT_EQ(function(1), 3.0);
}

Value<> int16AddExpression(Value<Int16> x) {
    Value<Int16> y = (int16_t) 5;
    return x + y;
}

TEST_P(ExpressionExecutionTest, addI16Test) {
    Value<Int16> tempx = (int16_t) 0;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt16Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return int16AddExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int16_t, int16_t>("execute");

    ASSERT_EQ(function(8), 13);
}

Value<> int32AddExpression(Value<Int32> x) {
    Value<Int32> y = 5;
    return x + y;
}

TEST_P(ExpressionExecutionTest, addI32Test) {
    Value<Int32> tempx = 0;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt32Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return int32AddExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int32_t, int32_t>("execute");
    ASSERT_EQ(function(8), 13);
}

Value<> int64AddExpression(Value<Int64> x) {
    Value<Int64> y = (int64_t) 7;
    return x + y;
}

TEST_P(ExpressionExecutionTest, addI64Test) {
    Value<Int64> tempx = (int64_t) 0;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt64Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return int64AddExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int64_t, int64_t>("execute");
    ASSERT_EQ(function(7), 14);
    ASSERT_EQ(function(-7), 0);
    ASSERT_EQ(function(-14), -7);
}

Value<> floatAddExpression(Value<Float> x) {
    Value<Float> y = 7.0f;
    return x + y;
}

TEST_P(ExpressionExecutionTest, addFloatTest) {
    Value<Float> tempx = 0.0f;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createFloatStamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return floatAddExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<float, float>("execute");
    ASSERT_EQ(function(7.0), 14.0);
    ASSERT_EQ(function(-7.0), 0.0);
    ASSERT_EQ(function(-14.0), -7.0);
}

Value<> doubleAddExpression(Value<Double> x) {
    Value<Double> y = 7.0;
    return x + y;
}

TEST_P(ExpressionExecutionTest, addDoubleTest) {
    Value<Double> tempx = 0.0;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createDoubleStamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return doubleAddExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<double, double>("execute");
    ASSERT_EQ(function(7.0), 14.0);
    ASSERT_EQ(function(-7.0), 0.0);
    ASSERT_EQ(function(-14.0), -7.0);
}

Value<> castFloatToDoubleAddExpression(Value<Float> x) {
    Value<Double> y = 7.0;
    return x + y;
}

TEST_P(ExpressionExecutionTest, castFloatToDoubleTest) {
    Value<Float> tempx = 0.0f;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createFloatStamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return castFloatToDoubleAddExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<double, float>("execute");
    ASSERT_EQ(function(7.0), 14.0);
    ASSERT_EQ(function(-7.0), 0.0);
    ASSERT_EQ(function(-14.0), -7.0);
}

Value<> castInt8ToInt64AddExpression(Value<Int8> x) {
    Value<Int64> y = (int64_t) 7;
    return x + y;
}

TEST_P(ExpressionExecutionTest, castInt8ToInt64Test) {
    Value<Int8> tempx = (int8_t) 0;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt8Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return castInt8ToInt64AddExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int64_t, int8_t>("execute");
    ASSERT_EQ(function(7), 14);
    ASSERT_EQ(function(-7), 0);
    ASSERT_EQ(function(-14), -7);
}

Value<> castInt8ToInt64AddExpression2(Value<> x) {
    Value<Int64> y = (int64_t) 42;
    return y + x;
}

TEST_P(ExpressionExecutionTest, castInt8ToInt64Test2) {
    Value<Int8> tempx = (int8_t) 0;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt8Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return castInt8ToInt64AddExpression2(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int64_t, int8_t>("execute");
    ASSERT_EQ(function(8), 50);
    ASSERT_EQ(function(-2), 40);
}

// Tests all registered compilation backends.
// To select a specific compilation backend use ::testing::Values("MLIR") instead of ValuesIn.
INSTANTIATE_TEST_CASE_P(testExpressions,
                        ExpressionExecutionTest,
                        ::testing::ValuesIn(Backends::CompilationBackendRegistry::getPluginNames().begin(),
                                            Backends::CompilationBackendRegistry::getPluginNames().end()),
                        [](const testing::TestParamInfo<ExpressionExecutionTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Nautilus
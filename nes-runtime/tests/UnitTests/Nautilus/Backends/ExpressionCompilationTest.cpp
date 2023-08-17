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
#include <Nautilus/Tracing/TraceContext.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <TestUtils/AbstractCompilationBackendTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <cstdint>
#include <functional>
#include <memory>

namespace NES::Nautilus {

/**
 * @brief This test tests execution of scala expression
 */
class ExpressionExecutionTest : public Testing::TestWithErrorHandling, public AbstractCompilationBackendTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ExpressionExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup ExpressionExecutionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ExpressionExecutionTest test class."); }
};

Value<> int8AddExpression(Value<Int8> x) {
    Value<Int8> y = -2_s8;
    return x + y;
}

TEST_P(ExpressionExecutionTest, addI8Test) {
    Value<Int8> tempx(0_s8);
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt8Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return int8AddExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int8_t, int8_t>("execute");

    ASSERT_EQ(function(1), -1.0);
}

Value<> int16AddExpression(Value<Int16> x) {
    Value<Int16> y(5_s16);
    return x + y;
}

TEST_P(ExpressionExecutionTest, addI16Test) {
    Value<Int16> tempx(0_s16);
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

Value<> uint32AddExpression(Value<UInt32> x) {
    Value<UInt32> y = 5_u32;
    return x + y;
}

TEST_P(ExpressionExecutionTest, addUI32Test) {
    Value<UInt32> tempx = 0_u32;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt32Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return uint32AddExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<uint32_t, uint32_t>("execute");
    ASSERT_EQ(function(8), 13);
}

Value<> int64AddExpression(Value<Int64> x) {
    Value<Int64> y(7_s64);
    return x + y;
}

TEST_P(ExpressionExecutionTest, addI64Test) {
    Value<Int64> tempx(0_s64);
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

Value<> uint64ModExpression(Value<UInt64> x) {
    Value<UInt64> y = 7_u64;
    return x % y;
}

TEST_P(ExpressionExecutionTest, modUI64Test) {
    Value<UInt64> tempx = 0_u64;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createUInt64Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return uint64ModExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<uint64_t, uint64_t>("execute");
    ASSERT_EQ(function(3), 3);
    ASSERT_EQ(function(14), 0);
    ASSERT_EQ(function(18), 4);
}

Value<> int64ModExpression(Value<Int64> x) {
    Value<Int64> y(7_s64);
    return x % y;
}

TEST_P(ExpressionExecutionTest, modI64Test) {
    Value<Int64> tempx(0_s64);
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt64Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return int64ModExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int64_t, int64_t>("execute");
    ASSERT_EQ(function(3), 3);
    ASSERT_EQ(function(14), 0);
    ASSERT_EQ(function(18), 4);
}

Value<> int64BitWiseAndExpression(Value<Int64> x) {
    Value<Int64> y(7_s64);
    return x & y;
}

TEST_P(ExpressionExecutionTest, bitWiseAndI64Test) {
    Value<Int64> tempx(0_s64);
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt64Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return int64BitWiseAndExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int64_t, int64_t>("execute");
    ASSERT_EQ(function(3), 3);
    ASSERT_EQ(function(14), 6);
    ASSERT_EQ(function(18), 2);
}

Value<> int64BitWiseOrExpression(Value<Int64> x) {
    Value<Int64> y(7_s64);
    return x | y;
}

TEST_P(ExpressionExecutionTest, BitWiseOrI64Test) {
    Value<Int64> tempx(0_s64);
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt64Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return int64BitWiseOrExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int64_t, int64_t>("execute");
    ASSERT_EQ(function(3), ((3_s64) | 7));
    ASSERT_EQ(function(14), ((14_s64) | 7));
    ASSERT_EQ(function(18), ((18_s64) | 7));
}

Value<> int64BitWiseXorExpression(Value<Int64> x) {
    Value<Int64> y(7_s64);
    return x ^ y;
}

TEST_P(ExpressionExecutionTest, BitWiseXorI64Test) {
    Value<Int64> tempx(0_s64);
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt64Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return int64BitWiseXorExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int64_t, int64_t>("execute");
    ASSERT_EQ(function(3), ((3_s64) ^ 7));
    ASSERT_EQ(function(14), ((14_s64) ^ 7));
    ASSERT_EQ(function(18), ((18_s64) ^ 7));
}

Value<> int64BitWiseLeftShiftExpression(Value<Int64> x) {
    Value<Int64> y(7_s64);
    return x << y;
}

TEST_P(ExpressionExecutionTest, BitWiseLeftShiftI64Test) {
    Value<Int64> tempx(0_s64);
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt64Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return int64BitWiseLeftShiftExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int64_t, int64_t>("execute");
    ASSERT_EQ(function(3), ((3_s64) << 7));
    ASSERT_EQ(function(14), ((14_s64) << 7));
    ASSERT_EQ(function(18), ((18_s64) << 7));
}

Value<> int64BitWiseRightShiftExpression(Value<Int64> x) {
    Value<Int64> y(2_s64);
    return x >> y;
}

TEST_P(ExpressionExecutionTest, BitWiseRightShiftI64Test) {
    Value<Int64> tempx(0_s64);
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt64Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return int64BitWiseRightShiftExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int64_t, int64_t>("execute");
    ASSERT_EQ(function(3), ((3_s64) >> 2));
    ASSERT_EQ(function(14), ((14_s64) >> 2));
    ASSERT_EQ(function(18), ((18_s64) >> 2));
}

Value<> int64BitWiseNotExpression(Value<Int64> x) { return ~x; }

TEST_P(ExpressionExecutionTest, BitWiseNotI64Test) {
    Value<Int64> tempx(0_s64);
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt64Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([tempx]() {
        return int64BitWiseNotExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int64_t, int64_t>("execute");
    ASSERT_EQ(function(-1), 0);
    ASSERT_EQ(function(0x1234), ~0x1234);
    ASSERT_EQ(function(18), ~18);
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
    Value<Int64> y(7_s64);
    return x + y;
}

TEST_P(ExpressionExecutionTest, castInt8ToInt64Test) {
    Value<Int8> tempx(0_s8);
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
    Value<Int64> y(42_s64);
    return y + x;
}

TEST_P(ExpressionExecutionTest, castInt8ToInt64Test2) {
    Value<Int8> tempx(0_s8);
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
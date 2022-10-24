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
#include <Runtime/BufferManager.hpp>
#include <TestUtils/AbstractCompilationBackendTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <memory>

namespace NES::Nautilus {

/**
 * @brief This test tests execution of scala expression
 */
class ExpressionExecutionTest : public testing::Test, public AbstractCompilationBackendTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ExpressionExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup ExpressionExecutionTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup ExpressionExecutionTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down ExpressionExecutionTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down ExpressionExecutionTest test class." << std::endl; }
};

Value<> int8AddExpression(Value<Int8> x) {
    Value<Int8> y = (int8_t) 2;
    return x + y;
}

TEST_P(ExpressionExecutionTest, addI8Test) {
    Value<Int8> tempx = (int8_t) 0;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt8Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([tempx]() {
        return int8AddExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int8_t (*)(int8_t)>("execute");
    ASSERT_EQ(function(1), 3.0);
}

Value<> int16AddExpression(Value<Int16> x) {
    Value<Int16> y = (int16_t) 5;
    return x + y;
}

TEST_P(ExpressionExecutionTest, addI16Test) {
    Value<Int16> tempx = (int16_t) 0;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt16Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([tempx]() {
        return int16AddExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int16_t (*)(int16_t)>("execute");

    ASSERT_EQ(function(8), 13);
}

Value<> int32AddExpression(Value<Int32> x) {
    Value<Int32> y = 5;
    return x + y;
}

TEST_P(ExpressionExecutionTest, addI32Test) {
    Value<Int32> tempx = 0;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt32Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([tempx]() {
        return int32AddExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int32_t (*)(int32_t)>("execute");
    ASSERT_EQ(function(8), 13);
}

Value<> int64AddExpression(Value<Int64> x) {
    Value<Int64> y = 7l;
    return x + y;
}

TEST_P(ExpressionExecutionTest, addI64Test) {
    Value<Int64> tempx = (int64_t) 0;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt64Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([tempx]() {
        return int64AddExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int64_t (*)(int64_t)>("execute");
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
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([tempx]() {
        return floatAddExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<float (*)(float)>("execute");
    ASSERT_EQ(function(7.0), 14.0);
    ASSERT_EQ(function(-7.0), 0.0);
    ASSERT_EQ(function(-14.0), -7.0);
}

Value<> doubleAddExpression(Value<Double> x) {
    Value<Double> y = 7.0;
    return x + y;
}

TEST_P(ExpressionExecutionTest, addDobleTest) {
    Value<Double> tempx = 0.0;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createDoubleStamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([tempx]() {
        return doubleAddExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<double (*)(double)>("execute");
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
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([tempx]() {
        return castFloatToDoubleAddExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<double (*)(float)>("execute");
    ASSERT_EQ(function(7.0), 14.0);
    ASSERT_EQ(function(-7.0), 0.0);
    ASSERT_EQ(function(-14.0), -7.0);
}

Value<> castInt8ToInt64AddExpression(Value<Int8> x) {
    Value<Int64> y = 7l;
    return x + y;
}

TEST_P(ExpressionExecutionTest, castInt8ToInt64Test) {
    Value<Int8> tempx = (int8_t) 0;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt8Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([tempx]() {
        return castInt8ToInt64AddExpression(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int8_t (*)(int8_t)>("execute");
    ASSERT_EQ(function(7), 14);
    ASSERT_EQ(function(-7), 0);
    ASSERT_EQ(function(-14), -7);
}

Value<> castInt8ToInt64AddExpression2(Value<> x) {
    Value<> y = 42l;
    return y + x;
}

TEST_P(ExpressionExecutionTest, castInt8ToInt64Test2) {
    Value<Int8> tempx = (int8_t) 0;
    tempx.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createInt8Stamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([tempx]() {
        return castInt8ToInt64AddExpression2(tempx);
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int8_t (*)(int8_t)>("execute");
    ASSERT_EQ(function(8), 50);
    ASSERT_EQ(function(-2), 40);
}

// Tests all registered compilation backends.
// To select a specific compilation backend use ::testing::Values("MLIR") instead of ValuesIn.
auto pluginNames = Backends::CompilationBackendRegistry::getPluginNames();
INSTANTIATE_TEST_CASE_P(testExpressions,
                        ExpressionExecutionTest,
                        ::testing::ValuesIn(pluginNames.begin(), pluginNames.end()),
                        [](const testing::TestParamInfo<ExpressionExecutionTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Nautilus
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

#include <Nautilus/Interface/DataTypes/InvocationPlugin.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Runtime/BufferManager.hpp>
#include <TestUtils/AbstractCompilationBackendTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Nautilus {

class TypeCompilationTest : public testing::Test, public AbstractCompilationBackendTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TypeCompilationTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup TypeCompilationTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup TypeCompilationTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down TypeCompilationTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down TypeCompilationTest test class." << std::endl; }
};

Value<> negativeIntegerTest() {
    Value four = Value<>(4);
    Value five = Value<>(5);
    Value minusOne = four - five;
    return minusOne;
}

TEST_P(TypeCompilationTest, negativeIntegerTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return negativeIntegerTest();
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int32_t (*)()>("execute");
    ASSERT_EQ(function(), -1);
}

Value<> unsignedIntegerTest() {
    uint32_t four = 4;
    uint32_t five = 5;
    Value unsignedFour = Value(four);
    Value unsignedFive = Value(five);
    Value minusOne = unsignedFour - unsignedFive;
    return minusOne;
}

// We should be able to create Values with unsigned ints, but currently we cannot.
TEST_P(TypeCompilationTest, DISABLED_unsignedIntegerTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return unsignedIntegerTest();
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<uint32_t (*)()>("execute");
    ASSERT_EQ(function(), UINT32_MAX);
}

Value<> boolCompareTest() {
    Value value = Value(1);
    Value iw = Value(true);
    if (iw == false) {
        return value + 41;
    } else {
        return value;
    }
}

// Should return 1, but returns 41 (Value(true) in interpreted as 0).
TEST_P(TypeCompilationTest, DISABLED_boolCompareTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return boolCompareTest();
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int32_t (*)()>("execute");
    ASSERT_EQ(function(), 1);
}

Value<> floatTest() {
    // Value iw = Value(1.3);
    // return iw;
    return Value(1);
}

// Above approach, to return a float Value, does not work.
TEST_P(TypeCompilationTest, DISABLED_floatTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return floatTest();
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int64_t (*)()>("execute");
    ASSERT_EQ(function(), 1);
}

Value<> mixBoolAndIntTest() {
    Value boolValue = Value(true);
    Value intValue = Value(4);
    return boolValue + intValue;
}

// Should return 5, but returns 4. Could extend to check for bool-int edge cases
TEST_P(TypeCompilationTest, DISABLED_mixBoolAndIntTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return mixBoolAndIntTest();
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int64_t (*)()>("execute");
    ASSERT_EQ(function(), 5);
}

class CustomType : public Any {
  public:
    static const inline auto type = TypeIdentifier::create<CustomType>();
    CustomType(Value<> x, Value<> y) : Any(&type), x(x), y(y){};

    std::shared_ptr<CustomType> add(const CustomType& other) const {
        return std::make_unique<CustomType>(x + other.x, y + other.y);
    }

    std::shared_ptr<CustomType> mulInt(const Int64& other) const {
        return std::make_unique<CustomType>(x * other.getValue(), y * other.getValue());
    }

    std::shared_ptr<Int64> power(const CustomType& other) const { return std::make_unique<Int64>(x * other.x - y); }

    std::shared_ptr<Any> copy() override { return std::make_shared<CustomType>(x, y); }

    Value<> x;
    Value<> y;
};

class CustomTypeInvocationPlugin : public InvocationPlugin {
  public:
    std::optional<Value<>> Add(const Value<>& left, const Value<>& right) const override {
        if (isa<CustomType>(left.value) && isa<CustomType>(right.value)) {
            auto& ct1 = left.getValue().staticCast<CustomType>();
            auto& ct2 = right.getValue().staticCast<CustomType>();
            return Value(ct1.add(ct2));
        }
        return std::nullopt;
    }

    std::optional<Value<>> Mul(const Value<>& left, const Value<>& right) const override {
        if (isa<CustomType>(left.value) && isa<Int64>(right.value)) {
            auto& ct1 = left.getValue().staticCast<CustomType>();
            auto& ct2 = right.getValue().staticCast<Int64>();
            return Value<CustomType>(ct1.mulInt(ct2));
        }
        return std::nullopt;
    }
};

[[maybe_unused]] static InvocationPluginRegistry::Add<CustomTypeInvocationPlugin> cPlugin;

Value<> customValueType() {

    auto c1 = Value<CustomType>(CustomType(32l, 32l));
    auto c2 = Value<CustomType>(CustomType(32l, 32l));

    c1 = c1 + c2;
    c1 = c1 * 2l;
    return c1.getValue().x;
}

TEST_P(TypeCompilationTest, customValueTypeTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return customValueType();
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int64_t (*)()>("execute");
    ASSERT_EQ(function(), 128);
}

// Tests all registered compilation backends.
// To select a specific compilation backend use ::testing::Values("MLIR") instead of ValuesIn.
auto pluginNames = Backends::CompilationBackendRegistry::getPluginNames();
INSTANTIATE_TEST_CASE_P(testTypeCompilation,
                        TypeCompilationTest,
                        ::testing::ValuesIn(pluginNames.begin(), pluginNames.end()),
                        [](const testing::TestParamInfo<TypeCompilationTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Nautilus
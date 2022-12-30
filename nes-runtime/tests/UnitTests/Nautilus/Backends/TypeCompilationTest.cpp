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
#include <Nautilus/Interface/DataTypes/List/List.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/TypedRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <TestUtils/AbstractCompilationBackendTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Nautilus {

class TypeCompilationTest : public Testing::NESBaseTest, public AbstractCompilationBackendTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TypeCompilationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TypeCompilationTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down TypeCompilationTest test class."); }
};

Value<> negativeIntegerTest() {
    Value four = Value<>(4);
    Value five = Value<>(5);
    Value minusOne = four - five;
    return minusOne;
}

TEST_P(TypeCompilationTest, negativeIntegerTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
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
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
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
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
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
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
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
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
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

    auto c1 = Value<CustomType>(CustomType((int64_t) 32, (int64_t) 32));
    auto c2 = Value<CustomType>(CustomType((int64_t) 32, (int64_t) 32));

    c1 = c1 + c2;
    c1 = c1 * (int64_t) 2;
    return c1.getValue().x;
}

TEST_P(TypeCompilationTest, customValueTypeTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
        return customValueType();
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int64_t (*)()>("execute");
    ASSERT_EQ(function(), 128);
}

Value<> listLengthTest(Value<List>& list) { return list->length() + 4; }

/*
TEST_P(TypeCompilationTest, compileListLengthFunctionTest) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<Runtime::WorkerContext>(0, bm, 100);

    auto list = RawList(10);
    auto listRef = TypedRef<RawList>(list);
    Value<List> valueList = Value<TypedList<Int32>>(TypedList<Int32>(listRef));
    listRef.value->ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());

    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([&]() {
        Nautilus::Tracing::getThreadLocalTraceContext()->addTraceArgument(valueList.value->rawReference.value->ref);
        return listLengthTest(valueList);
    });

    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int64_t (*)(void*)>("execute");
    ASSERT_EQ(function(&listRef.get()), 14);
}
 */

/**
 * @brief Simple text function that calls text length, uppercase and manipulates the text content.
 * @param text
 * @return
 */
Value<> textTestFunction(Value<Text>& text) {
    auto length = text->length();
    auto list2 = text->upper();
    for (Value<UInt32> i = (uint32_t) 0; i < text->length(); i = i + (uint32_t) 1) {
        text[i] = (int8_t) 'o';
    }
    return list2->length();
}

TEST_P(TypeCompilationTest, compileTextFunctionTest) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<Runtime::WorkerContext>(0, bm, 100);

    auto textA = Value<Text>("test");
    auto listRef = textA.value->getReference();

    listRef.value->ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());

    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([&]() {
        Nautilus::Tracing::TraceContext::get()->addTraceArgument(listRef.value->ref);
        return textTestFunction(textA);
    });

    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int64_t (*)(void*)>("execute");
    ASSERT_EQ(function(listRef.get()), 4);
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
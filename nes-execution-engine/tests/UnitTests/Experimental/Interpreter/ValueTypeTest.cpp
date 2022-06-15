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
#include <Experimental/Interpreter/DataValue/Float/Double.hpp>
#include <Experimental/Interpreter/DataValue/Float/Float.hpp>
#include <Experimental/Interpreter/DataValue/Integer/Int.hpp>
#include <Experimental/Interpreter/DataValue/InvocationPlugin.hpp>
#include <Experimental/Interpreter/DataValue/MemRef.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Experimental/Interpreter/Expressions/ReadFieldExpression.hpp>
#include <Experimental/Interpreter/Expressions/WriteFieldExpression.hpp>
#include <Experimental/Interpreter/FunctionCall.hpp>
#include <Experimental/Interpreter/Operators/Selection.hpp>
#include <Experimental/NESIR/Types/IntegerStamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <boost/preprocessor/stringize.hpp>
#include <cxxabi.h>
#include <dlfcn.h>
#include <execinfo.h>
#include <gtest/gtest.h>
#include <memory>

namespace NES::ExecutionEngine::Experimental::Interpreter {

class ValueTypeTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ValueTypeTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup ValueTypeTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup ValueTypeTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down ValueTypeTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down ValueTypeTest test class." << std::endl; }
};

class CustomType : public Any {
  public:
    static const inline auto type = TypeIdentifier::create<CustomType>();
    CustomType(Value<> x, Value<> y) : Any(&type), x(x), y(y){};

    std::unique_ptr<CustomType> add(const CustomType& other) const {
        return std::make_unique<CustomType>(x + other.x, y + other.y);
    }

    std::unique_ptr<Int64> power(const CustomType& other) const {
        return std::make_unique<Int64>(x * other.x - y);
    }

    Value<> x;
    Value<> y;
    std::unique_ptr<Any> copy() override { return std::make_unique<CustomType>(x, y); }
};

class CustomTypeInvocationPlugin : public InvocationPlugin {
  public:
    std::optional<Value<>> Add(const Value<>& left, const Value<>& right) const override {
        if (isa<CustomType>(left.value) && isa<CustomType>(right.value)) {
            auto& ct1 = left.getValue().staticCast<CustomType>();
            auto& ct2 = right.getValue().staticCast<CustomType>();
            return Value(ct1.add(ct2));
        } else {
            return std::nullopt;
        }
    }
};

[[maybe_unused]] static InvocationPluginRegistry::Add<CustomTypeInvocationPlugin> cPlugin;

TEST_F(ValueTypeTest, customValueTypeTest) {
    Value<Int64> x = 32l;
    Value<Int64> y = 32l;

    auto c1 = Value<CustomType>(std::make_unique<CustomType>(x, y));
    auto c2 = Value<CustomType>(std::make_unique<CustomType>(x, y));

    auto c3 = c1 + c2;
    c1 = c2;
    NES_DEBUG(c3.value);
}

TEST_F(ValueTypeTest, FloatTest) {
    auto f1 = Value<Float>(0.1f);
    ASSERT_EQ(f1.value->getValue(), 0.1f);
    ASSERT_TRUE(f1.value->getType()->isFloat());

    Value<Float> f2 = 0.2f;
    ASSERT_EQ(cast<Float>(f2.value)->getValue(), 0.2f);
    ASSERT_TRUE(f2.value->getType()->isFloat());
}

TEST_F(ValueTypeTest, Int8Test) {
    auto f1 = Value<Int8>((int8_t) 42);
    ASSERT_EQ(f1.value->getValue(), (int8_t) 42);
    ASSERT_TRUE(f1.value->getType()->isInteger());
    auto stamp = f1.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp)->getNumberOfBits(), 8);

    Value<Int8> f2 = (int8_t) 32;
    ASSERT_EQ(f2.value->getValue(), (int8_t) 32);
    ASSERT_TRUE(f2.value->getType()->isInteger());
    auto stamp2 = f2.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp2)->getNumberOfBits(), 8);

    auto f3 = f1 + f2;
    ASSERT_EQ(f3.as<Int8>().value->getValue(), (int8_t) 74);
}

TEST_F(ValueTypeTest, Int16Test) {
    auto f1 = Value<Int16>((int16_t) 42);
    ASSERT_EQ(f1.value->getValue(), (int16_t) 42);
    ASSERT_TRUE(f1.value->getType()->isInteger());
    auto stamp = f1.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp)->getNumberOfBits(), 16);

    Value<Int16> f2 = (int16_t) 32;
    ASSERT_EQ(f2.value->getValue(), (int16_t) 32);
    ASSERT_TRUE(f2.value->getType()->isInteger());
    auto stamp2 = f2.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp2)->getNumberOfBits(), 16);
    auto f3 = f1 + f2;
    ASSERT_EQ(f3.as<Int16>().value->getValue(), (int16_t) 74);
}

TEST_F(ValueTypeTest, Int64Test) {
    auto f1 = Value<Int64>((int64_t) 42);
    ASSERT_EQ(f1.value->getValue(), (int64_t) 42);
    ASSERT_TRUE(f1.value->getType()->isInteger());
    auto stamp = f1.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp)->getNumberOfBits(), 64);

    Value<Int64> f2 = (int64_t) 32;
    ASSERT_EQ(f2.value->getValue(), (int64_t) 32);
    ASSERT_TRUE(f2.value->getType()->isInteger());
    auto stamp2 = f2.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp2)->getNumberOfBits(), 64);
    auto f3 = f1 + f2;
    ASSERT_EQ(f3.as<Int64>().value->getValue(), (int64_t) 74);
}

TEST_F(ValueTypeTest, IntCastTest) {
    Value<Int8> f1 = (int8_t) 32;
    Value<Int64> f2 = (int64_t) 32;
    auto f3 = f1 + f2;
    ASSERT_EQ(f3.as<Int64>().getValue().getValue(), (int64_t) 64);

    Value<UInt64> u1 = (uint64_t) 32;

    ASSERT_ANY_THROW(u1 + f1);
}

TEST_F(ValueTypeTest, UInt8Test) {
    auto f1 = Value<UInt8>((uint8_t) 42);
    ASSERT_EQ(f1.value->getValue(), (uint8_t) 42);
    ASSERT_TRUE(f1.value->getType()->isInteger());
    auto stamp = f1.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp)->getNumberOfBits(), 8);

    Value<UInt8> f2 = (uint8_t) 32;
    ASSERT_EQ(f2.value->getValue(), (uint8_t) 32);
    ASSERT_TRUE(f2.value->getType()->isInteger());
    auto stamp2 = f2.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp2)->getNumberOfBits(), 8);
    auto f3 = f1 + f2;
    ASSERT_EQ(f3.as<UInt8>().value->getValue(), (uint8_t) 74);
}

TEST_F(ValueTypeTest, UInt16Test) {
    auto f1 = Value<UInt16>((uint16_t) 42);
    ASSERT_EQ(f1.value->getValue(), (uint16_t) 42);
    ASSERT_TRUE(f1.value->getType()->isInteger());
    auto stamp = f1.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp)->getNumberOfBits(), 16);

    Value<UInt16> f2 = (uint16_t) 32;
    ASSERT_EQ(f2.value->getValue(), (uint16_t) 32);
    ASSERT_TRUE(f2.value->getType()->isInteger());
    auto stamp2 = f2.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp2)->getNumberOfBits(), 16);
    auto f3 = f1 + f2;
    ASSERT_EQ(f3.as<UInt16>().value->getValue(), (uint16_t) 74);
}

TEST_F(ValueTypeTest, UInt64Test) {
    auto f1 = Value<UInt64>((uint64_t) 42);
    ASSERT_EQ(f1.value->getValue(), (uint64_t) 42);
    ASSERT_TRUE(f1.value->getType()->isInteger());
    auto stamp = f1.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp)->getNumberOfBits(), 64);

    Value<UInt64> f2 = (uint64_t) 32;
    ASSERT_EQ(f2.value->getValue(), (uint64_t) 32);
    ASSERT_TRUE(f2.value->getType()->isInteger());
    auto stamp2 = f2.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp2)->getNumberOfBits(), 64);
    auto f3 = f1 + f2;
    ASSERT_EQ(f3.as<UInt64>().value->getValue(), (uint64_t) 74);
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter
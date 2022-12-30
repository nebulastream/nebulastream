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

#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Nautilus/Interface/DataTypes/Float/Float.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
namespace NES::Nautilus {

class DataTypeTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("DataTypeTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DataTypeTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down DataTypeTest test class."); }
};

TEST_F(DataTypeTest, ConstructValueTest) {
    // construct primitive
    auto f1 = Value<Int8>((int8_t) 42);
    ASSERT_EQ(f1, (int8_t) 42);
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(f1.value->getType())->getNumberOfBits(), 8);

    // construct by rvalue
    auto f2 = Value<>(Int8(42));
    ASSERT_EQ(f2.as<Int8>()->getValue(), (int8_t) 42);
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(f2.value->getType())->getNumberOfBits(), 8);

    // construct by lvalue
    auto lvalue = Int8(42);
    auto f3 = Value<>(lvalue);
    ASSERT_EQ(f3.as<Int8>()->getValue(), (int8_t) 42);
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(f3.value->getType())->getNumberOfBits(), 8);

    // construct by shared ptr
    auto f4 = Value<>(std::make_shared<Int8>(42));
    ASSERT_EQ(f4.as<Int8>()->getValue(), (int8_t) 42);
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(f4.value->getType())->getNumberOfBits(), 8);

    // construct by assignment to any
    Value<> f5 = (int8_t) 42;
    ASSERT_EQ(f5.as<Int8>()->getValue(), (int8_t) 42);
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(f5.value->getType())->getNumberOfBits(), 8);

    // construct by assignment to typed
    Value<Int8> f6 = (int8_t) 42;
    ASSERT_EQ(f6.as<Int8>()->getValue(), (int8_t) 42);
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(f6.value->getType())->getNumberOfBits(), 8);
}

TEST_F(DataTypeTest, AssignmentValueTest) {

    {
        // Assign same type
        Value<Int8> a = (int8_t) 0;
        Value<Int8> b = (int8_t) 42;
        a = b;
        ASSERT_EQ(a->getValue(), (int8_t) 42);
        ASSERT_EQ(cast<IR::Types::IntegerStamp>(a.value->getType())->getNumberOfBits(), 8);
    }

    {
        // Assign type to any type
        Value<> a = 32;
        Value<Int8> b = (int8_t) 42;
        a = b;
        ASSERT_EQ(a.as<Int8>(), (int8_t) 42);
        ASSERT_EQ(cast<IR::Types::IntegerStamp>(a.value->getType())->getNumberOfBits(), 8);
    }

    {
        // Assign any to any type
        Value<> a = 32;
        Value<> b = 42.0f;
        a = b;
        ASSERT_EQ(a.as<Float>(), (float) 42.0);
    }
}

TEST_F(DataTypeTest, Int8Test) {
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

TEST_F(DataTypeTest, Int16Test) {
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

TEST_F(DataTypeTest, Int64Test) {
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

TEST_F(DataTypeTest, IntCastTest) {
    Value<Int8> f1 = (int8_t) 32;
    Value<Int64> f2 = (int64_t) 32;
    auto f3 = f1 + f2;
    ASSERT_EQ(f3.as<Int64>().getValue().getValue(), (int64_t) 64);
    Value<UInt64> u1 = (uint64_t) 32;
    ASSERT_ANY_THROW(u1 + f1);
}

TEST_F(DataTypeTest, UInt8Test) {
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

TEST_F(DataTypeTest, UInt16Test) {
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

TEST_F(DataTypeTest, UInt64Test) {
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

TEST_F(DataTypeTest, FloatTest) {
    auto f1 = Value<Float>(0.1f);
    ASSERT_EQ(f1.value->getValue(), 0.1f);
    ASSERT_TRUE(f1.value->getType()->isFloat());

    Value<Float> f2 = 0.2f;
    ASSERT_EQ(cast<Float>(f2.value)->getValue(), 0.2f);
    ASSERT_TRUE(f2.value->getType()->isFloat());
}

}// namespace NES::Nautilus
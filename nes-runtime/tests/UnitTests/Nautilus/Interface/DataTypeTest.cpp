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
#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <gtest/gtest.h>
#include <memory>
namespace NES::Nautilus {

class DataTypeTest : public Testing::TestWithErrorHandling {
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
    auto f1 = Value<Int8>(42_s8);
    ASSERT_EQ(f1, 42_s8);
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(f1.value->getType())->getNumberOfBits(), 8);

    // construct by rvalue
    auto f2 = Value<>(Int8(42));
    ASSERT_EQ(f2.as<Int8>()->getValue(), 42_s8);
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(f2.value->getType())->getNumberOfBits(), 8);

    // construct by lvalue
    auto lvalue = Int8(42);
    auto f3 = Value<>(lvalue);
    ASSERT_EQ(f3.as<Int8>()->getValue(), 42_s8);
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(f3.value->getType())->getNumberOfBits(), 8);

    // construct by shared ptr
    auto f4 = Value<>(std::make_shared<Int8>(42));
    ASSERT_EQ(f4.as<Int8>()->getValue(), 42_s8);
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(f4.value->getType())->getNumberOfBits(), 8);

    // construct by assignment to any
    Value<> f5(42_s8);
    ASSERT_EQ(f5.as<Int8>()->getValue(), 42_s8);
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(f5.value->getType())->getNumberOfBits(), 8);

    // construct by assignment to typed
    Value<Int8> f6(42_s8);
    ASSERT_EQ(f6.as<Int8>()->getValue(), 42_s8);
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(f6.value->getType())->getNumberOfBits(), 8);
}

TEST_F(DataTypeTest, AssignmentValueTest) {

    {
        // Assign same type
        Value<Int8> a(0_s8);
        Value<Int8> b(42_s8);
        a = b;
        ASSERT_EQ(a->getValue(), 42_s8);
        ASSERT_EQ(cast<IR::Types::IntegerStamp>(a.value->getType())->getNumberOfBits(), 8);
    }

    {
        // Assign type to any type
        Value<> a = 32;
        Value<Int8> b(42_s8);
        a = b;
        ASSERT_EQ(a.as<Int8>(), 42_s8);
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
    auto f1 = Value<Int8>(42_s8);
    ASSERT_EQ(f1.value->getValue(), 42_s8);
    ASSERT_TRUE(f1.value->getType()->isInteger());
    auto stamp = f1.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp)->getNumberOfBits(), 8);

    Value<Int8> f2(32_s8);
    ASSERT_EQ(f2.value->getValue(), 32_s8);
    ASSERT_TRUE(f2.value->getType()->isInteger());
    auto stamp2 = f2.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp2)->getNumberOfBits(), 8);

    auto f3 = f1 + f2;
    ASSERT_EQ(f3.as<Int8>().value->getValue(), 74_s8);
}

TEST_F(DataTypeTest, Int16Test) {
    auto f1 = Value<Int16>(42_s16);
    ASSERT_EQ(f1.value->getValue(), 42_s16);
    ASSERT_TRUE(f1.value->getType()->isInteger());
    auto stamp = f1.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp)->getNumberOfBits(), 16);

    Value<Int16> f2(32_s16);
    ASSERT_EQ(f2.value->getValue(), 32_s16);
    ASSERT_TRUE(f2.value->getType()->isInteger());
    auto stamp2 = f2.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp2)->getNumberOfBits(), 16);
    auto f3 = f1 + f2;
    ASSERT_EQ(f3.as<Int16>().value->getValue(), 74_s16);
}

TEST_F(DataTypeTest, Int64Test) {
    auto f1 = Value<Int64>(42_s64);
    ASSERT_EQ(f1.value->getValue(), 42_s64);
    ASSERT_TRUE(f1.value->getType()->isInteger());
    auto stamp = f1.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp)->getNumberOfBits(), 64);

    Value<Int64> f2(32_s64);
    ASSERT_EQ(f2.value->getValue(), 32_s64);
    ASSERT_TRUE(f2.value->getType()->isInteger());
    auto stamp2 = f2.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp2)->getNumberOfBits(), 64);
    auto f3 = f1 + f2;
    ASSERT_EQ(f3.as<Int64>().value->getValue(), 74_s64);
}

TEST_F(DataTypeTest, UInt8Test) {
    auto f1 = Value<UInt8>(42_u8);
    ASSERT_EQ(f1.value->getValue(), 42_u8);
    ASSERT_TRUE(f1.value->getType()->isInteger());
    auto stamp = f1.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp)->getNumberOfBits(), 8);

    Value<UInt8> f2 = 32_u8;
    ASSERT_EQ(f2.value->getValue(), 32_u8);
    ASSERT_TRUE(f2.value->getType()->isInteger());
    auto stamp2 = f2.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp2)->getNumberOfBits(), 8);
    auto f3 = f1 + f2;
    ASSERT_EQ(f3.as<UInt8>().value->getValue(), 74_u8);
}

TEST_F(DataTypeTest, UInt16Test) {
    auto f1 = Value<UInt16>(42_u16);
    ASSERT_EQ(f1.value->getValue(), 42_u16);
    ASSERT_TRUE(f1.value->getType()->isInteger());
    auto stamp = f1.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp)->getNumberOfBits(), 16);

    Value<UInt16> f2 = 32_u16;
    ASSERT_EQ(f2.value->getValue(), 32_u16);
    ASSERT_TRUE(f2.value->getType()->isInteger());
    auto stamp2 = f2.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp2)->getNumberOfBits(), 16);
    auto f3 = f1 + f2;
    ASSERT_EQ(f3.as<UInt16>().value->getValue(), 74_u16);
}

TEST_F(DataTypeTest, UInt64Test) {
    auto f1 = Value<UInt64>(42_u64);
    ASSERT_EQ(f1.value->getValue(), 42_u64);
    ASSERT_TRUE(f1.value->getType()->isInteger());
    auto stamp = f1.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp)->getNumberOfBits(), 64);

    Value<UInt64> f2 = 32_u64;
    ASSERT_EQ(f2.value->getValue(), 32_u64);
    ASSERT_TRUE(f2.value->getType()->isInteger());
    auto stamp2 = f2.value->getType();
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(stamp2)->getNumberOfBits(), 64);
    auto f3 = f1 + f2;
    ASSERT_EQ(f3.as<UInt64>().value->getValue(), 74_u64);
}

TEST_F(DataTypeTest, IntCastTest) {

    Value<Int8> i8(32_s8);
    Value<Int16> i16(32_s16);
    Value<Int32> i32(32_s32);
    Value<Int64> i64(32_s64);

    {
        // cast i8 to i16
        auto v1 = i8 + i16;
        ASSERT_EQ(v1->getTypeIdentifier(), &Int16::type);
        ASSERT_EQ(v1.as<Int16>()->getValue(), 64_s16);
    }

    {
        // cast i8 to i32
        auto v1 = i8 + i32;
        ASSERT_EQ(v1->getTypeIdentifier(), &Int32::type);
        ASSERT_EQ(v1.as<Int32>()->getValue(), 64_s32);
    }

    {
        // cast i8 to i64
        auto v1 = i8 + i64;
        ASSERT_EQ(v1->getTypeIdentifier(), &Int64::type);
        ASSERT_EQ(v1.as<UInt64>()->getValue(), 64_s64);
    }

    {
        // cast i16 to i32
        auto v1 = i16 + i32;
        ASSERT_EQ(v1->getTypeIdentifier(), &Int32::type);
        ASSERT_EQ(v1.as<Int32>()->getValue(), 64_s32);
    }

    {
        // cast i16 to i64
        auto v1 = i16 + i64;
        ASSERT_EQ(v1->getTypeIdentifier(), &Int64::type);
        ASSERT_EQ(v1.as<UInt64>()->getValue(), 64_s64);
    }

    {
        // cast i32 to i64
        auto v1 = i32 + i64;
        ASSERT_EQ(v1->getTypeIdentifier(), &Int64::type);
        ASSERT_EQ(v1.as<Int64>()->getValue(), 64_s64);
    }
}

TEST_F(DataTypeTest, UIntCastTest) {
    Value<UInt8> ui8 = 32_u8;
    Value<UInt16> ui16 = 32_u16;
    Value<UInt32> ui32 = 32_u32;
    Value<UInt64> ui64 = 32_u64;

    {
        // cast ui8 to ui16
        auto v1 = ui8 + ui16;
        ASSERT_EQ(v1->getTypeIdentifier(), &UInt16::type);
        ASSERT_EQ(v1.as<UInt16>()->getValue(), 64_u16);
    }

    {
        // cast ui8 to ui32
        auto v1 = ui8 + ui32;
        ASSERT_EQ(v1->getTypeIdentifier(), &UInt32::type);
        ASSERT_EQ(v1.as<UInt32>()->getValue(), 64_u32);
    }

    {
        // cast ui8 to ui64
        auto v1 = ui8 + ui64;
        ASSERT_EQ(v1->getTypeIdentifier(), &UInt64::type);
        ASSERT_EQ(v1.as<UInt64>()->getValue(), 64_u64);
    }

    {
        // cast ui16 to ui32
        auto v1 = ui16 + ui32;
        ASSERT_EQ(v1->getTypeIdentifier(), &UInt32::type);
        ASSERT_EQ(v1.as<UInt32>()->getValue(), 64_u32);
    }

    {
        // cast ui16 to ui64
        auto v1 = ui16 + ui64;
        ASSERT_EQ(v1->getTypeIdentifier(), &UInt64::type);
        ASSERT_EQ(v1.as<UInt64>()->getValue(), 64_u64);
    }

    {
        // cast ui32 to ui64
        auto v1 = ui32 + ui64;
        ASSERT_EQ(v1->getTypeIdentifier(), &UInt64::type);
        ASSERT_EQ(v1.as<UInt64>()->getValue(), 64_u64);
    }
}

TEST_F(DataTypeTest, UIntAndIntCastTest) {
    Value<UInt8> ui8 = 32_u8;
    Value<UInt16> ui16 = 32_u16;
    Value<UInt32> ui32 = 32_u32;
    Value<UInt64> ui64 = 32_u64;

    Value<Int8> i8(32_s8);
    Value<Int16> i16(32_s16);
    Value<Int32> i32(32_s32);
    Value<Int64> i64(32_s64);

    {
        // cast ui8 and i8
        auto v1 = ui8 + i8;
        ASSERT_EQ(v1->getTypeIdentifier(), &UInt8::type);
        ASSERT_EQ(v1.as<UInt8>()->getValue(), 64_u8);
    }

    {
        // cast ui8 and i16
        auto v1 = ui8 + i16;
        ASSERT_EQ(v1->getTypeIdentifier(), &Int16::type);
        ASSERT_EQ(v1.as<Int16>()->getValue(), 64_s16);
    }

    {
        // cast ui8 to i32
        auto v1 = ui8 + i32;
        ASSERT_EQ(v1->getTypeIdentifier(), &Int32::type);
        ASSERT_EQ(v1.as<Int32>()->getValue(), 64_s32);
    }

    {
        // cast ui8 to i64
        auto v1 = ui8 + i64;
        ASSERT_EQ(v1->getTypeIdentifier(), &Int64::type);
        ASSERT_EQ(v1.as<Int64>()->getValue(), 64_s64);
    }

    {
        // cast ui16 to i8
        auto v1 = ui16 + i8;
        ASSERT_EQ(v1->getTypeIdentifier(), &UInt16::type);
        ASSERT_EQ(v1.as<UInt16>()->getValue(), 64_u16);
    }

    {
        // cast ui16 to i16
        auto v1 = ui16 + i16;
        ASSERT_EQ(v1->getTypeIdentifier(), &UInt16::type);
        ASSERT_EQ(v1.as<UInt16>()->getValue(), 64_u16);
    }

    {
        // cast ui16 to i32
        auto v1 = ui16 + i32;
        ASSERT_EQ(v1->getTypeIdentifier(), &Int32::type);
        ASSERT_EQ(v1.as<Int32>()->getValue(), 64_s32);
    }

    {
        // cast ui16 to i64
        auto v1 = ui16 + i64;
        ASSERT_EQ(v1->getTypeIdentifier(), &Int64::type);
        ASSERT_EQ(v1.as<Int64>()->getValue(), 64_s64);
    }

    {
        // cast ui32 to i8
        auto v1 = ui32 + i8;
        ASSERT_EQ(v1->getTypeIdentifier(), &UInt32::type);
        ASSERT_EQ(v1.as<UInt32>()->getValue(), 64_u32);
    }
    {
        // cast ui32 to i16
        auto v1 = ui32 + i16;
        ASSERT_EQ(v1->getTypeIdentifier(), &UInt32::type);
        ASSERT_EQ(v1.as<UInt32>()->getValue(), 64_u32);
    }

    {
        // cast ui32 to i32
        auto v1 = ui32 + i32;
        ASSERT_EQ(v1->getTypeIdentifier(), &UInt32::type);
        ASSERT_EQ(v1.as<UInt32>()->getValue(), 64_u32);
    }
    {
        // cast ui32 to i64
        auto v1 = ui32 + i64;
        ASSERT_EQ(v1->getTypeIdentifier(), &Int64::type);
        ASSERT_EQ(v1.as<Int64>()->getValue(), 64_s64);
    }

    {
        // cast ui64 to i64
        auto v1 = ui64 + i64;
        ASSERT_EQ(v1->getTypeIdentifier(), &UInt64::type);
        ASSERT_EQ(v1.as<UInt64>()->getValue(), 64_u64);
    }
}

TEST_F(DataTypeTest, FloatTest) {
    auto f1 = Value<Float>(0.1f);
    ASSERT_EQ(f1.value->getValue(), 0.1f);
    ASSERT_TRUE(f1.value->getType()->isFloat());

    Value<Float> f2 = 0.2f;
    ASSERT_EQ(cast<Float>(f2.value)->getValue(), 0.2f);
    ASSERT_TRUE(f2.value->getType()->isFloat());
}

TEST_F(DataTypeTest, FloatCastTest) {
    auto i16 = Value<Int16>(42_s16);
    auto i32 = Value<Int32>(42_s32);
    auto i64 = Value<Int64>(42_s64);
    auto floatV = Value<Float>(1.0f);
    auto doubleV = Value<Double>(1.0);

    {
        // cast i16 to floatV
        auto v1 = i16 + floatV;
        ASSERT_EQ(v1->getTypeIdentifier(), &Float::type);
        ASSERT_EQ(v1.as<Float>()->getValue(), (float) 43);
    }

    {
        // cast i32 to floatV
        auto v1 = i32 + floatV;
        ASSERT_EQ(v1->getTypeIdentifier(), &Float::type);
        ASSERT_EQ(v1.as<Float>()->getValue(), (float) 43);
    }

    {
        // cast i64 to floatV
        auto v1 = i64 + floatV;
        ASSERT_EQ(v1->getTypeIdentifier(), &Float::type);
        ASSERT_EQ(v1.as<Float>()->getValue(), (float) 43);
    }

    {
        // cast i16 to doubleV
        auto v1 = i16 + doubleV;
        ASSERT_EQ(v1->getTypeIdentifier(), &Double::type);
        ASSERT_EQ(v1.as<Double>()->getValue(), (double) 43);
    }

    {
        // cast i32 to doubleV
        auto v1 = i32 + doubleV;
        ASSERT_EQ(v1->getTypeIdentifier(), &Double::type);
        ASSERT_EQ(v1.as<Double>()->getValue(), (double) 43);
    }

    {
        // cast i64 to doubleV
        auto v1 = i64 + doubleV;
        ASSERT_EQ(v1->getTypeIdentifier(), &Double::type);
        ASSERT_EQ(v1.as<Double>()->getValue(), (double) 43);
    }
}

struct TestS {
    uint64_t x;
    uint64_t y;
    uint64_t z;
};

TEST_F(DataTypeTest, LoadMemberTest) {
    auto test = TestS{10, 20, 30};
    Value<MemRef> ref = Value<MemRef>((int8_t*) &test);
    ASSERT_EQ(getMember(ref, TestS, x).load<UInt64>(), 10_u64);
    ASSERT_EQ(getMember(ref, TestS, y).load<UInt64>(), 20_u64);
    ASSERT_EQ(getMember(ref, TestS, z).load<UInt64>(), 30_u64);
}

}// namespace NES::Nautilus
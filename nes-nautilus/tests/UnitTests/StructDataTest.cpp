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

#include <array>
#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/FixedSizedData.hpp>
#include <Nautilus/DataTypes/StructData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class StructDataTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("StructDataTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup StructDataTest class.");
    }
    static void TearDownTestCase() { NES_INFO("Tear down StructDataTest class."); }
};

namespace
{
DataType primitive(DataType::Type t)
{
    return DataType{t, DataType::NULLABLE::NOT_NULLABLE};
}

DataType fixedArray(DataType::Type element, uint32_t count)
{
    return DataType{DataType::Type::FIXEDSIZED, DataType::NULLABLE::NOT_NULLABLE, element, count};
}
}

TEST_F(StructDataTest, GettersReflectConstructorArgs)
{
    /// `Point { x: INT32, y: INT32 }` — two 4-byte primitives, 8 bytes total.
    std::array<int32_t, 2> bytes = {7, -3};
    std::vector<std::pair<std::string, DataType>> fields{
        {"x", primitive(DataType::Type::INT32)}, {"y", primitive(DataType::Type::INT32)}};
    const StructData s(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(bytes.data())), fields);

    EXPECT_EQ(s.getNumFields(), 2U);
    EXPECT_EQ(s.getTotalSizeInBytes(), 2U * sizeof(int32_t));
    EXPECT_EQ(s.getFields(), fields);
}

TEST_F(StructDataTest, AtReturnsTypedPrimitiveByIndex)
{
    /// Heterogeneous primitives. Layout: i64 (8) | u8 (1) | f32 (4) | char (1) = 14 bytes.
#pragma pack(push, 1)
    struct Layout
    {
        int64_t a;
        uint8_t b;
        float c;
        char d;
    };
#pragma pack(pop)
    Layout buf{.a = -1234567890, .b = 200, .c = 3.5F, .d = 'Z'};
    std::vector<std::pair<std::string, DataType>> fields{
        {"a", primitive(DataType::Type::INT64)},
        {"b", primitive(DataType::Type::UINT8)},
        {"c", primitive(DataType::Type::FLOAT32)},
        {"d", primitive(DataType::Type::CHAR)}};
    const StructData s(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(&buf)), fields);

    EXPECT_EQ(s.at(0U).getRawValueAs<nautilus::val<int64_t>>(), -1234567890);
    EXPECT_EQ(s.at(1U).getRawValueAs<nautilus::val<uint8_t>>(), 200);
    EXPECT_EQ(s.at(2U).getRawValueAs<nautilus::val<float>>(), 3.5F);
    EXPECT_EQ(s.at(3U).getRawValueAs<nautilus::val<char>>(), 'Z');
}

TEST_F(StructDataTest, AtReturnsTypedPrimitiveByName)
{
    std::array<int32_t, 2> bytes = {42, 100};
    std::vector<std::pair<std::string, DataType>> fields{
        {"first", primitive(DataType::Type::INT32)}, {"second", primitive(DataType::Type::INT32)}};
    const StructData s(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(bytes.data())), fields);

    EXPECT_EQ(s.at("first").getRawValueAs<nautilus::val<int32_t>>(), 42);
    EXPECT_EQ(s.at("second").getRawValueAs<nautilus::val<int32_t>>(), 100);
}

TEST_F(StructDataTest, AtUnknownFieldThrows)
{
    std::array<int32_t, 1> bytes = {1};
    std::vector<std::pair<std::string, DataType>> fields{{"x", primitive(DataType::Type::INT32)}};
    const StructData s(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(bytes.data())), fields);

    try
    {
        (void)s.at("nope");
        ADD_FAILURE() << "expected at(\"nope\") to throw FieldNotFound";
    }
    catch (const Exception& e)
    {
        EXPECT_EQ(e.code(), ErrorCode::FieldNotFound);
    }
}

TEST_F(StructDataTest, AtIndexOutOfRangeThrows)
{
    std::array<int32_t, 1> bytes = {1};
    std::vector<std::pair<std::string, DataType>> fields{{"x", primitive(DataType::Type::INT32)}};
    const StructData s(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(bytes.data())), fields);

    try
    {
        (void)s.at(static_cast<size_t>(5));
        ADD_FAILURE() << "expected at(5) to throw OutOfRangeAccess";
    }
    catch (const Exception& e)
    {
        EXPECT_EQ(e.code(), ErrorCode::OutOfRangeAccess);
    }
}

TEST_F(StructDataTest, FixedSizedFieldYieldsFixedSizedDataView)
{
    /// `Frame { pixels: FIXEDSIZED<UINT16, 4> }` — single inline array field.
    std::array<uint16_t, 4> pixels = {10, 20, 30, 40};
    std::vector<std::pair<std::string, DataType>> fields{{"pixels", fixedArray(DataType::Type::UINT16, 4)}};
    const StructData s(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(pixels.data())), fields);

    EXPECT_EQ(s.getTotalSizeInBytes(), 4U * sizeof(uint16_t));

    const VarVal pixelField = s.at("pixels");
    const auto fixed = pixelField.getRawValueAs<FixedSizedData>();
    EXPECT_EQ(fixed.getNumElements(), 4U);
    EXPECT_EQ(fixed.getElementType(), DataType::Type::UINT16);
    for (uint64_t i = 0; i < pixels.size(); ++i)
    {
        EXPECT_EQ(fixed.at(nautilus::val<uint64_t>(i)).getRawValueAs<nautilus::val<uint16_t>>(), pixels[i]);
    }
}

TEST_F(StructDataTest, NestedStructFieldYieldsStructDataView)
{
    /// Outer { tag: UINT8, inner: STRUCT { value: UINT16 } } — exercises the
    /// recursive layout (offset of `inner` is 1 byte, then UINT16 inline).
#pragma pack(push, 1)
    struct Layout
    {
        uint8_t tag;
        uint16_t value;
    };
#pragma pack(pop)
    Layout buf{.tag = 9, .value = 1234};

    std::vector<std::pair<std::string, DataType>> innerFields{{"value", primitive(DataType::Type::UINT16)}};
    DataType innerStruct{DataType::Type::STRUCT, DataType::NULLABLE::NOT_NULLABLE, std::string{"Inner"}, innerFields};
    std::vector<std::pair<std::string, DataType>> outerFields{{"tag", primitive(DataType::Type::UINT8)}, {"inner", innerStruct}};
    const StructData outer(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(&buf)), outerFields);

    EXPECT_EQ(outer.getTotalSizeInBytes(), 1U + sizeof(uint16_t));
    EXPECT_EQ(outer.at("tag").getRawValueAs<nautilus::val<uint8_t>>(), 9);

    const VarVal innerVal = outer.at("inner");
    const auto innerStructData = innerVal.getRawValueAs<StructData>();
    EXPECT_EQ(innerStructData.getNumFields(), 1U);
    EXPECT_EQ(innerStructData.at("value").getRawValueAs<nautilus::val<uint16_t>>(), 1234);
}

TEST_F(StructDataTest, EqualityIdenticalContent)
{
    std::array<int32_t, 2> a = {7, -3};
    std::array<int32_t, 2> b = a;
    std::vector<std::pair<std::string, DataType>> fields{
        {"x", primitive(DataType::Type::INT32)}, {"y", primitive(DataType::Type::INT32)}};
    const StructData lhs(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(a.data())), fields);
    const StructData rhs(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(b.data())), fields);

    EXPECT_TRUE(lhs == rhs);
    EXPECT_FALSE(lhs != rhs);
}

TEST_F(StructDataTest, EqualityDifferentContent)
{
    std::array<int32_t, 2> a = {7, -3};
    std::array<int32_t, 2> b = {7, 0};
    std::vector<std::pair<std::string, DataType>> fields{
        {"x", primitive(DataType::Type::INT32)}, {"y", primitive(DataType::Type::INT32)}};
    const StructData lhs(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(a.data())), fields);
    const StructData rhs(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(b.data())), fields);

    EXPECT_FALSE(lhs == rhs);
    EXPECT_TRUE(lhs != rhs);
}

TEST_F(StructDataTest, EqualityDifferentLayoutFails)
{
    std::array<int32_t, 2> data = {7, -3};
    std::vector<std::pair<std::string, DataType>> fields1{
        {"x", primitive(DataType::Type::INT32)}, {"y", primitive(DataType::Type::INT32)}};
    std::vector<std::pair<std::string, DataType>> fields2{
        {"a", primitive(DataType::Type::INT32)}, {"b", primitive(DataType::Type::INT32)}};
    const StructData lhs(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(data.data())), fields1);
    const StructData rhs(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(data.data())), fields2);

    EXPECT_FALSE(lhs == rhs);
}

TEST_F(StructDataTest, WriteAtPrimitiveByName)
{
    /// Round-trip a primitive field write: write a known value via writeAt, read it back via at.
    std::array<int32_t, 2> bytes = {0, 0};
    std::vector<std::pair<std::string, DataType>> fields{
        {"x", primitive(DataType::Type::INT32)}, {"y", primitive(DataType::Type::INT32)}};
    const StructData s(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(bytes.data())), fields);

    s.writeAt("x", VarVal{nautilus::val<int32_t>(123)});
    s.writeAt("y", VarVal{nautilus::val<int32_t>(-42)});

    EXPECT_EQ(s.at("x").getRawValueAs<nautilus::val<int32_t>>(), 123);
    EXPECT_EQ(s.at("y").getRawValueAs<nautilus::val<int32_t>>(), -42);
}

TEST_F(StructDataTest, WriteAtPrimitiveByIndex)
{
    /// Same idea but addressing fields positionally — verifies index dispatch
    /// and that the offset arithmetic is in sync with `at(index)`.
    std::array<int32_t, 2> bytes = {0, 0};
    std::vector<std::pair<std::string, DataType>> fields{
        {"x", primitive(DataType::Type::INT32)}, {"y", primitive(DataType::Type::INT32)}};
    const StructData s(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(bytes.data())), fields);

    s.writeAt(static_cast<size_t>(0), VarVal{nautilus::val<int32_t>(7)});
    s.writeAt(static_cast<size_t>(1), VarVal{nautilus::val<int32_t>(11)});

    EXPECT_EQ(s.at(0U).getRawValueAs<nautilus::val<int32_t>>(), 7);
    EXPECT_EQ(s.at(1U).getRawValueAs<nautilus::val<int32_t>>(), 11);
}

TEST_F(StructDataTest, WriteAtFixedSizedFieldCopiesInlineBytes)
{
    /// FIXEDSIZED field write goes through nautilus::memcpy of the source's
    /// inline bytes. Source and destination buffers are independent so we can
    /// confirm bytes were actually copied (not just aliased).
    std::array<uint16_t, 4> destBuffer = {0, 0, 0, 0};
    std::vector<std::pair<std::string, DataType>> fields{{"pixels", fixedArray(DataType::Type::UINT16, 4)}};
    const StructData s(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(destBuffer.data())), fields);

    std::array<uint16_t, 4> sourcePixels = {10, 20, 30, 40};
    const FixedSizedData sourceView(
        nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(sourcePixels.data())), sourcePixels.size(), DataType::Type::UINT16);
    s.writeAt("pixels", VarVal{sourceView});

    /// Read back through the StructData → FixedSizedData view; values must
    /// match the source byte-for-byte.
    const auto view = s.at("pixels").getRawValueAs<FixedSizedData>();
    for (uint64_t i = 0; i < sourcePixels.size(); ++i)
    {
        EXPECT_EQ(view.at(nautilus::val<uint64_t>(i)).getRawValueAs<nautilus::val<uint16_t>>(), sourcePixels[i]);
    }
    /// And the dest buffer itself should have been mutated.
    EXPECT_EQ(destBuffer, sourcePixels);
}

TEST_F(StructDataTest, WriteAtNestedStructCopiesInlineBytes)
{
    /// Same as the FIXEDSIZED case but for a nested STRUCT field.
#pragma pack(push, 1)
    struct InnerLayout
    {
        uint16_t value;
    };
    struct Outer
    {
        uint8_t tag;
        InnerLayout inner;
    };
#pragma pack(pop)
    Outer destBuf{.tag = 0, .inner = {.value = 0}};

    std::vector<std::pair<std::string, DataType>> innerFields{{"value", primitive(DataType::Type::UINT16)}};
    DataType innerStructType{DataType::Type::STRUCT, DataType::NULLABLE::NOT_NULLABLE, std::string{"Inner"}, innerFields};
    std::vector<std::pair<std::string, DataType>> outerFields{
        {"tag", primitive(DataType::Type::UINT8)}, {"inner", innerStructType}};
    const StructData outer(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(&destBuf)), outerFields);

    InnerLayout sourceInner{.value = 4242};
    const StructData sourceInnerStruct(
        nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(&sourceInner)), innerFields);
    outer.writeAt("inner", VarVal{sourceInnerStruct});

    EXPECT_EQ(outer.at("inner").getRawValueAs<StructData>().at("value").getRawValueAs<nautilus::val<uint16_t>>(), 4242);
    EXPECT_EQ(destBuf.inner.value, 4242);
}

TEST_F(StructDataTest, WriteAtUnknownFieldThrows)
{
    std::array<int32_t, 1> bytes = {1};
    std::vector<std::pair<std::string, DataType>> fields{{"x", primitive(DataType::Type::INT32)}};
    const StructData s(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(bytes.data())), fields);

    try
    {
        s.writeAt("nope", VarVal{nautilus::val<int32_t>(0)});
        ADD_FAILURE() << "expected writeAt(\"nope\", ...) to throw FieldNotFound";
    }
    catch (const Exception& e)
    {
        EXPECT_EQ(e.code(), ErrorCode::FieldNotFound);
    }
}

TEST_F(StructDataTest, WriteAtIndexOutOfRangeThrows)
{
    std::array<int32_t, 1> bytes = {1};
    std::vector<std::pair<std::string, DataType>> fields{{"x", primitive(DataType::Type::INT32)}};
    const StructData s(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(bytes.data())), fields);

    try
    {
        s.writeAt(static_cast<size_t>(7), VarVal{nautilus::val<int32_t>(0)});
        ADD_FAILURE() << "expected writeAt(7, ...) to throw OutOfRangeAccess";
    }
    catch (const Exception& e)
    {
        EXPECT_EQ(e.code(), ErrorCode::OutOfRangeAccess);
    }
}

TEST_F(StructDataTest, CopyAndAssign)
{
    std::array<int32_t, 2> data = {1, 2};
    std::vector<std::pair<std::string, DataType>> fields{
        {"x", primitive(DataType::Type::INT32)}, {"y", primitive(DataType::Type::INT32)}};
    const StructData original(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(data.data())), fields);

    /// NOLINTNEXTLINE(performance-unnecessary-copy-initialization) - intentional
    const StructData copy = original;
    EXPECT_EQ(copy.getNumFields(), 2U);
    EXPECT_EQ(copy.at("y").getRawValueAs<nautilus::val<int32_t>>(), 2);
}

}

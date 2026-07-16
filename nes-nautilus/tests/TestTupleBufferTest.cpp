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

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <numbers>
#include <optional>
#include <string>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <TestTupleBuffer.hpp>

namespace NES
{

class TestTupleBufferTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("TestTupleBufferTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup TestTupleBufferTest class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down TestTupleBufferTest class."); }

    void SetUp() override
    {
        Testing::BaseUnitTest::SetUp();
        bufferManager = BufferManager::create();
    }

    std::shared_ptr<BufferManager> bufferManager;
};

/// NOLINTBEGIN(readability-magic-numbers)

TEST_F(TestTupleBufferTest, AppendAndReadSingleField)
{
    auto schema = Testing::TestSchema{UnqualifiedUnboundField{Identifier::parse("value"), DataType::Type::INT64}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    view.append(int64_t{42});

    ASSERT_EQ(view.getNumberOfTuples(), 1);
    EXPECT_EQ(view[0]["value"].as<int64_t>(), 42);
}

TEST_F(TestTupleBufferTest, AppendAndReadMultipleFields)
{
    auto schema = Testing::TestSchema{
        UnqualifiedUnboundField{Identifier::parse("a"), DataType::Type::INT64},
        UnqualifiedUnboundField{Identifier::parse("b"), DataType::Type::UINT32}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    view.append(int64_t{100}, uint32_t{200});

    ASSERT_EQ(view.getNumberOfTuples(), 1);
    EXPECT_EQ(view[0]["a"].as<int64_t>(), 100);
    EXPECT_EQ(view[0]["b"].as<uint32_t>(), 200);
}

TEST_F(TestTupleBufferTest, AppendMultipleRecords)
{
    auto schema = Testing::TestSchema{UnqualifiedUnboundField{Identifier::parse("x"), DataType::Type::INT32}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    view.append(int32_t{10});
    view.append(int32_t{20});
    view.append(int32_t{30});

    ASSERT_EQ(view.getNumberOfTuples(), 3);
    EXPECT_EQ(view[0]["x"].as<int32_t>(), 10);
    EXPECT_EQ(view[1]["x"].as<int32_t>(), 20);
    EXPECT_EQ(view[2]["x"].as<int32_t>(), 30);
}

TEST_F(TestTupleBufferTest, IndexedWriteAndRead)
{
    auto schema = Testing::TestSchema{UnqualifiedUnboundField{Identifier::parse("val"), DataType::Type::INT64}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    /// Append a record to have at least one tuple.
    view.append(int64_t{0});

    /// Overwrite via indexed access.
    view[0]["val"] = Testing::FieldValue(int64_t{999});

    EXPECT_EQ(view[0]["val"].as<int64_t>(), 999);
}

TEST_F(TestTupleBufferTest, OutOfBoundsThrows)
{
    auto schema = Testing::TestSchema{UnqualifiedUnboundField{Identifier::parse("f"), DataType::Type::INT32}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    view.append(int32_t{1});
    view.append(int32_t{2});
    view.append(int32_t{3});

    EXPECT_ANY_THROW(view[5]);
}

TEST_F(TestTupleBufferTest, UnknownFieldThrows)
{
    auto schema = Testing::TestSchema{UnqualifiedUnboundField{Identifier::parse("exists"), DataType::Type::INT32}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    view.append(int32_t{1});

    EXPECT_ANY_THROW(view[0]["nonexistent"]);
}

TEST_F(TestTupleBufferTest, TypeMismatchThrows)
{
    auto schema = Testing::TestSchema{UnqualifiedUnboundField{Identifier::parse("i64"), DataType::Type::INT64}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    view.append(int64_t{1});

    /// Assigning int32_t to an INT64 field should throw.
    EXPECT_ANY_THROW(view[0]["i64"] = Testing::FieldValue(int32_t{42}));
}

TEST_F(TestTupleBufferTest, DanglingFieldViewThrows)
{
    auto schema = Testing::TestSchema{UnqualifiedUnboundField{Identifier::parse("f"), DataType::Type::INT32}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    Testing::FieldView captured;
    {
        auto buffer = bufferManager->getBufferBlocking();
        auto view = ttb.open(buffer);
        view.append(int32_t{42});
        captured = view[0]["f"];
    }

    /// The view's impl has been destroyed; using the captured FieldView should throw.
    EXPECT_ANY_THROW(captured.as<int32_t>());
}

TEST_F(TestTupleBufferTest, StringField)
{
    auto schema = Testing::TestSchema{UnqualifiedUnboundField{Identifier::parse("name"), DataType::Type::VARSIZED}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer, bufferManager.get());

    view.append(std::string("hello world"));

    ASSERT_EQ(view.getNumberOfTuples(), 1);
    EXPECT_EQ(view[0]["name"].as<std::string>(), "hello world");
}

TEST_F(TestTupleBufferTest, MixedFixedAndVarSizedFields)
{
    auto schema = Testing::TestSchema{
        UnqualifiedUnboundField{Identifier::parse("id"), DataType::Type::INT64},
        UnqualifiedUnboundField{Identifier::parse("label"), DataType::Type::VARSIZED}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer, bufferManager.get());

    view.append(int64_t{1}, std::string("first"));
    view.append(int64_t{2}, std::string("second"));

    ASSERT_EQ(view.getNumberOfTuples(), 2);
    EXPECT_EQ(view[0]["id"].as<int64_t>(), 1);
    EXPECT_EQ(view[0]["label"].as<std::string>(), "first");
    EXPECT_EQ(view[1]["id"].as<int64_t>(), 2);
    EXPECT_EQ(view[1]["label"].as<std::string>(), "second");
}

TEST_F(TestTupleBufferTest, BooleanField)
{
    auto schema = Testing::TestSchema{UnqualifiedUnboundField{Identifier::parse("flag"), DataType::Type::BOOLEAN}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    view.append(true);
    view.append(false);

    ASSERT_EQ(view.getNumberOfTuples(), 2);
    EXPECT_EQ(view[0]["flag"].as<bool>(), true);
    EXPECT_EQ(view[1]["flag"].as<bool>(), false);
}

TEST_F(TestTupleBufferTest, FloatingPointFields)
{
    auto schema = Testing::TestSchema{
        UnqualifiedUnboundField{Identifier::parse("f32"), DataType::Type::FLOAT32},
        UnqualifiedUnboundField{Identifier::parse("f64"), DataType::Type::FLOAT64}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    view.append(3.14F, std::numbers::e);

    ASSERT_EQ(view.getNumberOfTuples(), 1);
    EXPECT_FLOAT_EQ(view[0]["f32"].as<float>(), 3.14F);
    EXPECT_DOUBLE_EQ(view[0]["f64"].as<double>(), std::numbers::e);
}

/// Plain int literals are implicitly cast to the schema's field types.
TEST_F(TestTupleBufferTest, AppendImplicitCastFromInt)
{
    auto schema = Testing::TestSchema{
        UnqualifiedUnboundField{Identifier::parse("a"), DataType::Type::INT64},
        UnqualifiedUnboundField{Identifier::parse("b"), DataType::Type::UINT32}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    /// 10 and 200 are plain int, cast to int64_t and uint32_t by the schema.
    view.append(10, 200);

    ASSERT_EQ(view.getNumberOfTuples(), 1);
    EXPECT_EQ(view[0]["a"].as<int64_t>(), 10);
    EXPECT_EQ(view[0]["b"].as<uint32_t>(), 200);
}

/// Multiple records with implicit casts across different numeric types.
TEST_F(TestTupleBufferTest, AppendImplicitCastMultipleRecords)
{
    auto schema = Testing::TestSchema{
        UnqualifiedUnboundField{Identifier::parse("i8"), DataType::Type::INT8},
        UnqualifiedUnboundField{Identifier::parse("u64"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("f32"), DataType::Type::FLOAT32}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    /// All plain literals — int, int, double — cast to int8_t, uint64_t, float.
    view.append(42, 999, 3.14);
    view.append(-1, 0, 0.0);

    ASSERT_EQ(view.getNumberOfTuples(), 2);
    EXPECT_EQ(view[0]["i8"].as<int8_t>(), 42);
    EXPECT_EQ(view[0]["u64"].as<uint64_t>(), 999U);
    EXPECT_FLOAT_EQ(view[0]["f32"].as<float>(), 3.14F);
    EXPECT_EQ(view[1]["i8"].as<int8_t>(), -1);
    EXPECT_EQ(view[1]["u64"].as<uint64_t>(), 0U);
    EXPECT_FLOAT_EQ(view[1]["f32"].as<float>(), 0.0F);
}

/// String literals (const char[]) are implicitly converted to std::string for VARSIZED fields.
TEST_F(TestTupleBufferTest, AppendImplicitCastStringLiteral)
{
    auto schema = Testing::TestSchema{
        UnqualifiedUnboundField{Identifier::parse("id"), DataType::Type::INT32},
        UnqualifiedUnboundField{Identifier::parse("name"), DataType::Type::VARSIZED}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer, bufferManager.get());

    /// 1 is plain int → INT32, "alice" is const char[] → VARSIZED string.
    view.append(1, "alice");

    ASSERT_EQ(view.getNumberOfTuples(), 1);
    EXPECT_EQ(view[0]["id"].as<int32_t>(), 1);
    EXPECT_EQ(view[0]["name"].as<std::string>(), "alice");
}

/// Assigning a string to a numeric field via append should throw.
TEST_F(TestTupleBufferTest, AppendImplicitCastStringToNumericThrows)
{
    auto schema = Testing::TestSchema{UnqualifiedUnboundField{Identifier::parse("value"), DataType::Type::INT64}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    EXPECT_ANY_THROW(view.append("not a number"));
}

/// Nullable fields: append accepts plain values, std::optional, and std::nullopt; reads
/// require the std::optional<T> form and return nullopt for null values.
TEST_F(TestTupleBufferTest, NullableFieldRoundTrip)
{
    auto schema = Testing::TestSchema{
        UnqualifiedUnboundField{Identifier::parse("v"), DataType{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE}}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer, bufferManager.get());

    /// Plain value → non-null write
    view.append(int64_t{42});
    /// std::optional with value → non-null write
    view.append(std::optional<int64_t>{7});
    /// std::nullopt → null write
    view.append(std::nullopt);
    /// Empty std::optional → null write
    view.append(std::optional<int64_t>{});

    ASSERT_EQ(view.getNumberOfTuples(), 4);
    EXPECT_EQ(view[0]["v"].as<std::optional<int64_t>>(), std::optional<int64_t>{42});
    EXPECT_EQ(view[1]["v"].as<std::optional<int64_t>>(), std::optional<int64_t>{7});
    EXPECT_FALSE(view[2]["v"].as<std::optional<int64_t>>().has_value());
    EXPECT_FALSE(view[3]["v"].as<std::optional<int64_t>>().has_value());
}

/// Reading a nullable field with the non-optional form must fail.
TEST_F(TestTupleBufferTest, NullableFieldReadWithoutOptionalThrows)
{
    auto schema = Testing::TestSchema{
        UnqualifiedUnboundField{Identifier::parse("v"), DataType{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE}}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer, bufferManager.get());
    view.append(int64_t{42});

    EXPECT_ANY_THROW(view[0]["v"].as<int64_t>());
}

/// Reading a non-nullable field with the optional form must fail.
TEST_F(TestTupleBufferTest, NonNullableFieldReadWithOptionalThrows)
{
    auto schema = Testing::TestSchema{UnqualifiedUnboundField{Identifier::parse("v"), DataType::Type::INT64}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer, bufferManager.get());
    view.append(int64_t{42});

    EXPECT_ANY_THROW(view[0]["v"].as<std::optional<int64_t>>());
}

/// Writing null to a non-nullable field must fail (both via append and assignment).
TEST_F(TestTupleBufferTest, NullWriteToNonNullableFieldThrows)
{
    auto schema = Testing::TestSchema{UnqualifiedUnboundField{Identifier::parse("v"), DataType::Type::INT64}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer, bufferManager.get());
    view.append(int64_t{1});

    EXPECT_ANY_THROW(view.append(std::nullopt));
    EXPECT_ANY_THROW(view[0]["v"] = std::nullopt);
}

/// `as<T>` with the wrong underlying type must fail even on a non-nullable field.
TEST_F(TestTupleBufferTest, GetWithWrongTypeThrows)
{
    auto schema = Testing::TestSchema{UnqualifiedUnboundField{Identifier::parse("v"), DataType::Type::INT64}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer, bufferManager.get());
    view.append(int64_t{42});

    EXPECT_ANY_THROW(view[0]["v"].as<int32_t>());
}

/// Nullable VARSIZED — null and non-null payloads round-trip correctly.
TEST_F(TestTupleBufferTest, NullableVarsizedRoundTrip)
{
    auto schema = Testing::TestSchema{
        UnqualifiedUnboundField{Identifier::parse("s"), DataType{DataType::Type::VARSIZED, DataType::NULLABLE::IS_NULLABLE}}};
    auto ttb = Testing::TestTupleBuffer::fromSchema(schema, bufferManager->getBufferSize());

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer, bufferManager.get());
    view.append(std::string{"hello"});
    view.append(std::nullopt);

    ASSERT_EQ(view.getNumberOfTuples(), 2);
    EXPECT_EQ(view[0]["s"].as<std::optional<std::string>>(), std::optional<std::string>{"hello"});
    EXPECT_FALSE(view[1]["s"].as<std::optional<std::string>>().has_value());
}

/// NOLINTEND(readability-magic-numbers)

}

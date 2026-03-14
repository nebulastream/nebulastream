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
#include <memory>
#include <string>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
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
    static void SetUpTestCase()
    {
        Logger::setupLogging("TestTupleBufferTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup TestTupleBufferTest class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down TestTupleBufferTest class."); }

    void SetUp() override
    {
        Testing::BaseUnitTest::SetUp();
        bufferManager = BufferManager::create();
    }

    std::shared_ptr<BufferManager> bufferManager;
};

TEST_F(TestTupleBufferTest, AppendAndReadSingleField)
{
    auto schema = Schema().addField("value", DataType::Type::INT64);
    Testing::TestTupleBuffer ttb(schema);

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    view.append(int64_t{42});

    ASSERT_EQ(view.getNumberOfTuples(), 1);
    EXPECT_EQ(view[0]["value"].as<int64_t>(), 42);
}

TEST_F(TestTupleBufferTest, AppendAndReadMultipleFields)
{
    auto schema = Schema().addField("a", DataType::Type::INT64).addField("b", DataType::Type::UINT32);
    Testing::TestTupleBuffer ttb(schema);

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    view.append(int64_t{100}, uint32_t{200});

    ASSERT_EQ(view.getNumberOfTuples(), 1);
    EXPECT_EQ(view[0]["a"].as<int64_t>(), 100);
    EXPECT_EQ(view[0]["b"].as<uint32_t>(), 200);
}

TEST_F(TestTupleBufferTest, AppendMultipleRecords)
{
    auto schema = Schema().addField("x", DataType::Type::INT32);
    Testing::TestTupleBuffer ttb(schema);

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
    auto schema = Schema().addField("val", DataType::Type::INT64);
    Testing::TestTupleBuffer ttb(schema);

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
    auto schema = Schema().addField("f", DataType::Type::INT32);
    Testing::TestTupleBuffer ttb(schema);

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    view.append(int32_t{1});
    view.append(int32_t{2});
    view.append(int32_t{3});

    EXPECT_ANY_THROW(view[5]);
}

TEST_F(TestTupleBufferTest, UnknownFieldThrows)
{
    auto schema = Schema().addField("exists", DataType::Type::INT32);
    Testing::TestTupleBuffer ttb(schema);

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    view.append(int32_t{1});

    EXPECT_ANY_THROW(view[0]["nonexistent"]);
}

TEST_F(TestTupleBufferTest, TypeMismatchThrows)
{
    auto schema = Schema().addField("i64", DataType::Type::INT64);
    Testing::TestTupleBuffer ttb(schema);

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    view.append(int64_t{1});

    /// Assigning int32_t to an INT64 field should throw.
    EXPECT_ANY_THROW(view[0]["i64"] = Testing::FieldValue(int32_t{42}));
}

TEST_F(TestTupleBufferTest, DanglingFieldViewThrows)
{
    auto schema = Schema().addField("f", DataType::Type::INT32);
    Testing::TestTupleBuffer ttb(schema);

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
    auto schema = Schema().addField("name", DataType::Type::VARSIZED);
    Testing::TestTupleBuffer ttb(schema);

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer, bufferManager.get());

    view.append(std::string("hello world"));

    ASSERT_EQ(view.getNumberOfTuples(), 1);
    EXPECT_EQ(view[0]["name"].as<std::string>(), "hello world");
}

TEST_F(TestTupleBufferTest, MixedFixedAndVarSizedFields)
{
    auto schema = Schema().addField("id", DataType::Type::INT64).addField("label", DataType::Type::VARSIZED);
    Testing::TestTupleBuffer ttb(schema);

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
    auto schema = Schema().addField("flag", DataType::Type::BOOLEAN);
    Testing::TestTupleBuffer ttb(schema);

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
    auto schema = Schema().addField("f32", DataType::Type::FLOAT32).addField("f64", DataType::Type::FLOAT64);
    Testing::TestTupleBuffer ttb(schema);

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    view.append(3.14f, 2.718281828);

    ASSERT_EQ(view.getNumberOfTuples(), 1);
    EXPECT_FLOAT_EQ(view[0]["f32"].as<float>(), 3.14f);
    EXPECT_DOUBLE_EQ(view[0]["f64"].as<double>(), 2.718281828);
}

/// Plain int literals are implicitly cast to the schema's field types.
TEST_F(TestTupleBufferTest, AppendImplicitCastFromInt)
{
    auto schema = Schema().addField("a", DataType::Type::INT64).addField("b", DataType::Type::UINT32);
    Testing::TestTupleBuffer ttb(schema);

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
    auto schema
        = Schema().addField("i8", DataType::Type::INT8).addField("u64", DataType::Type::UINT64).addField("f32", DataType::Type::FLOAT32);
    Testing::TestTupleBuffer ttb(schema);

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    /// All plain literals — int, int, double — cast to int8_t, uint64_t, float.
    view.append(42, 999, 3.14);
    view.append(-1, 0, 0.0);

    ASSERT_EQ(view.getNumberOfTuples(), 2);
    EXPECT_EQ(view[0]["i8"].as<int8_t>(), 42);
    EXPECT_EQ(view[0]["u64"].as<uint64_t>(), 999u);
    EXPECT_FLOAT_EQ(view[0]["f32"].as<float>(), 3.14f);
    EXPECT_EQ(view[1]["i8"].as<int8_t>(), -1);
    EXPECT_EQ(view[1]["u64"].as<uint64_t>(), 0u);
    EXPECT_FLOAT_EQ(view[1]["f32"].as<float>(), 0.0f);
}

/// String literals (const char[]) are implicitly converted to std::string for VARSIZED fields.
TEST_F(TestTupleBufferTest, AppendImplicitCastStringLiteral)
{
    auto schema = Schema().addField("id", DataType::Type::INT32).addField("name", DataType::Type::VARSIZED);
    Testing::TestTupleBuffer ttb(schema);

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
    auto schema = Schema().addField("value", DataType::Type::INT64);
    Testing::TestTupleBuffer ttb(schema);

    auto buffer = bufferManager->getBufferBlocking();
    auto view = ttb.open(buffer);

    EXPECT_ANY_THROW(view.append("not a number"));
}

}

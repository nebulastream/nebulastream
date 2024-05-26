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

#include <API/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <cstdint>
#include <generation/CSVAdapter.hpp>
#include <generation/JSONAdapter.hpp>
#include <generation/SchemaBuffer.hpp>
#include <gtest/gtest.h>
#include <string_view>

using namespace NES::DataParser;
class SchemaBufferTest : public ::testing::Test {
  protected:
    constexpr static size_t BUFFER_SIZE = 8192;
};

TEST_F(SchemaBufferTest, TupleOffset) {
    using namespace std::literals;
    using Schema1 =
        StaticSchema<Field<UINT8, "b">, Field<FLOAT32, "a">, Field<TEXT, "yes">, Field<UINT32, "c">, Field<INT64, "d">>;

    Schema1::TupleType t = {8, 423.4F, "yoyoyo"sv, 321, -454};
    auto offsets = Schema1::Offsets;
    std::span<uint8_t> data{reinterpret_cast<uint8_t*>(&t), sizeof(t)};

    EXPECT_EQ(*reinterpret_cast<uint8_t*>(data.subspan(offsets[0]).data()), 8);
    EXPECT_EQ(*reinterpret_cast<float*>(data.subspan(offsets[1]).data()), 423.4F);
    EXPECT_EQ(*reinterpret_cast<std::string_view*>(data.subspan(offsets[2]).data()), "yoyoyo");
    EXPECT_EQ(*reinterpret_cast<uint32_t*>(data.subspan(offsets[3]).data()), 321);
    EXPECT_EQ(*reinterpret_cast<int64_t*>(data.subspan(offsets[4]).data()), -454);
}

TEST_F(SchemaBufferTest, CompatibilityWithTestTupleBuffer) {
    using Schema1 =
        StaticSchema<Field<UINT8, "b">, Field<FLOAT32, "a">, Field<TEXT, "yes">, Field<UINT32, "c">, Field<INT64, "d">>;
    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(BUFFER_SIZE);
    auto buffer = bufferManager->getBufferBlocking();

    auto uut = SchemaBuffer<Schema1, BUFFER_SIZE>(buffer);

    uut.writeTuple(std::make_tuple(256, 42.4f, "This is a var length Text", 32, -64));
    uut.writeTuple(std::make_tuple(28, -42.4f, "This is a shorter Text", UINT16_MAX + 1, INT64_MIN));

    auto schema = NES::Schema::create(NES::Schema::MemoryLayoutType::ROW_LAYOUT)
                      ->addField("uint8", NES::BasicType::UINT8)
                      ->addField("float32", NES::BasicType::FLOAT32)
                      ->addField("text", NES::BasicType::TEXT)
                      ->addField("uint32", NES::BasicType::UINT32)
                      ->addField("int64", NES::BasicType::INT64);

    auto dbuffer = NES::Runtime::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, schema);

    EXPECT_EQ(dbuffer.getNumberOfTuples(), 2);
    EXPECT_EQ(dbuffer.getBuffer().getNumberOfChildrenBuffer(), 2);

    EXPECT_EQ(dbuffer[0]["uint8"].read<uint8_t>(), 0) << "Expected 256 to be truncated to 0";
    EXPECT_FLOAT_EQ(dbuffer[0]["float32"].read<float>(), 42.4F) << "float is incorrect";
    EXPECT_EQ(dbuffer[0]["text"].read<uint32_t>(), 0) << "expected text to be at child buffer 0";
    EXPECT_EQ(*dbuffer.getBuffer().loadChildBuffer(0).getBuffer<uint32_t>(), strlen("This is a var length Text"))
        << "Text values does not have proper size";
    EXPECT_EQ(
        std::string_view((dbuffer.getBuffer().loadChildBuffer(0).getBuffer<char>() + 4), strlen("This is a var length Text")),
        std::string("This is a var length Text"))
        << "Text value does not match";
    EXPECT_EQ(dbuffer[0]["uint32"].read<uint32_t>(), 32) << "u32 is incorrect";
    EXPECT_EQ(dbuffer[0]["int64"].read<int64_t>(), -64) << "i64 is incorrect";

    EXPECT_EQ(dbuffer[1]["uint8"].read<uint8_t>(), 28) << "Expected 256 to be truncated to 0";
    EXPECT_FLOAT_EQ(dbuffer[1]["float32"].read<float>(), -42.4F) << "float is incorrect";
    EXPECT_EQ(dbuffer[1]["text"].read<uint32_t>(), 1) << "expected text to be at child buffer 0";
    EXPECT_EQ(*dbuffer.getBuffer().loadChildBuffer(1).getBuffer<uint32_t>(), strlen("This is a shorter Text"))
        << "Text values does not have proper size";
    EXPECT_EQ(std::string_view((dbuffer.getBuffer().loadChildBuffer(1).getBuffer<char>() + 4), strlen("This is a shorter Text")),
              std::string("This is a shorter Text"))
        << "Text value does not match";
    EXPECT_EQ(dbuffer[1]["uint32"].read<uint32_t>(), UINT16_MAX + 1) << "u32 is incorrect";
    EXPECT_EQ(dbuffer[1]["int64"].read<int64_t>(), INT64_MIN) << "i64 is incorrect";
}

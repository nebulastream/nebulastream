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
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <DataParser/RuntimeSchemaAdapter.hpp>
#include <DataParser/StaticParser.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/FormatType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/StdInt.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <future>
#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>
using namespace NES::DataParser;
using namespace std::literals;
class ParserTest : public ::testing::Test {
  protected:
    void SetUp() override { NES::Logger::setupLogging("ParserTest.log", NES::LogLevel::LOG_DEBUG); }
    constexpr static size_t BUFFER_SIZE = 8192;
};

TEST_F(ParserTest, CSVCompatibilityWithTestTupleBuffer) {
    using Schema = StaticSchema<Field<TEXT, "Name">, Field<UINT16, "Age">, Field<FLOAT64, "Height">>;
    StaticParser<Schema, NES::FormatTypes::CSV_FORMAT> parser;

    auto bm = std::make_shared<NES::Runtime::BufferManager>(8192);
    auto buffer = bm->getBufferBlocking();
    auto rest = parser.parseIntoBuffer(R"("Jeff",45,1.70
"OlderJeff",55,1.69
"GigaJeff",99,3.00
)"sv,
                                       buffer);

    auto testBuffer = NES::Runtime::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, runtimeSchema<Schema>());

    EXPECT_EQ(testBuffer[0].readVarSized(0_u64), "Jeff");
    EXPECT_EQ(testBuffer[0][1].read<uint16_t>(), 45);
    EXPECT_EQ(testBuffer[0][2].read<double>(), 1.70);

    EXPECT_EQ(testBuffer[1].readVarSized(0_u64), "OlderJeff");
    EXPECT_EQ(testBuffer[1][1].read<uint16_t>(), 55);
    EXPECT_EQ(testBuffer[1][2].read<double>(), 1.69);

    EXPECT_EQ(testBuffer[2].readVarSized(0_u64), "GigaJeff");
    EXPECT_EQ(testBuffer[2][1].read<uint16_t>(), 99);
    EXPECT_EQ(testBuffer[2][2].read<double>(), 3.00);

    EXPECT_THAT(rest, ::testing::IsEmpty());
}

TEST_F(ParserTest, QueryInternalTupleByFieldOffset) {
    using Schema = StaticSchema<Field<TEXT, "Name">, Field<UINT16, "Age">, Field<FLOAT64, "Height">>;
    StaticParser<Schema, NES::FormatTypes::CSV_FORMAT> parser;

    auto testData = R"("Jeff",45,1.70
"OlderJeff",55,1.69
"GigaJeff",99,3.00
)"sv;

    auto offsets = parser.getFieldOffsets();
    EXPECT_THAT(offsets, ::testing::SizeIs(3));

    auto rest = parser.parse(testData);
    EXPECT_EQ(rest, R"("OlderJeff",55,1.69
"GigaJeff",99,3.00
)"sv);

    EXPECT_EQ(*reinterpret_cast<std::string_view*>(offsets[0]), "Jeff"sv);
    EXPECT_EQ(*reinterpret_cast<uint16_t*>(offsets[1]), 45);
    EXPECT_EQ(*reinterpret_cast<double*>(offsets[2]), 1.70);

    rest = parser.parse(rest);
    EXPECT_EQ(rest, R"("GigaJeff",99,3.00
)"sv);

    EXPECT_EQ(*reinterpret_cast<std::string_view*>(offsets[0]), "OlderJeff"sv);
    EXPECT_EQ(*reinterpret_cast<uint16_t*>(offsets[1]), 55);
    EXPECT_EQ(*reinterpret_cast<double*>(offsets[2]), 1.69);

    rest = parser.parse(rest);

    EXPECT_THAT(rest, ::testing::IsEmpty());

    EXPECT_EQ(*reinterpret_cast<std::string_view*>(offsets[0]), "GigaJeff"sv);
    EXPECT_EQ(*reinterpret_cast<uint16_t*>(offsets[1]), 99);
    EXPECT_EQ(*reinterpret_cast<double*>(offsets[2]), 3.00);
}

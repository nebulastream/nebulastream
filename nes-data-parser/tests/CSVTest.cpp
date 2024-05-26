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

#include <DataParser/RuntimeSchemaAdapter.hpp>
#include <FormattedDataGenerator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <generation/CSVAdapter.hpp>
#include <generation/SchemaBuffer.hpp>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>

using namespace std::literals;

class CSVFormatTest : public ::testing::Test {
  protected:
    void SetUp() override { NES::Logger::setupLogging("CSVFormatTest.log", NES::LogLevel::LOG_DEBUG); }

  protected:
    constexpr static size_t BUFFER_SIZE = 8192;
};

TEST_F(CSVFormatTest, BasicSchemaTest) {
    using Schema = NES::DataParser::StaticSchema<NES::DataParser::Field<NES::DataParser::UINT8, "a">,
                                                 NES::DataParser::Field<NES::DataParser::FLOAT32, "b">,
                                                 NES::DataParser::Field<NES::DataParser::TEXT, "c">,
                                                 NES::DataParser::Field<NES::DataParser::UINT32, "d">,
                                                 NES::DataParser::Field<NES::DataParser::INT64, "e">>;
    using SchemaBuffer = NES::DataParser::SchemaBuffer<Schema, 8192>;
    using Adapter = NES::DataParser::CSV::CSVAdapter<SchemaBuffer>;

    auto data = generateCSVData<uint8_t, float, std::string, uint32_t, int64_t>(4);

    auto bm = std::make_shared<NES::Runtime::BufferManager>();
    auto buffer = bm->getBufferBlocking();

    auto rest = Adapter::parseIntoBuffer(data, buffer);

    EXPECT_EQ(buffer.getNumberOfTuples(), 4);
    EXPECT_THAT(rest, ::testing::IsEmpty());
}

TEST_F(CSVFormatTest, Woawa) {
    auto result =
        scn::scan<int, std::string_view, float, std::string_view, int, std::string_view>("    1231 ,     41.2 ,    123123"sv,
                                                                                         "{}{:[^,]},{}{:[^,]},{}{:[^,]}");
    EXPECT_TRUE(result);
    EXPECT_EQ(result->values(), std::make_tuple(1231, std::string_view{}, 41.2F, ""sv, 123123, ""sv));
}

TEST_F(CSVFormatTest, WhiteSpaceParsing) {
    using Schema = NES::DataParser::StaticSchema<NES::DataParser::Field<NES::DataParser::UINT8, "a">,
                                                 NES::DataParser::Field<NES::DataParser::FLOAT32, "b">,
                                                 NES::DataParser::Field<NES::DataParser::TEXT, "c">,
                                                 NES::DataParser::Field<NES::DataParser::UINT32, "d">,
                                                 NES::DataParser::Field<NES::DataParser::INT64, "e">>;
    using SchemaBuffer = NES::DataParser::SchemaBuffer<Schema, 8192>;
    using Adapter = NES::DataParser::CSV::CSVAdapter<SchemaBuffer>;

    auto data = "   32, 42.4  ,\"YES\" , 1,  -32"sv;

    auto bm = std::make_shared<NES::Runtime::BufferManager>();
    auto buffer = bm->getBufferBlocking();
    auto rest = Adapter::parseIntoBuffer(data, buffer);

    auto testBuffer = NES::Runtime::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, runtimeSchema<Schema>());

    EXPECT_EQ(testBuffer[0][0].read<uint8_t>(), 32);
    EXPECT_EQ(buffer.getNumberOfTuples(), 1);
    EXPECT_THAT(rest, ::testing::IsEmpty());
}

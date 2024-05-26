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
#include <Common/DataTypes/BasicTypes.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <DataParser/ParserFactory.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/FormatType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <future>
#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>
using namespace NES::DataParser;
using namespace std::literals;
class ParserFactoryTest : public ::testing::Test {
  protected:
    void SetUp() override { NES::Logger::setupLogging("ParserFactoryTest.log", NES::LogLevel::LOG_DEBUG); }
    constexpr static size_t BUFFER_SIZE = 8192;
};

TEST_F(ParserFactoryTest, CSVCompatibilityWithTestTupleBuffer) {
    auto cppCompiler = NES::Compiler::CPPCompiler::create();
    auto jitCompiler = NES::Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
    ParserFactory uut(jitCompiler);

    auto schema = NES::Schema::create(NES::Schema::MemoryLayoutType::ROW_LAYOUT)
                      ->addField("uint8", NES::BasicType::UINT8)
                      ->addField("float32", NES::BasicType::FLOAT32)
                      ->addField("text", NES::BasicType::TEXT)
                      ->addField("uint32", NES::BasicType::UINT32)
                      ->addField("int64", NES::BasicType::INT64);

    auto parser = uut.createParser(schema, NES::FormatTypes::CSV_FORMAT).get();

    auto bm = std::make_shared<NES::Runtime::BufferManager>(8192);
    auto buffer = bm->getBufferBlocking();
    auto rest = parser->parseIntoBuffer("8,123.00,\"Actual Text\",32,54\n", buffer);

    auto testBuffer = NES::Runtime::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, schema);

    EXPECT_EQ(testBuffer[0][0].read<uint8_t>(), 8);
    EXPECT_EQ(testBuffer[0][1].read<float>(), 123.00F);
    EXPECT_EQ(testBuffer[0].readVarSized(2u), "Actual Text");
    EXPECT_EQ(testBuffer[0][3].read<uint32_t>(), 32);
    EXPECT_EQ(testBuffer[0][4].read<int64_t>(), 54);
    EXPECT_THAT(rest, ::testing::IsEmpty());
}

TEST_F(ParserFactoryTest, QueryInternalTupleByFieldOffset) {
    auto cppCompiler = NES::Compiler::CPPCompiler::create();
    auto jitCompiler = NES::Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
    ParserFactory uut(jitCompiler);

    auto schema = NES::Schema::create(NES::Schema::MemoryLayoutType::ROW_LAYOUT)
                      ->addField("uint8", NES::BasicType::UINT8)
                      ->addField("float32", NES::BasicType::FLOAT32)
                      ->addField("text", NES::BasicType::TEXT)
                      ->addField("uint32", NES::BasicType::UINT32)
                      ->addField("int64", NES::BasicType::INT64);

    auto parser = uut.createParser(schema, NES::FormatTypes::CSV_FORMAT).get();
    EXPECT_THAT(parser->getFieldOffsets(), ::testing::SizeIs(5));

    auto rest = parser->parse("8,123.00,\"Actual Text\",32,54\n"
                              "9,123.32,\"Actual Text2\",65,-34\n");
    EXPECT_THAT(rest, ::testing::Not(::testing::IsEmpty()));
    EXPECT_EQ(*static_cast<uint8_t*>(parser->getFieldOffsets()[0]), 8);
    EXPECT_EQ(*static_cast<float*>(parser->getFieldOffsets()[1]), 123.00F);
    EXPECT_EQ(*static_cast<std::string_view*>(parser->getFieldOffsets()[2]), "Actual Text"sv);

    EXPECT_THAT(parser->parse(rest), ::testing::IsEmpty());
    EXPECT_EQ(*static_cast<uint8_t*>(parser->getFieldOffsets()[0]), 9);
    EXPECT_EQ(*static_cast<float*>(parser->getFieldOffsets()[1]), 123.32F);
    EXPECT_EQ(*static_cast<std::string_view*>(parser->getFieldOffsets()[2]), "Actual Text2"sv);
}

TEST_F(ParserFactoryTest, JSONCompatibilityWithTestTupleBuffer) {
    auto cppCompiler = NES::Compiler::CPPCompiler::create();
    auto jitCompiler = NES::Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
    ParserFactory uut(jitCompiler);

    auto schema = NES::Schema::create(NES::Schema::MemoryLayoutType::ROW_LAYOUT)
                      ->addField("a", NES::BasicType::UINT8)
                      ->addField("b", NES::BasicType::FLOAT32)
                      ->addField("c", NES::BasicType::TEXT)
                      ->addField("d", NES::BasicType::UINT32)
                      ->addField("e", NES::BasicType::INT64);

    auto parser_fut = uut.createParser(schema, NES::FormatTypes::JSON_FORMAT);
    auto parser = parser_fut.get();

    auto bm = std::make_shared<NES::Runtime::BufferManager>(8192);
    auto buffer = bm->getBufferBlocking();
    auto rest = parser->parseIntoBuffer(R"({"a": 8, "b": 123.00, "c": "Actual Text", "d": 32, "e": 54}
{"b": -64.43, "a": 8, "e": 100, "c": "More Text But Longer", "d": 1}
)",
                                        buffer);

    auto testBuffer = NES::Runtime::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, schema);

    EXPECT_THAT(rest, ::testing::IsEmpty());

    EXPECT_EQ(testBuffer.getNumberOfTuples(), 2);

    EXPECT_EQ(testBuffer[0][0].read<uint8_t>(), 8);
    EXPECT_EQ(testBuffer[0][1].read<float>(), 123.00F);
    EXPECT_EQ(testBuffer[0].readVarSized(2u), "Actual Text");
    EXPECT_EQ(testBuffer[0][3].read<uint32_t>(), 32);
    EXPECT_EQ(testBuffer[0][4].read<int64_t>(), 54);

    EXPECT_EQ(testBuffer[1][0].read<uint8_t>(), 8);
    EXPECT_EQ(testBuffer[1][1].read<float>(), -64.43F);
    EXPECT_EQ(testBuffer[1].readVarSized(2u), "More Text But Longer");
    EXPECT_EQ(testBuffer[1][3].read<uint32_t>(), 1);
    EXPECT_EQ(testBuffer[1][4].read<int64_t>(), 100);
}

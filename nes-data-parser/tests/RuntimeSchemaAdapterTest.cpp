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
#include <DataParser/RuntimeSchemaAdapter.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <future>
#include <generation/SchemaBuffer.hpp>
#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>
using namespace NES::DataParser;
class RuntimeSchemaAdapterTest : public ::testing::Test {
  protected:
    void SetUp() override { NES::Logger::setupLogging("ParserFactoryTest.log", NES::LogLevel::LOG_DEBUG); }
    constexpr static size_t BUFFER_SIZE = 8192;
};

TEST_F(RuntimeSchemaAdapterTest, CompatibilityWithRuntimeSchema) {
    using Schema =
        StaticSchema<Field<UINT8, "a">, Field<FLOAT32, "b">, Field<TEXT, "yes">, Field<UINT32, "c">, Field<INT64, "d">>;
    auto expectedSchema = NES::Schema::create()
                              ->addField("a", NES::BasicType::UINT8)
                              ->addField("b", NES::BasicType::FLOAT32)
                              ->addField("yes", NES::BasicType::TEXT)
                              ->addField("c", NES::BasicType::UINT32)
                              ->addField("d", NES::BasicType::INT64);

    EXPECT_TRUE(expectedSchema->equals(runtimeSchema<Schema>()));
    EXPECT_EQ(expectedSchema->getFieldNames()[0], "a");
}

TEST_F(RuntimeSchemaAdapterTest, CompatibilityWithRuntimeSchemaWhenEmpty) {
    using Schema = StaticSchema<>;
    auto expectedSchema = NES::Schema::create();
    EXPECT_TRUE(expectedSchema->equals(runtimeSchema<Schema>()));
}

// The list of types we want to test.
typedef ::testing::Types<UINT8, UINT16, UINT32, UINT64, INT8, INT16, INT32, INT64, TEXT> Implementations;

template<NES::DataParser::Concepts::Type T>
class SchemaTypeTest : public ::testing::Test {};

TYPED_TEST_CASE(SchemaTypeTest, Implementations);

TYPED_TEST(SchemaTypeTest, runtimeSchemaCreation) {
    using Schema = StaticSchema<Field<TypeParam, "long name with spaces">>;
    auto schema = runtimeSchema<Schema>();
    EXPECT_EQ(schema->getSize(), 1);
    EXPECT_EQ(schema->getFieldNames()[0], "long name with spaces");
}

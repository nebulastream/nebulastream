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

#include <Configurations/Coordinator/SchemaType.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sinks/Formats/FormatIterators/Iterator.hpp>
#include <Util/Core.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <Util/StdInt.hpp>

namespace NES {

// This test checks that data values are serialized correctly according to the JSON specification.
TEST(IteratorTest, testJsonDataTypes) {

    // Create a tuple buffer containing a single row with a value for each data type
    auto schema = Schema::createFromSchemaType(Configurations::SchemaType::create({
        {"int8Field", "INT8"},
        {"int16Field", "INT16"},
        {"int32Field", "INT32"},
        {"int64Field", "INT64"},
        {"uint8Field", "UINT8"},
        {"uint16Field", "UINT16"},
        {"uint32Field", "UINT32"},
        {"uint64Field", "UINT64"},
        {"float32Field", "FLOAT32"},
        {"float64Field", "FLOAT64"},
        {"trueField", "BOOLEAN"},
        {"falseField", "BOOLEAN"},
        {"textField", "TEXT"},
        }));
    auto bufferManager = std::make_shared<Runtime::BufferManager>();
    auto buffer = bufferManager->getBufferBlocking();
    auto testTupleBuffer = Runtime::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, schema);
    size_t row = 0;
    testTupleBuffer[row][0].write<int8_t>(-8);
    testTupleBuffer[row][1].write<int16_t>(-16);
    testTupleBuffer[row][2].write<int32_t>(-32);
    testTupleBuffer[row][3].write<int64_t>(-64);
    testTupleBuffer[row][4].write<uint8_t>(8);
    testTupleBuffer[row][5].write<uint16_t>(16);
    testTupleBuffer[row][6].write<uint32_t>(32);
    testTupleBuffer[row][7].write<uint64_t>(64);
    testTupleBuffer[row][8].write<float>(-1.0);
    testTupleBuffer[row][9].write<double>(1.0);
    testTupleBuffer[row][10].write<bool>(true);
    testTupleBuffer[row][11].write<bool>(false);
    testTupleBuffer[row].writeVarSized(12_u64, "string_value", bufferManager.get());

    // Serialize to JSON
    Iterator iterator {row, buffer, schema, FormatTypes::JSON_FORMAT};
    auto jsonLiteral = *iterator;
    nlohmann::json json = nlohmann::json::parse(jsonLiteral);

    // Verify values
    EXPECT_EQ(json["int8Field"], -8);
    EXPECT_EQ(json["int16Field"], -16);
    EXPECT_EQ(json["int32Field"], -32);
    EXPECT_EQ(json["int64Field"], -64);
    EXPECT_EQ(json["uint8Field"], 8);
    EXPECT_EQ(json["uint16Field"], 16);
    EXPECT_EQ(json["uint32Field"], 32);
    EXPECT_EQ(json["uint64Field"], 64);
    EXPECT_EQ(json["float32Field"], -1.0);
    EXPECT_EQ(json["float64Field"], 1.0);
    EXPECT_EQ(json["trueField"], true);
    EXPECT_EQ(json["falseField"], false);
    EXPECT_EQ(json["textField"], std::string("string_value"));
}

};
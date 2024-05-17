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
#include <BaseIntegrationTest.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Sinks/Formats/JsonFormat.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <nlohmann/json.hpp>
#include <gtest/gtest.h>
#include <iostream>

#include <vector>

namespace NES::Runtime::MemoryLayouts {

/**
 * @brief Testing the functionality of the iterating over the json format.
 *        Since the created json objects may store the key-value-pairs in a different order,
 *        compared to our schema, we just check if the results contain the expected strings.
 */
class JsonFormatTest : public Testing::BaseUnitTest {
  public:
    struct JsonKVPair {
        std::string key;
        std::string value;
    };
    BufferManagerPtr bufferManager;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("JsonFormatTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup JsonFormatTest test class.");
    }

    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        bufferManager = std::make_shared<BufferManager>(4096, 10);
    }

    /**
     * @brief Create a json key value pair.
     */
    template <typename T>
    static auto createJsonKVPair(const std::pair<std::string, T> kvPair) {
        return JsonKVPair{.key = kvPair.first, .value = std::to_string(kvPair.second)};
    }

    /**
     * @brief Create a json key value pair from a string value.
     */
    static auto createJsonKVPair(const std::pair<std::string, std::string> kvPair) {
        return JsonKVPair{.key = kvPair.first, .value = kvPair.second};
    }

    /**
     * @brief Create a json key value pair from a char (std::to_string returns the ASCII number).
     */
    static auto createJsonKVPair(const std::pair<std::string, char> kvPair) {
        return JsonKVPair{.key = kvPair.first, .value = std::string() + kvPair.second};
    }

    /**
     * Takes a schema and creates a TestTupleBuffer with row layout (column layout currently breaks the iterator).
     * @param schema
     * @return
     */
    auto createTestTupleBuffer(SchemaPtr schema) const {
        auto tupleBuffer = bufferManager->getBufferBlocking();
        RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
        return std::make_unique<Runtime::MemoryLayouts::TestTupleBuffer>(rowLayout, tupleBuffer);
    }

    /**
     * @brief Emplaces the field name and the value for the current field in the current expected KV pairs vector.
     * @param schema the schema of the tuple containing the field name
     * @param value the value for the current field
     * @return the value for the current field, which is inserted into a tuple
     */
    template <typename T>
    T setExpectedValue(T value, const SchemaPtr& schema, std::vector<JsonKVPair>& expectedKVPairs) {
        assert(expectedKVPairs.size() < schema->getFieldNames().size());
        expectedKVPairs.emplace_back(createJsonKVPair(std::make_pair(schema->getFieldNames().at(expectedKVPairs.size()), value)));
        return value;
    }

    /**
     * @brief Process a single tuple containing Values, inserting it into the testTupleBuffer.
     * @param schema the schema used in the current test
     * @param testTupleBuffer the buffer in which the tuple is written
     * @param expectedKVPairs the vector in which we store vectors for the expected output
     * @param values the values of the current tuple
     */
    template <typename... Values>
    void processTuple(SchemaPtr schema, NES::Runtime::MemoryLayouts::TestTupleBuffer* testTupleBuffer,
                      std::vector<std::vector<JsonKVPair>>& expectedKVPairs, const Values&... values)
    {
        // Iterate over all values in the current tuple and add them to the expected KV pairs.
        expectedKVPairs.push_back(std::vector<JsonKVPair>());
        auto testTuple = std::make_tuple(setExpectedValue(values, schema, expectedKVPairs.back())...);
        testTupleBuffer->pushRecordToBuffer(testTuple);
    }

    /**
     * @brief Calls processTuple for every tuple supplied by the current test.
     * @param schema the schema used in the current test
     * @param expectedKVPairs the vector in which we store vectors for the expected output
     * @param tuples the tuples created in the current test that are written to the testTupleBuffer
     */
    template <typename... Tuples>
    auto processTuples(SchemaPtr schema, std::vector<std::vector<JsonKVPair>>& expectedKVPairs, const Tuples&... tuples) {
        auto testTupleBuffer = createTestTupleBuffer(schema);
        (std::apply([&](const auto&... values) {
            processTuple(schema, testTupleBuffer.get(), expectedKVPairs, values...);
        }, tuples), ...);
        return testTupleBuffer;
    }

    /**
     * @brief Process a single tuple containing Values with Text, inserting it into the testTupleBuffer.
     * @param schema the schema used in the current test
     * @param testTupleBuffer the buffer in which the tuple is written
     * @param expectedKVPairs the vector in which we store vectors for the expected output
     * @param values the values of the current tuple
     */
    template <typename... Values>
    void processTupleWithString(SchemaPtr schema, NES::Runtime::MemoryLayouts::TestTupleBuffer* testTupleBuffer,
                                std::vector<std::vector<JsonKVPair>>& expectedKVPairs, const Values&... values)
    {
        expectedKVPairs.push_back(std::vector<JsonKVPair>());
        auto testTuple = std::make_tuple(setExpectedValue(values, schema, expectedKVPairs.back())...);
        testTupleBuffer->pushRecordToBuffer(testTuple, bufferManager.get());
    }

    /**
     * @brief Calls processTupleWithString for every tuple supplied by the current test.
     * @param schema the schema used in the current test
     * @param expectedKVPairs the vector in which we store the expected output
     * @param tuples the tuples created in the current test that are written to the testTupleBuffer
     */
    template <typename... Tuples>
    auto processTuplesWithString(SchemaPtr schema, std::vector<std::vector<JsonKVPair>>& expectedKVPairs,
                                 const Tuples&... tuples)
    {
        auto testTupleBuffer = createTestTupleBuffer(schema);
        (std::apply([&](const auto&... values) {
            processTupleWithString(schema, testTupleBuffer.get(), expectedKVPairs, values...);
        }, tuples), ...);
        return testTupleBuffer;
    }

    /**
     * @brief Takes a string produced by the json format iterator and checks that all expected KV pairs are contained in it.
     * @param resultString the string produced by the json format iterator with the current test tuple as input
     * @param expectedKVPairs a vector containing all KV pairs expected for the current test tuple
     */
    auto areExpectedStringsInJson(const std::string_view resultString, const std::vector<JsonKVPair>& expectedKVPairs) const {
        bool allExpectedStringsContained = true;
        auto jsonObject = nlohmann::json::parse(resultString);
        for(const auto& expectedKVPair : expectedKVPairs) {
            if(not (jsonObject.contains(expectedKVPair.key) && jsonObject.at(expectedKVPair.key) == expectedKVPair.value)) {
                allExpectedStringsContained = false;
                NES_ERROR("Expected \"{}\":\"{}\" to be contained in resultString {}, but it was not.", expectedKVPair.key, expectedKVPair.value, resultString);
            }
        }
        return allExpectedStringsContained;
    }

    /**
     * @brief Uses the json format iterator to read all test tuples and checks that they match the expected KV pairs.
     * @param schema the schema used in the current test
     * @param expectedKVPairs the vector in which all vectors for the expected output are stored
     * @return a json string, obtained by reading a tuple buffer using a json format iterator
     */
    bool validateJsonIterator(SchemaPtr schema, NES::Runtime::MemoryLayouts::TestTupleBuffer* testTupleBuffer,
                              const std::vector<std::vector<JsonKVPair>>& expectedKVPairs)
    {
        bool areAllKVPairsContained = true;
        auto buffer = testTupleBuffer->getBuffer();
        auto jsonIterator = std::make_unique<JsonFormat>(schema, bufferManager);
        auto jsonTupleIterator = jsonIterator->getTupleIterator(buffer).begin();
        for(const auto& expectedKVPair : expectedKVPairs) {
            areAllKVPairsContained &= areExpectedStringsInJson(*jsonTupleIterator, expectedKVPair);
            ++jsonTupleIterator;
        }
        return areAllKVPairsContained;
    }
};

/**
 * @brief Tests that we can construct a json iterator.
 */
TEST_F(JsonFormatTest, createJsonIterator) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    auto jsonIterator = std::make_unique<JsonFormat>(schema, bufferManager);
    ASSERT_NE(jsonIterator, nullptr);
}

/**
 * @brief Tests that we can convert a tuple buffer with a single integer to json.
 */
TEST_F(JsonFormatTest, useJsonIteratorWithASingleInteger) {
    std::vector<std::vector<JsonKVPair>> expectedKVPairs;
    using TestTuple = std::tuple<uint8_t>;
    SchemaPtr schema = Schema::create()->addField("U8", BasicType::UINT8);

    // Fill test tuple buffer with values, which also sets up the expected KV pairs that must be contained in the JSON.
    auto testTupleBuffer = processTuples(schema, expectedKVPairs,
        TestTuple(1)
    );

    // Assert that all expected KV pairs are contained in the generated JSON string.
    ASSERT_TRUE(validateJsonIterator(schema, testTupleBuffer.get(), expectedKVPairs));
}

/**
 * @brief Tests that we can convert a tuple buffer with unsigned integers to json.
 */
TEST_F(JsonFormatTest, useJsonIteratorWithUnsignedIntegers) {
    std::vector<std::vector<JsonKVPair>> expectedKVPairs;
    using TestTuple = std::tuple<uint8_t, uint16_t, uint32_t, uint64_t>;
    SchemaPtr schema = Schema::create()->addField("U8", BasicType::UINT8)->addField("U16", BasicType::UINT16)
                                       ->addField("U32", BasicType::UINT32)->addField("U64", BasicType::UINT64);
    auto testTupleBuffer = processTuples(schema, expectedKVPairs,
        TestTuple(1, 256, 65536, 4294967296)
     );
    // Assert that all expected KV pairs are contained in the generated JSON string.
    ASSERT_TRUE(validateJsonIterator(schema, testTupleBuffer.get(), expectedKVPairs));
}

/**
 * @brief Tests that we can convert a tuple buffer with signed integers to json.
 */
TEST_F(JsonFormatTest, useJsonIteratorWithSignedIntegers) {
    std::vector<std::vector<JsonKVPair>> expectedKVPairs;
    SchemaPtr schema = Schema::create()->addField("I8", BasicType::INT8)->addField("I16", BasicType::INT16)
                                       ->addField("I32", BasicType::INT32)->addField("I64", BasicType::INT64);
    using TestTuple = std::tuple<int8_t, int16_t, int32_t, int64_t>;
    auto testTupleBuffer = processTuples(schema, expectedKVPairs,
        TestTuple(1, 128, 32768, 2147483648)
    );
    // Assert that all expected KV pairs are contained in the generated JSON string.
    ASSERT_TRUE(validateJsonIterator(schema, testTupleBuffer.get(), expectedKVPairs));
}

/**
 * @brief Tests that we can convert a tuple buffer with a lower and uppercase char and a true and a false bool to json.
 */
TEST_F(JsonFormatTest, useJsonIteratorWithSignedBoolAndChar) {
    std::vector<std::vector<JsonKVPair>> expectedKVPairs;
        SchemaPtr schema = Schema::create()->addField("C1", BasicType::CHAR)->addField("C2", BasicType::CHAR)
                                           ->addField("B1", BasicType::BOOLEAN)->addField("B2", BasicType::BOOLEAN);
    using TestTuple = std::tuple<char, char, bool, bool>;
    auto testTupleBuffer = processTuples(schema, expectedKVPairs,
        TestTuple('A', 'a', true, false)
    );
    // Assert that all expected KV pairs are contained in the generated JSON string.
    ASSERT_TRUE(validateJsonIterator(schema, testTupleBuffer.get(), expectedKVPairs));
}

/**
 * @brief Tests that we can convert a tuple buffer with single and a double precision to json.
 */
TEST_F(JsonFormatTest, useJsonIteratorWithFloatingPoints) {
    std::vector<std::vector<JsonKVPair>> expectedKVPairs;
    SchemaPtr schema = Schema::create()->addField("F", BasicType::FLOAT32)->addField("D", BasicType::FLOAT64);
    using TestTuple = std::tuple<float, double>;
    auto testTupleBuffer = processTuples(schema, expectedKVPairs,
        TestTuple(4.2, 13.37)
    );
    // Assert that all expected KV pairs are contained in the generated JSON string.
    ASSERT_TRUE(validateJsonIterator(schema, testTupleBuffer.get(), expectedKVPairs));
}

/**
 * @brief Tests that we can convert a tuple buffer containing Text to json.
 */
TEST_F(JsonFormatTest, useJsonIteratorWithText) {
    std::vector<std::vector<JsonKVPair>> expectedKVPairs;
    SchemaPtr schema = Schema::create()->addField("T", DataTypeFactory::createText());
    using TestTuple = std::tuple<std::string>;
    auto testTupleBuffer = processTuplesWithString(schema, expectedKVPairs,
        TestTuple("42 is the answer")
    );
    // Assert that all expected KV pairs are contained in the generated JSON string.
    ASSERT_TRUE(validateJsonIterator(schema, testTupleBuffer.get(), expectedKVPairs));
}

/**
 * @brief Tests that we can convert a tuple buffer with a number and text to json.
 */
TEST_F(JsonFormatTest, useJsonIteratorWithNumberAndText) {
    std::vector<std::vector<JsonKVPair>> expectedKVPairs;
    SchemaPtr schema = Schema::create()->addField("U8", BasicType::UINT8)->addField("T", DataTypeFactory::createText());
    using TestTuple = std::tuple<uint8_t, std::string>;
    auto testTupleBuffer = processTuplesWithString(schema, expectedKVPairs,
        TestTuple(42, "is correct")
    );
    // Assert that all expected KV pairs are contained in the generated JSON string.
    ASSERT_TRUE(validateJsonIterator(schema, testTupleBuffer.get(), expectedKVPairs));
}

/**
 * @brief Tests that we can convert a tuple buffer with many different, and multiple Text types to json.
 */
TEST_F(JsonFormatTest, useJsonIteratorWithMixedDataTypes) {
    std::vector<std::vector<JsonKVPair>> expectedKVPairs;
    SchemaPtr schema = Schema::create()->addField("T1", DataTypeFactory::createText())->addField("U8", BasicType::UINT8)
            ->addField("T2", DataTypeFactory::createText())->addField("D", BasicType::FLOAT64)
            ->addField("I16", BasicType::INT16)->addField("F", BasicType::FLOAT32)
            ->addField("B", BasicType::BOOLEAN)->addField("C", BasicType::CHAR)
            ->addField("T3", DataTypeFactory::createText());
    using TestTuple = std::tuple<std::string, uint8_t, std::string, double, int16_t, float, bool, char, std::string>;
    auto testTupleBuffer = processTuplesWithString(schema, expectedKVPairs,
        TestTuple("First Text", 42, "Second Text", 13.37, 666, 7.77, true, 'C', "Third Text"),
        TestTuple("Fourth Text", 43, "Fifth Text", 3.14, 676, 7.67, true, 'c', "Combo Breaker")
    );
    // Assert that all expected KV pairs are contained in the generated JSON string.
    ASSERT_TRUE(validateJsonIterator(schema, testTupleBuffer.get(), expectedKVPairs));
}


}// namespace NES::Runtime::MemoryLayouts

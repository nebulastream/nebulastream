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

// nes-runtime tests has no include directory
#include "FormatIteratorTestUtil.hpp"
#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Sinks/Formats/JsonFormat.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <variant>

namespace NES::Runtime {

/**
 * @brief In this test, the expected output is consists of JSON key value (KV) pairs.
 */
struct JsonKVPair {
    std::string key;
    std::variant<uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t, float, double, char, bool, std::string>
        value;
};

/**
 * @brief Testing the functionality of the iterating over the json format.
 *        Since the created json objects may store the key-value-pairs in a different order,
 *        compared to our schema, we just check if the results contain the expected strings.
 */
class JsonFormatTest : public Testing::BaseUnitTest, public FormatIteratorTestUtil<JsonKVPair> {
  public:
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
    template<typename T>
    static auto createJsonKVPair(const std::pair<std::string, T>& kvPair) {
        return JsonKVPair{.key = kvPair.first, .value = kvPair.second};
    }
    /**
     * @brief Wraps createJsonKVPair, making it possible to pass it through functions until it is used.
     * @return auto: A lambda function that calls createJsonKVPair (avoids the need to specify a return type)
     */
    auto createJsonKVPairLambda() {
        return [this](const auto& kvPair) {
            return this->createJsonKVPair(kvPair);
        };
    }

    /**
     * @brief Checks if given value is contained in json object using the string value.
     * @param value: The value that must be contained in the json object.
     * @param jsonObject: The json object for the current tuple.
     * @param key: The specific key that must map to the expected value.
     * @return true, if value is contained in json object with given key, else false.
     */
    template<typename T>
    bool isInJsonObject(T value, const nlohmann::basic_json<>& jsonObject, const std::string& key) const {
        if (not(jsonObject.contains(key) && jsonObject.at(key) == value)) {
            NES_ERROR("Expected \"{}\":\"{}\" to be contained in resultString {}, but it was not.",
                      key,
                      value,
                      nlohmann::to_string(jsonObject));
            return false;
        }
        return true;
    }

    /**
     * @brief Wraps isInJsonObject, making it possible to pass it to the FormatIteratorTestUtil before it is used.
     * @return auto: A lambda function that calls isInJsonObject (avoids the need to specify a return type).
     */
    auto checkExpectedValue(const nlohmann::basic_json<>& jsonObject, const std::string& key) const {
        return [jsonObject, key, this](const auto& value) {
            return this->isInJsonObject(value, jsonObject, key);
        };
    }

    /**
     * @brief Uses the json format iterator to read all test tuples and checks that they match the expected KV pairs.
     * @param schema: The schema used in the current test.
     * @param expectedKVPairs: The vector in which all vectors for the expected output are stored.
     * @return A json string, obtained by reading a tuple buffer using a json format iterator.
     */
    bool validateJsonIterator(SchemaPtr schema,
                              NES::Runtime::MemoryLayouts::TestTupleBuffer* testTupleBuffer,
                              const std::vector<std::vector<JsonKVPair>>& expectedKVPairs) {
        // Get the test tuple buffer and use it to create a json iterator.
        auto buffer = testTupleBuffer->getBuffer();
        auto jsonIterator = std::make_unique<JsonFormat>(schema, bufferManager);
        auto jsonTupleIterator = jsonIterator->getTupleIterator(buffer).begin();

        // Iterate over all expected key-value pair tuples and parse tuples to json.
        bool areAllKVPairsContained = true;
        for (const auto& expectedKVPairTuple : expectedKVPairs) {
            auto resultString = *jsonTupleIterator;
            auto jsonObject = nlohmann::json::parse(resultString);
            // Iterate over each key-value pair tuple and check for every key, if the value matches the expected value.
            for (const auto& expectedKVPair : expectedKVPairTuple) {
                areAllKVPairsContained &=
                    validateExpectedKVPairForVariant(expectedKVPair, checkExpectedValue(jsonObject, expectedKVPair.key));
            }
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
    auto testTupleBuffer = processTuples(schema, expectedKVPairs, createJsonKVPairLambda(), TestTuple(1));

    // Assert that all expected KV pairs are contained in the generated JSON string.
    ASSERT_TRUE(validateJsonIterator(schema, testTupleBuffer.get(), expectedKVPairs));
}

/**
 * @brief Tests that we can convert a tuple buffer with unsigned integers to json.
 */
TEST_F(JsonFormatTest, useJsonIteratorWithUnsignedIntegers) {
    std::vector<std::vector<JsonKVPair>> expectedKVPairs;
    using TestTuple = std::tuple<uint8_t, uint16_t, uint32_t, uint64_t>;
    SchemaPtr schema = Schema::create()
                           ->addField("U8", BasicType::UINT8)
                           ->addField("U16", BasicType::UINT16)
                           ->addField("U32", BasicType::UINT32)
                           ->addField("U64", BasicType::UINT64);
    auto testTupleBuffer = processTuples(schema, expectedKVPairs, createJsonKVPairLambda(), TestTuple(1, 256, 65536, 4294967296));
    // Assert that all expected KV pairs are contained in the generated JSON string.
    ASSERT_TRUE(validateJsonIterator(schema, testTupleBuffer.get(), expectedKVPairs));
}

/**
 * @brief Tests that we can convert a tuple buffer with signed integers to json.
 */
TEST_F(JsonFormatTest, useJsonIteratorWithSignedIntegers) {
    std::vector<std::vector<JsonKVPair>> expectedKVPairs;
    SchemaPtr schema = Schema::create()
                           ->addField("I8", BasicType::INT8)
                           ->addField("I16", BasicType::INT16)
                           ->addField("I32", BasicType::INT32)
                           ->addField("I64", BasicType::INT64);
    using TestTuple = std::tuple<int8_t, int16_t, int32_t, int64_t>;
    auto testTupleBuffer = processTuples(schema, expectedKVPairs, createJsonKVPairLambda(), TestTuple(1, 128, 32768, 2147483648));
    // Assert that all expected KV pairs are contained in the generated JSON string.
    ASSERT_TRUE(validateJsonIterator(schema, testTupleBuffer.get(), expectedKVPairs));
}

/**
 * @brief Tests that we can convert a tuple buffer with a lower and uppercase char and a true and a false bool to json.
 */
TEST_F(JsonFormatTest, useJsonIteratorWithSignedBoolAndChar) {
    std::vector<std::vector<JsonKVPair>> expectedKVPairs;
    SchemaPtr schema = Schema::create()
                           ->addField("C1", BasicType::CHAR)
                           ->addField("C2", BasicType::CHAR)
                           ->addField("B1", BasicType::BOOLEAN)
                           ->addField("B2", BasicType::BOOLEAN);
    using TestTuple = std::tuple<char, char, bool, bool>;
    auto testTupleBuffer = processTuples(schema, expectedKVPairs, createJsonKVPairLambda(), TestTuple('A', 'a', true, false));
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
    auto testTupleBuffer = processTuples(schema, expectedKVPairs, createJsonKVPairLambda(), TestTuple(4.2, 13.37));
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
    auto testTupleBuffer =
        processTuplesWithString(schema, expectedKVPairs, createJsonKVPairLambda(), TestTuple("42 is the answer"));
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
    auto testTupleBuffer =
        processTuplesWithString(schema, expectedKVPairs, createJsonKVPairLambda(), TestTuple(42, "is the answer"));
    // Assert that all expected KV pairs are contained in the generated JSON string.
    ASSERT_TRUE(validateJsonIterator(schema, testTupleBuffer.get(), expectedKVPairs));
}

/**
 * @brief Tests that we can convert a tuple buffer with many different basic types, and multiple Text types to json.
 */
TEST_F(JsonFormatTest, useJsonIteratorWithMixedDataTypes) {
    std::vector<std::vector<JsonKVPair>> expectedKVPairs;
    SchemaPtr schema = Schema::create()
                           ->addField("T1", DataTypeFactory::createText())
                           ->addField("U8", BasicType::UINT8)
                           ->addField("T2", DataTypeFactory::createText())
                           ->addField("D", BasicType::FLOAT64)
                           ->addField("I16", BasicType::INT16)
                           ->addField("F", BasicType::FLOAT32)
                           ->addField("B", BasicType::BOOLEAN)
                           ->addField("C", BasicType::CHAR)
                           ->addField("T3", DataTypeFactory::createText());
    using TestTuple = std::tuple<std::string, uint8_t, std::string, double, int16_t, float, bool, char, std::string>;
    auto testTupleBuffer =
        processTuplesWithString(schema,
                                expectedKVPairs,
                                createJsonKVPairLambda(),
                                TestTuple("First Text", 42, "Second Text", 13.37, 666, 7.77, true, 'C', "Third Text"),
                                TestTuple("Fourth Text", 43, "Fifth Text", 3.14, 676, 7.67, true, 'c', "Combo Breaker"));
    // Assert that all expected KV pairs are contained in the generated JSON string.
    ASSERT_TRUE(validateJsonIterator(schema, testTupleBuffer.get(), expectedKVPairs));
}

}// namespace NES::Runtime

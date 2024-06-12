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

#ifndef NES_RUNTIME_TESTS_UNITTESTS_RUNTIME_FORMATS_FORMATITERATORTESTUTIL_HPP_
#define NES_RUNTIME_TESTS_UNITTESTS_RUNTIME_FORMATS_FORMATITERATORTESTUTIL_HPP_

#include <API/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Util/TestTupleBuffer.hpp>

namespace NES::Runtime {

/**
 * @brief Provides utility function to write multiple tuples to a test tuple buffer and to create expected output.
 *        Currently, the functionality is limited to a single buffer containing all test tuples.
 * @tparam ExpectedType: the type of the expected data that is validated in the final step of the test.
 */
template<typename ExpectedType>
class FormatIteratorTestUtil {
  public:
    BufferManagerPtr bufferManager;

    /**
     * Takes a schema and creates a TestTupleBuffer with row layout (column layout currently breaks the iterator).
     * @param schema the schema of the tuple containing the field name
     * @return a test tuple buffer that the test tuples will be written into
     */
    auto createTestTupleBuffer(SchemaPtr schema) const {
        auto tupleBuffer = bufferManager->getBufferBlocking();
        MemoryLayouts::RowLayoutPtr rowLayout = MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize());
        return std::make_unique<Runtime::MemoryLayouts::TestTupleBuffer>(rowLayout, tupleBuffer);
    }

    /**
     * @brief Emplaces the field name and the value for the current field in the current expected KV pairs vector.
     * @param schema the schema of the tuple containing the field name
     * @param value the value for the current field
     * @return the value for the current field, which is inserted into a tuple
     */
    template<typename CreateExpectedTypeFunc, typename T>
    auto setExpectedValue(T value,
                          const SchemaPtr& schema,
                          std::vector<ExpectedType>& expectedKVPairs,
                          CreateExpectedTypeFunc createExpectedType) {
        assert(expectedKVPairs.size() < schema->getFieldNames().size());
        expectedKVPairs.emplace_back(
            createExpectedType(std::make_pair(schema->getFieldNames().at(expectedKVPairs.size()), value)));
        return value;
    }

    /**
     * @brief Process a single tuple containing Values, inserting it into the testTupleBuffer.
     * @param schema the schema used in the current test
     * @param testTupleBuffer the buffer in which the tuple is written
     * @param expectedKVPairs the vector in which we store vectors for the expected output
     * @param values the values of the current tuple
     */
    template<typename CreateExpectedTypeFunc, typename... Values>
    auto processTuple(SchemaPtr schema,
                      NES::Runtime::MemoryLayouts::TestTupleBuffer* testTupleBuffer,
                      std::vector<std::vector<ExpectedType>>& expectedKVPairs,
                      CreateExpectedTypeFunc createExpectedType,
                      const Values&... values) {
        // Iterate over all values in the current tuple and add them to the expected KV pairs.
        expectedKVPairs.push_back(std::vector<ExpectedType>());
        auto testTuple = std::make_tuple(setExpectedValue(values, schema, expectedKVPairs.back(), createExpectedType)...);
        testTupleBuffer->pushRecordToBuffer(testTuple);
    }

    /**
     * @brief Calls processTuple for every tuple supplied by the current test.
     * @param schema the schema used in the current test
     * @param expectedKVPairs the vector in which we store vectors for the expected output
     * @param tuples the tuples created in the current test that are written to the testTupleBuffer
     */
    template<typename CreateExpectedTypeFunc, typename... Tuples>
    auto processTuples(SchemaPtr schema,
                       std::vector<std::vector<ExpectedType>>& expectedKVPairs,
                       CreateExpectedTypeFunc createExpectedType,
                       const Tuples&... tuples) {
        auto testTupleBuffer = createTestTupleBuffer(schema);
        (std::apply(
             [&](const auto&... values) {
                 processTuple(schema, testTupleBuffer.get(), expectedKVPairs, createExpectedType, values...);
             },
             tuples),
         ...);
        return testTupleBuffer;
    }

    /**
     * @brief Process a single tuple containing Values with Text, inserting it into the testTupleBuffer.
     * @param schema the schema used in the current test
     * @param testTupleBuffer the buffer in which the tuple is written
     * @param expectedKVPairs the vector in which we store vectors for the expected output
     * @param values the values of the current tuple
     */
    template<typename CreateExpectedTypeFunc, typename... Values>
    auto processTupleWithString(SchemaPtr schema,
                                NES::Runtime::MemoryLayouts::TestTupleBuffer* testTupleBuffer,
                                std::vector<std::vector<ExpectedType>>& expectedKVPairs,
                                CreateExpectedTypeFunc createExpectedType,
                                const Values&... values) {
        expectedKVPairs.push_back(std::vector<ExpectedType>());
        auto testTuple = std::make_tuple(setExpectedValue(values, schema, expectedKVPairs.back(), createExpectedType)...);
        testTupleBuffer->pushRecordToBuffer(testTuple, bufferManager.get());
    }

    /**
     * @brief Calls processTupleWithString for every tuple supplied by the current test.
     * @param schema the schema used in the current test
     * @param expectedKVPairs the vector in which we store the expected output
     * @param tuples the tuples created in the current test that are written to the testTupleBuffer
     */
    template<typename CreateExpectedTypeFunc, typename... Tuples>
    auto processTuplesWithString(SchemaPtr schema,
                                 std::vector<std::vector<ExpectedType>>& expectedKVPairs,
                                 CreateExpectedTypeFunc createExpectedType,
                                 const Tuples&... tuples) {
        auto testTupleBuffer = createTestTupleBuffer(schema);
        (std::apply(
             [&](const auto&... values) {
                 processTupleWithString(schema, testTupleBuffer.get(), expectedKVPairs, createExpectedType, values...);
             },
             tuples),
         ...);
        return testTupleBuffer;
    }

    /**
     * @brief Gets the a value from std::variant with the correct type and validates if it is in the expected values.
              Expects ExpectedType to have a value member of type std::variant.
     * @param expectedKVPair: The expected key-value pair containing the expected value as a std::variant.
     * @param checkExpectedValueFunc: The function that is used to check if the value is in the expected values.
     * @return auto: true, if the value is in the expected values, false if not.
     */
    template<typename CheckExpectedValueFunc>
    auto validateExpectedKVPairForVariant(const ExpectedType& expectedKVPair,
                                          CheckExpectedValueFunc checkExpectedValueFunc) const {
        if (std::holds_alternative<uint8_t>(expectedKVPair.value)) {
            return checkExpectedValueFunc(std::get<uint8_t>(expectedKVPair.value));
        } else if (std::holds_alternative<uint16_t>(expectedKVPair.value)) {
            return checkExpectedValueFunc(std::get<uint16_t>(expectedKVPair.value));
        } else if (std::holds_alternative<uint32_t>(expectedKVPair.value)) {
            return checkExpectedValueFunc(std::get<uint32_t>(expectedKVPair.value));
        } else if (std::holds_alternative<uint64_t>(expectedKVPair.value)) {
            return checkExpectedValueFunc(std::get<uint64_t>(expectedKVPair.value));
        } else if (std::holds_alternative<int8_t>(expectedKVPair.value)) {
            return checkExpectedValueFunc(std::get<int8_t>(expectedKVPair.value));
        } else if (std::holds_alternative<int16_t>(expectedKVPair.value)) {
            return checkExpectedValueFunc(std::get<int16_t>(expectedKVPair.value));
        } else if (std::holds_alternative<int32_t>(expectedKVPair.value)) {
            return checkExpectedValueFunc(std::get<int32_t>(expectedKVPair.value));
        } else if (std::holds_alternative<int64_t>(expectedKVPair.value)) {
            return checkExpectedValueFunc(std::get<int64_t>(expectedKVPair.value));
        } else if (std::holds_alternative<float>(expectedKVPair.value)) {
            return checkExpectedValueFunc(std::get<float>(expectedKVPair.value));
        } else if (std::holds_alternative<double>(expectedKVPair.value)) {
            return checkExpectedValueFunc(std::get<double>(expectedKVPair.value));
        } else if (std::holds_alternative<bool>(expectedKVPair.value)) {
            return checkExpectedValueFunc(std::get<bool>(expectedKVPair.value));
        } else if (std::holds_alternative<char>(expectedKVPair.value)) {
            return checkExpectedValueFunc(std::get<char>(expectedKVPair.value));
        } else if (std::holds_alternative<std::string>(expectedKVPair.value)) {
            return checkExpectedValueFunc(std::get<std::string>(expectedKVPair.value));
        }
        return false;
    }
};

}// namespace NES::Runtime

#endif// NES_RUNTIME_TESTS_UNITTESTS_RUNTIME_FORMATS_FORMATITERATORTESTUTIL_HPP_

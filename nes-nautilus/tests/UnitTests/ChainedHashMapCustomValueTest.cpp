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

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iterator>
#include <map>
#include <memory>
#include <random>
#include <ranges>
#include <sstream>
#include <tuple>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>

#include <Util/ExecutionMode.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>
#include <BaseUnitTest.hpp>
#include <ChainedHashMapCustomValueTestUtils.hpp>
#include <ChainedHashMapTestUtils.hpp>
#include <NautilusTestUtils.hpp>
#include <val.hpp>
#include <val_ptr.hpp>
namespace NES::Nautilus::Interface
{
class ChainedHashMapCustomValueTest
    : public Testing::BaseUnitTest,
      public testing::WithParamInterface<
          std::tuple<int, std::vector<DataType::Type>, std::vector<DataType::Type>, Nautilus::Configurations::ExecutionMode>>,
      public TestUtils::ChainedHashMapCustomValueTestUtils
{
public:
    /// This is fine, as we are writing all items for each number of keys
    static constexpr TestUtils::MinMaxValue MIN_MAX_NUMBER_OF_ITEMS = {.min = 100, .max = 2000};
    static constexpr TestUtils::MinMaxValue MIN_MAX_NUMBER_OF_BUCKETS = {.min = 1, .max = 2048};
    static constexpr TestUtils::MinMaxValue MIN_MAX_PAGE_SIZE = {.min = 512, .max = 10240};
    Configurations::ExecutionMode backend;

    static void SetUpTestSuite()
    {
        Logger::setupLogging("ChainedHashMapCustomValue.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup ChainedHashMapCustomValue class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        const auto& [_, keyBasicTypes, valueBasicTypes, backend] = GetParam();
        this->backend = backend;

        /// Creating the current test param
        params = TestUtils::TestParams(MIN_MAX_NUMBER_OF_ITEMS, MIN_MAX_NUMBER_OF_BUCKETS, MIN_MAX_PAGE_SIZE);
        ChainedHashMapTestUtils::setUpChainedHashMapTest(keyBasicTypes, valueBasicTypes, backend);
    }

    static void TearDownTestSuite() { NES_INFO("Tear down ChainedHashMapCustomValue class."); }
};

/// Test for inserting compound keys and custom values. We choose a PagedVector as the custom value. This is similar to a multimap.
TEST_P(ChainedHashMapCustomValueTest, pagedVector)
{
    /// Creating the destructor callback for each item, i.e. keys and PagedVector
    auto destructorCallback = [&](const ChainedHashMapEntry* entry)
    {
        const auto* memArea = reinterpret_cast<const int8_t*>(entry) + sizeof(ChainedHashMapEntry) + keySize;
        const auto* pagedVector = reinterpret_cast<const PagedVector*>(memArea);
        pagedVector->~PagedVector();
    };

    /// Resetting the entriesPerPage, as we have a paged vector as the value.
    valueSize = sizeof(PagedVector);
    const auto totalSizeOfEntry = (sizeof(ChainedHashMapEntry) + keySize + valueSize);
    entriesPerPage = params.pageSize / (totalSizeOfEntry);

    /// Creating the hash map
    auto hashMap = ChainedHashMap(keySize, valueSize, params.numberOfBuckets, params.pageSize);
    hashMap.setDestructorCallback(destructorCallback);
    ASSERT_EQ(hashMap.getNumberOfTuples(), 0);

    /// We are writing the keys and values to the paged vector. This does not make sense in a real world scenario, but it is a good way to test the hashmap
    /// with a custom value being a paged vector. We need to create a projection for all fields, as we are writing all fields to the paged vector.
    std::vector<Record::RecordFieldIdentifier> projectionAllFields;
    std::ranges::copy(projectionKeys, std::back_inserter(projectionAllFields));
    std::ranges::copy(projectionValues, std::back_inserter(projectionAllFields));
    auto findAndInsertIntoPagedVector = compileFindAndInsertIntoPagedVector(projectionAllFields);

    /// We use a seed for the key position in each buffer, to have some randomness in the test.
    /// We log the seed to gain reproducibility of the test
    const auto seedForKeyPositionInBuffer = std::random_device()();
    NES_INFO("Seed for keyPositionInBuffer: {}", seedForKeyPositionInBuffer);
    std::srand(seedForKeyPositionInBuffer);

    /// Now calling the compiled function with the random key and value buffers.
    /// Additionally, we are writing the keys and values to a hashmap to be able to compare the values later.
    /// We need a multimap, as we are storing multiple values for each key in the chained hash map via the paged vector
    std::multimap<TestUtils::RecordWithFields, Record> exactMap;
    std::vector<uint64_t> allKeyPositions;
    for (auto& bufferKey : inputBuffers)
    {
        /// Getting the keyPositionInBuffer for the current buffer. The min is 0 and the max is the number of tuples in the buffer - 1
        const auto keyPositionInBuffer = std::rand() % bufferKey.getNumberOfTuples();

        /// Writing the key and values to the exact map to compare the values later.
        const RecordBuffer recordBufferKey(nautilus::val<const Memory::TupleBuffer*>(std::addressof(bufferKey)));
        nautilus::val<uint64_t> keyPositionInBufferVal = keyPositionInBuffer;
        auto recordKey = memoryProviderInputBuffer->readRecord(projectionKeys, recordBufferKey, keyPositionInBufferVal);

        /// Writing all values to the paged vector and the exact map
        for (auto& bufferValue : inputBuffers)
        {
            /// We are writing the values to the paged vector
            findAndInsertIntoPagedVector(
                std::addressof(bufferKey), std::addressof(bufferValue), keyPositionInBuffer, bufferManager.get(), std::addressof(hashMap));


            /// Writing the values to the exact map
            const RecordBuffer recordBufferValue(nautilus::val<const Memory::TupleBuffer*>(std::addressof(bufferValue)));
            for (nautilus::val<uint64_t> i = 0; i < recordBufferValue.getNumRecords(); i = i + 1)
            {
                auto recordValue = memoryProviderInputBuffer->readRecord(projectionAllFields, recordBufferValue, i);
                exactMap.insert({{recordKey, projectionKeys}, recordValue});
            }
        }

        /// Adding the key position to the vector to be able to compare the values later.
        allKeyPositions.emplace_back(keyPositionInBuffer);

        /// Logging the progress of the test
        if (constexpr auto logInterval = 5; allKeyPositions.size() % logInterval == 0)
        {
            NES_DEBUG("Processed {} of {} buffers", allKeyPositions.size(), inputBuffers.size());
        }
    }

    ASSERT_TRUE(not allKeyPositions.empty()) << "The key positions should not be empty";
    ASSERT_EQ(allKeyPositions.size(), inputBuffers.size()) << "The key positions should have the same size as the input buffers";

    /// Now we are searching for the entries and checking if the values are correct.
    auto writeAllRecordsIntoOutputBuffer = compileWriteAllRecordsIntoOutputBuffer(projectionAllFields);
    for (auto [buffer, keyPositionInBuffer] : std::views::zip(inputBuffers, allKeyPositions))
    {
        /// Getting the record key from the input buffer, so that we can compare the values with the exact map.
        const RecordBuffer recordBufferKey(nautilus::val<const Memory::TupleBuffer*>(std::addressof(buffer)));
        nautilus::val<uint64_t> keyPositionInBufferVal = keyPositionInBuffer;
        auto recordKey = memoryProviderInputBuffer->readRecord(projectionKeys, recordBufferKey, keyPositionInBufferVal);

        /// Getting the iterator for the exact map to compare the values.
        auto [recordValueExactStart, recordValueExactEnd] = exactMap.equal_range({recordKey, projectionKeys});
        const auto numberOfRecordsExact = std::distance(recordValueExactStart, recordValueExactEnd);

        /// Acquiring a buffer to write the values to that has the needed size
        const auto neededBytes = memoryProviderInputBuffer->getMemoryLayout()->getSchema().getSizeOfSchemaInBytes() * numberOfRecordsExact;
        auto outputBufferOpt = bufferManager->getUnpooledBuffer(neededBytes);
        if (not outputBufferOpt)
        {
            NES_ERROR("Could not allocate buffer for size {}", neededBytes);
            ASSERT_TRUE(false);
        }
        auto outputBuffer = outputBufferOpt.value();

        /// Calling the compiled method to write all values of the hash map for a specific key position to the output buffer.
        writeAllRecordsIntoOutputBuffer(
            std::addressof(buffer), keyPositionInBuffer, std::addressof(outputBuffer), bufferManager.get(), std::addressof(hashMap));
        const auto writtenBytes
            = outputBuffer.getNumberOfTuples() * memoryProviderInputBuffer->getMemoryLayout()->getSchema().getSizeOfSchemaInBytes();
        ASSERT_LE(writtenBytes, outputBuffer.getBufferSize());
        ASSERT_EQ(outputBuffer.getNumberOfTuples(), std::distance(recordValueExactStart, recordValueExactEnd));

        /// Now we are comparing the values in the output buffer with the exact values from the map.
        nautilus::val<uint64_t> currentPosition = 0;
        for (auto exactIt = recordValueExactStart; exactIt != recordValueExactEnd; ++exactIt)
        {
            /// Printing an error message, if the values are not equal.
            const RecordBuffer recordBufferOutput(nautilus::val<const Memory::TupleBuffer*>(std::addressof(outputBuffer)));
            auto recordValueActual = memoryProviderInputBuffer->readRecord(projectionAllFields, recordBufferOutput, currentPosition);
            std::stringstream ss;
            ss << compareRecords(recordValueActual, exactIt->second, projectionAllFields);
            if (not ss.str().empty())
            {
                EXPECT_TRUE(false) << ss.str();
            }
            ++currentPosition;
        }
    }

    hashMap.clear();
}

INSTANTIATE_TEST_SUITE_P(
    ChainedHashMapCustomValue,
    ChainedHashMapCustomValueTest,
    ::testing::Combine(
        /// Running the test for 3 times for each key, value schema and backend.
        /// This entails three different random number of items, number of buckets and page size.
        ::testing::Range(0, 3),
        ::testing::ValuesIn<std::vector<DataType::Type>>(
            {{DataType::Type::UINT8},
             {DataType::Type::INT64, DataType::Type::UINT64, DataType::Type::INT8, DataType::Type::INT16, DataType::Type::INT32},
             {DataType::Type::INT64,
              DataType::Type::INT32,
              DataType::Type::INT16,
              DataType::Type::INT8,
              DataType::Type::UINT64,
              DataType::Type::UINT32,
              DataType::Type::UINT16,
              DataType::Type::UINT8}}),
        ::testing::ValuesIn<std::vector<DataType::Type>>(
            {{DataType::Type::INT8},
             {DataType::Type::INT64,
              DataType::Type::INT32,
              DataType::Type::INT16,
              DataType::Type::INT8,
              DataType::Type::FLOAT32,
              DataType::Type::UINT64,
              DataType::Type::UINT32,
              DataType::Type::UINT16,
              DataType::Type::UINT8,
              DataType::Type::FLOAT64}}),
        ::testing::Values(Nautilus::Configurations::ExecutionMode::COMPILER, Nautilus::Configurations::ExecutionMode::INTERPRETER)),
    [](const testing::TestParamInfo<ChainedHashMapCustomValueTest::ParamType>& info)
    {
        const auto iteration = std::get<0>(info.param);
        const auto keyBasicTypes = std::get<1>(info.param);
        const auto valueBasicTypes = std::get<2>(info.param);
        const auto backend = std::get<3>(info.param);

        std::stringstream ss;
        ss << "noI_" << iteration << "_keyTypes_";
        for (const auto& keyBasicType : keyBasicTypes)
        {
            ss << magic_enum::enum_name(keyBasicType) << "_";
        }
        ss << "valTypes_";
        for (const auto& valueBasicType : valueBasicTypes)
        {
            ss << magic_enum::enum_name(valueBasicType) << "_";
        }
        ss << magic_enum::enum_name(backend);
        return ss.str();
    });
}

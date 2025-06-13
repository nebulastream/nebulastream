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

#include <cstring>
#include <memory>
#include <sstream>
#include <tuple>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>

#include <Util/ExecutionMode.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>
#include <nautilus/Engine.hpp>
#include <BaseUnitTest.hpp>
#include <ChainedHashMapTestUtils.hpp>
#include <NautilusTestUtils.hpp>

namespace NES::Nautilus::Interface
{
class ChainedHashMapTest : public Testing::BaseUnitTest,
                           public testing::WithParamInterface<
                               std::tuple<int, std::vector<DataType::Type>, std::vector<DataType::Type>, Configurations::ExecutionMode>>,
                           public TestUtils::ChainedHashMapTestUtils
{
public:
    static constexpr TestUtils::MinMaxValue MIN_MAX_NUMBER_OF_ITEMS = {.min = 100, .max = 10000};
    static constexpr TestUtils::MinMaxValue MIN_MAX_NUMBER_OF_BUCKETS = {.min = 10, .max = 2048};
    static constexpr TestUtils::MinMaxValue MIN_MAX_PAGE_SIZE = {.min = 1024, .max = 10240};

    static void SetUpTestSuite()
    {
        Logger::setupLogging("ChainedHashMapTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup ChainedHashMapTest class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        const auto& [_, keyBasicTypes, valueBasicTypes, backend] = GetParam();

        /// Creating the current test param
        params = TestUtils::TestParams(MIN_MAX_NUMBER_OF_ITEMS, MIN_MAX_NUMBER_OF_BUCKETS, MIN_MAX_PAGE_SIZE);

        ChainedHashMapTestUtils::setUpChainedHashMapTest(keyBasicTypes, valueBasicTypes, backend);
    }

    static void TearDownTestSuite() { NES_INFO("Tear down ChainedHashMapTest class."); }
};

TEST_P(ChainedHashMapTest, fixedDataTypesSingleInsert)
{
    /// Creating the hash map
    auto hashMap = ChainedHashMap(keySize, valueSize, params.numberOfBuckets, params.pageSize);

    /// Check if the hash map is empty.
    ASSERT_EQ(hashMap.getNumberOfTuples(), 0);

    /// We are inserting the records from the random key and value buffers into a map.
    /// Thus, we can check if the provided values are correct.
    /// We are testing here the findOrCreate method that only inserts a value if it does not exist, i.e., no update.
    /// Therefore, we MUST NOT overwrite an existing key in this loop here to be able to test the findOrCreate method.
    const auto exactMap = createExactMap(ExactMapInsert::INSERT);

    /// Check if we can insert the entry and then read the values back.
    auto findAndInsert = compileFindAndInsert();
    for (auto& buffer : inputBuffers)
    {
        findAndInsert(std::addressof(buffer), bufferManager.get(), std::addressof(hashMap));
    }

    /// Now we are searching for the entries and checking if the values are correct.
    checkIfValuesAreCorrectViaFindEntry(hashMap, exactMap);

    /// Check if our entry iterator reads all the entries
    checkEntryIterator(hashMap, exactMap);
}

TEST_P(ChainedHashMapTest, fixedDataTypesUpdate)
{
    /// Creating the hash map
    auto hashMap = ChainedHashMap(keySize, valueSize, params.numberOfBuckets, params.pageSize);

    /// Check if the hash map is empty.
    ASSERT_EQ(hashMap.getNumberOfTuples(), 0);

    /// Getting new values for updating the values in the hash map.
    inputBuffers = createMonotonicallyIncreasingValues(inputSchema, params.numberOfItems, *bufferManager);

    /// We are inserting the records from the random key and value buffers into a map.
    /// Thus, we can check if the provided values are correct.
    /// We are testing here the findOrCreate and the insertOrUpdateEntry method, i.e., we are updating the values for existing keys.
    /// Therefore, we MUST overwrite an existing key in this loop here to be able to test the findOrCreate method.
    const auto exactMap = createExactMap(ExactMapInsert::OVERWRITE);
    auto findAndUpdate = compileFindAndUpdate();
    for (auto& buffer : inputBuffers)
    {
        findAndUpdate(std::addressof(buffer), std::addressof(buffer), bufferManager.get(), std::addressof(hashMap));
    }

    /// Now we are searching for the entries and checking if the values are correct.
    checkIfValuesAreCorrectViaFindEntry(hashMap, exactMap);

    /// Check if our entry iterator reads all the entries
    checkEntryIterator(hashMap, exactMap);
}

INSTANTIATE_TEST_SUITE_P(
    ChainedHashMapTest,
    ChainedHashMapTest,
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
    [](const testing::TestParamInfo<ChainedHashMapTest::ParamType>& info)
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

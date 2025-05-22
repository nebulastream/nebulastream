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

#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <sstream>
#include <vector>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/NautilusBackend.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>
#include <BaseUnitTest.hpp>
#include <Engine.hpp>
#include <NautilusTestUtils.hpp>
#include <options.hpp>
#include <val.hpp>
#include <val_ptr.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>

#include <PagedVectorTestUtils.hpp>

namespace NES::Nautilus::Interface
{

class PagedVectorTest : public Testing::BaseUnitTest,
                        public TestUtils::NautilusTestUtils,
                        public testing::WithParamInterface<Nautilus::Configurations::NautilusBackend>
{
public:
    static constexpr uint64_t PAGE_SIZE = 4096;
    std::shared_ptr<Memory::BufferManager> bufferManager;
    std::unique_ptr<nautilus::engine::NautilusEngine> nautilusEngine;
    Nautilus::Configurations::NautilusBackend backend = Nautilus::Configurations::NautilusBackend::INTERPRETER;
    uint64_t numberOfItems{};
    static constexpr auto minNumberOfItems = 50;
    static constexpr auto maxNumberOfItems = 2000;

    static void SetUpTestSuite()
    {
        Logger::setupLogging("PagedVectorTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup PagedVectorTest class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        backend = GetParam();
        /// Setting the correct options for the engine, depending on the enum value from the backend
        nautilus::engine::Options options;
        const bool compilation = (backend == Nautilus::Configurations::NautilusBackend::COMPILER);
        NES_INFO("Backend: {} and compilation: {}", magic_enum::enum_name(backend), compilation);
        options.setOption("engine.Compilation", compilation);
        nautilusEngine = std::make_unique<nautilus::engine::NautilusEngine>(options);

        /// Getting a new random seed and then generating a random number for the no. items
        /// We print the seed, so that we can reproduce the test if necessary
        const auto seed = std::random_device()();
        NES_INFO("Seed: {}", seed);
        std::srand(seed);
        numberOfItems = std::rand() % (maxNumberOfItems - minNumberOfItems + 1) + minNumberOfItems;
    }

    static void TearDownTestSuite() { NES_INFO("Tear down PagedVectorTest class."); }
};

TEST_P(PagedVectorTest, storeAndRetrieveFixedSizeValues)
{
    bufferManager = Memory::BufferManager::create();
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("value2", BasicType::UINT64)
                                ->addField("value3", BasicType::UINT64);
    constexpr auto pageSize = PAGE_SIZE;
    const auto projections = testSchema->getFieldNames();
    const auto allRecords = createMonotonicallyIncreasingValues(testSchema, numberOfItems, *bufferManager);

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(pageSize, testSchema);
    PagedVector pagedVector;
    TestUtils::runStoreTest(pagedVector, testSchema, pageSize, projections, allRecords, *nautilusEngine, *bufferManager);
    TestUtils::runRetrieveTest(pagedVector, testSchema, pageSize, projections, allRecords, *nautilusEngine, *bufferManager);
}

TEST_P(PagedVectorTest, storeAndRetrieveVarSizeValues)
{
    bufferManager = Memory::BufferManager::create();
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", DataTypeProvider::provideDataType(LogicalType::VARSIZED))
                                ->addField("value2", DataTypeProvider::provideDataType(LogicalType::VARSIZED))
                                ->addField("value3", DataTypeProvider::provideDataType(LogicalType::VARSIZED));
    constexpr auto pageSize = PAGE_SIZE;
    const auto projections = testSchema->getFieldNames();
    const auto allRecords = createMonotonicallyIncreasingValues(testSchema, numberOfItems, *bufferManager);

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(pageSize, testSchema);
    PagedVector pagedVector;
    TestUtils::runStoreTest(pagedVector, testSchema, pageSize, projections, allRecords, *nautilusEngine, *bufferManager);
    TestUtils::runRetrieveTest(pagedVector, testSchema, pageSize, projections, allRecords, *nautilusEngine, *bufferManager);
}

TEST_P(PagedVectorTest, storeAndRetrieveLargeValues)
{
    bufferManager = Memory::BufferManager::create();
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", DataTypeProvider::provideDataType(LogicalType::VARSIZED));
    /// smallest possible pageSize ensures that the text is split over multiple pages
    constexpr auto pageSize = 8UL;
    constexpr auto sizeVarSizedData = 2 * pageSize;

    const auto projections = testSchema->getFieldNames();
    const auto allRecords = createMonotonicallyIncreasingValues(testSchema, numberOfItems, *bufferManager, sizeVarSizedData);

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(pageSize, testSchema);
    PagedVector pagedVector;
    TestUtils::runStoreTest(pagedVector, testSchema, pageSize, projections, allRecords, *nautilusEngine, *bufferManager);
    TestUtils::runRetrieveTest(pagedVector, testSchema, pageSize, projections, allRecords, *nautilusEngine, *bufferManager);
}

TEST_P(PagedVectorTest, storeAndRetrieveMixedValueTypes)
{
    bufferManager = Memory::BufferManager::create();
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("value2", DataTypeProvider::provideDataType(LogicalType::VARSIZED))
                                ->addField("value3", BasicType::FLOAT64);
    constexpr auto pageSize = PAGE_SIZE;
    const auto projections = testSchema->getFieldNames();
    const auto allRecords = createMonotonicallyIncreasingValues(testSchema, numberOfItems, *bufferManager);

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(pageSize, testSchema);
    PagedVector pagedVector;
    TestUtils::runStoreTest(pagedVector, testSchema, pageSize, projections, allRecords, *nautilusEngine, *bufferManager);
    TestUtils::runRetrieveTest(pagedVector, testSchema, pageSize, projections, allRecords, *nautilusEngine, *bufferManager);
}

TEST_P(PagedVectorTest, storeAndRetrieveFixedValuesNonDefaultPageSize)
{
    bufferManager = Memory::BufferManager::create();
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("value2", BasicType::UINT64);
    constexpr auto pageSize = 73UL;
    const auto projections = testSchema->getFieldNames();
    const auto allRecords = createMonotonicallyIncreasingValues(testSchema, numberOfItems, *bufferManager);

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(pageSize, testSchema);
    PagedVector pagedVector;
    TestUtils::runStoreTest(pagedVector, testSchema, pageSize, projections, allRecords, *nautilusEngine, *bufferManager);
    TestUtils::runRetrieveTest(pagedVector, testSchema, pageSize, projections, allRecords, *nautilusEngine, *bufferManager);
}

TEST_P(PagedVectorTest, appendAllPagesTwoVectors)
{
    bufferManager = Memory::BufferManager::create();
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("value2", DataTypeProvider::provideDataType(LogicalType::VARSIZED));
    const auto entrySize = testSchema->getSchemaSizeInBytes();
    constexpr auto pageSize = PAGE_SIZE;
    constexpr auto numVectors = 2UL;
    const auto projections = testSchema->getFieldNames();

    std::vector<std::vector<Memory::TupleBuffer>> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0UL; i < numVectors; ++i)
    {
        auto records = createMonotonicallyIncreasingValues(testSchema, numberOfItems, *bufferManager);
        allRecords.emplace_back(records);
    }

    std::vector<Memory::TupleBuffer> allRecordsAfterAppendAll;
    for (auto i = 0UL; i < numVectors; ++i)
    {
        allRecordsAfterAppendAll.insert(allRecordsAfterAppendAll.end(), allRecords[i].begin(), allRecords[i].end());
    }

    TestUtils::insertAndAppendAllPagesTest(
        projections, testSchema, entrySize, pageSize, allRecords, allRecordsAfterAppendAll, 0, *nautilusEngine, *bufferManager);
}

TEST_P(PagedVectorTest, appendAllPagesMultipleVectors)
{
    bufferManager = Memory::BufferManager::create();
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("value2", DataTypeProvider::provideDataType(LogicalType::VARSIZED))
                                ->addField("value3", BasicType::FLOAT64);
    const auto entrySize = testSchema->getSchemaSizeInBytes();
    constexpr auto pageSize = PAGE_SIZE;
    constexpr auto numVectors = 4UL;
    const auto projections = testSchema->getFieldNames();

    std::vector<std::vector<Memory::TupleBuffer>> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0UL; i < numVectors; ++i)
    {
        auto records = createMonotonicallyIncreasingValues(testSchema, numberOfItems, *bufferManager);
        allRecords.emplace_back(records);
    }

    std::vector<Memory::TupleBuffer> allRecordsAfterAppendAll;
    for (auto i = 0UL; i < numVectors; ++i)
    {
        allRecordsAfterAppendAll.insert(allRecordsAfterAppendAll.end(), allRecords[i].begin(), allRecords[i].end());
    }

    TestUtils::insertAndAppendAllPagesTest(
        projections, testSchema, entrySize, pageSize, allRecords, allRecordsAfterAppendAll, 0, *nautilusEngine, *bufferManager);
}

TEST_P(PagedVectorTest, appendAllPagesMultipleVectorsColumnarLayout)
{
    bufferManager = Memory::BufferManager::create();
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("value2", DataTypeProvider::provideDataType(LogicalType::VARSIZED))
                                ->addField("value3", BasicType::FLOAT64);
    const auto entrySize = testSchema->getSchemaSizeInBytes();
    constexpr auto pageSize = PAGE_SIZE;
    constexpr auto numVectors = 4UL;
    const auto projections = testSchema->getFieldNames();

    std::vector<std::vector<Memory::TupleBuffer>> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0UL; i < numVectors; ++i)
    {
        auto records = createMonotonicallyIncreasingValues(testSchema, numberOfItems, *bufferManager);
        allRecords.emplace_back(records);
    }

    std::vector<Memory::TupleBuffer> allRecordsAfterAppendAll;
    for (auto i = 0UL; i < numVectors; ++i)
    {
        allRecordsAfterAppendAll.insert(allRecordsAfterAppendAll.end(), allRecords[i].begin(), allRecords[i].end());
    }

    TestUtils::insertAndAppendAllPagesTest(
        projections, testSchema, entrySize, pageSize, allRecords, allRecordsAfterAppendAll, 0, *nautilusEngine, *bufferManager);
}

TEST_P(PagedVectorTest, appendAllPagesMultipleVectorsWithDifferentPageSizes)
{
    bufferManager = Memory::BufferManager::create();
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("value2", DataTypeProvider::provideDataType(LogicalType::VARSIZED))
                                ->addField("value3", BasicType::FLOAT64);
    const auto entrySize = testSchema->getSchemaSizeInBytes();
    constexpr auto pageSize = PAGE_SIZE;
    constexpr auto numVectors = 4UL;
    const auto projections = testSchema->getFieldNames();

    std::vector<std::vector<Memory::TupleBuffer>> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0UL; i < numVectors; ++i)
    {
        auto records = createMonotonicallyIncreasingValues(testSchema, numberOfItems, *bufferManager);
        allRecords.emplace_back(records);
    }

    std::vector<Memory::TupleBuffer> allRecordsAfterAppendAll;
    for (auto i = 0UL; i < numVectors; ++i)
    {
        allRecordsAfterAppendAll.insert(allRecordsAfterAppendAll.end(), allRecords[i].begin(), allRecords[i].end());
    }

    TestUtils::insertAndAppendAllPagesTest(
        projections, testSchema, entrySize, pageSize, allRecords, allRecordsAfterAppendAll, 1, *nautilusEngine, *bufferManager);
}

INSTANTIATE_TEST_CASE_P(
    PagedVectorTest,
    PagedVectorTest,
    ::testing::Values(Nautilus::Configurations::NautilusBackend::INTERPRETER, Nautilus::Configurations::NautilusBackend::COMPILER),
    [](const testing::TestParamInfo<PagedVectorTest::ParamType>& info)
    {
        std::stringstream ss;
        ss << magic_enum::enum_name(info.param);
        return ss.str();
    });
}

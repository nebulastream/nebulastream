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
#include <cstring>
#include <functional>
#include <memory>
#include <numeric>
#include <sstream>
#include <vector>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Configurations/Enums/NautilusBackend.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <Engine.hpp>
#include <NautilusTestUtils.hpp>
#include <magic_enum.hpp>
#include <options.hpp>
#include <val.hpp>
#include <val_ptr.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>

namespace NES::Nautilus::Interface
{

class PagedVectorTest : public Testing::BaseUnitTest,
                        public NautilusTestUtils,
                        public testing::WithParamInterface<QueryCompilation::NautilusBackend>
{
public:
    static constexpr uint64_t PAGE_SIZE = 4096;
    Memory::BufferManagerPtr bufferManager;
    std::unique_ptr<nautilus::engine::NautilusEngine> nautilusEngine;
    QueryCompilation::NautilusBackend backend;

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
        const bool compilation = (backend == QueryCompilation::NautilusBackend::COMPILER);
        NES_INFO("Backend: {} and compilation: {}", magic_enum::enum_name(backend), compilation);
        options.setOption("engine.Compilation", compilation);
        nautilusEngine = std::make_unique<nautilus::engine::NautilusEngine>(options);
    }

    static void TearDownTestSuite() { NES_INFO("Tear down PagedVectorTest class."); }

    void runStoreTest(
        PagedVector& pagedVector,
        const SchemaPtr& testSchema,
        const uint64_t pageSize,
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const std::vector<Memory::TupleBuffer>& allRecords) const
    {
        /// Creating the memory provider for the paged vector
        const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(pageSize, testSchema);

        /// Compiling the function that inserts the records into the PagedVector, if it is not already compiled.

        const auto memoryProviderInputBuffer = MemoryProvider::TupleBufferMemoryProvider::create(allRecords[0].getBufferSize(), testSchema);

        /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
        /// ReSharper disable once CppPassValueParameterByConstReference
        auto insertIntoPagedVector = nautilusEngine->registerFunction(std::function(
            [=](nautilus::val<Memory::TupleBuffer*> inputBufferRef,
                /// ReSharper disable once CppPassValueParameterByConstReference
                nautilus::val<Memory::AbstractBufferProvider*> bufferProviderVal,
                /// ReSharper disable once CppPassValueParameterByConstReference
                nautilus::val<PagedVector*> pagedVectorVal)
            {
                const RecordBuffer recordBuffer(inputBufferRef);
                const PagedVectorRef pagedVectorRef(pagedVectorVal, memoryProvider, bufferProviderVal);
                for (nautilus::val<uint64_t> i = 0; i < recordBuffer.getNumRecords(); i = i + 1)
                {
                    const auto record = memoryProviderInputBuffer->readRecord(projections, recordBuffer, i);
                    pagedVectorRef.writeRecord(record);
                }
            }));

        /// Inserting each tuple by iterating over all tuple buffers
        for (auto buf : allRecords)
        {
            insertIntoPagedVector(std::addressof(buf), bufferManager.get(), std::addressof(pagedVector));
        }


        /// Calculating the expected number of entries and pages
        const uint64_t expectedNumberOfEntries = std::accumulate(
            allRecords.begin(),
            allRecords.end(),
            0UL,
            [](const auto& sum, const auto& buffer) { return sum + buffer.getNumberOfTuples(); });
        const uint64_t capacityPerPage = memoryProvider->getMemoryLayoutPtr()->getCapacity();
        const uint64_t numberOfPages = std::ceil(static_cast<double>(expectedNumberOfEntries) / capacityPerPage);
        ASSERT_EQ(pagedVector.getTotalNumberOfEntries(), expectedNumberOfEntries);
        ASSERT_EQ(pagedVector.getNumberOfPages(), numberOfPages);

        /// As we do lazy allocation, we do not create a new page if the last tuple fit on the page
        const bool lastTupleFitsOntoLastPage = (expectedNumberOfEntries % capacityPerPage) == 0;
        const uint64_t numTuplesLastPage = lastTupleFitsOntoLastPage ? capacityPerPage : (expectedNumberOfEntries % capacityPerPage);
        ASSERT_EQ(pagedVector.getLastPage().getNumberOfTuples(), numTuplesLastPage);
    }

    void runRetrieveTest(
        PagedVector& pagedVector,
        const SchemaPtr& testSchema,
        const uint64_t pageSize,
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const std::vector<Memory::TupleBuffer>& allRecords) const
    {
        /// Creating a buffer to store the retrieved records and then calling the compiled method to retrieve the records
        const uint64_t numberOfExpectedTuples = std::accumulate(
            allRecords.begin(),
            allRecords.end(),
            0UL,
            [](const auto& sum, const auto& buffer) { return sum + buffer.getNumberOfTuples(); });
        ASSERT_EQ(pagedVector.getTotalNumberOfEntries(), numberOfExpectedTuples);
        auto outputBufferVal = bufferManager->getUnpooledBuffer(numberOfExpectedTuples * testSchema->getSchemaSizeInBytes());
        ASSERT_TRUE(outputBufferVal.has_value());
        auto outputBuffer = outputBufferVal.value();

        /// Creating the memory provider for the input buffers
        const auto memoryProviderActualBuffer = MemoryProvider::TupleBufferMemoryProvider::create(outputBuffer.getBufferSize(), testSchema);


        /// Compiling the function that reads the records from the PagedVector, if it is not already compiled
        const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(pageSize, testSchema);

        /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
        /// ReSharper disable once CppPassValueParameterByConstReference
        auto readFromPagedVectorIntoTupleBuffer = nautilusEngine->registerFunction(std::function(
            [=](nautilus::val<Memory::TupleBuffer*> outputBufferRef,
                /// ReSharper disable once CppPassValueParameterByConstReference
                nautilus::val<Memory::AbstractBufferProvider*> bufferProviderVal,
                /// ReSharper disable once CppPassValueParameterByConstReference
                nautilus::val<PagedVector*> pagedVectorVal)
            {
                RecordBuffer recordBuffer(outputBufferRef);
                PagedVectorRef const pagedVectorRef(pagedVectorVal, memoryProvider, bufferProviderVal);
                nautilus::val<uint64_t> numberOfTuples = 0;
                for (auto it = pagedVectorRef.begin(projections); it != pagedVectorRef.end(projections); ++it)
                {
                    auto record = *it;
                    memoryProviderActualBuffer->writeRecord(numberOfTuples, recordBuffer, record);
                    numberOfTuples = numberOfTuples + 1;
                    recordBuffer.setNumRecords(numberOfTuples);
                }
            }));

        /// Retrieving the records from the PagedVector, by calling the compiled function
        readFromPagedVectorIntoTupleBuffer(std::addressof(outputBuffer), bufferManager.get(), std::addressof(pagedVector));


        /// Checking for correctness of the retrieved records
        RecordBuffer const recordBufferActual(nautilus::val<Memory::TupleBuffer*>(std::addressof(outputBuffer)));
        nautilus::val<uint64_t> recordBufferIndexActual = 0;
        const auto memoryProviderInputBuffer = MemoryProvider::TupleBufferMemoryProvider::create(allRecords[0].getBufferSize(), testSchema);
        for (auto inputBuf : allRecords)
        {
            RecordBuffer const recordBufferInput(nautilus::val<Memory::TupleBuffer*>(std::addressof(inputBuf)));
            for (nautilus::val<uint64_t> i = 0; i < inputBuf.getNumberOfTuples(); ++i)
            {
                auto recordActual = memoryProviderActualBuffer->readRecord(projections, recordBufferActual, recordBufferIndexActual);
                auto recordExpected = memoryProviderInputBuffer->readRecord(projections, recordBufferInput, i);

                /// Printing an error message, if the values are not equal.
                auto compareResult = compareRecords(recordActual, recordExpected, projections);
                if (not compareResult.empty())
                {
                    EXPECT_TRUE(false) << compareResult;
                }

                /// Incrementing the index of the actual record buffer
                recordBufferIndexActual = recordBufferIndexActual + 1;
            }
        }

        ASSERT_EQ(recordBufferIndexActual, numberOfExpectedTuples);
    }

    void insertAndAppendAllPagesTest(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const SchemaPtr& schema,
        const uint64_t entrySize,
        const uint64_t pageSize,
        const std::vector<std::vector<Memory::TupleBuffer>>& allRecordsAndVectors,
        const std::vector<Memory::TupleBuffer>& expectedRecordsAfterAppendAll,
        uint64_t differentPageSizes)
    {
        /// Inserting data into each PagedVector and checking for correct values
        std::vector<std::unique_ptr<PagedVector>> allPagedVectors;
        auto varPageSize = pageSize;
        for (const auto& allRecords : allRecordsAndVectors)
        {
            if (differentPageSizes != 0)
            {
                differentPageSizes++;
            }
            varPageSize += differentPageSizes * entrySize;
            const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(varPageSize, schema);
            allPagedVectors.emplace_back(std::make_unique<PagedVector>());
            runStoreTest(*allPagedVectors.back(), schema, pageSize, projections, allRecords);
            runRetrieveTest(*allPagedVectors.back(), schema, pageSize, projections, allRecords);
        }

        ((void)expectedRecordsAfterAppendAll);
        /// Appending and deleting all PagedVectors except for the first one
        const auto& firstPagedVec = allPagedVectors[0];
        if (allRecordsAndVectors.size() > 1)
        {
            for (uint64_t i = 1; i < allPagedVectors.size(); ++i)
            {
                auto& otherPagedVec = allPagedVectors[i];
                firstPagedVec->appendAllPages(*otherPagedVec);
                EXPECT_EQ(otherPagedVec->getNumberOfPages(), 0);
            }

            allPagedVectors.erase(allPagedVectors.begin() + 1, allPagedVectors.end());
        }

        /// Checking for number of pagedVectors and correct values
        EXPECT_EQ(allPagedVectors.size(), 1);
        const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(pageSize, schema);
        runRetrieveTest(*firstPagedVec, schema, pageSize, projections, expectedRecordsAfterAppendAll);
    }
};

TEST_P(PagedVectorTest, storeAndRetrieveFixedSizeValues)
{
    bufferManager = Memory::BufferManager::create();
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("value2", BasicType::UINT64)
                                ->addField("value3", BasicType::UINT64);
    constexpr auto pageSize = PAGE_SIZE;
    constexpr auto numItems = 507UL;
    const auto projections = testSchema->getFieldNames();
    const auto allRecords = createMonotonicIncreasingValues(testSchema, numItems, backend, *bufferManager);

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(pageSize, testSchema);
    PagedVector pagedVector;
    runStoreTest(pagedVector, testSchema, pageSize, projections, allRecords);
    runRetrieveTest(pagedVector, testSchema, pageSize, projections, allRecords);
}

TEST_P(PagedVectorTest, storeAndRetrieveVarSizeValues)
{
    bufferManager = Memory::BufferManager::create();
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", DataTypeFactory::createVariableSizedData())
                                ->addField("value2", DataTypeFactory::createVariableSizedData())
                                ->addField("value3", DataTypeFactory::createVariableSizedData());
    constexpr auto pageSize = PAGE_SIZE;
    constexpr auto numItems = 507UL;
    const auto projections = testSchema->getFieldNames();
    const auto allRecords = createMonotonicIncreasingValues(testSchema, numItems, backend, *bufferManager);

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(pageSize, testSchema);
    PagedVector pagedVector;
    runStoreTest(pagedVector, testSchema, pageSize, projections, allRecords);
    runRetrieveTest(pagedVector, testSchema, pageSize, projections, allRecords);
}

TEST_P(PagedVectorTest, storeAndRetrieveLargeValues)
{
    bufferManager = Memory::BufferManager::create();
    const auto testSchema
        = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField("value1", DataTypeFactory::createVariableSizedData());
    /// smallest possible pageSize ensures that the text is split over multiple pages
    constexpr auto pageSize = 8UL;
    constexpr auto numItems = 507UL;
    constexpr auto sizeVarSizedData = 2 * pageSize;

    const auto projections = testSchema->getFieldNames();
    const auto allRecords = createMonotonicIncreasingValues(testSchema, numItems, backend, *bufferManager, sizeVarSizedData);

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(pageSize, testSchema);
    PagedVector pagedVector;
    runStoreTest(pagedVector, testSchema, pageSize, projections, allRecords);
    runRetrieveTest(pagedVector, testSchema, pageSize, projections, allRecords);
}

TEST_P(PagedVectorTest, storeAndRetrieveMixedValueTypes)
{
    bufferManager = Memory::BufferManager::create();
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("value2", DataTypeFactory::createVariableSizedData())
                                ->addField("value3", BasicType::FLOAT64);
    constexpr auto pageSize = PAGE_SIZE;
    constexpr auto numItems = 507UL;
    const auto projections = testSchema->getFieldNames();
    const auto allRecords = createMonotonicIncreasingValues(testSchema, numItems, backend, *bufferManager);

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(pageSize, testSchema);
    PagedVector pagedVector;
    runStoreTest(pagedVector, testSchema, pageSize, projections, allRecords);
    runRetrieveTest(pagedVector, testSchema, pageSize, projections, allRecords);
}

TEST_P(PagedVectorTest, storeAndRetrieveFixedValuesNonDefaultPageSize)
{
    bufferManager = Memory::BufferManager::create();
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("value2", BasicType::UINT64);
    constexpr auto pageSize = 73UL;
    constexpr auto numItems = 507UL;
    const auto projections = testSchema->getFieldNames();
    const auto allRecords = createMonotonicIncreasingValues(testSchema, numItems, backend, *bufferManager);

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(pageSize, testSchema);
    PagedVector pagedVector;
    runStoreTest(pagedVector, testSchema, pageSize, projections, allRecords);
    runRetrieveTest(pagedVector, testSchema, pageSize, projections, allRecords);
}

TEST_P(PagedVectorTest, appendAllPagesTwoVectors)
{
    bufferManager = Memory::BufferManager::create();
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("value2", DataTypeFactory::createVariableSizedData());
    const auto entrySize = testSchema->getSchemaSizeInBytes();
    constexpr auto pageSize = PAGE_SIZE;
    constexpr auto numItems = 507UL;
    constexpr auto numVectors = 2UL;
    const auto projections = testSchema->getFieldNames();

    std::vector<std::vector<Memory::TupleBuffer>> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0UL; i < numVectors; ++i)
    {
        auto records = createMonotonicIncreasingValues(testSchema, numItems, backend, *bufferManager);
        allRecords.emplace_back(records);
    }

    std::vector<Memory::TupleBuffer> allRecordsAfterAppendAll;
    for (auto i = 0UL; i < numVectors; ++i)
    {
        allRecordsAfterAppendAll.insert(allRecordsAfterAppendAll.end(), allRecords[i].begin(), allRecords[i].end());
    }

    insertAndAppendAllPagesTest(projections, testSchema, entrySize, pageSize, allRecords, allRecordsAfterAppendAll, 0);
}

TEST_P(PagedVectorTest, appendAllPagesMultipleVectors)
{
    bufferManager = Memory::BufferManager::create();
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("value2", DataTypeFactory::createVariableSizedData())
                                ->addField("value3", BasicType::FLOAT64);
    const auto entrySize = testSchema->getSchemaSizeInBytes();
    constexpr auto pageSize = PAGE_SIZE;
    constexpr auto numItems = 507UL;
    constexpr auto numVectors = 4UL;
    const auto projections = testSchema->getFieldNames();

    std::vector<std::vector<Memory::TupleBuffer>> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0UL; i < numVectors; ++i)
    {
        auto records = createMonotonicIncreasingValues(testSchema, numItems, backend, *bufferManager);
        allRecords.emplace_back(records);
    }

    std::vector<Memory::TupleBuffer> allRecordsAfterAppendAll;
    for (auto i = 0UL; i < numVectors; ++i)
    {
        allRecordsAfterAppendAll.insert(allRecordsAfterAppendAll.end(), allRecords[i].begin(), allRecords[i].end());
    }

    insertAndAppendAllPagesTest(projections, testSchema, entrySize, pageSize, allRecords, allRecordsAfterAppendAll, 0);
}

TEST_P(PagedVectorTest, appendAllPagesMultipleVectorsColumnarLayout)
{
    bufferManager = Memory::BufferManager::create();
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("value2", DataTypeFactory::createVariableSizedData())
                                ->addField("value3", BasicType::FLOAT64);
    const auto entrySize = testSchema->getSchemaSizeInBytes();
    constexpr auto pageSize = PAGE_SIZE;
    constexpr auto numItems = 507UL;
    constexpr auto numVectors = 4UL;
    const auto projections = testSchema->getFieldNames();

    std::vector<std::vector<Memory::TupleBuffer>> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0UL; i < numVectors; ++i)
    {
        auto records = createMonotonicIncreasingValues(testSchema, numItems, backend, *bufferManager);
        allRecords.emplace_back(records);
    }

    std::vector<Memory::TupleBuffer> allRecordsAfterAppendAll;
    for (auto i = 0UL; i < numVectors; ++i)
    {
        allRecordsAfterAppendAll.insert(allRecordsAfterAppendAll.end(), allRecords[i].begin(), allRecords[i].end());
    }

    insertAndAppendAllPagesTest(projections, testSchema, entrySize, pageSize, allRecords, allRecordsAfterAppendAll, 0);
}

TEST_P(PagedVectorTest, appendAllPagesMultipleVectorsWithDifferentPageSizes)
{
    bufferManager = Memory::BufferManager::create();
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("value2", DataTypeFactory::createVariableSizedData())
                                ->addField("value3", BasicType::FLOAT64);
    const auto entrySize = testSchema->getSchemaSizeInBytes();
    constexpr auto pageSize = PAGE_SIZE;
    constexpr auto numItems = 507UL;
    constexpr auto numVectors = 4UL;
    const auto projections = testSchema->getFieldNames();

    std::vector<std::vector<Memory::TupleBuffer>> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0UL; i < numVectors; ++i)
    {
        auto records = createMonotonicIncreasingValues(testSchema, numItems, backend, *bufferManager);
        allRecords.emplace_back(records);
    }

    std::vector<Memory::TupleBuffer> allRecordsAfterAppendAll;
    for (auto i = 0UL; i < numVectors; ++i)
    {
        allRecordsAfterAppendAll.insert(allRecordsAfterAppendAll.end(), allRecords[i].begin(), allRecords[i].end());
    }

    insertAndAppendAllPagesTest(projections, testSchema, entrySize, pageSize, allRecords, allRecordsAfterAppendAll, 1);
}


INSTANTIATE_TEST_CASE_P(
    PagedVectorTest,
    PagedVectorTest,
    ::testing::Values(QueryCompilation::NautilusBackend::INTERPRETER, QueryCompilation::NautilusBackend::COMPILER),
    [](const testing::TestParamInfo<PagedVectorTest::ParamType>& info)
    {
        std::stringstream ss;
        ss << magic_enum::enum_name(info.param);
        return ss.str();
    });
}

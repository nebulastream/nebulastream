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
#include <memory>
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
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <val.hpp>
#include <val_ptr.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>

namespace NES::Nautilus::Interface
{

class PagedVectorTest : public Testing::BaseUnitTest
{
public:
    static constexpr uint64_t PAGE_SIZE = 4096;
    Memory::BufferManagerPtr bufferManager;
    std::vector<Memory::TupleBuffer> testStrings;

    static void SetUpTestSuite()
    {
        Logger::setupLogging("PagedVectorTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup PagedVectorTest class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down PagedVectorTest class."); }

    std::vector<Record> createRecords(const SchemaPtr& schema, const uint64_t numRecords, const uint64_t minTextLength)
    {
        std::vector<Record> allRecords;
        auto allFields = schema->getFieldNames();
        for (auto i = 0UL; i < numRecords; ++i)
        {
            Record newRecord;
            auto fieldCnt = 0UL;
            for (auto& field : allFields)
            {
                auto const fieldType = schema->getFieldByName(field).value()->getDataType();
                auto tupleNo = (schema->getSchemaSizeInBytes() * i) + fieldCnt++;

                if (NES::Util::instanceOf<VariableSizedDataType>(fieldType))
                {
                    std::stringstream ss;
                    ss << "testing TextValue" << tupleNo;
                    auto buffer = bufferManager->getUnpooledBuffer(ss.str().length());
                    if (buffer.has_value())
                    {
                        const VarVal varSizedData(VariableSizedData(buffer.value().getBuffer(), ss.str().length()));
                        auto* sizeRef = reinterpret_cast<uint32_t*>(varSizedData.cast<VariableSizedData>().getReference().value);
                        *sizeRef = ss.str().length();
                        std::memcpy(varSizedData.cast<VariableSizedData>().getContent().value, ss.str().c_str(), ss.str().length());
                        newRecord.write(field, varSizedData);

                        NES_ASSERT2_FMT(ss.str().length() > minTextLength, "Length of the generated text is not long enough!");
                        testStrings.emplace_back(buffer.value());
                    }
                    else
                    {
                        INVARIANT(false, "No unpooled TupleBuffer available!");
                    }
                }
                else if (NES::Util::instanceOf<Integer>(fieldType))
                {
                    newRecord.write(field, nautilus::val<uint64_t>(tupleNo));
                }
                else
                {
                    newRecord.write(field, nautilus::val<double_t>(static_cast<double_t>(tupleNo) / numRecords));
                }
            }
            allRecords.emplace_back(newRecord);
        }
        return allRecords;
    }

    static void runStoreTest(
        PagedVector& pagedVector,
        const std::shared_ptr<MemoryProvider::TupleBufferMemoryProvider>& memoryProvider,
        const std::vector<Record>& allRecords)
    {
        const uint64_t expectedNumberOfEntries = allRecords.size();
        const uint64_t capacityPerPage = memoryProvider->getMemoryLayoutPtr()->getCapacity();
        const uint64_t numberOfPages = std::ceil(static_cast<double>(expectedNumberOfEntries) / capacityPerPage);
        auto pagedVectorRef = PagedVectorRef(nautilus::val<PagedVector*>(&pagedVector), memoryProvider);

        for (const auto& record : allRecords)
        {
            pagedVectorRef.writeRecord(record);
        }

        ASSERT_EQ(pagedVector.getTotalNumberOfEntries(), expectedNumberOfEntries);
        ASSERT_EQ(pagedVector.getNumberOfPages(), numberOfPages);

        /// As we do lazy allocation, we do not create a new page if the last tuple fit on the page
        const bool lastTupleFitsOntoLastPage = (expectedNumberOfEntries % capacityPerPage) == 0;
        const uint64_t numTuplesLastPage = lastTupleFitsOntoLastPage ? capacityPerPage : (expectedNumberOfEntries % capacityPerPage);
        ASSERT_EQ(pagedVector.getLastPage().getNumberOfTuples(), numTuplesLastPage);
    }

    static void runRetrieveTest(
        PagedVector& pagedVector,
        const std::shared_ptr<MemoryProvider::TupleBufferMemoryProvider>& memoryProvider,
        const std::vector<Record>& allRecords)
    {
        auto pagedVectorRef = PagedVectorRef(nautilus::val<PagedVector*>(&pagedVector), memoryProvider);
        ASSERT_EQ(pagedVector.getTotalNumberOfEntries(), allRecords.size());

        auto itemPos = 0UL;
        const auto projections = memoryProvider->getMemoryLayoutPtr()->getSchema()->getFieldNames();
        for (auto it = pagedVectorRef.begin(projections); it != pagedVectorRef.end(projections); ++it)
        {
            ASSERT_EQ(*it, allRecords[itemPos++]);
        }
        ASSERT_EQ(itemPos, allRecords.size());
    }

    void insertAndAppendAllPagesTest(
        const SchemaPtr& schema,
        const uint64_t entrySize,
        uint64_t pageSize,
        const std::vector<std::vector<Record>>& allRecordsAndVectors,
        const std::vector<Record>& expectedRecordsAfterAppendAll,
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
            allPagedVectors.emplace_back(std::make_unique<PagedVector>(bufferManager, memoryProvider->getMemoryLayoutPtr()));
            runStoreTest(*allPagedVectors.back(), memoryProvider, allRecords);
            runRetrieveTest(*allPagedVectors.back(), memoryProvider, allRecords);
        }

        /// Appending and deleting all PagedVectors except for the first one
        auto& firstPagedVec = allPagedVectors[0];
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
        runRetrieveTest(*firstPagedVec, memoryProvider, expectedRecordsAfterAppendAll);
    }
};

TEST_F(PagedVectorTest, storeAndRetrieveFixedSizeValues)
{
    bufferManager = Memory::BufferManager::create();
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("value1", BasicType::UINT64)
                          ->addField("value2", BasicType::UINT64)
                          ->addField("value3", BasicType::UINT64);
    const auto pageSize = PAGE_SIZE;
    const auto numItems = 507UL;
    auto allRecords = createRecords(testSchema, numItems, 0);

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(pageSize, testSchema);
    PagedVector pagedVector(bufferManager, memoryProvider->getMemoryLayoutPtr());
    runStoreTest(pagedVector, memoryProvider, allRecords);
    runRetrieveTest(pagedVector, memoryProvider, allRecords);
}

TEST_F(PagedVectorTest, storeAndRetrieveVarSizeValues)
{
    bufferManager = Memory::BufferManager::create();
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("value1", DataTypeFactory::createVariableSizedData())
                          ->addField("value2", DataTypeFactory::createVariableSizedData())
                          ->addField("value3", DataTypeFactory::createVariableSizedData());
    const auto pageSize = PAGE_SIZE;
    const auto numItems = 507UL;
    auto allRecords = createRecords(testSchema, numItems, 0);

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(pageSize, testSchema);
    PagedVector pagedVector(bufferManager, memoryProvider->getMemoryLayoutPtr());
    runStoreTest(pagedVector, memoryProvider, allRecords);
    runRetrieveTest(pagedVector, memoryProvider, allRecords);
}

TEST_F(PagedVectorTest, storeAndRetrieveLargeValues)
{
    bufferManager = Memory::BufferManager::create();
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField("value1", DataTypeFactory::createVariableSizedData());
    /// smallest possible pageSize ensures that the text is split over multiple pages
    const auto pageSize = 8UL;
    const auto numItems = 507UL;
    auto allRecords = createRecords(testSchema, numItems, static_cast<uint64_t>(2 * pageSize));

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(pageSize, testSchema);
    PagedVector pagedVector(bufferManager, memoryProvider->getMemoryLayoutPtr());
    runStoreTest(pagedVector, memoryProvider, allRecords);
    runRetrieveTest(pagedVector, memoryProvider, allRecords);
}

TEST_F(PagedVectorTest, storeAndRetrieveMixedValueTypes)
{
    bufferManager = Memory::BufferManager::create();
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("value1", BasicType::UINT64)
                          ->addField("value2", DataTypeFactory::createVariableSizedData())
                          ->addField("value3", BasicType::FLOAT64);
    const auto pageSize = PAGE_SIZE;
    const auto numItems = 507UL;
    auto allRecords = createRecords(testSchema, numItems, 0);

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(pageSize, testSchema);
    PagedVector pagedVector(bufferManager, memoryProvider->getMemoryLayoutPtr());
    runStoreTest(pagedVector, memoryProvider, allRecords);
    runRetrieveTest(pagedVector, memoryProvider, allRecords);
}

TEST_F(PagedVectorTest, storeAndRetrieveFixedValuesNonDefaultPageSize)
{
    bufferManager = Memory::BufferManager::create();
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("value1", BasicType::UINT64)
                          ->addField("value2", BasicType::UINT64);
    const auto pageSize = 73UL;
    const auto numItems = 507UL;
    auto allRecords = createRecords(testSchema, numItems, 0);

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(pageSize, testSchema);
    PagedVector pagedVector(bufferManager, memoryProvider->getMemoryLayoutPtr());
    runStoreTest(pagedVector, memoryProvider, allRecords);
    runRetrieveTest(pagedVector, memoryProvider, allRecords);
}

TEST_F(PagedVectorTest, appendAllPagesTwoVectors)
{
    bufferManager = Memory::BufferManager::create();
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("value1", BasicType::UINT64)
                          ->addField("value2", DataTypeFactory::createVariableSizedData());
    const auto entrySize = 2 * sizeof(uint64_t);
    const auto pageSize = PAGE_SIZE;
    const auto numItems = 507UL;
    const auto numVectors = 2UL;

    std::vector<std::vector<Record>> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0UL; i < numVectors; ++i)
    {
        auto records = createRecords(testSchema, numItems, 0);
        allRecords.emplace_back(records);
    }

    std::vector<Record> allRecordsAfterAppendAll;
    for (auto i = 0UL; i < numVectors; ++i)
    {
        allRecordsAfterAppendAll.insert(allRecordsAfterAppendAll.end(), allRecords[i].begin(), allRecords[i].end());
    }

    insertAndAppendAllPagesTest(testSchema, entrySize, pageSize, allRecords, allRecordsAfterAppendAll, 0);
}

TEST_F(PagedVectorTest, appendAllPagesMultipleVectors)
{
    bufferManager = Memory::BufferManager::create();
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("value1", BasicType::UINT64)
                          ->addField("value2", DataTypeFactory::createVariableSizedData())
                          ->addField("value3", BasicType::FLOAT64);
    const auto entrySize = (2 * sizeof(uint64_t)) + sizeof(double_t);
    const auto pageSize = PAGE_SIZE;
    const auto numItems = 507UL;
    const auto numVectors = 4UL;

    std::vector<std::vector<Record>> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0UL; i < numVectors; ++i)
    {
        auto records = createRecords(testSchema, numItems, 0);
        allRecords.emplace_back(records);
    }

    std::vector<Record> allRecordsAfterAppendAll;
    for (auto i = 0UL; i < numVectors; ++i)
    {
        allRecordsAfterAppendAll.insert(allRecordsAfterAppendAll.end(), allRecords[i].begin(), allRecords[i].end());
    }

    insertAndAppendAllPagesTest(testSchema, entrySize, pageSize, allRecords, allRecordsAfterAppendAll, 0);
}

TEST_F(PagedVectorTest, appendAllPagesMultipleVectorsColumnarLayout)
{
    bufferManager = Memory::BufferManager::create();
    auto testSchema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
                          ->addField("value1", BasicType::UINT64)
                          ->addField("value2", DataTypeFactory::createVariableSizedData())
                          ->addField("value3", BasicType::FLOAT64);
    const auto entrySize = (2 * sizeof(uint64_t)) + sizeof(double_t);
    const auto pageSize = PAGE_SIZE;
    const auto numItems = 507UL;
    const auto numVectors = 4UL;

    std::vector<std::vector<Record>> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0UL; i < numVectors; ++i)
    {
        auto records = createRecords(testSchema, numItems, 0);
        allRecords.emplace_back(records);
    }

    std::vector<Record> allRecordsAfterAppendAll;
    for (auto i = 0UL; i < numVectors; ++i)
    {
        allRecordsAfterAppendAll.insert(allRecordsAfterAppendAll.end(), allRecords[i].begin(), allRecords[i].end());
    }

    insertAndAppendAllPagesTest(testSchema, entrySize, pageSize, allRecords, allRecordsAfterAppendAll, 0);
}

TEST_F(PagedVectorTest, appendAllPagesMultipleVectorsWithDifferentPageSizes)
{
    bufferManager = Memory::BufferManager::create();
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("value1", BasicType::UINT64)
                          ->addField("value2", DataTypeFactory::createVariableSizedData())
                          ->addField("value3", BasicType::FLOAT64);
    const auto entrySize = (2 * sizeof(uint64_t)) + sizeof(double_t);
    const auto pageSize = PAGE_SIZE;
    const auto numItems = 507UL;
    const auto numVectors = 4UL;

    std::vector<std::vector<Record>> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0UL; i < numVectors; ++i)
    {
        auto records = createRecords(testSchema, numItems, 0);
        allRecords.emplace_back(records);
    }

    std::vector<Record> allRecordsAfterAppendAll;
    for (auto i = 0UL; i < numVectors; ++i)
    {
        allRecordsAfterAppendAll.insert(allRecordsAfterAppendAll.end(), allRecords[i].begin(), allRecords[i].end());
    }

    insertAndAppendAllPagesTest(testSchema, entrySize, pageSize, allRecords, allRecordsAfterAppendAll, 1);
}

}

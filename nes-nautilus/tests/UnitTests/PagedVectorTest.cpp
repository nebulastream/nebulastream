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

#include <API/AttributeField.hpp>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::Interface
{

class PagedVectorTest : public Testing::BaseUnitTest
{
public:
    static constexpr uint64_t PAGE_SIZE = 4096;
    Memory::BufferManagerPtr bufferManager;

    static void SetUpTestCase()
    {
        Logger::setupLogging("PagedVectorTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup PagedVectorTest class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down PagedVectorTest class."); }

    std::vector<Record> createRecords(const SchemaPtr& schema, const uint64_t numRecords, const uint64_t minTextLength) const
    {
        std::vector<Record> allRecords;
        auto allFields = schema->getFieldNames();
        for (auto i = 0UL; i < numRecords; ++i)
        {
            Record newRecord;
            auto fieldCnt = 0;
            for (auto& field : allFields)
            {
                auto const fieldType = schema->get(field)->getDataType();
                auto tupleNo = (schema->getSize() * i) + fieldCnt++;

                if (fieldType->isText())
                {
                    auto buffer = bufferManager->getUnpooledBuffer(PAGE_SIZE);
                    if (buffer.has_value())
                    {
                        std::stringstream ss;
                        ss << "testing TextValue" << tupleNo;
                        const nautilus::val<int8_t*> ptrToVariableSized(buffer.value().getBuffer());
                        const VarVal varSizedData{VariableSizedData(ptrToVariableSized, ss.str().length())};
                        std::memcpy(varSizedData.cast<VariableSizedData>().getContent().value, ss.str().c_str(), ss.str().length());
                        newRecord.write(field, varSizedData);

                        NES_ASSERT2_FMT(ss.str().length() > minTextLength, "Length of the generated text is not long enough!");
                    }
                    else
                    {
                        NES_THROW_RUNTIME_ERROR("No unpooled TupleBuffer available!");
                    }
                }
                else if (fieldType->isInteger())
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
        PagedVector& pagedVector, const Memory::MemoryLayouts::MemoryLayoutPtr& memoryLayout, const std::vector<Record>& allRecords)
    {
        ASSERT_EQ(memoryLayout->getTupleSize(), pagedVector.getEntrySize());
        const uint64_t capacityPerPage = memoryLayout->getBufferSize() / memoryLayout->getTupleSize();
        const uint64_t expectedNumberOfEntries = allRecords.size();
        const uint64_t numberOfPages = std::ceil(static_cast<double>(expectedNumberOfEntries) / capacityPerPage);
        auto pagedVectorRef = PagedVectorRef(nautilus::val<PagedVector*>(&pagedVector), memoryLayout);

        for (const auto& record : allRecords)
        {
            pagedVectorRef.writeRecord(record);
        }

        ASSERT_EQ(pagedVector.getTotalNumberOfEntries(), expectedNumberOfEntries);
        ASSERT_EQ(pagedVector.getPages().size(), numberOfPages);

        // As we do lazy allocation, we do not create a new page if the last tuple fit on the page
        bool const lastTupleFitsOntoLastPage = (expectedNumberOfEntries % capacityPerPage) == 0;
        const uint64_t numTuplesLastPage = lastTupleFitsOntoLastPage ? capacityPerPage : (expectedNumberOfEntries % capacityPerPage);
        ASSERT_EQ(pagedVector.getPages().back()->getNumberOfTuples(), numTuplesLastPage);
    }

    static void runRetrieveTest(
        PagedVector& pagedVector, const Memory::MemoryLayouts::MemoryLayoutPtr& memoryLayout, const std::vector<Record>& allRecords)
    {
        auto pagedVectorRef = PagedVectorRef(nautilus::val<PagedVector*>(&pagedVector), memoryLayout);
        ASSERT_EQ(pagedVector.getTotalNumberOfEntries(), allRecords.size());

        auto itemPos = 0;
        for (auto record : pagedVectorRef)
        {
            ASSERT_EQ(record, allRecords[itemPos++]);
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
        // Inserting data into each PagedVector and checking for correct values
        std::vector<std::unique_ptr<PagedVector>> allPagedVectors;
        auto varPageSize = pageSize;
        for (const auto& allRecords : allRecordsAndVectors)
        {
            if (differentPageSizes != 0)
            {
                differentPageSizes++;
            }
            varPageSize += differentPageSizes * entrySize;
            auto memoryLayout = ::NES::Util::createMemoryLayout(schema, varPageSize);
            allPagedVectors.emplace_back(std::make_unique<PagedVector>(bufferManager, memoryLayout));
            runStoreTest(*allPagedVectors.back(), memoryLayout, allRecords);
            runRetrieveTest(*allPagedVectors.back(), memoryLayout, allRecords);
        }

        // Appending and deleting all PagedVectors except for the first one
        auto& firstPagedVec = allPagedVectors[0];
        if (allRecordsAndVectors.size() > 1)
        {
            for (uint64_t i = 1; i < allPagedVectors.size(); ++i)
            {
                auto& otherPagedVec = allPagedVectors[i];
                firstPagedVec->appendAllPages(*otherPagedVec);
                EXPECT_EQ(otherPagedVec->getPages().size(), 0);
                EXPECT_EQ(otherPagedVec->getTotalNumberOfEntries(), 0);
                EXPECT_EQ(otherPagedVec->getPages().back()->getNumberOfTuples(), 0);
            }

            allPagedVectors.erase(allPagedVectors.begin() + 1, allPagedVectors.end());
        }

        // Checking for number of pagedVectors and correct values
        EXPECT_EQ(allPagedVectors.size(), 1);
        auto memoryLayout = ::NES::Util::createMemoryLayout(schema, pageSize);
        runRetrieveTest(*firstPagedVec, memoryLayout, expectedRecordsAfterAppendAll);
    }
};

TEST_F(PagedVectorTest, storeAndRetrieveFixedSizeValues)
{
    bufferManager = Memory::BufferManager::create();
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", BasicType::UINT64))
                          ->addField(createField("value3", BasicType::UINT64));
    const auto pageSize = PAGE_SIZE;
    const auto numItems = 507;
    auto allRecords = createRecords(testSchema, numItems, 0);

    auto memoryLayout = ::NES::Util::createMemoryLayout(testSchema, pageSize);
    PagedVector pagedVector(bufferManager, memoryLayout);
    runStoreTest(pagedVector, memoryLayout, allRecords);
    runRetrieveTest(pagedVector, memoryLayout, allRecords);
}

TEST_F(PagedVectorTest, storeAndRetrieveVarSizeValues)
{
    bufferManager = Memory::BufferManager::create();
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", DataTypeFactory::createText()))
                          ->addField(createField("value2", DataTypeFactory::createText()))
                          ->addField(createField("value3", DataTypeFactory::createText()));
    const auto pageSize = PAGE_SIZE;
    const auto numItems = 507;
    auto allRecords = createRecords(testSchema, numItems, 0);

    auto memoryLayout = ::NES::Util::createMemoryLayout(testSchema, pageSize);
    PagedVector pagedVector(bufferManager, memoryLayout);
    runStoreTest(pagedVector, memoryLayout, allRecords);
    runRetrieveTest(pagedVector, memoryLayout, allRecords);
}

TEST_F(PagedVectorTest, storeAndRetrieveLargeValues)
{
    bufferManager = Memory::BufferManager::create();
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField(createField("value1", DataTypeFactory::createText()));
    // smallest possible pageSize ensures that the text is split over multiple pages
    const auto pageSize = 8;
    const auto numItems = 507;
    auto allRecords = createRecords(testSchema, numItems, static_cast<uint64_t>(2 * pageSize));

    auto memoryLayout = ::NES::Util::createMemoryLayout(testSchema, pageSize);
    PagedVector pagedVector(bufferManager, memoryLayout);
    runStoreTest(pagedVector, memoryLayout, allRecords);
    runRetrieveTest(pagedVector, memoryLayout, allRecords);
}

TEST_F(PagedVectorTest, storeAndRetrieveMixedValueTypes)
{
    bufferManager = Memory::BufferManager::create();
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", DataTypeFactory::createText()))
                          ->addField(createField("value3", BasicType::FLOAT64));
    const auto pageSize = PAGE_SIZE;
    const auto numItems = 507;
    auto allRecords = createRecords(testSchema, numItems, 0);

    auto memoryLayout = ::NES::Util::createMemoryLayout(testSchema, pageSize);
    PagedVector pagedVector(bufferManager, memoryLayout);
    runStoreTest(pagedVector, memoryLayout, allRecords);
    runRetrieveTest(pagedVector, memoryLayout, allRecords);
}

TEST_F(PagedVectorTest, storeAndRetrieveFixedValuesNonDefaultPageSize)
{
    bufferManager = Memory::BufferManager::create();
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", BasicType::UINT64));
    const auto pageSize = 73;
    const auto numItems = 507;
    auto allRecords = createRecords(testSchema, numItems, 0);

    auto memoryLayout = ::NES::Util::createMemoryLayout(testSchema, pageSize);
    PagedVector pagedVector(bufferManager, memoryLayout);
    runStoreTest(pagedVector, memoryLayout, allRecords);
    runRetrieveTest(pagedVector, memoryLayout, allRecords);
}

TEST_F(PagedVectorTest, appendAllPagesTwoVectors)
{
    bufferManager = Memory::BufferManager::create();
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", DataTypeFactory::createText()));
    const auto entrySize = 2 * sizeof(uint64_t);
    const auto pageSize = PAGE_SIZE;
    const auto numItems = 507;
    const auto numVectors = 2;

    std::vector<std::vector<Record>> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0; i < numVectors; ++i)
    {
        auto records = createRecords(testSchema, numItems, 0);
        allRecords.emplace_back(records);
    }

    std::vector<Record> allRecordsAfterAppendAll;
    for (auto i = 0; i < numVectors; ++i)
    {
        allRecordsAfterAppendAll.insert(allRecordsAfterAppendAll.end(), allRecords[i].begin(), allRecords[i].end());
    }

    insertAndAppendAllPagesTest(testSchema, entrySize, pageSize, allRecords, allRecordsAfterAppendAll, 0);
}

TEST_F(PagedVectorTest, appendAllPagesMultipleVectors)
{
    bufferManager = Memory::BufferManager::create();
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", DataTypeFactory::createText()))
                          ->addField(createField("value3", BasicType::FLOAT64));
    const auto entrySize = (2 * sizeof(uint64_t)) + sizeof(double_t);
    const auto pageSize = PAGE_SIZE;
    const auto numItems = 507;
    const auto numVectors = 4;

    std::vector<std::vector<Record>> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0; i < numVectors; ++i)
    {
        auto records = createRecords(testSchema, numItems, 0);
        allRecords.emplace_back(records);
    }

    std::vector<Record> allRecordsAfterAppendAll;
    for (auto i = 0; i < numVectors; ++i)
    {
        allRecordsAfterAppendAll.insert(allRecordsAfterAppendAll.end(), allRecords[i].begin(), allRecords[i].end());
    }

    insertAndAppendAllPagesTest(testSchema, entrySize, pageSize, allRecords, allRecordsAfterAppendAll, 0);
}

TEST_F(PagedVectorTest, appendAllPagesMultipleVectorsColumnarLayout)
{
    bufferManager = Memory::BufferManager::create();
    auto testSchema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", DataTypeFactory::createText()))
                          ->addField(createField("value3", BasicType::FLOAT64));
    const auto entrySize = (2 * sizeof(uint64_t)) + sizeof(double_t);
    const auto pageSize = PAGE_SIZE;
    const auto numItems = 507;
    const auto numVectors = 4;

    std::vector<std::vector<Record>> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0; i < numVectors; ++i)
    {
        auto records = createRecords(testSchema, numItems, 0);
        allRecords.emplace_back(records);
    }

    std::vector<Record> allRecordsAfterAppendAll;
    for (auto i = 0; i < numVectors; ++i)
    {
        allRecordsAfterAppendAll.insert(allRecordsAfterAppendAll.end(), allRecords[i].begin(), allRecords[i].end());
    }

    insertAndAppendAllPagesTest(testSchema, entrySize, pageSize, allRecords, allRecordsAfterAppendAll, 0);
}

TEST_F(PagedVectorTest, appendAllPagesMultipleVectorsWithDifferentPageSizes)
{
    bufferManager = Memory::BufferManager::create();
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", DataTypeFactory::createText()))
                          ->addField(createField("value3", BasicType::FLOAT64));
    const auto entrySize = (2 * sizeof(uint64_t)) + sizeof(double_t);
    const auto pageSize = PAGE_SIZE;
    const auto numItems = 507;
    const auto numVectors = 4;

    std::vector<std::vector<Record>> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0; i < numVectors; ++i)
    {
        auto records = createRecords(testSchema, numItems, 0);
        allRecords.emplace_back(records);
    }

    std::vector<Record> allRecordsAfterAppendAll;
    for (auto i = 0; i < numVectors; ++i)
    {
        allRecordsAfterAppendAll.insert(allRecordsAfterAppendAll.end(), allRecords[i].begin(), allRecords[i].end());
    }

    insertAndAppendAllPagesTest(testSchema, entrySize, pageSize, allRecords, allRecordsAfterAppendAll, 1);
}

}

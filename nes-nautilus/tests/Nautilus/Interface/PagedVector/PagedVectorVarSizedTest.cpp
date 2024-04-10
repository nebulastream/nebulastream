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
#include <BaseIntegrationTest.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRowLayout.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSizedRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/BufferManager.hpp>

namespace NES::Nautilus::Interface {
class PagedVectorVarSizedTest : public Testing::BaseUnitTest {
  public:
    std::shared_ptr<Runtime::BufferManager> bufferManager;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("PagedVectorVarSizedTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup PagedVectorVarSizedTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        bufferManager = std::make_shared<Runtime::BufferManager>();
        NES_INFO("Setup PagedVectorVarSizedTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("Tear down PagedVectorVarSizedTest test case.");
        Testing::BaseUnitTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down PagedVectorVarSizedTest test class."); }

    std::vector<Record> createRecords(const SchemaPtr& schema, const uint64_t numRecords) {
        std::vector<Record> allRecords;
        auto allFields = schema->getFieldNames();
        for (auto i = 0_u64; i < numRecords; ++i) {

            Record newRecord;
            auto fieldCnt = 0_u64;
            for (auto& field : allFields) {
                auto const fieldType = schema->get(field)->getDataType();
                auto tupleNo = schema->getSize() * i + fieldCnt++;

                if (fieldType->isText()) {
                    auto buffer = bufferManager->getUnpooledBuffer(100);
                    if (buffer.has_value()) {
                        std::stringstream ss;
                        ss << "TextValue" << tupleNo;
                        auto textValue = TextValue::create(buffer.value(), ss.str().length());
                        std::memcpy(textValue->str(), ss.str().c_str(), ss.str().length());
                        newRecord.write(field, Text(textValue));
                    } else {
                        NES_THROW_RUNTIME_ERROR("No unpooled TupleBuffer available!");
                    }
                } else if (fieldType->isInteger()) {
                    newRecord.write(field, Value<UInt64>(tupleNo));
                } else {
                    newRecord.write(field, Value<Double>((double_t) tupleNo / numRecords));
                }
            }
            allRecords.emplace_back(newRecord);
        }
        return allRecords;
    }

    void runStoreTest(NES::Runtime::PagedVectorRowLayout& pagedVector,
                      const SchemaPtr& schema,
                      const uint64_t entrySize,
                      const uint64_t pageSize,
                      const std::vector<Record>& allRecords) {
        ASSERT_EQ(entrySize, pagedVector.getEntrySize());
        const uint64_t capacityPerPage = pageSize / entrySize;
        const uint64_t expectedNumberOfEntries = allRecords.size();
        const uint64_t numberOfPages = std::ceil((double) expectedNumberOfEntries / capacityPerPage);
        auto pagedVectorVarSizedRef = PagedVectorVarSizedRef(Value<MemRef>(reinterpret_cast<int8_t*>(&pagedVector)), schema);

        for (auto& record : allRecords) {
            pagedVectorVarSizedRef.writeRecord(record);
        }

        // As we do lazy allocation, we do not create a new page if the last tuple fit on the page
        bool lastTupleFitsOntoLastPage = (expectedNumberOfEntries % capacityPerPage) == 0;
        const uint64_t numTuplesLastPage =
            lastTupleFitsOntoLastPage ? capacityPerPage : (expectedNumberOfEntries % capacityPerPage);
    }

    void runRetrieveTest(const NES::Runtime::PagedVectorRowLayout& pagedVector,
                         const SchemaPtr& schema,
                         const std::vector<Record>& allRecords) {
        auto pagedVectorVarSizedRef = PagedVectorVarSizedRef(Value<MemRef>(reinterpret_cast<int8_t*>(&pagedVector)), schema);
        ASSERT_EQ(pagedVector.size(), allRecords.size());

        auto itemPos = 0_u64;
        for (auto record : pagedVectorVarSizedRef) {
            ASSERT_EQ(record, allRecords[itemPos]);
            itemPos++;
        }
        ASSERT_EQ(itemPos, allRecords.size());
    }

    void insertAndAppendAllPagesTest(const SchemaPtr& schema,
                                     const uint64_t entrySize,
                                     uint64_t pageSize,
                                     const uint64_t,
                                     std::vector<std::vector<Record>>& allRecordsAndVectors,
                                     std::vector<Record>& expectedRecordsAfterAppendAll,
                                     uint64_t differentPageSizes) {
        // Inserting data into each PagedVector and checking for correct values
        std::vector<std::unique_ptr<NES::Runtime::PagedVectorRowLayout>> allPagedVectors;
        for (auto& allRecords : allRecordsAndVectors) {
            if (differentPageSizes != 0) {
                differentPageSizes++;
            }
            pageSize += differentPageSizes * entrySize;
            allPagedVectors.emplace_back(
                std::make_unique<NES::Runtime::PagedVectorRowLayout>(bufferManager, entrySize, pageSize));
            runStoreTest(*allPagedVectors.back(), schema, entrySize, pageSize, allRecords);
            runRetrieveTest(*allPagedVectors.back(), schema, allRecords);
        }

        // Appending and deleting all PagedVectors except for the first one
        auto& firstPagedVec = allPagedVectors[0];
        if (allRecordsAndVectors.size() > 1) {
            for (uint64_t i = 1; i < allPagedVectors.size(); ++i) {
                auto& otherPagedVec = allPagedVectors[i];
                firstPagedVec->takePagesFrom(std::move(*otherPagedVec));
                EXPECT_EQ(otherPagedVec->size(), 0);
            }

            allPagedVectors.erase(allPagedVectors.begin() + 1, allPagedVectors.end());
        }

        // Checking for number of pagedVectors and correct values
        EXPECT_EQ(allPagedVectors.size(), 1);
        runRetrieveTest(*firstPagedVec, schema, expectedRecordsAfterAppendAll);
    }
};

TEST_F(PagedVectorVarSizedTest, storeAndRetrieveFixedSizeValues) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", BasicType::UINT64))
                          ->addField(createField("value3", BasicType::UINT64));
    const auto entrySize = testSchema->getSchemaSizeInBytes();
    const auto numItems = 507_u64;
    auto allRecords = createRecords(testSchema, numItems);

    NES::Runtime::PagedVectorRowLayout pagedVector(bufferManager,
                                                   testSchema->getSchemaSizeInBytes(),
                                                   Runtime::detail::EntryPage::ENTRY_PAGE_SIZE);
    runStoreTest(pagedVector, testSchema, entrySize, Runtime::detail::EntryPage::ENTRY_PAGE_SIZE, allRecords);
    runRetrieveTest(pagedVector, testSchema, allRecords);
}

TEST_F(PagedVectorVarSizedTest, storeAndRetrieveVarSizeValues) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::TEXT))
                          ->addField(createField("value2", BasicType::TEXT))
                          ->addField(createField("value3", BasicType::TEXT));
    const auto entrySize = 3 * sizeof(uint64_t) + 3 * sizeof(size_t);
    const auto pageSize = NES::Runtime::detail::EntryPage::ENTRY_PAGE_SIZE;
    const auto numItems = 507_u64;
    auto allRecords = createRecords(testSchema, numItems);

    NES::Runtime::PagedVectorRowLayout pagedVector(bufferManager, entrySize, pageSize);
    runStoreTest(pagedVector, testSchema, entrySize, pageSize, allRecords);
    runRetrieveTest(pagedVector, testSchema, allRecords);
}

TEST_F(PagedVectorVarSizedTest, storeAndRetrieveMixedValueTypes) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", BasicType::TEXT))
                          ->addField(createField("value3", BasicType::FLOAT64));
    const auto entrySize = 2 * sizeof(uint64_t) + sizeof(double_t) + sizeof(size_t);
    const auto pageSize = NES::Runtime::detail::EntryPage::ENTRY_PAGE_SIZE;
    const auto numItems = 507_u64;
    auto allRecords = createRecords(testSchema, numItems);

    NES::Runtime::PagedVectorRowLayout pagedVector(bufferManager, entrySize, pageSize);
    runStoreTest(pagedVector, testSchema, entrySize, pageSize, allRecords);
    runRetrieveTest(pagedVector, testSchema, allRecords);
}

TEST_F(PagedVectorVarSizedTest, appendAllPagesTwoVectors) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", BasicType::TEXT));
    const auto entrySize = 2 * sizeof(uint64_t) + sizeof(size_t);
    const auto pageSize = NES::Runtime::detail::EntryPage::ENTRY_PAGE_SIZE;
    const auto numItems = 507_u64;
    const auto numVectors = 2_u64;
    const auto totalNumTextFields = 1 * numItems * numVectors;

    std::vector<std::vector<Record>> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0_u64; i < numVectors; ++i) {
        auto records = createRecords(testSchema, numItems);
        allRecords.emplace_back(records);
    }

    std::vector<Record> allRecordsAfterAppendAll;
    for (auto i = 0_u64; i < numVectors; ++i) {
        allRecordsAfterAppendAll.insert(allRecordsAfterAppendAll.end(), allRecords[i].begin(), allRecords[i].end());
    }

    insertAndAppendAllPagesTest(testSchema, entrySize, pageSize, totalNumTextFields, allRecords, allRecordsAfterAppendAll, 0);
}

TEST_F(PagedVectorVarSizedTest, appendAllPagesMultipleVectors) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", BasicType::TEXT))
                          ->addField(createField("value3", BasicType::FLOAT64));
    const auto entrySize = 2 * sizeof(uint64_t) + sizeof(double_t) + sizeof(size_t);
    const auto pageSize = NES::Runtime::detail::EntryPage::ENTRY_PAGE_SIZE;
    const auto numItems = 507_u64;
    const auto numVectors = 4_u64;
    const auto totalNumTextFields = 1 * numItems * numVectors;

    std::vector<std::vector<Record>> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0_u64; i < numVectors; ++i) {
        auto records = createRecords(testSchema, numItems);
        allRecords.emplace_back(records);
    }

    std::vector<Record> allRecordsAfterAppendAll;
    for (auto i = 0_u64; i < numVectors; ++i) {
        allRecordsAfterAppendAll.insert(allRecordsAfterAppendAll.end(), allRecords[i].begin(), allRecords[i].end());
    }

    insertAndAppendAllPagesTest(testSchema, entrySize, pageSize, totalNumTextFields, allRecords, allRecordsAfterAppendAll, 0);
}

TEST_F(PagedVectorVarSizedTest, appendAllPagesMultipleVectorsWithDifferentPageSizes) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", BasicType::TEXT))
                          ->addField(createField("value3", BasicType::FLOAT64));
    const auto entrySize = 2 * sizeof(uint64_t) + sizeof(double_t);
    const auto pageSize = PagedVectorVarSized::PAGE_SIZE;
    const auto numItems = 507_u64;
    const auto numVectors = 4_u64;
    const auto totalNumTextFields = 1 * numItems * numVectors;

    std::vector<std::vector<Record>> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0_u64; i < numVectors; ++i) {
        auto records = createRecords(testSchema, numItems, 0);
        allRecords.emplace_back(records);
    }

    std::vector<Record> allRecordsAfterAppendAll;
    for (auto i = 0_u64; i < numVectors; ++i) {
        allRecordsAfterAppendAll.insert(allRecordsAfterAppendAll.end(), allRecords[i].begin(), allRecords[i].end());
    }

    insertAndAppendAllPagesTest(testSchema, entrySize, pageSize, totalNumTextFields, allRecords, allRecordsAfterAppendAll, 1);
}

}// namespace NES::Nautilus::Interface

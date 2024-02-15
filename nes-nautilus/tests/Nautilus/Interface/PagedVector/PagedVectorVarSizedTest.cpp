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

#include <BaseIntegrationTest.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSized.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSizedRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/BufferManager.hpp>
#include <API/AttributeField.hpp>

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
        bufferManager =  std::make_shared<Runtime::BufferManager>();
        NES_INFO("Setup PagedVectorVarSizedTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("Tear down PagedVectorVarSizedTest test case.");
        Testing::BaseUnitTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down PagedVectorVarSizedTest test class."); }

    void runStoreTest(PagedVectorVarSized& pagedVector,
                      const SchemaPtr& schema,
                      const uint64_t entrySize,
                      const uint64_t pageSize,
                      const std::vector<Record>& allRecords) {
        ASSERT_EQ(entrySize, sizeof(Record));
        const uint64_t capacityPerPage = pageSize / entrySize;
        const uint64_t expectedNumberOfEntries = allRecords.size();
        const uint64_t numberOfPages = std::ceil((double) expectedNumberOfEntries / capacityPerPage);
        auto pagedVectorRef = PagedVectorVarSizedRef(Value<MemRef>((int8_t*) &pagedVector), schema);

        for (auto& record : allRecords) {
            pagedVectorRef.writeRecord(record);
        }

        ASSERT_EQ(pagedVectorRef.getTotalNumberOfEntries(), expectedNumberOfEntries);
        ASSERT_EQ(pagedVector.getNumberOfPages(), numberOfPages);

        // TODO assert that the number of tuples on the last page is correct?
    }

    void runRetrieveTest(const PagedVectorVarSized& pagedVector,
                         const SchemaPtr& schema,
                         const std::vector<Record>& allRecords) {
        auto pagedVectorRef = PagedVectorVarSizedRef(Value<MemRef>((int8_t*) &pagedVector), schema);
        ASSERT_EQ(pagedVectorRef.getTotalNumberOfEntries(), allRecords.size());

        auto itemPos = 0_u64;
        for (auto record : pagedVectorRef) {
            ASSERT_EQ(record, allRecords[itemPos++]);
        }
        ASSERT_EQ(itemPos, allRecords.size());
    }
};

TEST_F(PagedVectorVarSizedTest, storeAndRetrieveFixedSizeValues) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", BasicType::UINT64));
    const auto entrySize = 2 * sizeof(uint64_t);
    const auto pageSize = PagedVectorVarSized::PAGE_SIZE;
    const auto numItems = 1234_u64;

    std::vector<Record> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0_u64; i < numItems; ++i) {
        Record newRecord;
        auto fieldCnt = 0_u64;
        for (auto& field : allFields) {
            newRecord.write(field, Value<UInt64>(testSchema->getSize() * i + fieldCnt++));
        }
        allRecords.emplace_back(newRecord);
    }

    PagedVectorVarSized pagedVector(bufferManager, testSchema, pageSize);
    runStoreTest(pagedVector, testSchema, entrySize, pageSize, allRecords);
    runRetrieveTest(pagedVector, testSchema, allRecords);
}

TEST_F(PagedVectorVarSizedTest, storeAndRetrieveVarSizeValues) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::TEXT))
                          ->addField(createField("value2", BasicType::TEXT));
    const auto entrySize = 2 * sizeof(int8_t*) + 2 * sizeof(uint32_t);
    const auto pageSize = PagedVectorVarSized::PAGE_SIZE;
    const auto numItems = 1234_u64;

    std::vector<Record> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0_u64; i < numItems; ++i) {
        Record newRecord;
        auto fieldCnt = 0_u64;
        for (auto& field : allFields) {
            std::stringstream ss;
            ss << "TextValue" << (testSchema->getSize() * i + fieldCnt++);
            newRecord.write(field, Text(TextValue::create(ss.str())));
        }
        allRecords.emplace_back(newRecord);
    }

    PagedVectorVarSized pagedVector(bufferManager, testSchema, pageSize);
    runStoreTest(pagedVector, testSchema, entrySize, pageSize, allRecords);
    runRetrieveTest(pagedVector, testSchema, allRecords);
}

TEST_F(PagedVectorVarSizedTest, storeAndRetrieveMixedValueTypes) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", BasicType::TEXT))
                          ->addField(createField("value3", BasicType::FLOAT32));
    const auto entrySize = sizeof(uint64_t) + sizeof(int8_t*) + sizeof(uint32_t) + sizeof(float_t);
    const auto pageSize = PagedVectorVarSized::PAGE_SIZE;
    const auto numItems = 1234_u64;

    std::vector<Record> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0_u64; i < numItems; ++i) {
        Record newRecord;
        auto fieldCnt = 0_u64;
        for (auto& field : allFields) {
            auto const fieldType = testSchema->getField(field)->getDataType();
            auto tupleNo = testSchema->getSize() * i + fieldCnt++;
            if (fieldType->isText()) {
                std::stringstream ss;
                ss << "TextValue" << tupleNo;
                newRecord.write(field, Text(TextValue::create(ss.str())));
            } else if (fieldType->isFloat()) {
                newRecord.write(field, Value<Float>((float) tupleNo / numItems));
            } else {
                newRecord.write(field, Value<UInt64>(tupleNo));
            }
        }
        allRecords.emplace_back(newRecord);
    }

    PagedVectorVarSized pagedVector(bufferManager, testSchema, pageSize);
    runStoreTest(pagedVector, testSchema, entrySize, pageSize, allRecords);
    runRetrieveTest(pagedVector, testSchema, allRecords);
}

TEST_F(PagedVectorVarSizedTest, storeAndRetrieveFixedValuesNonDefaultPageSize) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", BasicType::UINT64));
    const auto entrySize = 2 * sizeof(uint64_t);
    const auto pageSize = (10 * entrySize) + 2;
    const auto numItems = 1234_u64;

    std::vector<Record> allRecords;
    auto allFields = testSchema->getFieldNames();
    for (auto i = 0_u64; i < numItems; ++i) {
        Record newRecord;
        auto fieldCnt = 0_u64;
        for (auto& field : allFields) {
            newRecord.write(field, Value<UInt64>(testSchema->getSize() * i + fieldCnt++));
        }
        allRecords.emplace_back(newRecord);
    }

    PagedVectorVarSized pagedVector(bufferManager, testSchema, pageSize);
    runStoreTest(pagedVector, testSchema, entrySize, pageSize, allRecords);
    runRetrieveTest(pagedVector, testSchema, allRecords);
}

} // namespace NES::Nautilus::Interface

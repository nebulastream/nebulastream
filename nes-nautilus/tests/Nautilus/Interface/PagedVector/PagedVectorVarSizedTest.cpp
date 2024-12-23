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
#include <Nautilus/Interface/PagedVector/PagedVectorVarSized.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSizedRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/BufferManager.hpp>
#include <random>

namespace NES::Nautilus::Interface {
class PagedVectorVarSizedTest : public Testing::BaseUnitTest {
  public:
    std::shared_ptr<Runtime::BufferManager> bufferManager;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        Logger::setupLogging("PagedVectorVarSizedTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup PagedVectorVarSizedTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        BaseUnitTest::SetUp();
        bufferManager = std::make_shared<Runtime::BufferManager>();
        NES_INFO("Setup PagedVectorVarSizedTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("Tear down PagedVectorVarSizedTest test case.");
        BaseUnitTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down PagedVectorVarSizedTest test class."); }

    std::vector<Record> createRecords(const SchemaPtr& schema, const uint64_t numRecords, const uint64_t minTextLength) {
        std::vector<Record> allRecords;
        auto allFields = schema->getFieldNames();
        for (auto i = 0_u64; i < numRecords; ++i) {

            Record newRecord;
            auto fieldCnt = 0_u64;
            for (auto& field : allFields) {
                auto const fieldType = schema->get(field)->getDataType();
                auto tupleNo = schema->getSize() * i + fieldCnt++;

                if (fieldType->isText()) {
                    auto buffer = bufferManager->getUnpooledBuffer(PagedVectorVarSized::PAGE_SIZE);
                    if (buffer.has_value()) {
                        std::stringstream ss;
                        ss << "testing TextValue" << tupleNo;
                        auto textValue = TextValue::create(buffer.value(), ss.str().length());
                        std::memcpy(textValue->str(), ss.str().c_str(), ss.str().length());
                        newRecord.write(field, Text(textValue));

                        NES_ASSERT2_FMT(ss.str().length() > minTextLength, "Length of the generated text is not long enough!");
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

    std::vector<Record> createRecordsRandomTs(const SchemaPtr& schema,
                                              const uint64_t numRecords,
                                              const uint64_t minTextLength,
                                              const bool differentStringLengths) {
        std::vector<Record> allRecords;
        auto allFields = schema->getFieldNames();
        for (auto i = 0_u64; i < numRecords; ++i) {

            Record newRecord;
            auto fieldCnt = 0_u64;
            for (auto& field : allFields) {
                auto const fieldType = schema->get(field)->getDataType();
                auto tupleNo = schema->getSize() * i + fieldCnt++;

                if (fieldType->isText()) {
                    auto buffer = bufferManager->getUnpooledBuffer(PagedVectorVarSized::PAGE_SIZE);
                    if (buffer.has_value()) {
                        std::stringstream ss;
                        if (!differentStringLengths) {
                            ss << "testing TextValue" << tupleNo;
                        } else {
                            if (i % 3 == 0) {
                                ss << "" << tupleNo;
                            } else if (i % 3 == 1) {
                                ss << "short" << tupleNo;
                            } else if (i % 3 == 2) {
                                ss << "medium length string" << tupleNo;
                            } else {
                                ss << "this is a long string, whose purpose is to bring some variety. The content of this string "
                                      "is not so important as its length, because the varSizedDataPages might not be able to "
                                      "store it on a simple page. This would at least be the case if the pageSize is small"
                                   << tupleNo;

                                NES_ASSERT2_FMT(ss.str().length() > minTextLength,
                                                "Length of the generated text is not long enough!");
                            }
                        }
                        auto textValue = TextValue::create(buffer.value(), ss.str().length());
                        std::memcpy(textValue->str(), ss.str().c_str(), ss.str().length());
                        newRecord.write(field, Text(textValue));
                    } else {
                        NES_THROW_RUNTIME_ERROR("No unpooled TupleBuffer available!");
                    }
                } else if (fieldType->isInteger()) {
                    if (schema->get(field)->getName() == "ts") {
                        std::random_device rd;// Obtain a random number from hardware
                        auto seed = rd();
                        std::mt19937 gen(rd());// Seed the generator

                        // Define the distribution range [0, x]
                        std::uniform_int_distribution<> distrib(0, numRecords);

                        // Generate and return the random number
                        auto val = distrib(gen);
                        newRecord.write(field, Value<UInt64>((uint64_t) val));
                    } else {
                        newRecord.write(field, Value<UInt64>(tupleNo));
                    }
                } else {
                    newRecord.write(field, Value<Double>((double_t) tupleNo / numRecords));
                }
            }
            allRecords.emplace_back(newRecord);
        }
        return allRecords;
    }

    /**
     * removes records from allRecords (the result reference) as well as from the paged vectors. Therefore, the store test should be
     * run before and the retrieve test after. It removes all records, whose timestamp is >= the specified timestamp cutoff.
     * @param allRecords a vector of all records not saved on the page
     * @param pagedVectorDeleteStrings the pagedVectorDeleteStrings that has previously stored the same records that are in allRecords.
     * For this pagedVector if a record gets deleted the corresponding string gets deleted as well.
     * @param pagedVectorKeepStrings same records as other pagedVector, but while deleting records it keeps strings of deleted records
     * @param schema schema of each record
     * @param timestampCutoff the timestamp that all remaining records must be smaller than
     */
    std::tuple<std::vector<Record>, PagedVectorVarSized, PagedVectorVarSized>
    removeRecordsAccordingToTimestamp(std::vector<Record> allRecords,
                                      PagedVectorVarSized pagedVectorDeleteStrings,
                                      PagedVectorVarSized pagedVectorKeepStrings,
                                      SchemaPtr schema,
                                      uint64_t timestampCutoff) {
        // Get the position of records to remove from the reference vector. This means the paged vector needs to have the positions
        // stored in the same order for this to work
        std::vector<Value<UInt64>> recordsToRemovePositions;
        for (auto i = 0_u64; i < allRecords.size(); i++) {
            auto record = allRecords[i];
            if (record.read("ts").as<UInt64>()->getValue() >= timestampCutoff) {
                recordsToRemovePositions.push_back(Value<UInt64>(i));
            }
        }
        std::reverse(recordsToRemovePositions.begin(), recordsToRemovePositions.end());// Refer to removeRecords...()

        NES_INFO("will remove {} positions", recordsToRemovePositions.size())

        // Remove records from the paged vectors with the corresponding method
        auto pagedVectorVarSizedRefDeleteStrings =
            PagedVectorVarSizedRef(Value<MemRef>(reinterpret_cast<int8_t*>(&pagedVectorDeleteStrings)), schema);
        pagedVectorVarSizedRefDeleteStrings.removeRecordsAndAdjustPositions(recordsToRemovePositions, false);

        auto pagedVectorVarSizedRefKeepStrings =
            PagedVectorVarSizedRef(Value<MemRef>(reinterpret_cast<int8_t*>(&pagedVectorKeepStrings)), schema);
        pagedVectorVarSizedRefKeepStrings.removeRecordsAndAdjustPositions(recordsToRemovePositions, true);

        // Remove items from the reference vector
        auto iteratorItemsToRemove = std::remove_if(allRecords.begin(), allRecords.end(), [timestampCutoff](Record rec) {
            return rec.read("ts").as<UInt64>()->getValue() >= timestampCutoff;
        });
        allRecords.erase(iteratorItemsToRemove, allRecords.end());

        return std::make_tuple(allRecords, pagedVectorDeleteStrings, pagedVectorKeepStrings);
    }

    void runStoreTest(PagedVectorVarSized& pagedVector,
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

        ASSERT_EQ(pagedVector.getNumberOfEntries(), expectedNumberOfEntries);
        ASSERT_EQ(pagedVector.getNumberOfPages(), numberOfPages);

        // As we do lazy allocation, we do not create a new page if the last tuple fit on the page
        bool lastTupleFitsOntoLastPage = (expectedNumberOfEntries % capacityPerPage) == 0;
        const uint64_t numTuplesLastPage =
            lastTupleFitsOntoLastPage ? capacityPerPage : (expectedNumberOfEntries % capacityPerPage);
        ASSERT_EQ(pagedVector.getNumberOfEntriesOnCurrentPage(), numTuplesLastPage);
    }

    void
    runRetrieveUnorderedTest(PagedVectorVarSized& pagedVector, const SchemaPtr& schema, const std::vector<Record>& allRecords) {
        auto pagedVectorVarSizedRef = PagedVectorVarSizedRef(Value<MemRef>(reinterpret_cast<int8_t*>(&pagedVector)), schema);
        ASSERT_EQ(pagedVector.getNumberOfEntries(), allRecords.size());

        std::vector<Record> allRecordsCopy = allRecords;

        auto itemPos = 0_u64;
        for (auto record : pagedVectorVarSizedRef) {
            //auto tmpPos = itemPos;
            //auto actualRecord = allRecords[tmpPos];
            NES_INFO("record {} has to be found in {} records that are left", record.toStringShort(), allRecordsCopy.size())
            auto removeRecordPos = 999999999_u64;
            for (auto i = 0_u64; i < allRecordsCopy.size(); i++) {
                //NES_INFO("allRecordsCopy[i] {}", allRecordsCopy[i].toStringShort())
                if (record == allRecordsCopy[i]) {
                    removeRecordPos = i;
                    break;
                }
            }
            if (removeRecordPos != 999999999_u64) {
                allRecordsCopy.erase(allRecordsCopy.begin() + removeRecordPos);
            } else {
                //ASSERT_EQ(0,1);
            }
        }

        ASSERT_EQ(0, allRecordsCopy.size());
    }

    void runRetrieveTest(PagedVectorVarSized& pagedVector, const SchemaPtr& schema, const std::vector<Record>& allRecords) {
        auto pagedVectorVarSizedRef = PagedVectorVarSizedRef(Value<MemRef>(reinterpret_cast<int8_t*>(&pagedVector)), schema);
        ASSERT_EQ(pagedVector.getNumberOfEntries(), allRecords.size());

        auto itemPos = 0_u64;
        for (auto record : pagedVectorVarSizedRef) {
            auto tmpPos = itemPos;
            auto actualRecord = allRecords[tmpPos];
            ASSERT_EQ(record, allRecords[itemPos++]);
        }
        ASSERT_EQ(itemPos, allRecords.size());
    }

    void insertAndAppendAllPagesTest(const SchemaPtr& schema,
                                     const uint64_t entrySize,
                                     uint64_t pageSize,
                                     const uint64_t totalNumTextFields,
                                     const std::vector<std::vector<Record>>& allRecordsAndVectors,
                                     const std::vector<Record>& expectedRecordsAfterAppendAll,
                                     uint64_t differentPageSizes) {
        // Inserting data into each PagedVector and checking for correct values
        std::vector<std::unique_ptr<PagedVectorVarSized>> allPagedVectors;
        for (auto& allRecords : allRecordsAndVectors) {
            if (differentPageSizes != 0) {
                differentPageSizes++;
            }
            pageSize += differentPageSizes * entrySize;
            allPagedVectors.emplace_back(std::make_unique<PagedVectorVarSized>(bufferManager, schema, pageSize));
            runStoreTest(*allPagedVectors.back(), schema, entrySize, pageSize, allRecords);
            runRetrieveTest(*allPagedVectors.back(), schema, allRecords);
        }

        // Appending and deleting all PagedVectors except for the first one
        auto& firstPagedVec = allPagedVectors[0];
        if (allRecordsAndVectors.size() > 1) {
            for (uint64_t i = 1; i < allPagedVectors.size(); ++i) {
                auto& otherPagedVec = allPagedVectors[i];
                firstPagedVec->appendAllPages(*otherPagedVec);
                EXPECT_EQ(otherPagedVec->getNumberOfPages(), 0);
                EXPECT_EQ(otherPagedVec->getNumberOfVarSizedPages(), 0);
                EXPECT_EQ(otherPagedVec->getNumberOfEntries(), 0);
                EXPECT_EQ(otherPagedVec->getNumberOfEntriesOnCurrentPage(), 0);
                EXPECT_EQ(otherPagedVec->getVarSizedDataEntryMapCounter(), 0);
                EXPECT_TRUE(otherPagedVec->varSizedDataEntryMapEmpty());
            }

            allPagedVectors.erase(allPagedVectors.begin() + 1, allPagedVectors.end());
        }

        // Checking for number of pagedVectors and correct values
        EXPECT_EQ(allPagedVectors.size(), 1);
        EXPECT_EQ(firstPagedVec->getVarSizedDataEntryMapCounter(), totalNumTextFields);
        runRetrieveTest(*firstPagedVec, schema, expectedRecordsAfterAppendAll);
    }
};

TEST_F(PagedVectorVarSizedTest, storeAndRetrieveFixedSizeValues) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", BasicType::UINT64))
                          ->addField(createField("value3", BasicType::UINT64));
    const auto entrySize = 3 * sizeof(uint64_t);
    const auto pageSize = PagedVectorVarSized::PAGE_SIZE;
    const auto numItems = 507_u64;
    auto allRecords = createRecords(testSchema, numItems, 0);

    PagedVectorVarSized pagedVector(bufferManager, testSchema, pageSize);
    runStoreTest(pagedVector, testSchema, entrySize, pageSize, allRecords);
    runRetrieveTest(pagedVector, testSchema, allRecords);
}

TEST_F(PagedVectorVarSizedTest, storeAndRetrieveVarSizeValues) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", DataTypeFactory::createText()))
                          ->addField(createField("value2", DataTypeFactory::createText()))
                          ->addField(createField("value3", DataTypeFactory::createText()));
    const auto entrySize = 3 * sizeof(uint64_t);
    const auto pageSize = PagedVectorVarSized::PAGE_SIZE;
    const auto numItems = 507_u64;
    auto allRecords = createRecords(testSchema, numItems, 0);

    PagedVectorVarSized pagedVector(bufferManager, testSchema, pageSize);
    runStoreTest(pagedVector, testSchema, entrySize, pageSize, allRecords);
    runRetrieveTest(pagedVector, testSchema, allRecords);
}

TEST_F(PagedVectorVarSizedTest, storeAndRetrieveLargeVarSizedValues) {
    auto testSchema =
        Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField(createField("value1", DataTypeFactory::createText()));
    const auto entrySize = 1 * sizeof(uint64_t);
    const auto pageSize = 8_u64;
    const auto numItems = 507_u64;
    auto allRecords = createRecords(testSchema, numItems, 2 * pageSize);

    PagedVectorVarSized pagedVector(bufferManager, testSchema, pageSize);
    runStoreTest(pagedVector, testSchema, entrySize, pageSize, allRecords);
    runRetrieveTest(pagedVector, testSchema, allRecords);
}

TEST_F(PagedVectorVarSizedTest, storeAndRetrieveMixedValueTypes) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", DataTypeFactory::createText()))
                          ->addField(createField("value3", BasicType::FLOAT64));
    const auto entrySize = 2 * sizeof(uint64_t) + sizeof(double_t);
    const auto pageSize = PagedVectorVarSized::PAGE_SIZE;
    const auto numItems = 507_u64;
    auto allRecords = createRecords(testSchema, numItems, 0);

    PagedVectorVarSized pagedVector(bufferManager, testSchema, pageSize);
    runStoreTest(pagedVector, testSchema, entrySize, pageSize, allRecords);
    runRetrieveTest(pagedVector, testSchema, allRecords);
}

TEST_F(PagedVectorVarSizedTest, storeAndRetrieveMixedValueTypesDeleteWrongTimestamps) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("id", BasicType::UINT64))
                          ->addField(createField("valueText", DataTypeFactory::createText()))//BasicType::UINT64))
                          ->addField(createField("ts", BasicType::UINT64));
    const auto entrySize = 3 * sizeof(uint64_t);
    const auto pageSize = PagedVectorVarSized::PAGE_SIZE;
    const auto numItems = 507_u64;
    auto allRecords = createRecordsRandomTs(testSchema, numItems, 0, false);

    PagedVectorVarSized pagedVectorDeleteStrings(bufferManager, testSchema, pageSize);
    PagedVectorVarSized pagedVectorKeepStrings(bufferManager, testSchema, pageSize);

    runStoreTest(pagedVectorDeleteStrings, testSchema, entrySize, pageSize, allRecords);
    runStoreTest(pagedVectorKeepStrings, testSchema, entrySize, pageSize, allRecords);

    auto expectedDeleteKeep =
        removeRecordsAccordingToTimestamp(allRecords, pagedVectorDeleteStrings, pagedVectorKeepStrings, testSchema, 400_u64);
    allRecords = std::get<0>(expectedDeleteKeep);
    pagedVectorDeleteStrings = std::get<1>(expectedDeleteKeep);
    pagedVectorKeepStrings = std::get<2>(expectedDeleteKeep);

    runRetrieveTest(pagedVectorDeleteStrings, testSchema, allRecords);
    runRetrieveTest(pagedVectorKeepStrings, testSchema, allRecords);
}

TEST_F(PagedVectorVarSizedTest, storeAndRetrieveMixedValueTypesDeleteWrongTimestampsAllTuples) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("id", BasicType::UINT64))
                          ->addField(createField("valueText", DataTypeFactory::createText()))//BasicType::UINT64))
                          ->addField(createField("ts", BasicType::UINT64));
    const auto entrySize = 3 * sizeof(uint64_t);
    const auto pageSize = PagedVectorVarSized::PAGE_SIZE;
    const auto numItems = 507_u64;
    auto allRecords = createRecordsRandomTs(testSchema, numItems, 0, false);

    PagedVectorVarSized pagedVectorDeleteStrings(bufferManager, testSchema, pageSize);
    PagedVectorVarSized pagedVectorKeepStrings(bufferManager, testSchema, pageSize);

    runStoreTest(pagedVectorDeleteStrings, testSchema, entrySize, pageSize, allRecords);
    runStoreTest(pagedVectorKeepStrings, testSchema, entrySize, pageSize, allRecords);

    auto expectedDeleteKeep =
        removeRecordsAccordingToTimestamp(allRecords, pagedVectorDeleteStrings, pagedVectorKeepStrings, testSchema, 0_u64);
    allRecords = std::get<0>(expectedDeleteKeep);
    pagedVectorDeleteStrings = std::get<1>(expectedDeleteKeep);
    pagedVectorKeepStrings = std::get<2>(expectedDeleteKeep);

    runRetrieveTest(pagedVectorDeleteStrings, testSchema, allRecords);
    runRetrieveTest(pagedVectorKeepStrings, testSchema, allRecords);
}

TEST_F(PagedVectorVarSizedTest, storeAndRetrieveMixedValueTypesDeleteWrongTimestampsMultipleTextFields) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("id", BasicType::UINT64))
                          ->addField(createField("valueText", DataTypeFactory::createText()))
                          ->addField(createField("valueText2", DataTypeFactory::createText()))
                          ->addField(createField("valueText3", DataTypeFactory::createText()))
                          ->addField(createField("ts", BasicType::UINT64));
    const auto entrySize = 5 * sizeof(uint64_t);
    const auto pageSize = PagedVectorVarSized::PAGE_SIZE;
    const auto numItems = 507_u64;
    auto allRecords = createRecordsRandomTs(testSchema, numItems, 0, false);

    PagedVectorVarSized pagedVectorDeleteStrings(bufferManager, testSchema, pageSize);
    PagedVectorVarSized pagedVectorKeepStrings(bufferManager, testSchema, pageSize);

    runStoreTest(pagedVectorDeleteStrings, testSchema, entrySize, pageSize, allRecords);
    runStoreTest(pagedVectorKeepStrings, testSchema, entrySize, pageSize, allRecords);

    auto expectedDeleteKeep =
        removeRecordsAccordingToTimestamp(allRecords, pagedVectorDeleteStrings, pagedVectorKeepStrings, testSchema, 400_u64);
    allRecords = std::get<0>(expectedDeleteKeep);
    pagedVectorDeleteStrings = std::get<1>(expectedDeleteKeep);
    pagedVectorKeepStrings = std::get<2>(expectedDeleteKeep);

    runRetrieveTest(pagedVectorDeleteStrings, testSchema, allRecords);
    runRetrieveTest(pagedVectorKeepStrings, testSchema, allRecords);
}

TEST_F(PagedVectorVarSizedTest, storeAndRetrieveMixedValueTypesDeleteWrongTimestampsSmallPage) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("id", BasicType::UINT64))
                          ->addField(createField("valueText", DataTypeFactory::createText()))
                          ->addField(createField("ts", BasicType::UINT64));
    const auto entrySize = 3 * sizeof(uint64_t);
    const auto pageSize = 3 * entrySize;
    const auto numItems = 507_u64;
    auto allRecords = createRecordsRandomTs(testSchema, numItems, 0, false);

    PagedVectorVarSized pagedVectorDeleteStrings(bufferManager, testSchema, pageSize);
    PagedVectorVarSized pagedVectorKeepStrings(bufferManager, testSchema, pageSize);

    runStoreTest(pagedVectorDeleteStrings, testSchema, entrySize, pageSize, allRecords);
    runStoreTest(pagedVectorKeepStrings, testSchema, entrySize, pageSize, allRecords);

    auto expectedDeleteKeep =
        removeRecordsAccordingToTimestamp(allRecords, pagedVectorDeleteStrings, pagedVectorKeepStrings, testSchema, 250_u64);
    allRecords = std::get<0>(expectedDeleteKeep);
    pagedVectorDeleteStrings = std::get<1>(expectedDeleteKeep);
    pagedVectorKeepStrings = std::get<2>(expectedDeleteKeep);

    runRetrieveTest(pagedVectorDeleteStrings, testSchema, allRecords);
    runRetrieveTest(pagedVectorKeepStrings, testSchema, allRecords);
}

TEST_F(PagedVectorVarSizedTest, storeAndRetrieveMixedValueTypesDeleteWrongTimestampsSmallPageDifferentTextLengths) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("id", BasicType::UINT64))
                          ->addField(createField("valueText", DataTypeFactory::createText()))
                          ->addField(createField("ts", BasicType::UINT64));
    const auto entrySize = 3 * sizeof(uint64_t);
    const auto pageSize = 3 * entrySize;
    const auto numItems = 507_u64;
    auto allRecords = createRecordsRandomTs(testSchema, numItems, pageSize, true);

    PagedVectorVarSized pagedVectorDeleteStrings(bufferManager, testSchema, pageSize);
    PagedVectorVarSized pagedVectorKeepStrings(bufferManager, testSchema, pageSize);

    runStoreTest(pagedVectorDeleteStrings, testSchema, entrySize, pageSize, allRecords);
    runStoreTest(pagedVectorKeepStrings, testSchema, entrySize, pageSize, allRecords);

    auto expectedDeleteKeep =
        removeRecordsAccordingToTimestamp(allRecords, pagedVectorDeleteStrings, pagedVectorKeepStrings, testSchema, 250_u64);
    allRecords = std::get<0>(expectedDeleteKeep);
    pagedVectorDeleteStrings = std::get<1>(expectedDeleteKeep);
    pagedVectorKeepStrings = std::get<2>(expectedDeleteKeep);

    runRetrieveTest(pagedVectorDeleteStrings, testSchema, allRecords);
    runRetrieveTest(pagedVectorKeepStrings, testSchema, allRecords);
}

TEST_F(PagedVectorVarSizedTest, storeAndRetrieveMixedValueTypesDeleteAndAddAndDeleteRecords) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("id", BasicType::UINT64))
                          ->addField(createField("valueText", DataTypeFactory::createText()))
                          ->addField(createField("ts", BasicType::UINT64));
    const auto entrySize = 3 * sizeof(uint64_t);
    const auto pageSize = 3 * entrySize;
    const auto numItems = 507_u64;
    auto allRecords = createRecordsRandomTs(testSchema, numItems, pageSize, true);

    PagedVectorVarSized pagedVectorDeleteStrings(bufferManager, testSchema, pageSize);
    PagedVectorVarSized pagedVectorKeepStrings(bufferManager, testSchema, pageSize);

    runStoreTest(pagedVectorDeleteStrings, testSchema, entrySize, pageSize, allRecords);
    runStoreTest(pagedVectorKeepStrings, testSchema, entrySize, pageSize, allRecords);

    auto expectedDeleteKeep =
        removeRecordsAccordingToTimestamp(allRecords, pagedVectorDeleteStrings, pagedVectorKeepStrings, testSchema, 250_u64);
    allRecords = std::get<0>(expectedDeleteKeep);
    pagedVectorDeleteStrings = std::get<1>(expectedDeleteKeep);
    pagedVectorKeepStrings = std::get<2>(expectedDeleteKeep);

    runRetrieveTest(pagedVectorDeleteStrings, testSchema, allRecords);
    runRetrieveTest(pagedVectorKeepStrings, testSchema, allRecords);

    auto newRecords = createRecordsRandomTs(testSchema, numItems, pageSize, true);
    auto pagedVectorVarSizedRefDeleteStrings =
        PagedVectorVarSizedRef(Value<MemRef>(reinterpret_cast<int8_t*>(&pagedVectorDeleteStrings)), testSchema);
    auto pagedVectorVarSizedRefKeepStrings =
        PagedVectorVarSizedRef(Value<MemRef>(reinterpret_cast<int8_t*>(&pagedVectorKeepStrings)), testSchema);

    //write other new records to each pagedVector and add them to the reference vektor. Test retrieve after
    for (auto& record : newRecords) {
        pagedVectorVarSizedRefDeleteStrings.writeRecord(record);
        pagedVectorVarSizedRefKeepStrings.writeRecord(record);
    }
    allRecords.insert(allRecords.end(), newRecords.begin(), newRecords.end());

    runRetrieveTest(pagedVectorDeleteStrings, testSchema, allRecords);
    runRetrieveTest(pagedVectorKeepStrings, testSchema, allRecords);

    // Delete records again (new ts) and test retrieve
    expectedDeleteKeep =
        removeRecordsAccordingToTimestamp(allRecords, pagedVectorDeleteStrings, pagedVectorKeepStrings, testSchema, 310_u64);
    allRecords = std::get<0>(expectedDeleteKeep);
    pagedVectorDeleteStrings = std::get<1>(expectedDeleteKeep);
    pagedVectorKeepStrings = std::get<2>(expectedDeleteKeep);

    runRetrieveTest(pagedVectorDeleteStrings, testSchema, allRecords);
    runRetrieveTest(pagedVectorKeepStrings, testSchema, allRecords);
}

TEST_F(PagedVectorVarSizedTest, storeAndRetrieveFixedValuesNonDefaultPageSize) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", BasicType::UINT64));
    const auto entrySize = 2 * sizeof(uint64_t);
    const auto pageSize = (10 * entrySize) + 3;
    const auto numItems = 507_u64;
    auto allRecords = createRecords(testSchema, numItems, 0);

    PagedVectorVarSized pagedVector(bufferManager, testSchema, pageSize);
    runStoreTest(pagedVector, testSchema, entrySize, pageSize, allRecords);
    runRetrieveTest(pagedVector, testSchema, allRecords);
}

TEST_F(PagedVectorVarSizedTest, appendAllPagesTwoVectors) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", DataTypeFactory::createText()));
    const auto entrySize = 2 * sizeof(uint64_t);
    const auto pageSize = PagedVectorVarSized::PAGE_SIZE;
    const auto numItems = 507_u64;
    const auto numVectors = 2_u64;
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

    insertAndAppendAllPagesTest(testSchema, entrySize, pageSize, totalNumTextFields, allRecords, allRecordsAfterAppendAll, 0);
}

TEST_F(PagedVectorVarSizedTest, appendAllPagesMultipleVectors) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", DataTypeFactory::createText()))
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

    insertAndAppendAllPagesTest(testSchema, entrySize, pageSize, totalNumTextFields, allRecords, allRecordsAfterAppendAll, 0);
}

TEST_F(PagedVectorVarSizedTest, appendAllPagesMultipleVectorsWithDifferentPageSizes) {
    auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(createField("value1", BasicType::UINT64))
                          ->addField(createField("value2", DataTypeFactory::createText()))
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

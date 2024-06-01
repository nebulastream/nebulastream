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
#include <API/AttributeField.hpp>
#include <API/TestSchemas.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/RecordBuffer.hpp>
#include <random>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSizedRef.hpp>
#include <iostream>

namespace NES::Runtime::Execution {

auto constexpr DEFAULT_LEFT_PAGE_SIZE = 1024;
auto constexpr DEFAULT_RIGHT_PAGE_SIZE = 256;

class NLJSliceSerializationTest: public Testing::BaseUnitTest {
  public:
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<NLJSlice> expectedNLJSlice;
    SchemaPtr leftSchema;
    SchemaPtr rightSchema;
    const uint64_t leftPageSize = DEFAULT_LEFT_PAGE_SIZE;
    const uint64_t rightPageSize = DEFAULT_RIGHT_PAGE_SIZE;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NLJSliceSerializationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NLJSliceSerializationTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        BaseUnitTest::SetUp();
        NES_INFO("Setup NLJSliceSerializationTest test case.");

        leftSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("test1");
        rightSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("test2");
        bm = std::make_shared<BufferManager>(8196, 5000);
        expectedNLJSlice = std::make_shared<NLJSlice>(0, 2000, 0, bm, leftSchema, leftPageSize, rightSchema, rightPageSize);
    }

    void generateAndInsertRecordsToSlice(uint64_t numberOfRecordsLeft, uint64_t numberOfRecordsRight) {
        auto allLeftRecords = createRandomRecords(numberOfRecordsLeft, QueryCompilation::JoinBuildSideType::Left);
        auto allRightRecords = createRandomRecords(numberOfRecordsRight, QueryCompilation::JoinBuildSideType::Right);

        for (auto& leftRecord : allLeftRecords) {
            auto leftPagedVectorRef =
                Nautilus::Value<Nautilus::MemRef>((int8_t*) expectedNLJSlice->getPagedVectorRefLeft(/*workerId*/ 0));
            Nautilus::Interface::PagedVectorVarSizedRef leftPagedVector(leftPagedVectorRef, leftSchema);
            leftPagedVector.writeRecord(leftRecord);
        }

        for (auto& rightRecord : allRightRecords) {
            auto rightPagedVectorRef =
                Nautilus::Value<Nautilus::MemRef>((int8_t*) expectedNLJSlice->getPagedVectorRefRight(/*workerId*/ 0));
            Nautilus::Interface::PagedVectorVarSizedRef rightPagedVector(rightPagedVectorRef, rightSchema);
            rightPagedVector.writeRecord(rightRecord);
        }
    }

        /**
     * @brief creates numberOfRecords many tuples for either left or right schema
     * @param numberOfRecords
     * @param joinBuildSide
     * @param minValue default is 0
     * @param maxValue default is 1000
     * @param randomSeed default is 42
     * @return the vector of records
     */
    std::vector<Record> createRandomRecords(uint64_t numberOfRecords,
                                            QueryCompilation::JoinBuildSideType joinBuildSide,
                                            uint64_t minValue = 0,
                                            uint64_t maxValue = 1000,
                                            uint64_t randomSeed = 42) {
        std::vector<Record> retVector;
        std::mt19937 generator(randomSeed);
        std::uniform_int_distribution<uint64_t> distribution(minValue, maxValue);
        auto schema = joinBuildSide == QueryCompilation::JoinBuildSideType::Left ? leftSchema : rightSchema;

        auto firstSchemaField = schema->get(0)->getName();
        auto secondSchemaField = schema->get(1)->getName();
        auto thirdSchemaField = schema->get(2)->getName();

        for (auto i = 0_u64; i < numberOfRecords; ++i) {
            retVector.emplace_back(Record({{firstSchemaField, Value<UInt64>(i)},
                                           {secondSchemaField, Value<UInt64>(distribution(generator))},
                                           {thirdSchemaField, Value<UInt64>(i)}}));
        }

        return retVector;
    }
};

TEST_F(NLJSliceSerializationTest, nljSliceSerialization) {
    const auto numberOfRecordsLeft = 250;
    const auto numberOfRecordsRight = 250;

    generateAndInsertRecordsToSlice(numberOfRecordsLeft, numberOfRecordsRight);

    std::ofstream ser_file("test_serialization.txt", std::ios::in);

    if (!ser_file.is_open()) {
        NES_ERROR("Error: Failed to open file for writing.");
    }

    expectedNLJSlice->serialize(ser_file);

    auto deserealizedSlice = std::make_shared<NLJSlice>(0, 2000, 0, bm, leftSchema, leftPageSize, rightSchema, rightPageSize);

    std::ifstream deser_file("test_serialization.txt", std::ios::binary);

    if (!deser_file.is_open()) {
        NES_ERROR("Error: Failed to open file for reading.");
    }

    deserealizedSlice->deserialize(deser_file);

    ASSERT_EQ(expectedNLJSlice->getNumberOfTuplesLeft(), deserealizedSlice->getNumberOfTuplesLeft());
    ASSERT_EQ(expectedNLJSlice->getNumberOfTuplesRight(), deserealizedSlice->getNumberOfTuplesRight());
}
}// namespace NES::Runtime::Execution
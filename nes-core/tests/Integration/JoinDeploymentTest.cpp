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

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-copy-dtor"
#include <NesBaseTest.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#pragma clang diagnostic pop
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <Common/Identifiers.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>

namespace NES::Runtime::Execution {

class JoinDeploymentTest : public Testing::TestWithErrorHandling<testing::Test>,
                           public ::testing::WithParamInterface<QueryCompilation::QueryCompilerOptions::QueryCompiler> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("JoinDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("QueryExecutionTest: Setup JoinDeploymentTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO("QueryExecutionTest: Setup JoinDeploymentTest test class.");
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
        auto queryCompiler = this->GetParam();
        executionEngine = std::make_shared<TestExecutionEngine>(queryCompiler);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("QueryExecutionTest: Tear down JoinDeploymentTest test case.");
        EXPECT_TRUE(executionEngine->stop());
        Testing::TestWithErrorHandling<testing::Test>::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("QueryExecutionTest: Tear down JoinDeploymentTest test class."); }

    std::shared_ptr<TestExecutionEngine> executionEngine;
};

std::vector<PhysicalTypePtr> getPhysicalTypes(SchemaPtr schema) {
    std::vector<PhysicalTypePtr> retVector;

    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
    for (const auto& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        retVector.push_back(physicalField);
    }

    return retVector;
}

std::istream& operator>>(std::istream& is, std::string& l) {
    std::getline(is, l);
    return is;
}

Runtime::MemoryLayouts::DynamicTupleBuffer fillBuffer(const std::string& csvFileName,
                                                      Runtime::MemoryLayouts::DynamicTupleBuffer buffer,
                                                      const SchemaPtr schema,
                                                      BufferManagerPtr bufferManager) {

    auto fullPath = std::string(TEST_DATA_DIRECTORY) + csvFileName;
    NES_ASSERT2_FMT(std::filesystem::exists(std::filesystem::path(fullPath)), "File " << fullPath << " does not exist!!!");
    const std::string delimiter = ",";
    auto parser = std::make_shared<CSVParser>(schema->fields.size(), getPhysicalTypes(schema), delimiter);

    std::ifstream inputFile(fullPath);
    std::istream_iterator<std::string> beginIt(inputFile);
    std::istream_iterator<std::string> endIt;
    auto tupleCount = 0;
    for (auto it = beginIt; it != endIt; ++it) {
        std::string line = *it;
        parser->writeInputTupleToTupleBuffer(line, tupleCount, buffer, schema, bufferManager);
        tupleCount++;
    }
    buffer.setNumberOfTuples(tupleCount);
    return buffer;
}

/**
 * @brief Merges a vector of TupleBuffers into one TupleBuffer. If the buffers in the vector do not fit into one TupleBuffer, the
 *        buffers that do not fit will be discarded.
 * @param buffersToBeMerged
 * @param schema
 * @param bufferManager
 * @return merged TupleBuffer
 */
Runtime::TupleBuffer mergeBuffers(std::vector<Runtime::TupleBuffer>& buffersToBeMerged,
                                  const SchemaPtr schema, BufferManagerPtr bufferManager) {

    auto retBuffer = bufferManager->getBufferBlocking();
    auto retBufferPtr = retBuffer.getBuffer();

    auto maxPossibleTuples = retBuffer.getBufferSize() / schema->getSchemaSizeInBytes();
    auto cnt = 0UL;
    for (auto& buffer : buffersToBeMerged) {
        cnt += buffer.getNumberOfTuples();
        if (cnt > maxPossibleTuples) {
            NES_WARNING("Too many tuples to fit in a single buffer.");
            return retBuffer;
        }

        auto bufferSize = buffer.getNumberOfTuples() * schema->getSchemaSizeInBytes();
        std::memcpy(retBufferPtr, buffer.getBuffer(), bufferSize);

        retBufferPtr += bufferSize;
        retBuffer.setNumberOfTuples(cnt);
    }

    return retBuffer;
}

/**
 * @brief checks if the buffers contain the same tuples
 * @param buffer1
 * @param buffer2
 * @param schema
 * @return boolean if the buffers contain the same tuples
 */
bool checkIfBuffersAreEqual(Runtime::TupleBuffer buffer1, Runtime::TupleBuffer buffer2, const uint64_t schemaSizeInByte) {
    NES_DEBUG("Checking if the buffers are equal, so if they contain the same tuples");
    if (buffer1.getNumberOfTuples() != buffer2.getNumberOfTuples()) {
        NES_DEBUG("Buffers do not contain the same tuples, as they do not have the same number of tuples");
        return false;
    }

    std::set<size_t> sameTupleIndices;
    for (auto idxBuffer1 = 0UL; idxBuffer1 < buffer1.getNumberOfTuples(); ++idxBuffer1) {
        bool idxFoundInBuffer2 = false;
        for (auto idxBuffer2 = 0UL; buffer2.getNumberOfTuples(); ++idxBuffer2) {
            if (sameTupleIndices.contains(idxBuffer2)) {
                continue;
            }
            auto startPosBuffer1 = buffer1.getBuffer() + schemaSizeInByte * idxBuffer1;
            auto startPosBuffer2 = buffer2.getBuffer() + schemaSizeInByte * idxBuffer2;
            auto equalTuple = (memcmp(startPosBuffer1, startPosBuffer2, schemaSizeInByte) == 0);
            if (equalTuple) {
                sameTupleIndices.insert(idxBuffer2);
                idxFoundInBuffer2 = true;
                break;
            }
        }

        if (!idxFoundInBuffer2) {
            NES_DEBUG("Buffers do not contain the same tuples, as tuple could not be found in both buffers!");
            return false;
        }
    }

    return (sameTupleIndices.size() == buffer1.getNumberOfTuples());
}

/**
 * Test deploying join query with source on two different worker node using top down strategy.
 */
//TODO: this test will be enabled once we have the renaming function using as
//TODO: prevent self join
TEST_P(JoinDeploymentTest, DISABLED_testSelfJoinTumblingWindow) {
/*
    struct Window {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto windowSchema = Schema::create()
                            ->addField("value", DataTypeFactory::createUInt64())
                            ->addField("id", DataTypeFactory::createUInt64())
                            ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window), windowSchema->getSchemaSizeInBytes());

    auto csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType->setNumberOfBuffersToProduce(2);
    csvSourceType->setSkipHeader(true);

    string query =
        R"(Query::from("window").as("w1").joinWith(Query::from("window").as("w2")).where(Attribute("id")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("window", windowSchema)
                                  .attachWorkerWithCSVSourceToCoordinator("window", csvSourceType)
                                  .validate()
                                  .setupTopology();

    struct Output {
        int64_t start;
        int64_t end;
        int64_t key;
        uint64_t value1;
        uint64_t id1;
        uint64_t timestamp1;
        uint64_t value2;
        uint64_t id2;
        uint64_t timestamp2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && key == rhs.key && value1 == rhs.value1 && id1 == rhs.id1
                    && timestamp1 == rhs.timestamp1 && value2 == rhs.value2 && id2 == rhs.id2 && timestamp2 == rhs.timestamp2);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 4, 1, 4, 1002, 1, 4, 1002},
                                          {1000, 2000, 12, 1, 12, 1001, 1, 12, 1001},
                                          {2000, 3000, 1, 2, 1, 2000, 2, 1, 2000},
                                          {2000, 3000, 11, 2, 11, 2001, 2, 11, 2001},
                                          {2000, 3000, 16, 2, 16, 2002, 2, 16, 2002}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));*/
}

/**
* Test deploying join with same data and same schema
 * */
TEST_P(JoinDeploymentTest, testJoinWithSameSchemaTumblingWindow) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("test1$value", BasicType::UINT64)
                                ->addField("test1$id", BasicType::UINT64)
                                ->addField("test1$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("test2$value", BasicType::UINT64)
                                 ->addField("test2$id", BasicType::UINT64)
                                 ->addField("test2$timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "test1$id";
    const auto joinFieldNameRight = "test2$id";
    const auto timeStampField = "timestamp";

    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffersLeft("window.csv");
    const std::string fileNameBuffersRight("window.csv");
    const std::string fileNameBuffersSink("window_sink.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto leftBuffer = fillBuffer(fileNameBuffersLeft, executionEngine->getBuffer(leftSchema), leftSchema, bufferManager);
    auto rightBuffer = fillBuffer(fileNameBuffersRight, executionEngine->getBuffer(rightSchema), rightSchema, bufferManager);
    auto expectedSinkBuffer = fillBuffer(fileNameBuffersSink, executionEngine->getBuffer(joinSchema), joinSchema, bufferManager);

    auto testSink = executionEngine->createDataSink(joinSchema, 20);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptorLeft = executionEngine->createDataSource(leftSchema);
    auto testSourceDescriptorRight = executionEngine->createDataSource(rightSchema);

    auto query = TestQuery::from(testSourceDescriptorLeft)
                     .joinWith(TestQuery::from(testSourceDescriptorRight))
                     .where(Attribute(joinFieldNameLeft))
                     .equalsTo(Attribute(joinFieldNameRight))
                     .window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .sink(testSinkDescriptor);

    NES_INFO("Submitting query: " << query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto sourceLeft = executionEngine->getDataSource(queryPlan, 0);
    auto sourceRight = executionEngine->getDataSource(queryPlan, 1);
    ASSERT_TRUE(!!sourceLeft);
    ASSERT_TRUE(!!sourceRight);

    sourceLeft->emitBuffer(leftBuffer);
    sourceRight->emitBuffer(rightBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 20);
    auto resultBuffer = mergeBuffers(testSink->resultBuffers, joinSchema, bufferManager);

    NES_DEBUG("resultBuffer: " << NES::Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
    NES_DEBUG("expectedSinkBuffer: " << NES::Util::printTupleBufferAsCSV(expectedSinkBuffer.getBuffer(), joinSchema));

    ASSERT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    ASSERT_TRUE(checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffer.getBuffer(), joinSchema->getSchemaSizeInBytes()));
}

/**
 * Test deploying join with same data but different names in the schema
 */
TEST_P(JoinDeploymentTest, testJoinWithDifferentSchemaNamesButSameInputTumblingWindow) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("test1$value1", BasicType::UINT64)
                                ->addField("test1$id1", BasicType::UINT64)
                                ->addField("test1$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("test2$value2", BasicType::UINT64)
                                 ->addField("test2$id2", BasicType::UINT64)
                                 ->addField("test2$timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "test1$id1";
    const auto joinFieldNameRight = "test2$id2";
    const auto timeStampField = "timestamp";

    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffersLeft("window.csv");
    const std::string fileNameBuffersRight("window.csv");
    const std::string fileNameBuffersSink("window_sink.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto leftBuffer = fillBuffer(fileNameBuffersLeft, executionEngine->getBuffer(leftSchema), leftSchema, bufferManager);
    auto rightBuffer = fillBuffer(fileNameBuffersRight, executionEngine->getBuffer(rightSchema), rightSchema, bufferManager);
    auto expectedSinkBuffer = fillBuffer(fileNameBuffersSink, executionEngine->getBuffer(joinSchema), joinSchema, bufferManager);

    auto testSink = executionEngine->createDataSink(joinSchema, 20);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptorLeft = executionEngine->createDataSource(leftSchema);
    auto testSourceDescriptorRight = executionEngine->createDataSource(rightSchema);

    auto query = TestQuery::from(testSourceDescriptorLeft)
                     .joinWith(TestQuery::from(testSourceDescriptorRight))
                     .where(Attribute(joinFieldNameLeft))
                     .equalsTo(Attribute(joinFieldNameRight))
                     .window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .sink(testSinkDescriptor);

    NES_INFO("Submitting query: " << query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto sourceLeft = executionEngine->getDataSource(queryPlan, 0);
    auto sourceRight = executionEngine->getDataSource(queryPlan, 1);
    ASSERT_TRUE(!!sourceLeft);
    ASSERT_TRUE(!!sourceRight);

    sourceLeft->emitBuffer(leftBuffer);
    sourceRight->emitBuffer(rightBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 20);
    auto resultBuffer = mergeBuffers(testSink->resultBuffers, joinSchema, bufferManager);

    NES_DEBUG("resultBuffer: " << NES::Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
    NES_DEBUG("expectedSinkBuffer: " << NES::Util::printTupleBufferAsCSV(expectedSinkBuffer.getBuffer(), joinSchema));

    ASSERT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    ASSERT_TRUE(checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffer.getBuffer(), joinSchema->getSchemaSizeInBytes()));
}

/**
 * Test deploying join with different sources
 */
TEST_P(JoinDeploymentTest, testJoinWithDifferentSourceTumblingWindow) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("test1$value1", BasicType::UINT64)
                                ->addField("test1$id1", BasicType::UINT64)
                                ->addField("test1$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("test2$value2", BasicType::UINT64)
                                 ->addField("test2$id2", BasicType::UINT64)
                                 ->addField("test2$timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "test1$id1";
    const auto joinFieldNameRight = "test2$id2";
    const auto timeStampField = "timestamp";

    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffersLeft("window.csv");
    const std::string fileNameBuffersRight("window2.csv");
    const std::string fileNameBuffersSink("window_sink2.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto leftBuffer = fillBuffer(fileNameBuffersLeft, executionEngine->getBuffer(leftSchema), leftSchema, bufferManager);
    auto rightBuffer = fillBuffer(fileNameBuffersRight, executionEngine->getBuffer(rightSchema), rightSchema, bufferManager);
    auto expectedSinkBuffer = fillBuffer(fileNameBuffersSink, executionEngine->getBuffer(joinSchema), joinSchema, bufferManager);

    auto testSink = executionEngine->createDataSink(joinSchema, 20);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptorLeft = executionEngine->createDataSource(leftSchema);
    auto testSourceDescriptorRight = executionEngine->createDataSource(rightSchema);

    auto query = TestQuery::from(testSourceDescriptorLeft)
                     .joinWith(TestQuery::from(testSourceDescriptorRight))
                     .where(Attribute(joinFieldNameLeft))
                     .equalsTo(Attribute(joinFieldNameRight))
                     .window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .sink(testSinkDescriptor);

    NES_INFO("Submitting query: " << query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto sourceLeft = executionEngine->getDataSource(queryPlan, 0);
    auto sourceRight = executionEngine->getDataSource(queryPlan, 1);
    ASSERT_TRUE(!!sourceLeft);
    ASSERT_TRUE(!!sourceRight);

    sourceLeft->emitBuffer(leftBuffer);
    sourceRight->emitBuffer(rightBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 20);
    auto resultBuffer = mergeBuffers(testSink->resultBuffers, joinSchema, bufferManager);

    NES_DEBUG("resultBuffer: " << NES::Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
    NES_DEBUG("expectedSinkBuffer: " << NES::Util::printTupleBufferAsCSV(expectedSinkBuffer.getBuffer(), joinSchema));

    ASSERT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    ASSERT_TRUE(checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffer.getBuffer(), joinSchema->getSchemaSizeInBytes()));
}

/**
 * Test deploying join with different sources
 */
TEST_P(JoinDeploymentTest, testJoinWithDifferentNumberOfAttributesTumblingWindow) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("test1$win", BasicType::UINT64)
                                ->addField("test1$id1", BasicType::UINT64)
                                ->addField("test1$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("test2$id2", BasicType::UINT64)
                                 ->addField("test2$timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "test1$id1";
    const auto joinFieldNameRight = "test2$id2";
    const auto timeStampField = "timestamp";

    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffersLeft("window.csv");
    const std::string fileNameBuffersRight("window3.csv");
    const std::string fileNameBuffersSink("window_sink3.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto leftBuffer = fillBuffer(fileNameBuffersLeft, executionEngine->getBuffer(leftSchema), leftSchema, bufferManager);
    auto rightBuffer = fillBuffer(fileNameBuffersRight, executionEngine->getBuffer(rightSchema), rightSchema, bufferManager);
    auto expectedSinkBuffer = fillBuffer(fileNameBuffersSink, executionEngine->getBuffer(joinSchema), joinSchema, bufferManager);

    auto testSink = executionEngine->createDataSink(joinSchema, 20);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptorLeft = executionEngine->createDataSource(leftSchema);
    auto testSourceDescriptorRight = executionEngine->createDataSource(rightSchema);

    auto query = TestQuery::from(testSourceDescriptorLeft)
                     .joinWith(TestQuery::from(testSourceDescriptorRight))
                     .where(Attribute(joinFieldNameLeft))
                     .equalsTo(Attribute(joinFieldNameRight))
                     .window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .sink(testSinkDescriptor);

    NES_INFO("Submitting query: " << query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto sourceLeft = executionEngine->getDataSource(queryPlan, 0);
    auto sourceRight = executionEngine->getDataSource(queryPlan, 1);
    ASSERT_TRUE(!!sourceLeft);
    ASSERT_TRUE(!!sourceRight);

    sourceLeft->emitBuffer(leftBuffer);
    sourceRight->emitBuffer(rightBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 20);
    auto resultBuffer = mergeBuffers(testSink->resultBuffers, joinSchema, bufferManager);

    NES_DEBUG("resultBuffer: " << NES::Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
    NES_DEBUG("expectedSinkBuffer: " << NES::Util::printTupleBufferAsCSV(expectedSinkBuffer.getBuffer(), joinSchema));

    ASSERT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    ASSERT_TRUE(checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffer.getBuffer(), joinSchema->getSchemaSizeInBytes()));
}

/**
 * Test deploying join with different sources
 */
// TODO this test can be enabled and ported to the Nautilus compiler once #3353 is merged
TEST_P(JoinDeploymentTest, DISABLED_testJoinWithDifferentSourceSlidingWindow) {
    /*
    struct Window {
        int64_t win1;
        uint64_t id1;
        uint64_t timestamp;
    };

    struct Window2 {
        int64_t win2;
        uint64_t id2;
        uint64_t timestamp;
    };

    auto windowSchema = Schema::create()
                            ->addField("win1", DataTypeFactory::createInt64())
                            ->addField("id1", DataTypeFactory::createUInt64())
                            ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("win2", DataTypeFactory::createInt64())
                             ->addField("id2", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window), windowSchema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    csvSourceType1->setSkipHeader(true);

    auto csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    csvSourceType2->setSkipHeader(true);

    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(
        SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500))))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("window1", windowSchema)
                                  .addLogicalSource("window2", window2Schema)
                                  .attachWorkerWithCSVSourceToCoordinator("window1", csvSourceType1)
                                  .attachWorkerWithCSVSourceToCoordinator("window2", csvSourceType2)
                                  .validate()
                                  .setupTopology();

    struct Output {
        int64_t start;
        int64_t end;
        int64_t key;
        int64_t win1;
        uint64_t id1;
        uint64_t timestamp1;
        int64_t win2;
        uint64_t id2;
        uint64_t timestamp2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && key == rhs.key && win1 == rhs.win1 && id1 == rhs.id1
                    && timestamp1 == rhs.timestamp1 && win2 == rhs.win2 && id2 == rhs.id2 && timestamp2 == rhs.timestamp2);
        }
    };

    std::vector<Output> expectedOutput = {{2000, 3000, 1, 2, 1, 2000, 2, 1, 2010},
                                          {1500, 2500, 1, 2, 1, 2000, 2, 1, 2010},
                                          {1000, 2000, 4, 1, 4, 1002, 3, 4, 1102},
                                          {1000, 2000, 4, 1, 4, 1002, 3, 4, 1112},
                                          {500, 1500, 4, 1, 4, 1002, 3, 4, 1102},
                                          {500, 1500, 4, 1, 4, 1002, 3, 4, 1112},
                                          {2000, 3000, 11, 2, 11, 2001, 2, 11, 2301},
                                          {1500, 2500, 11, 2, 11, 2001, 2, 11, 2301},
                                          {1000, 2000, 12, 1, 12, 1001, 5, 12, 1011},
                                          {500, 1500, 12, 1, 12, 1001, 5, 12, 1011}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
    */
}

/**
 * Test deploying join with different sources
 */
// TODO this test can be enabled and ported to the Nautilus compiler once #3353 is merged
TEST_P(JoinDeploymentTest, DISABLED_testSlidingWindowDifferentAttributes) {
    /*
    struct Window {
        int64_t win;
        uint64_t id1;
        uint64_t timestamp;
    };

    struct Window2 {
        uint64_t id2;
        uint64_t timestamp;
    };

    auto windowSchema = Schema::create()
                            ->addField("win", DataTypeFactory::createInt64())
                            ->addField("id1", DataTypeFactory::createUInt64())
                            ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("id2", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window), windowSchema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    csvSourceType1->setSkipHeader(true);

    auto csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window3.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    csvSourceType2->setSkipHeader(true);

    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(
        SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500))))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("window1", windowSchema)
                                  .addLogicalSource("window2", window2Schema)
                                  .attachWorkerWithCSVSourceToCoordinator("window1", csvSourceType1)
                                  .attachWorkerWithCSVSourceToCoordinator("window2", csvSourceType2)
                                  .validate()
                                  .setupTopology();

    struct Output {
        int64_t start;
        int64_t end;
        int64_t key;
        int64_t win;
        uint64_t id1;
        uint64_t timestamp1;
        uint64_t id2;
        uint64_t timestamp2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && key == rhs.key && win == rhs.win && id1 == rhs.id1
                    && timestamp1 == rhs.timestamp1 && id2 == rhs.id2 && timestamp2 == rhs.timestamp2);
        }
    };

    std::vector<Output> expectedOutput = {{2000, 3000, 1, 2, 1, 2000, 1, 2000},
                                          {1500, 2500, 1, 2, 1, 2000, 1, 2000},
                                          {1000, 2000, 4, 1, 4, 1002, 4, 1002},
                                          {500, 1500, 4, 1, 4, 1002, 4, 1002},
                                          {2000, 3000, 11, 2, 11, 2001, 11, 2001},
                                          {1500, 2500, 11, 2, 11, 2001, 11, 2001},
                                          {1000, 2000, 12, 1, 12, 1001, 12, 1001},
                                          {500, 1500, 12, 1, 12, 1001, 12, 1001},
                                          {2000, 3000, 16, 2, 16, 2002, 16, 2002},
                                          {1500, 2500, 16, 2, 16, 2002, 16, 2002}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
    */
}

/**
 * @brief Test a join query that uses fixed-array as keys
 */
TEST_P(JoinDeploymentTest, testJoinWithFixedCharKey) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("test1$id1", BasicType::TEXT)
                                ->addField("test1$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("test2$id2", BasicType::TEXT)
                                 ->addField("test2$timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "test1$id1";
    const auto joinFieldNameRight = "test2$id2";
    const auto timeStampField = "timestamp";

    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffersLeft("window5.csv");
    const std::string fileNameBuffersRight("window6.csv");
    const std::string fileNameBuffersSink("window_sink4.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto leftBuffer = fillBuffer(fileNameBuffersLeft, executionEngine->getBuffer(leftSchema), leftSchema, bufferManager);
    auto rightBuffer = fillBuffer(fileNameBuffersRight, executionEngine->getBuffer(rightSchema), rightSchema, bufferManager);
    auto expectedSinkBuffer = fillBuffer(fileNameBuffersSink, executionEngine->getBuffer(joinSchema), joinSchema, bufferManager);

    auto testSink = executionEngine->createDataSink(joinSchema, 2);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptorLeft = executionEngine->createDataSource(leftSchema);
    auto testSourceDescriptorRight = executionEngine->createDataSource(rightSchema);

    auto query = TestQuery::from(testSourceDescriptorLeft)
                     .joinWith(TestQuery::from(testSourceDescriptorRight))
                     .where(Attribute(joinFieldNameLeft))
                     .equalsTo(Attribute(joinFieldNameRight))
                     .window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     //.project(Attribute("test1test2$start"),Attribute("test1test2$end"), Attribute("test1test2$key"),  Attribute(timeStampField))
                     .sink(testSinkDescriptor);

    NES_INFO("Submitting query: " << query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto sourceLeft = executionEngine->getDataSource(queryPlan, 0);
    auto sourceRight = executionEngine->getDataSource(queryPlan, 1);
    ASSERT_TRUE(!!sourceLeft);
    ASSERT_TRUE(!!sourceRight);

    sourceLeft->emitBuffer(leftBuffer);
    sourceRight->emitBuffer(rightBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 2);
    auto resultBuffer = mergeBuffers(testSink->resultBuffers, joinSchema, bufferManager);

    NES_DEBUG("resultBuffer: " << NES::Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
    NES_DEBUG("expectedSinkBuffer: " << NES::Util::printTupleBufferAsCSV(expectedSinkBuffer.getBuffer(), joinSchema));

    ASSERT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    ASSERT_TRUE(checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffer.getBuffer(), joinSchema->getSchemaSizeInBytes()));
}

INSTANTIATE_TEST_CASE_P(testJoinQueries,
                        JoinDeploymentTest,
                        ::testing::Values(QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER),
                        [](const testing::TestParamInfo<JoinDeploymentTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(info.param));
                        });
}// namespace NES::Runtime::Execution

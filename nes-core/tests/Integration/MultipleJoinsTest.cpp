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

#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <NesBaseTest.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestUtils.hpp>

namespace NES::Runtime::Execution {

class MultipleJoinsTest : public Testing::TestWithErrorHandling<testing::Test>,
                          public ::testing::WithParamInterface<QueryCompilation::QueryCompilerOptions::QueryCompiler> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MultipleJoinsTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("QueryExecutionTest: Setup MultipleJoinsTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO("QueryExecutionTest: Setup MultipleJoinsTest test class.");
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
        auto queryCompiler = this->GetParam();
        executionEngine = std::make_shared<TestExecutionEngine>(queryCompiler);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("QueryExecutionTest: Tear down MultipleJoinsTest test case.");
        EXPECT_TRUE(executionEngine->stop());
        Testing::TestWithErrorHandling<testing::Test>::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("QueryExecutionTest: Tear down MultipleJoinsTest test class."); }

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

TEST_P(MultipleJoinsTest, testJoins2WithDifferentSourceTumblingWindowOnCoodinator) {
    const auto window1 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("window1$win1", BasicType::UINT64)
                                ->addField("window1$id1", BasicType::UINT64)
                                ->addField("window1$timestamp", BasicType::UINT64);

    const auto window2 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("window2$win2", BasicType::UINT64)
                                 ->addField("window2$id2", BasicType::UINT64)
                                 ->addField("window2$timestamp", BasicType::UINT64);

    const auto window3 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window3$win3", BasicType::UINT64)
                             ->addField("window3$id3", BasicType::UINT64)
                             ->addField("window3$timestamp", BasicType::UINT64);

    const auto joinFieldName1 = "window1$id1";
    const auto joinFieldName2 = "window2$id2";
    const auto joinFieldName3 = "window3$id3";
    const auto timeStampField = "timestamp";

    const auto joinSchema = Util::createJoinSchema(Util::createJoinSchema(window1, window2, joinFieldName1), window3, joinFieldName1);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffers1("window.csv");
    const std::string fileNameBuffers2("window2.csv");
    const std::string fileNameBuffers3("window4.csv");
    const std::string fileNameBuffersSink("window_sink7.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto firstBuffer = fillBuffer(fileNameBuffers1, executionEngine->getBuffer(window1), window1, bufferManager);
    auto secondBuffer = fillBuffer(fileNameBuffers2, executionEngine->getBuffer(window2), window2, bufferManager);
    auto thirdBuffer = fillBuffer(fileNameBuffers3, executionEngine->getBuffer(window3), window3, bufferManager);
    auto expectedSinkBuffer = fillBuffer(fileNameBuffersSink, executionEngine->getBuffer(joinSchema), joinSchema, bufferManager);

    auto testSink = executionEngine->createDataSink(joinSchema, 6);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptor1 = executionEngine->createDataSource(window1);
    auto testSourceDescriptor2 = executionEngine->createDataSource(window2);
    auto testSourceDescriptor3 = executionEngine->createDataSource(window3);

    auto query = TestQuery::from(testSourceDescriptor1)
                     .joinWith(TestQuery::from(testSourceDescriptor2)).where(Attribute(joinFieldName1))
                     .equalsTo(Attribute(joinFieldName2)).window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .joinWith(TestQuery::from(testSourceDescriptor3)).where(Attribute(joinFieldName1))
                     .equalsTo(Attribute(joinFieldName3)).window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .sink(testSinkDescriptor);

    NES_INFO("Submitting query: " << query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto source1 = executionEngine->getDataSource(queryPlan, 0);
    auto source2 = executionEngine->getDataSource(queryPlan, 1);
    auto source3 = executionEngine->getDataSource(queryPlan, 2);
    ASSERT_TRUE(!!source1);
    ASSERT_TRUE(!!source2);
    ASSERT_TRUE(!!source3);

    source1->emitBuffer(firstBuffer);
    source2->emitBuffer(secondBuffer);
    source3->emitBuffer(thirdBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 6);
    auto resultBuffer = mergeBuffers(testSink->resultBuffers, joinSchema, bufferManager);

    NES_DEBUG("resultBuffer: " << NES::Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
    NES_DEBUG("expectedSinkBuffer: " << NES::Util::printTupleBufferAsCSV(expectedSinkBuffer.getBuffer(), joinSchema));

    ASSERT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    ASSERT_TRUE(checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffer.getBuffer(), joinSchema->getSchemaSizeInBytes()));
}

TEST_P(MultipleJoinsTest, testJoin3WithDifferentSourceTumblingWindowOnCoodinatorSequential) {
    const auto window1 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window1$win1", BasicType::UINT64)
                             ->addField("window1$id1", BasicType::UINT64)
                             ->addField("window1$timestamp", BasicType::UINT64);

    const auto window2 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window2$win2", BasicType::UINT64)
                             ->addField("window2$id2", BasicType::UINT64)
                             ->addField("window2$timestamp", BasicType::UINT64);

    const auto window3 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window3$win3", BasicType::UINT64)
                             ->addField("window3$id3", BasicType::UINT64)
                             ->addField("window3$timestamp", BasicType::UINT64);

    const auto window4 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window4$win4", BasicType::UINT64)
                             ->addField("window4$id4", BasicType::UINT64)
                             ->addField("window4$timestamp", BasicType::UINT64);

    const auto joinFieldName1 = "window1$id1";
    const auto joinFieldName2 = "window2$id2";
    const auto joinFieldName3 = "window3$id3";
    const auto joinFieldName4 = "window4$id4";
    const auto timeStampField = "timestamp";

    const auto joinSchema = Util::createJoinSchema(Util::createJoinSchema(Util::createJoinSchema(window1, window2, joinFieldName1), window3, joinFieldName1), window4, joinFieldName4);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffers1("window.csv");
    const std::string fileNameBuffers2("window2.csv");
    const std::string fileNameBuffers3("window4.csv");
    const std::string fileNameBuffers4("window4.csv");
    const std::string fileNameBuffersSink("window_sink8.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto firstBuffer = fillBuffer(fileNameBuffers1, executionEngine->getBuffer(window1), window1, bufferManager);
    auto secondBuffer = fillBuffer(fileNameBuffers2, executionEngine->getBuffer(window2), window2, bufferManager);
    auto thirdBuffer = fillBuffer(fileNameBuffers3, executionEngine->getBuffer(window3), window3, bufferManager);
    auto fourthBuffer = fillBuffer(fileNameBuffers4, executionEngine->getBuffer(window4), window4, bufferManager);
    auto expectedSinkBuffer = fillBuffer(fileNameBuffersSink, executionEngine->getBuffer(joinSchema), joinSchema, bufferManager);

    auto testSink = executionEngine->createDataSink(joinSchema, 6);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptor1 = executionEngine->createDataSource(window1);
    auto testSourceDescriptor2 = executionEngine->createDataSource(window2);
    auto testSourceDescriptor3 = executionEngine->createDataSource(window3);
    auto testSourceDescriptor4 = executionEngine->createDataSource(window4);

    auto query = TestQuery::from(testSourceDescriptor1)
                     .joinWith(TestQuery::from(testSourceDescriptor2)).where(Attribute(joinFieldName1))
                     .equalsTo(Attribute(joinFieldName2)).window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .joinWith(TestQuery::from(testSourceDescriptor3)).where(Attribute(joinFieldName1))
                     .equalsTo(Attribute(joinFieldName3)).window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .joinWith(TestQuery::from(testSourceDescriptor4)).where(Attribute(joinFieldName1))
                     .equalsTo(Attribute(joinFieldName4)).window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .sink(testSinkDescriptor);

    NES_INFO("Submitting query: " << query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto source1 = executionEngine->getDataSource(queryPlan, 0);
    auto source2 = executionEngine->getDataSource(queryPlan, 1);
    auto source3 = executionEngine->getDataSource(queryPlan, 2);
    auto source4 = executionEngine->getDataSource(queryPlan, 3);
    ASSERT_TRUE(!!source1);
    ASSERT_TRUE(!!source2);
    ASSERT_TRUE(!!source3);
    ASSERT_TRUE(!!source4);

    source1->emitBuffer(firstBuffer);
    source2->emitBuffer(secondBuffer);
    source3->emitBuffer(thirdBuffer);
    source4->emitBuffer(fourthBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 6);
    auto resultBuffer = mergeBuffers(testSink->resultBuffers, joinSchema, bufferManager);

    NES_DEBUG("resultBuffer: " << NES::Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
    NES_DEBUG("expectedSinkBuffer: " << NES::Util::printTupleBufferAsCSV(expectedSinkBuffer.getBuffer(), joinSchema));

    ASSERT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    ASSERT_TRUE(checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffer.getBuffer(), joinSchema->getSchemaSizeInBytes()));
}

TEST_P(MultipleJoinsTest, testJoin3WithDifferentSourceTumblingWindowOnCoodinatorNested) {
    const auto window1 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window1$win1", BasicType::UINT64)
                             ->addField("window1$id1", BasicType::UINT64)
                             ->addField("window1$timestamp", BasicType::UINT64);

    const auto window2 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window2$win2", BasicType::UINT64)
                             ->addField("window2$id2", BasicType::UINT64)
                             ->addField("window2$timestamp", BasicType::UINT64);

    const auto window3 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window3$win3", BasicType::UINT64)
                             ->addField("window3$id3", BasicType::UINT64)
                             ->addField("window3$timestamp", BasicType::UINT64);

    const auto window4 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window4$win4", BasicType::UINT64)
                             ->addField("window4$id4", BasicType::UINT64)
                             ->addField("window4$timestamp", BasicType::UINT64);

    const auto joinFieldName1 = "window1$id1";
    const auto joinFieldName2 = "window2$id2";
    const auto joinFieldName3 = "window3$id3";
    const auto joinFieldName4 = "window4$id4";
    const auto timeStampField = "timestamp";

    const auto joinSchema = Util::createJoinSchema(Util::createJoinSchema(Util::createJoinSchema(window1, window2, joinFieldName1), window3, joinFieldName1), window4, joinFieldName4);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffers1("window.csv");
    const std::string fileNameBuffers2("window2.csv");
    const std::string fileNameBuffers3("window4.csv");
    const std::string fileNameBuffers4("window4.csv");
    const std::string fileNameBuffersSink("window_sink9.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto firstBuffer = fillBuffer(fileNameBuffers1, executionEngine->getBuffer(window1), window1, bufferManager);
    auto secondBuffer = fillBuffer(fileNameBuffers2, executionEngine->getBuffer(window2), window2, bufferManager);
    auto thirdBuffer = fillBuffer(fileNameBuffers3, executionEngine->getBuffer(window3), window3, bufferManager);
    auto fourthBuffer = fillBuffer(fileNameBuffers4, executionEngine->getBuffer(window4), window4, bufferManager);
    auto expectedSinkBuffer = fillBuffer(fileNameBuffersSink, executionEngine->getBuffer(joinSchema), joinSchema, bufferManager);

    auto testSink = executionEngine->createDataSink(joinSchema, 6);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptor1 = executionEngine->createDataSource(window1);
    auto testSourceDescriptor2 = executionEngine->createDataSource(window2);
    auto testSourceDescriptor3 = executionEngine->createDataSource(window3);
    auto testSourceDescriptor4 = executionEngine->createDataSource(window4);

    auto query = TestQuery::from(testSourceDescriptor1)
                     .joinWith(TestQuery::from(testSourceDescriptor2)).where(Attribute(joinFieldName1)).equalsTo(Attribute(joinFieldName2))
                     .window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .joinWith(TestQuery::from(testSourceDescriptor3)).joinWith(TestQuery::from(testSourceDescriptor4)).where(Attribute(joinFieldName3)).equalsTo(Attribute(joinFieldName4))
                     .window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .where(Attribute(joinFieldName1)).equalsTo(Attribute(joinFieldName4)).window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .sink(testSinkDescriptor);

    NES_INFO("Submitting query: " << query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto source1 = executionEngine->getDataSource(queryPlan, 0);
    auto source2 = executionEngine->getDataSource(queryPlan, 1);
    auto source3 = executionEngine->getDataSource(queryPlan, 2);
    auto source4 = executionEngine->getDataSource(queryPlan, 3);
    ASSERT_TRUE(!!source1);
    ASSERT_TRUE(!!source2);
    ASSERT_TRUE(!!source3);
    ASSERT_TRUE(!!source4);

    source1->emitBuffer(firstBuffer);
    source2->emitBuffer(secondBuffer);
    source3->emitBuffer(thirdBuffer);
    source4->emitBuffer(fourthBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 6);
    auto resultBuffer = mergeBuffers(testSink->resultBuffers, joinSchema, bufferManager);

    NES_DEBUG("resultBuffer: " << NES::Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
    NES_DEBUG("expectedSinkBuffer: " << NES::Util::printTupleBufferAsCSV(expectedSinkBuffer.getBuffer(), joinSchema));

    ASSERT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    ASSERT_TRUE(checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffer.getBuffer(), joinSchema->getSchemaSizeInBytes()));
}

/**
 *
 *
 * Sliding window joins
 *
 */
// TODO this test can be enabled once #3353 is merged
TEST_P(MultipleJoinsTest, DISABLED_testJoins2WithDifferentSourceSlidingWindowOnCoodinator) {
    const auto window1 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window1$win1", BasicType::UINT64)
                             ->addField("window1$id1", BasicType::UINT64)
                             ->addField("window1$timestamp", BasicType::UINT64);

    const auto window2 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window2$win2", BasicType::UINT64)
                             ->addField("window2$id2", BasicType::UINT64)
                             ->addField("window2$timestamp", BasicType::UINT64);

    const auto window3 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window3$win3", BasicType::UINT64)
                             ->addField("window3$id3", BasicType::UINT64)
                             ->addField("window3$timestamp", BasicType::UINT64);

    const auto joinFieldName1 = "window1$id1";
    const auto joinFieldName2 = "window2$id2";
    const auto joinFieldName3 = "window3$id3";
    const auto timeStampField = "timestamp";

    const auto joinSchema = Util::createJoinSchema(Util::createJoinSchema(window1, window2, joinFieldName1), window3, joinFieldName1);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const auto windowSlide = 500UL;
    const std::string fileNameBuffers1("window.csv");
    const std::string fileNameBuffers2("window2.csv");
    const std::string fileNameBuffers3("window4.csv");
    const std::string fileNameBuffersSink("window_sink10.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto firstBuffer = fillBuffer(fileNameBuffers1, executionEngine->getBuffer(window1), window1, bufferManager);
    auto secondBuffer = fillBuffer(fileNameBuffers2, executionEngine->getBuffer(window2), window2, bufferManager);
    auto thirdBuffer = fillBuffer(fileNameBuffers3, executionEngine->getBuffer(window3), window3, bufferManager);
    auto expectedSinkBuffer = fillBuffer(fileNameBuffersSink, executionEngine->getBuffer(joinSchema), joinSchema, bufferManager);

    auto testSink = executionEngine->createDataSink(joinSchema, 12);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptor1 = executionEngine->createDataSource(window1);
    auto testSourceDescriptor2 = executionEngine->createDataSource(window2);
    auto testSourceDescriptor3 = executionEngine->createDataSource(window3);

    auto query = TestQuery::from(testSourceDescriptor1)
                     .joinWith(TestQuery::from(testSourceDescriptor2)).where(Attribute(joinFieldName1))
                     .equalsTo(Attribute(joinFieldName2)).window(SlidingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize), Milliseconds(windowSlide)))
                     .joinWith(TestQuery::from(testSourceDescriptor3)).where(Attribute(joinFieldName1))
                     .equalsTo(Attribute(joinFieldName3)).window(SlidingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize), Milliseconds(windowSlide)))
                     .sink(testSinkDescriptor);

    NES_INFO("Submitting query: " << query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto source1 = executionEngine->getDataSource(queryPlan, 0);
    auto source2 = executionEngine->getDataSource(queryPlan, 1);
    auto source3 = executionEngine->getDataSource(queryPlan, 2);
    ASSERT_TRUE(!!source1);
    ASSERT_TRUE(!!source2);
    ASSERT_TRUE(!!source3);

    source1->emitBuffer(firstBuffer);
    source2->emitBuffer(secondBuffer);
    source3->emitBuffer(thirdBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 12);
    auto resultBuffer = mergeBuffers(testSink->resultBuffers, joinSchema, bufferManager);

    NES_DEBUG("resultBuffer: " << NES::Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
    NES_DEBUG("expectedSinkBuffer: " << NES::Util::printTupleBufferAsCSV(expectedSinkBuffer.getBuffer(), joinSchema));

    ASSERT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    ASSERT_TRUE(checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffer.getBuffer(), joinSchema->getSchemaSizeInBytes()));
}

// TODO this test can be enabled once #3353 is merged
TEST_P(MultipleJoinsTest, DISABLED_testJoin3WithDifferentSourceSlidingWindowOnCoodinatorSequential) {
    const auto window1 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window1$win1", BasicType::UINT64)
                             ->addField("window1$id1", BasicType::UINT64)
                             ->addField("window1$timestamp", BasicType::UINT64);

    const auto window2 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window2$win2", BasicType::UINT64)
                             ->addField("window2$id2", BasicType::UINT64)
                             ->addField("window2$timestamp", BasicType::UINT64);

    const auto window3 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window3$win3", BasicType::UINT64)
                             ->addField("window3$id3", BasicType::UINT64)
                             ->addField("window3$timestamp", BasicType::UINT64);

    const auto window4 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window4$win4", BasicType::UINT64)
                             ->addField("window4$id4", BasicType::UINT64)
                             ->addField("window4$timestamp", BasicType::UINT64);

    const auto joinFieldName1 = "window1$id1";
    const auto joinFieldName2 = "window2$id2";
    const auto joinFieldName3 = "window3$id3";
    const auto joinFieldName4 = "window4$id4";
    const auto timeStampField = "timestamp";

    const auto joinSchema = Util::createJoinSchema(Util::createJoinSchema(Util::createJoinSchema(window1, window2, joinFieldName1), window3, joinFieldName1), window4, joinFieldName4);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const auto windowSlide = 500UL;
    const std::string fileNameBuffers1("window.csv");
    const std::string fileNameBuffers2("window2.csv");
    const std::string fileNameBuffers3("window4.csv");
    const std::string fileNameBuffers4("window4.csv");
    const std::string fileNameBuffersSink("window_sink11.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto firstBuffer = fillBuffer(fileNameBuffers1, executionEngine->getBuffer(window1), window1, bufferManager);
    auto secondBuffer = fillBuffer(fileNameBuffers2, executionEngine->getBuffer(window2), window2, bufferManager);
    auto thirdBuffer = fillBuffer(fileNameBuffers3, executionEngine->getBuffer(window3), window3, bufferManager);
    auto fourthBuffer = fillBuffer(fileNameBuffers4, executionEngine->getBuffer(window4), window4, bufferManager);
    auto expectedSinkBuffer = fillBuffer(fileNameBuffersSink, executionEngine->getBuffer(joinSchema), joinSchema, bufferManager);

    auto testSink = executionEngine->createDataSink(joinSchema, 2);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptor1 = executionEngine->createDataSource(window1);
    auto testSourceDescriptor2 = executionEngine->createDataSource(window2);
    auto testSourceDescriptor3 = executionEngine->createDataSource(window3);
    auto testSourceDescriptor4 = executionEngine->createDataSource(window4);

    auto query = TestQuery::from(testSourceDescriptor1)
                     .joinWith(TestQuery::from(testSourceDescriptor2)).where(Attribute(joinFieldName1))
                     .equalsTo(Attribute(joinFieldName2)).window(SlidingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize), Milliseconds(windowSlide)))
                     .joinWith(TestQuery::from(testSourceDescriptor3)).where(Attribute(joinFieldName1))
                     .equalsTo(Attribute(joinFieldName3)).window(SlidingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize), Milliseconds(windowSlide)))
                     .joinWith(TestQuery::from(testSourceDescriptor4)).where(Attribute(joinFieldName1))
                     .equalsTo(Attribute(joinFieldName4)).window(SlidingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize), Milliseconds(windowSlide)))
                     .sink(testSinkDescriptor);

    NES_INFO("Submitting query: " << query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto source1 = executionEngine->getDataSource(queryPlan, 0);
    auto source2 = executionEngine->getDataSource(queryPlan, 1);
    auto source3 = executionEngine->getDataSource(queryPlan, 2);
    auto source4 = executionEngine->getDataSource(queryPlan, 3);
    ASSERT_TRUE(!!source1);
    ASSERT_TRUE(!!source2);
    ASSERT_TRUE(!!source3);
    ASSERT_TRUE(!!source4);

    source1->emitBuffer(firstBuffer);
    source2->emitBuffer(secondBuffer);
    source3->emitBuffer(thirdBuffer);
    source4->emitBuffer(fourthBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 2);
    auto resultBuffer = mergeBuffers(testSink->resultBuffers, joinSchema, bufferManager);

    NES_DEBUG("resultBuffer: " << NES::Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
    NES_DEBUG("expectedSinkBuffer: " << NES::Util::printTupleBufferAsCSV(expectedSinkBuffer.getBuffer(), joinSchema));

    ASSERT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    ASSERT_TRUE(checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffer.getBuffer(), joinSchema->getSchemaSizeInBytes()));
}

// TODO this test can be enabled once #3353 is merged
TEST_P(MultipleJoinsTest, DISABLED_testJoin3WithDifferentSourceSlidingWindowOnCoodinatorNested) {
    const auto window1 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window1$win1", BasicType::UINT64)
                             ->addField("window1$id1", BasicType::UINT64)
                             ->addField("window1$timestamp", BasicType::UINT64);

    const auto window2 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window2$win2", BasicType::UINT64)
                             ->addField("window2$id2", BasicType::UINT64)
                             ->addField("window2$timestamp", BasicType::UINT64);

    const auto window3 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window3$win3", BasicType::UINT64)
                             ->addField("window3$id3", BasicType::UINT64)
                             ->addField("window3$timestamp", BasicType::UINT64);

    const auto window4 = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                             ->addField("window4$win4", BasicType::UINT64)
                             ->addField("window4$id4", BasicType::UINT64)
                             ->addField("window4$timestamp", BasicType::UINT64);

    const auto joinFieldName1 = "window1$id1";
    const auto joinFieldName2 = "window2$id2";
    const auto joinFieldName3 = "window3$id3";
    const auto joinFieldName4 = "window4$id4";
    const auto timeStampField = "timestamp";

    const auto joinSchema = Util::createJoinSchema(Util::createJoinSchema(Util::createJoinSchema(window1, window2, joinFieldName1), window3, joinFieldName1), window4, joinFieldName4);

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const auto windowSlide = 500UL;
    const std::string fileNameBuffers1("window.csv");
    const std::string fileNameBuffers2("window2.csv");
    const std::string fileNameBuffers3("window4.csv");
    const std::string fileNameBuffers4("window4.csv");
    const std::string fileNameBuffersSink("window_sink12.csv");

    auto bufferManager = executionEngine->getBufferManager();
    auto firstBuffer = fillBuffer(fileNameBuffers1, executionEngine->getBuffer(window1), window1, bufferManager);
    auto secondBuffer = fillBuffer(fileNameBuffers2, executionEngine->getBuffer(window2), window2, bufferManager);
    auto thirdBuffer = fillBuffer(fileNameBuffers3, executionEngine->getBuffer(window3), window3, bufferManager);
    auto fourthBuffer = fillBuffer(fileNameBuffers4, executionEngine->getBuffer(window4), window4, bufferManager);
    auto expectedSinkBuffer = fillBuffer(fileNameBuffersSink, executionEngine->getBuffer(joinSchema), joinSchema, bufferManager);

    auto testSink = executionEngine->createDataSink(joinSchema, 2);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto testSourceDescriptor1 = executionEngine->createDataSource(window1);
    auto testSourceDescriptor2 = executionEngine->createDataSource(window2);
    auto testSourceDescriptor3 = executionEngine->createDataSource(window3);
    auto testSourceDescriptor4 = executionEngine->createDataSource(window4);

    // TODO nested joins are currently not supported by NAUTILUS
    auto query = TestQuery::from(testSourceDescriptor1)
                     .joinWith(TestQuery::from(testSourceDescriptor2)).where(Attribute(joinFieldName1)).equalsTo(Attribute(joinFieldName2))
                     .window(SlidingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize), Milliseconds(windowSlide)))
                     /*.joinWith(TestQuery::from(testSourceDescriptor3)).joinWith(TestQuery::from(testSourceDescriptor4)).where(Attribute(joinFieldName3)).equalsTo(Attribute(joinFieldName4))
                     .window(SlidingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize), Milliseconds(windowSlide)))
                     .where(Attribute(joinFieldName1)).equalsTo(Attribute(joinFieldName4)).window(SlidingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize), Milliseconds(windowSlide)))*/
                     .sink(testSinkDescriptor);

    NES_INFO("Submitting query: " << query.getQueryPlan()->toString())
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    auto source1 = executionEngine->getDataSource(queryPlan, 0);
    auto source2 = executionEngine->getDataSource(queryPlan, 1);
    auto source3 = executionEngine->getDataSource(queryPlan, 2);
    auto source4 = executionEngine->getDataSource(queryPlan, 3);
    ASSERT_TRUE(!!source1);
    ASSERT_TRUE(!!source2);
    ASSERT_TRUE(!!source3);
    ASSERT_TRUE(!!source4);

    source1->emitBuffer(firstBuffer);
    source2->emitBuffer(secondBuffer);
    source3->emitBuffer(thirdBuffer);
    source4->emitBuffer(fourthBuffer);
    testSink->waitTillCompleted();

    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 2);
    auto resultBuffer = mergeBuffers(testSink->resultBuffers, joinSchema, bufferManager);

    NES_DEBUG("resultBuffer: " << NES::Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
    NES_DEBUG("expectedSinkBuffer: " << NES::Util::printTupleBufferAsCSV(expectedSinkBuffer.getBuffer(), joinSchema));

    ASSERT_EQ(resultBuffer.getNumberOfTuples(), expectedSinkBuffer.getNumberOfTuples());
    ASSERT_TRUE(checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffer.getBuffer(), joinSchema->getSchemaSizeInBytes()));
}

TEST_F(MultipleJoinsTest, testJoins2WithDifferentSourceSlidingWindowOnCoodinator) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    std::string window =
        R"(Schema::create()->addField(createField("win1", BasicType::UINT64))->addField(createField("id1", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window1", window);

    std::string window2 =
        R"(Schema::create()->addField(createField("win2", BasicType::UINT64))->addField(createField("id2", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window2", window2);

    std::string window3 =
        R"(Schema::create()->addField(createField("win3", BasicType::UINT64))->addField(createField("id3", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window3", window3);

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    auto physicalSource1 = PhysicalSource::create("window1", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    auto csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    auto physicalSource2 = PhysicalSource::create("window2", "test_stream", csvSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MultipleJoinsTest: Worker2 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->coordinatorPort = port;
    auto csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    auto physicalSource3 = PhysicalSource::create("window3", "test_stream", csvSourceType3);
    workerConfig3->physicalSources.add(physicalSource3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("MultipleJoinsTest: Worker3 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testTwoJoinsWithDifferentStreamSlidingWindowOnCoodinator.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("MultipleJoinsTest: Submit query");

    string query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .joinWith(Query::from("window3")).where(Attribute("id1")).equalsTo(Attribute("id3")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,window1window2$"
        "start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$"
        "timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:INTEGER,window3$id3:"
        "INTEGER,window3$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n"
        "1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300\n"
        "500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n"
        "500,1500,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}

/**
 * @brief This tests just outputs the default source for a hierarchy with one relay which also produces data by itself
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0] => Join 2
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0] => Join 1
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(MultipleJoinsTest, DISABLED_testJoin2WithDifferentSourceSlidingWindowDistributed) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    std::string window =
        R"(Schema::create()->addField(createField("win1", BasicType::UINT64))->addField(createField("id1", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window1", window);

    std::string window2 =
        R"(Schema::create()->addField(createField("win2", BasicType::UINT64))->addField(createField("id2", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window2", window2);

    std::string window3 =
        R"(Schema::create()->addField(createField("win3", BasicType::UINT64))->addField(createField("id3", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window3", window3);
    NES_DEBUG("MultipleJoinsTest: Coordinator started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    auto csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    auto physicalSource2 = PhysicalSource::create("window2", "test_stream", csvSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(1, 2);
    NES_INFO("MultipleJoinsTest: Worker2 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->coordinatorPort = port;
    auto csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    auto physicalSource3 = PhysicalSource::create("window3", "test_stream", csvSourceType3);
    workerConfig3->physicalSources.add(physicalSource3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    NES_INFO("MultipleJoinsTest: Worker3 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 4");
    WorkerConfigurationPtr workerConfig4 = WorkerConfiguration::create();
    workerConfig4->coordinatorPort = port;
    auto csvSourceType4 = CSVSourceType::create();
    csvSourceType4->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType4->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType4->setNumberOfBuffersToProduce(2);
    auto physicalSource4 = PhysicalSource::create("window3", "test_stream", csvSourceType4);
    workerConfig4->physicalSources.add(physicalSource4);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    wrk4->replaceParent(1, 2);
    NES_INFO("MultipleJoinsTest: Worker4 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testTwoJoinsWithDifferentStreamSlidingWindowDistributed.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("MultipleJoinsTest: Submit query");

    string query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .joinWith(Query::from("window3")).where(Attribute("id1")).equalsTo(Attribute("id3")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,window1window2$"
        "start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$"
        "timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:INTEGER,window3$id3:"
        "INTEGER,window3$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n"
        "1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300\n"
        "500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n"
        "500,1500,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}

TEST_F(MultipleJoinsTest, testJoin3WithDifferentSourceSlidingWindowOnCoodinatorSequential) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    std::string window =
        R"(Schema::create()->addField(createField("win1", BasicType::UINT64))->addField(createField("id1", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window1", window);

    std::string window2 =
        R"(Schema::create()->addField(createField("win2", BasicType::UINT64))->addField(createField("id2", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window2", window2);

    std::string window3 =
        R"(Schema::create()->addField(createField("win3", BasicType::UINT64))->addField(createField("id3", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window3", window3);

    std::string window4 =
        R"(Schema::create()->addField(createField("win4", BasicType::UINT64))->addField(createField("id4", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window4", window4);
    NES_DEBUG("MultipleJoinsTest: Coordinator started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    auto physicalSource1 = PhysicalSource::create("window1", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    auto csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    auto physicalSource2 = PhysicalSource::create("window2", "test_stream", csvSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MultipleJoinsTest: Worker2 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->coordinatorPort = port;
    auto csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    auto physicalSource3 = PhysicalSource::create("window3", "test_stream", csvSourceType3);
    workerConfig3->physicalSources.add(physicalSource3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("MultipleJoinsTest: Worker3 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 4");
    WorkerConfigurationPtr workerConfig4 = WorkerConfiguration::create();
    workerConfig4->coordinatorPort = port;
    auto csvSourceType4 = CSVSourceType::create();
    csvSourceType4->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType4->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType4->setNumberOfBuffersToProduce(2);
    auto physicalSource4 = PhysicalSource::create("window4", "test_stream", csvSourceType4);
    workerConfig4->physicalSources.add(physicalSource4);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_INFO("MultipleJoinsTest: Worker4 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testJoin4WithDifferentStreamSlidingWindowOnCoodinator.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("MultipleJoinsTest: Submit query");

    string query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .joinWith(Query::from("window3")).where(Attribute("id1")).equalsTo(Attribute("id3")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .joinWith(Query::from("window4")).where(Attribute("id1")).equalsTo(Attribute("id4")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3window4$start:INTEGER,window1window2window3window4$end:INTEGER,window1window2window3window4$key:"
        "INTEGER,window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,"
        "window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:"
        "INTEGER,window1$timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:"
        "INTEGER,window3$id3:INTEGER,window3$timestamp:INTEGER,window4$win4:INTEGER,window4$id4:INTEGER,window4$timestamp:"
        "INTEGER\n"
        "1000,2000,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "1000,2000,12,1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "1000,2000,12,500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "1000,2000,12,500,1500,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "500,1500,12,1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "500,1500,12,1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "500,1500,12,500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "500,1500,12,500,1500,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}

TEST_F(MultipleJoinsTest, testJoin3WithDifferentSourceSlidingWindowOnCoodinatorNested) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    std::string window =
        R"(Schema::create()->addField(createField("win1", BasicType::UINT64))->addField(createField("id1", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window1", window);

    std::string window2 =
        R"(Schema::create()->addField(createField("win2", BasicType::UINT64))->addField(createField("id2", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window2", window2);

    std::string window3 =
        R"(Schema::create()->addField(createField("win3", BasicType::UINT64))->addField(createField("id3", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window3", window3);

    std::string window4 =
        R"(Schema::create()->addField(createField("win4", BasicType::UINT64))->addField(createField("id4", BasicType::UINT64))->addField(createField("timestamp", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window4", window4);
    NES_DEBUG("MultipleJoinsTest: Coordinator started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    auto physicalSource1 = PhysicalSource::create("window1", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    auto csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    auto physicalSource2 = PhysicalSource::create("window2", "test_stream", csvSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MultipleJoinsTest: Worker2 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->coordinatorPort = port;
    auto csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    auto physicalSource3 = PhysicalSource::create("window3", "test_stream", csvSourceType3);
    workerConfig3->physicalSources.add(physicalSource3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("MultipleJoinsTest: Worker3 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 4");
    WorkerConfigurationPtr workerConfig4 = WorkerConfiguration::create();
    workerConfig4->coordinatorPort = port;
    auto csvSourceType4 = CSVSourceType::create();
    csvSourceType4->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType4->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType4->setNumberOfBuffersToProduce(2);
    auto physicalSource4 = PhysicalSource::create("window4", "test_stream", csvSourceType4);
    workerConfig4->physicalSources.add(physicalSource4);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_INFO("MultipleJoinsTest: Worker4 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testJoin4WithDifferentStreamSlidingWindowOnCoodinator.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("MultipleJoinsTest: Submit query");

    auto query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .joinWith((Query::from("window3")).joinWith(Query::from("window4")).where(Attribute("id3")).equalsTo(Attribute("id4")).window(SlidingWindow::of(EventTime(Attribute("timestamp"))
        ,Seconds(1),Milliseconds(500)))).where(Attribute("id1")).equalsTo(Attribute("id4")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3window4$start:INTEGER,window1window2window3window4$end:INTEGER,window1window2window3window4$key:"
        "INTEGER,window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$"
        "id1:INTEGER,window1$timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3window4$"
        "start:INTEGER,window3window4$end:INTEGER,window3window4$key:INTEGER,window3$win3:INTEGER,window3$id3:INTEGER,window3$"
        "timestamp:INTEGER,window4$win4:INTEGER,window4$id4:INTEGER,window4$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,1000,2000,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,500,1500,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,1000,2000,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,500,1500,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1102,1000,2000,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1102,500,1500,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1112,1000,2000,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1112,500,1500,4,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1102,1000,2000,4,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1102,500,1500,4,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1112,1000,2000,4,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1112,500,1500,4,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1102,1000,2000,4,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1102,500,1500,4,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1112,1000,2000,4,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1112,500,1500,4,4,4,1001,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1000,2000,12,1,12,1300,1,12,1300\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,500,1500,12,1,12,1300,1,12,1300\n"
        "1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1000,2000,12,1,12,1300,1,12,1300\n"
        "1000,2000,12,500,1500,12,1,12,1001,5,12,1011,500,1500,12,1,12,1300,1,12,1300\n"
        "500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1000,2000,12,1,12,1300,1,12,1300\n"
        "500,1500,12,1000,2000,12,1,12,1001,5,12,1011,500,1500,12,1,12,1300,1,12,1300\n"
        "500,1500,12,500,1500,12,1,12,1001,5,12,1011,1000,2000,12,1,12,1300,1,12,1300\n"
        "500,1500,12,500,1500,12,1,12,1001,5,12,1011,500,1500,12,1,12,1300,1,12,1300\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}
}// namespace NES

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

INSTANTIATE_TEST_CASE_P(testStreamJoinQueries,
                        MultipleJoinsTest,
                        ::testing::Values(QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER),
                        [](const testing::TestParamInfo<MultipleJoinsTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(info.param));
                        });
}// namespace NES::Runtime::Execution

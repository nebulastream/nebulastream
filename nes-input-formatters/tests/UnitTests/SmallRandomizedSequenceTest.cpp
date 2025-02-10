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

#include <cstdint>
#include <memory>
#include <ranges>
#include <utility>
#include <vector>
#include <API/Schema.hpp>
#include <InputFormatters/InputFormatterOperatorHandler.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <MemoryLayout/RowLayoutField.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/TestUtil.hpp>
#include <bits/random.h>
#include <BaseUnitTest.hpp>
#include <InputFormatterTestUtil.hpp>
#include <TestTaskQueue.hpp>


namespace NES
{
class SmallRandomizedSequenceTest : public Testing::BaseUnitTest
{
    std::vector<std::filesystem::path> tmpResultFilePaths;
    struct TestFile
    {
        std::string fileName;
        bool endsWithNewline;
        std::vector<InputFormatterTestUtil::TestDataTypes> schemaFieldTypes;
    };

    using enum InputFormatterTestUtil::TestDataTypes;
    std::unordered_map<std::string, TestFile> testFileMap{
        {"TwoIntegerColumns",
         TestFile{.fileName = "TwoIntegerColumns_20_Lines.csv", .endsWithNewline = true, .schemaFieldTypes = {INT32, INT32}}},
        {"Bimbo_1_1000", /// https://github.com/cwida/public_bi_benchmark/blob/master/benchmark/Bimbo/
         TestFile{
             .fileName = "Bimbo_1_1000_Lines.csv",
             .endsWithNewline = false,
             .schemaFieldTypes = {INT16, INT16, INT32, INT16, FLOAT64, INT32, INT16, INT32, INT16, INT16, FLOAT64, INT16}}},
        {"Food_1", /// https://github.com/cwida/public_bi_benchmark/blob/master/benchmark/Food/
         TestFile{
             .fileName = "Food_1_1000_Lines.csv",
             .endsWithNewline = true,
             .schemaFieldTypes = {INT16, INT32, VARSIZED, VARSIZED, INT16, FLOAT64}}},
        {"Spacecraft_Telemetry", /// generated
         TestFile{
             .fileName = "Spacecraft_Telemetry_1000_Lines.csv",
             .endsWithNewline = true,
             .schemaFieldTypes = {INT32, UINT32, BOOLEAN, CHAR, VARSIZED, FLOAT32, FLOAT64}}}};

public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("InputFormatterTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup InputFormatterTest test class.");
    }
    void SetUp() override { BaseUnitTest::SetUp(); }

    void TearDown() override
    {
        for (const auto& tmpResultFilePath : tmpResultFilePaths)
        {
            std::filesystem::remove(tmpResultFilePath);
        }
        BaseUnitTest::TearDown();
    }

    struct TestConfig
    {
        std::string testFileName;
        size_t numberOfIterations;
        size_t numberOfThreads;
        size_t sizeOfRawBuffers;
        TestTaskQueue::ProcessingMode processingMode;
    };
    void runTest(const TestConfig& testConfig)
    {
        const auto currentTestFile = testFileMap.at(testConfig.testFileName);
        const auto schema = InputFormatterTestUtil::createSchema(currentTestFile.schemaFieldTypes);

        const auto testFilePath = std::filesystem::path(INPUT_FORMATTER_TEST_DATA) / currentTestFile.fileName;
        const auto fileSizeInBytes = std::filesystem::file_size(testFilePath);
        const auto NUM_EXPECTED_RAW_BUFFERS = (fileSizeInBytes / testConfig.sizeOfRawBuffers)
            + static_cast<unsigned long>(fileSizeInBytes % testConfig.sizeOfRawBuffers != 0);
        /// Sources sometimes need an extra buffer (reason currently unknown)
        const size_t NUM_REQUIRED_SOURCE_BUFFERS = NUM_EXPECTED_RAW_BUFFERS + 1;

        /// Create vector for result buffers and create emit function to collect buffers from source
        std::vector<NES::Memory::TupleBuffer> rawBuffers;
        rawBuffers.reserve(NUM_EXPECTED_RAW_BUFFERS);
        const auto emitFunction = InputFormatterTestUtil::getEmitFunction(rawBuffers);

        /// Create file source, start it using the emit function, and wait for the file source to fill the result buffer vector
        std::shared_ptr<Memory::BufferManager> sourceBufferPool
            = Memory::BufferManager::create(testConfig.sizeOfRawBuffers, NUM_REQUIRED_SOURCE_BUFFERS);
        const auto fileSource
            = InputFormatterTestUtil::createFileSource(testFilePath, schema, std::move(sourceBufferPool), NUM_REQUIRED_SOURCE_BUFFERS);
        fileSource->start(std::move(emitFunction));
        InputFormatterTestUtil::waitForSource(rawBuffers, NUM_EXPECTED_RAW_BUFFERS);
        ASSERT_EQ(rawBuffers.size(), NUM_EXPECTED_RAW_BUFFERS);
        ASSERT_TRUE(fileSource->stop());

        for (size_t i = 0; i < testConfig.numberOfIterations; ++i)
        {
            auto inputFormatterTask = InputFormatterTestUtil::createInputFormatterTask(schema);
            std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> resultBuffers
                = std::make_shared<std::vector<std::vector<NES::Memory::TupleBuffer>>>(testConfig.numberOfThreads);
            std::shared_ptr<Memory::BufferManager> testBufferManager = Memory::BufferManager::create(schema->getSchemaSizeInBytes(), 1024);
            std::unique_ptr<TestTaskQueue> taskQueue
                = std::make_unique<TestTaskQueue>(testConfig.numberOfThreads, std::move(testBufferManager), resultBuffers);
            // Todo: somehow raw buffers end up in the result
            std::vector<TestablePipelineTask> pipelineTasks;
            pipelineTasks.reserve(NUM_EXPECTED_RAW_BUFFERS);
            for (size_t bufferIdx = 0; auto& rawBuffer : rawBuffers)
            {
                // Todo: assign different worker threads
                const auto currentWorkerThreadId = bufferIdx % testConfig.numberOfThreads;
                const auto currentSequenceNumber = SequenceNumber(bufferIdx + 1);
                rawBuffer.setSequenceNumber(currentSequenceNumber);
                auto pipelineTask
                    = TestablePipelineTask(WorkerThreadId(currentWorkerThreadId), currentSequenceNumber, rawBuffer, inputFormatterTask, {});
                pipelineTasks.emplace_back(std::move(pipelineTask));
                ++bufferIdx;
            }
            // InputFormatterTestUtil::shuffleWithSeed<true>(pipelineTasks, std::nullopt);
            taskQueue->processTasks(std::move(pipelineTasks), testConfig.processingMode);

            /// Combine results and sort
            auto combinedThreadResults = std::ranges::views::join(*resultBuffers);
            std::vector<NES::Memory::TupleBuffer> resultBufferVec(combinedThreadResults.begin(), combinedThreadResults.end());
            std::ranges::sort(
                resultBufferVec.begin(),
                resultBufferVec.end(),
                [](const Memory::TupleBuffer& left, const Memory::TupleBuffer& right)
                {
                    if (left.getSequenceNumber() == right.getSequenceNumber())
                    {
                        return left.getChunkNumber() < right.getChunkNumber();
                    }
                    return left.getSequenceNumber() < right.getSequenceNumber();
                });

            auto resultFilePath
                = std::filesystem::path(INPUT_FORMATTER_TMP_RESULT_DATA) / (std::string("result_") + currentTestFile.fileName);
            std::ofstream out(resultFilePath);
            for (const auto& buffer : resultBufferVec | std::views::take(resultBufferVec.size() - 1))
            {
                auto actualResultTestBuffer = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, schema);
                actualResultTestBuffer.setNumberOfTuples(buffer.getNumberOfTuples());
                out << actualResultTestBuffer.toString(schema, false);
            }
            auto lastActualResultTestBuffer = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(resultBufferVec.back(), schema);
            out << lastActualResultTestBuffer.toString(schema, false, currentTestFile.endsWithNewline);

            out.close();
            /// Destroy task queue first, to assure that it does not hold references to buffers anymore
            ASSERT_TRUE(taskQueue.release());
            ASSERT_TRUE(InputFormatterTestUtil::compareFiles(testFilePath, resultFilePath));
            tmpResultFilePaths.emplace_back(resultFilePath);
        }
    }
};

// Todo:
// - [ ] use InputFormatterTask in SLTs
// - [ ] clean up TestTaskQueue
// - [ ] clean up InputFormatterUtils
// - [ ] clean up CSVInputFormatter (get rid of ProgressTracker and replace with free functions)
//  -> requires different way to handle state


// [ ] 5. new design for InputFormatter (chunk/synchronize/parse)
// [ ] 6. multiple stages per task (allow splitting tasks)

TEST_F(SmallRandomizedSequenceTest, testTwoIntegerColumns)
{
    runTest(
        TestConfig{
            .testFileName = "TwoIntegerColumns",
            .numberOfIterations = 1,
            .numberOfThreads = 8,
            .sizeOfRawBuffers = 16,
            .processingMode = TestTaskQueue::ProcessingMode::ASYNCHRONOUS});
}

TEST_F(SmallRandomizedSequenceTest, testBimboData)
{
    runTest(
        TestConfig{
            .testFileName = "Bimbo_1_1000",
            .numberOfIterations = 1,
            .numberOfThreads = 8,
            .sizeOfRawBuffers = 16,
            .processingMode = TestTaskQueue::ProcessingMode::ASYNCHRONOUS});
}

TEST_F(SmallRandomizedSequenceTest, testFoodData)
{
    runTest(
        TestConfig{
            .testFileName = "Food_1",
            .numberOfIterations = 1,
            .numberOfThreads = 8,
            .sizeOfRawBuffers = 16,
            .processingMode = TestTaskQueue::ProcessingMode::ASYNCHRONOUS});
}

TEST_F(SmallRandomizedSequenceTest, testSpaceCraftTelemetryData)
{
    runTest(
        TestConfig{
            .testFileName = "Spacecraft_Telemetry",
            .numberOfIterations = 1,
            .numberOfThreads = 8,
            .sizeOfRawBuffers = 16,
            .processingMode = TestTaskQueue::ProcessingMode::ASYNCHRONOUS});
}

}

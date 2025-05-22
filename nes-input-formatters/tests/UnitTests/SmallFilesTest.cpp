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

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <memory>
#include <ranges>
#include <utility>
#include <vector>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <MemoryLayout/RowLayoutField.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <BaseUnitTest.hpp>
#include <InputFormatterTestUtil.hpp>
#include <TestTaskQueue.hpp>

namespace NES
{
class SmallFilesTest : public Testing::BaseUnitTest
{
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
             .endsWithNewline = true,
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

    void TearDown() override { BaseUnitTest::TearDown(); }

    struct TestConfig
    {
        std::string testFileName;
        size_t numberOfIterations;
        size_t numberOfThreads;
        size_t sizeOfRawBuffers;
        size_t sizeOfFormattedBuffers;
    };

    void runTest(const TestConfig& testConfig)
    {
        const auto currentTestFile = testFileMap.at(testConfig.testFileName);
        const auto schema = InputFormatterTestUtil::createSchema(currentTestFile.schemaFieldTypes);
        PRECONDITION(
            testConfig.sizeOfFormattedBuffers >= schema->getSchemaSizeInBytes(),
            "The formatted buffer must be large enough to hold at least one tuple.");

        const auto testFilePath = std::filesystem::path(INPUT_FORMATTER_TEST_DATA) / currentTestFile.fileName;
        const auto fileSizeInBytes = std::filesystem::file_size(testFilePath);
        const auto numberOfExpectedRawBuffers = (fileSizeInBytes / testConfig.sizeOfRawBuffers)
            + static_cast<unsigned long>(fileSizeInBytes % testConfig.sizeOfRawBuffers != 0);
        /// Sources sometimes need an extra buffer (reason currently unknown)
        const size_t numberOfRequiredSourceBuffers = numberOfExpectedRawBuffers + 1;

        /// Create vector for result buffers and create emit function to collect buffers from source
        InputFormatterTestUtil::ThreadSafeVector<NES::Memory::TupleBuffer> rawBuffers;
        rawBuffers.reserve(numberOfExpectedRawBuffers);

        /// Create file source, start it using the emit function, and wait for the file source to fill the result buffer vector
        std::shared_ptr<Memory::BufferManager> sourceBufferPool
            = Memory::BufferManager::create(testConfig.sizeOfRawBuffers, numberOfRequiredSourceBuffers);
        const auto fileSource
            = InputFormatterTestUtil::createFileSource(testFilePath, schema, std::move(sourceBufferPool), numberOfRequiredSourceBuffers);
        fileSource->start(InputFormatterTestUtil::getEmitFunction(rawBuffers));
        rawBuffers.waitForSize(numberOfExpectedRawBuffers);
        ASSERT_EQ(rawBuffers.size(), numberOfExpectedRawBuffers);
        ASSERT_EQ(fileSource->tryStop(std::chrono::milliseconds(1000)), Sources::SourceReturnType::TryStopResult::SUCCESS);

        /// We assume that we don't need more than two times the number of buffers to represent the formatted data than we need to represent the raw data
        const auto numberOfRequiredFormattedBuffers = rawBuffers.size() * 2;
        for (size_t i = 0; i < testConfig.numberOfIterations; ++i)
        {
            auto testBufferManager = Memory::BufferManager::create(testConfig.sizeOfFormattedBuffers, numberOfRequiredFormattedBuffers);
            auto inputFormatterTask = InputFormatterTestUtil::createInputFormatterTask(*schema);
            auto resultBuffers = std::make_shared<std::vector<std::vector<NES::Memory::TupleBuffer>>>(testConfig.numberOfThreads);

            std::vector<Runtime::Execution::TestablePipelineTask> pipelineTasks;
            pipelineTasks.reserve(numberOfExpectedRawBuffers);
            rawBuffers.modifyBuffer(
                [&](auto& rawBuffers)
                {
                    for (size_t bufferIdx = 0; auto& rawBuffer : rawBuffers)
                    {
                        const auto currentWorkerThreadId = bufferIdx % testConfig.numberOfThreads;
                        const auto currentSequenceNumber = SequenceNumber(bufferIdx + 1);
                        rawBuffer.setSequenceNumber(currentSequenceNumber);
                        auto pipelineTask = Runtime::Execution::TestablePipelineTask(
                            WorkerThreadId(currentWorkerThreadId), rawBuffer, inputFormatterTask);
                        pipelineTasks.emplace_back(std::move(pipelineTask));
                        ++bufferIdx;
                    }
                });
            auto taskQueue = std::make_unique<Runtime::Execution::MultiThreadedTestTaskQueue>(
                testConfig.numberOfThreads, pipelineTasks, testBufferManager, resultBuffers);
            taskQueue->startProcessing();
            taskQueue->waitForCompletion();

            /// Combine results and soraoeut
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
                const auto currentBufferAsString
                    = actualResultTestBuffer.toString(schema, Memory::MemoryLayouts::TestTupleBuffer::PrintMode::NO_HEADER_END_IN_NEWLINE);
                out << currentBufferAsString;
            }
            if (currentTestFile.endsWithNewline)
            {
                out << Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(resultBufferVec.back(), schema)
                           .toString(schema, Memory::MemoryLayouts::TestTupleBuffer::PrintMode::NO_HEADER_END_IN_NEWLINE);
            }
            else
            {
                out << Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(resultBufferVec.back(), schema)
                           .toString(schema, Memory::MemoryLayouts::TestTupleBuffer::PrintMode::NO_HEADER_END_WITHOUT_NEWLINE);
            }

            out.close();
            ASSERT_TRUE(InputFormatterTestUtil::compareFiles(testFilePath, resultFilePath));
            resultBuffers->clear();
            resultBufferVec.clear();
            testBufferManager->destroy();
        }
    }
};

TEST_F(SmallFilesTest, testTwoIntegerColumns)
{
    runTest(TestConfig{
        .testFileName = "TwoIntegerColumns",
        .numberOfIterations = 1,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 16,
        .sizeOfFormattedBuffers = 4096});
}

TEST_F(SmallFilesTest, testBimboData)
{
    runTest(TestConfig{
        .testFileName = "Bimbo_1_1000",
        .numberOfIterations = 1,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 16,
        .sizeOfFormattedBuffers = 4096});
}

TEST_F(SmallFilesTest, testFoodData)
{
    runTest(TestConfig{
        .testFileName = "Food_1", .numberOfIterations = 1, .numberOfThreads = 8, .sizeOfRawBuffers = 16, .sizeOfFormattedBuffers = 4096});
}

TEST_F(SmallFilesTest, testSpaceCraftTelemetryData)
{
    runTest(
        {.testFileName = "Spacecraft_Telemetry",
         .numberOfIterations = 1,
         .numberOfThreads = 8,
         .sizeOfRawBuffers = 16,
         .sizeOfFormattedBuffers = 4096});
}

}

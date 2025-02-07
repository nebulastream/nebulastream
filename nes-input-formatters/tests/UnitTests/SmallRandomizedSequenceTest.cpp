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
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceProvider.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/TestUtil.hpp>
#include <bits/random.h>
#include <BaseUnitTest.hpp>
#include <InputFormatterTestUtil.hpp>
#include <TestTaskQueue.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/SourceHandle.hpp>


namespace NES
{

class SmallRandomizedSequenceTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("InputFormatterTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup InputFormatterTest test class.");
    }
    void SetUp() override { BaseUnitTest::SetUp(); }

    void TearDown() override { BaseUnitTest::TearDown(); }

    Sources::ParserConfig validateAndFormatParserConfig(const std::unordered_map<std::string, std::string>& parserConfig)
    {
        auto validParserConfig = Sources::ParserConfig{};
        if (const auto parserType = parserConfig.find("type"); parserType != parserConfig.end())
        {
            validParserConfig.parserType = parserType->second;
        }
        else
        {
            throw InvalidConfigParameter("Parser configuration must contain: type");
        }
        if (const auto tupleDelimiter = parserConfig.find("tupleDelimiter"); tupleDelimiter != parserConfig.end())
        {
            /// Todo #651: Add full support for tuple delimiters that are larger than one byte.
            PRECONDITION(tupleDelimiter->second.size() == 1, "We currently do not support tuple delimiters larger than one byte.");
            validParserConfig.tupleDelimiter = tupleDelimiter->second;
        }
        else
        {
            NES_DEBUG("Parser configuration did not contain: tupleDelimiter, using default: \\n");
            validParserConfig.tupleDelimiter = '\n';
        }
        if (const auto fieldDelimiter = parserConfig.find("fieldDelimiter"); fieldDelimiter != parserConfig.end())
        {
            validParserConfig.fieldDelimiter = fieldDelimiter->second;
        }
        else
        {
            NES_DEBUG("Parser configuration did not contain: fieldDelimiter, using default: ,");
            validParserConfig.fieldDelimiter = ",";
        }
        return validParserConfig;
    }

    std::unique_ptr<Sources::SourceHandle> createFileSource(
        const std::string& filePath,
        std::shared_ptr<Schema> schema,
        std::shared_ptr<Memory::BufferManager> sourceBufferPool,
        const int numberOfLocalBuffersInSource)
    {
        std::unordered_map<std::string, std::string> fileSourceConfiguration{{"filePath", filePath}};
        auto validatedSourceConfiguration = Sources::SourceValidationProvider::provide("File", std::move(fileSourceConfiguration));

        const auto sourceDescriptor = Sources::SourceDescriptor(schema, "TestSource", "File", std::move(validatedSourceConfiguration));

        return Sources::SourceProvider::lower(
            NES::OriginId(1), sourceDescriptor, std::move(sourceBufferPool), numberOfLocalBuffersInSource);
    }

    std::shared_ptr<InputFormatters::InputFormatterTask> createInputFormatterTask(std::shared_ptr<Schema> schema)
    {
        std::unordered_map<std::string, std::string> parserConfiguration{
            {"type", "CSV"}, {"tupleDelimiter", "\n"}, {"fieldDelimiter", "|"}};
        auto validatedParserConfiguration = validateAndFormatParserConfig(parserConfiguration);

        std::unique_ptr<InputFormatters::InputFormatter> inputFormatter = InputFormatters::InputFormatterProvider::provideInputFormatter(
            validatedParserConfiguration.parserType,
            schema,
            validatedParserConfiguration.tupleDelimiter,
            validatedParserConfiguration.fieldDelimiter);

        return std::make_shared<InputFormatters::InputFormatterTask>(std::move(inputFormatter));
    }

    void waitForSource(const std::vector<NES::Memory::TupleBuffer>& resultBuffers, const size_t numExpectedBuffers)
    {
        /// Wait for the file source to fill all expected tuple buffers. Timeout after 1 second (it should never take that long).
        const auto timeout = std::chrono::seconds(1);
        const auto startTime = std::chrono::steady_clock::now();
        while (resultBuffers.size() < numExpectedBuffers and (std::chrono::steady_clock::now() - startTime < timeout))
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    auto getEmitFunction(std::vector<NES::Memory::TupleBuffer>& resultBuffers)
    {
        return [&resultBuffers](const OriginId, Sources::SourceReturnType::SourceReturnType returnType)
        {
            if (std::holds_alternative<Sources::SourceReturnType::Data>(returnType))
            {
                resultBuffers.emplace_back(std::get<Sources::SourceReturnType::Data>(returnType).buffer);
            }
            else if (std::holds_alternative<Sources::SourceReturnType::EoS>(returnType))
            {
                NES_DEBUG("Reached EoS in source");
            }
            else
            {
                const auto errorMessage = std::get<Sources::SourceReturnType::Error>(returnType);
                throw errorMessage.ex;
            }
        };
    }

    template <bool DEBUG_LOG>
    void shuffleWithSeed(std::vector<TestablePipelineTask>& pipelineTasks, std::optional<size_t> fixedSeed)
    {
        auto seed = std::chrono::system_clock::now().time_since_epoch().count();
        std::cout << "Using seed: " << seed << std::endl;
        std::mt19937 rng = (fixedSeed.has_value()) ? std::mt19937(fixedSeed.value()) : std::mt19937(seed);
        std::shuffle(pipelineTasks.begin(), pipelineTasks.end(), rng);

        if (DEBUG_LOG)
        {
            for (const auto& pipelineTask : pipelineTasks)
            {
                auto charBuffer = pipelineTask.tupleBuffer.getBuffer();
                std::stringstream stringStream;
                const auto bufferIdx = pipelineTask.tupleBuffer.getSequenceNumber().getRawValue();
                stringStream << "Start Buffer: " << bufferIdx << std::endl;
                for (size_t i = 0; i < pipelineTask.tupleBuffer.getNumberOfTuples(); i++)
                {
                    stringStream << charBuffer[i];
                }
                stringStream << "\nEnd Buffer: " << bufferIdx << std::endl;
                std::cout << stringStream.str();
            }
            std::cout << std::endl;
        }
    }

    bool compareFiles(const std::filesystem::path& file1, const std::filesystem::path& file2) {
        if (std::filesystem::file_size(file1) != std::filesystem::file_size(file2)) {
            NES_ERROR("File sizes do not match: {} vs. {}.", std::filesystem::file_size(file1), std::filesystem::file_size(file2));
            return false;
        }

        std::ifstream f1(file1, std::ifstream::binary);
        std::ifstream f2(file2, std::ifstream::binary);

        return std::equal(
            std::istreambuf_iterator<char>(f1.rdbuf()),
            std::istreambuf_iterator<char>(),
            std::istreambuf_iterator<char>(f2.rdbuf())
        );
    }

    struct TestFile
    {
        std::string fileName;
        size_t fileSizeInBytes;
        bool endsWithNewline;
        std::shared_ptr<Schema> schema;

        size_t getNumRequiredBuffers(const size_t sizeOfBuffer) const
        {
            return (fileSizeInBytes / sizeOfBuffer) + (fileSizeInBytes % sizeOfBuffer != 0);
        }
    };
    using enum InputFormatterTestUtil::TestDataTypes;
    std::unordered_map<std::string, TestFile> testFileMap{
            {"Bimbo_1_1000",
             TestFile{ /// https://github.com/cwida/public_bi_benchmark/blob/master/benchmark/Bimbo/
                 .fileName = "Bimbo_1_1000_Lines.csv",
                 .fileSizeInBytes = 41931,
                 .endsWithNewline = false,
                 .schema = InputFormatterTestUtil::createSchema(
                     {INT16, INT16, INT32, INT16, FLOAT64, INT32, INT16, INT32, INT16, INT16, FLOAT64, INT16})}}};
};


// [x] 1. finish utils refactoring
// [x] 2. enable binding tasks (here rawBuffers) to workerThreadIds
// [x] 3. write a decent basis for tests
// [ ] 4. write seeded randomized test with larger input data (potentially three sizes
//      - also think about how to determine buffer sizes
// ----------------------
// [ ] 5. new design for Informar (chunk/synchronize/parse)
// [ ] 6. multiple stages per task (allow splitting tasks)

TEST_F(SmallRandomizedSequenceTest, testBimboData)
{
    const auto currentTestFile = testFileMap.at("Bimbo_1_1000");
    constexpr size_t NUM_ITERATIONS = 1;
    constexpr size_t NUM_THREADS = 1;
    constexpr size_t BUFFER_SIZE = 16;
    const size_t NUM_EXPECTED_RAW_BUFFERS = currentTestFile.getNumRequiredBuffers(BUFFER_SIZE);
    const size_t NUM_REQUIRED_SOURCE_BUFFERS = NUM_EXPECTED_RAW_BUFFERS + 1;
    // Todo: use filepath instead of string
    const auto theFilePath = std::filesystem::path(INPUT_FORMATTER_TEST_DATA) / currentTestFile.fileName;

    /// Create vector for result buffers and create emit function to collect buffers from source
    std::vector<NES::Memory::TupleBuffer> rawBuffers;
    rawBuffers.reserve(NUM_EXPECTED_RAW_BUFFERS);
    const auto emitFunction = getEmitFunction(rawBuffers);

    /// Create file source, start it using the emit function, and wait for the file source to fill the result buffer vector
    std::shared_ptr<Memory::BufferManager> sourceBufferPool = Memory::BufferManager::create(BUFFER_SIZE, NUM_REQUIRED_SOURCE_BUFFERS);
    const auto fileSource = createFileSource(theFilePath, currentTestFile.schema, std::move(sourceBufferPool), NUM_REQUIRED_SOURCE_BUFFERS);
    fileSource->start(std::move(emitFunction));
    waitForSource(rawBuffers, NUM_EXPECTED_RAW_BUFFERS);
    ASSERT_EQ(rawBuffers.size(), NUM_EXPECTED_RAW_BUFFERS);
    ASSERT_TRUE(fileSource->stop());

    for (size_t i = 0; i < NUM_ITERATIONS; ++i)
    {
        std::cout << "Iteration: " << i << std::endl;
        auto inputFormatterTask = createInputFormatterTask(currentTestFile.schema);
        std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> resultBuffers
            = std::make_shared<std::vector<std::vector<NES::Memory::TupleBuffer>>>(NUM_THREADS);
        std::shared_ptr<Memory::BufferManager> testBufferManager = Memory::BufferManager::create(currentTestFile.schema->getSchemaSizeInBytes(), 1024);
        std::unique_ptr<TestTaskQueue> taskQueue
            = std::make_unique<TestTaskQueue>(NUM_THREADS, std::move(testBufferManager), resultBuffers);
        // Todo: somehow raw buffers end up in the result
        std::vector<TestablePipelineTask> pipelineTasks;
        pipelineTasks.reserve(NUM_EXPECTED_RAW_BUFFERS);
        for (size_t bufferIdx = 0; auto& rawBuffer : rawBuffers)
        {
            // Todo: assign different worker threads
            const auto currentWorkerThreadId = bufferIdx % NUM_THREADS;
            const auto currentSequenceNumber = SequenceNumber(bufferIdx + 1);
            rawBuffer.setSequenceNumber(currentSequenceNumber);
            auto pipelineTask
                = TestablePipelineTask(WorkerThreadId(currentWorkerThreadId), currentSequenceNumber, rawBuffer, inputFormatterTask, {});
            pipelineTasks.emplace_back(std::move(pipelineTask));
            ++bufferIdx;
        }
        taskQueue->processTasks(std::move(pipelineTasks), TestTaskQueue::ProcessingMode::ASYNCHRONOUS);

        /// Combine results and sort
        auto combinedThreadResults = std::ranges::views::join(*resultBuffers);
        std::vector<NES::Memory::TupleBuffer> resultBufferVec(combinedThreadResults.begin(), combinedThreadResults.end());
        // Todo: validate results
        // - order is messed up
        // -> can we write

        std::ranges::sort(resultBufferVec.begin(), resultBufferVec.end(), [](const Memory::TupleBuffer& left, const Memory::TupleBuffer& right)
        {
            if (left.getSequenceNumber() == right.getSequenceNumber())
            {
                return left.getChunkNumber() < right.getChunkNumber();
            }
            return left.getSequenceNumber() < right.getSequenceNumber();
        });

        // Todo: write content of buffers to file
        // - format floats correctly
        // - compare files
        auto resultFilePath = std::filesystem::path(INPUT_FORMATTER_TMP_RESULT_DATA) / (std::string("result_") + currentTestFile.fileName);
        std::ofstream out(resultFilePath);
        for (const auto& buffer : resultBufferVec | std::views::take(resultBufferVec.size() - 1))
        {
            auto actualResultTestBuffer = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, currentTestFile.schema);
            actualResultTestBuffer.setNumberOfTuples(buffer.getNumberOfTuples());
            out << actualResultTestBuffer.toString(currentTestFile.schema, false);
        }
        auto lastActualResultTestBuffer = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(resultBufferVec.back(), currentTestFile.schema);;
        out << lastActualResultTestBuffer.toString(currentTestFile.schema, false, currentTestFile.endsWithNewline);

        out.close();
        /// Destroy task queue first, to assure that it does not hold references to buffers anymore
        taskQueue.release();
        ASSERT_TRUE(compareFiles(theFilePath, resultFilePath));
        // std::filesystem::remove(resultFilePath);
    }
}

TEST_F(SmallRandomizedSequenceTest, testPrintingData)
{
    constexpr size_t NUM_THREADS = 4;
    constexpr size_t BUFFER_SIZE = 16;
    constexpr size_t NUM_EXPECTED_RAW_BUFFERS = 9;
    constexpr size_t NUM_REQUIRED_SOURCE_BUFFERS = 64; //min required to create fixed size pool for sources (numSourceLocalBuffers)

    auto schema = InputFormatterTestUtil::createSchema(
        {InputFormatterTestUtil::TestDataTypes::INT32, InputFormatterTestUtil::TestDataTypes::INT32});

    const auto theFilePath = INPUT_FORMATTER_TEST_DATA + std::string("/twoIntegerColumns.csv");

    /// Create vector for result buffers and create emit function to collect buffers from source
    std::vector<NES::Memory::TupleBuffer> rawBuffers;
    rawBuffers.reserve(NUM_EXPECTED_RAW_BUFFERS);
    const auto emitFunction = getEmitFunction(rawBuffers);

    /// Create file source, start it using the emit function, and wait for the file source to fill the result buffer vector
    std::shared_ptr<Memory::BufferManager> sourceBufferPool = Memory::BufferManager::create(BUFFER_SIZE, NUM_REQUIRED_SOURCE_BUFFERS);
    const auto fileSource = createFileSource(theFilePath, schema, std::move(sourceBufferPool), NUM_REQUIRED_SOURCE_BUFFERS);
    fileSource->start(std::move(emitFunction));
    waitForSource(rawBuffers, NUM_EXPECTED_RAW_BUFFERS);
    ASSERT_EQ(rawBuffers.size(), NUM_EXPECTED_RAW_BUFFERS);

    auto inputFormatterTask = createInputFormatterTask(schema);
    std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> resultBuffers
        = std::make_shared<std::vector<std::vector<NES::Memory::TupleBuffer>>>(NUM_THREADS);
    std::shared_ptr<Memory::BufferManager> testBufferManager = Memory::BufferManager::create(schema->getSchemaSizeInBytes(), 1024);
    std::unique_ptr<TestTaskQueue> taskQueue = std::make_unique<TestTaskQueue>(NUM_THREADS, std::move(testBufferManager), resultBuffers);
    // Todo: somehow,
    std::vector<TestablePipelineTask> pipelineTasks;
    pipelineTasks.reserve(NUM_EXPECTED_RAW_BUFFERS);
    for (size_t bufferIdx = 0; auto& rawBuffer : rawBuffers)
    {
        // Todo: assign different worker threads
        const auto currentWorkerThreadId = bufferIdx % NUM_THREADS;
        const auto currentSequenceNumber = SequenceNumber(bufferIdx + 1);
        rawBuffer.setSequenceNumber(currentSequenceNumber);
        auto pipelineTask = TestablePipelineTask(
            WorkerThreadId(currentWorkerThreadId), currentSequenceNumber, std::move(rawBuffer), inputFormatterTask, {});
        pipelineTasks.emplace_back(std::move(pipelineTask));
        ++bufferIdx;
    }
    // Todo: don't shuffle, but let threads process buffers async (no waiting)
    // shuffleWithSeed<false>(pipelineTasks, std::nullopt); /// 1738768165034398667
    taskQueue->processTasks(std::move(pipelineTasks), TestTaskQueue::ProcessingMode::SEQUENTIAL);

    /// Combine results and sort
    auto combinedThreadResults = std::ranges::views::join(*resultBuffers);
    std::vector<NES::Memory::TupleBuffer> resultBufferVec(combinedThreadResults.begin(), combinedThreadResults.end());

    /// log sorted results
    for (const auto& buffer : resultBufferVec)
    {
        auto actualResultTestBuffer = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, schema);
        actualResultTestBuffer.setNumberOfTuples(buffer.getNumberOfTuples());
        std::cout << "buffer " << buffer.getSequenceNumber() << ": " << actualResultTestBuffer.toString(schema, false);
    }

    /// Destroy task queue first, to assure that it does not hold references to buffers anymore
    taskQueue.release();
    ASSERT_TRUE(fileSource->stop());
}

}

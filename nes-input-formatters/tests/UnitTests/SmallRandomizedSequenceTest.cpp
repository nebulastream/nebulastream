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
#include <vector>
#include <utility>
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
#include "Runtime/BufferManager.hpp"
#include "Sources/SourceHandle.hpp"


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

    std::unique_ptr<Sources::SourceHandle>
    createFileSource(const std::string& filePath, std::shared_ptr<Memory::BufferManager> sourceBufferPool)
    {
        std::unordered_map<std::string, std::string> fileSourceConfiguration{{"filePath", filePath}};
        auto validatedSourceConfiguration = Sources::SourceValidationProvider::provide("File", std::move(fileSourceConfiguration));

        const auto sourceDescriptor
            = Sources::SourceDescriptor(schema, "TestSource", "File", std::move(validatedSourceConfiguration));

        return Sources::SourceProvider::lower(NES::OriginId(1), sourceDescriptor, std::move(sourceBufferPool));
    }

    std::shared_ptr<InputFormatters::InputFormatterTask> createInputFormatterTask()
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
                throw std::logic_error("Return type is not Sources::SourceReturnType::Data");
            }
        };
    }

    template <bool DEBUG_LOG>
    void shuffleWithSeed(std::vector<TestablePipelineTask>& pipelineTasks, std::optional<size_t> fixedSeed) {
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

public:
    std::shared_ptr<Schema> schema = InputFormatterTestUtil::createSchema(
        {InputFormatterTestUtil::TestDataTypes::INT32, InputFormatterTestUtil::TestDataTypes::INT32});
};


// [x] 1. finish utils refactoring
// [x] 2. enable binding tasks (here rawBuffers) to workerThreadIds
// [x] 3. write a decent basis for tests
// [ ] 4. write seeded randomized test with larger input data (potentially three sizes
//      - also think about how to determine buffer sizes
// ----------------------
// [ ] 5. new design for Informar (chunk/synchronize/parse)
// [ ] 6. multiple stages per task (allow splitting tasks)

// Todo:
// - create a test that reads in a file (or an inline text buffer)
// - that chunks that text into buffers using GIVEN BUFFER SIZE
// - that chuffles these buffers using a SEED
// - that formats all buffers in the randomized sequence
// - that checks that the output is overall correct
//      - Todo: how to check correctness?
// - Todo: use sequence queue for processing?

TEST_F(SmallRandomizedSequenceTest, testPrintingData)
{
    // Todo: read data from `.csv` file (using source?)
    // - planning to use: https://github.com/cwida/public_bi_benchmark/blob/master/benchmark/Bimbo/samples/Bimbo_1.sample.csv
    constexpr size_t NUM_THREADS = 1;
    constexpr size_t BUFFER_SIZE = 16;
    constexpr size_t NUM_EXPECTED_RAW_BUFFERS = 9;
    constexpr size_t NUM_REQUIRED_BUFFERS = 64; //min required to create fixed size pool for sources (numSourceLocalBuffers)

    const auto theFilePath = INPUT_FORMATTER_TEST_DATA + std::string("/twoIntegerColumns.csv");

    /// Create vector for result buffers and create emit function to collect buffers from source
    std::vector<NES::Memory::TupleBuffer> rawBuffers;
    rawBuffers.reserve(NUM_EXPECTED_RAW_BUFFERS);
    const auto emitFunction = getEmitFunction(rawBuffers);

    /// Create file source, start it using the emit function, and wait for the file source to fill the result buffer vector

    std::shared_ptr<Memory::BufferManager> sourceBufferPool = Memory::BufferManager::create(BUFFER_SIZE, NUM_REQUIRED_BUFFERS);
    const auto fileSource = createFileSource(theFilePath, std::move(sourceBufferPool));
    fileSource->start(std::move(emitFunction));
    waitForSource(rawBuffers, NUM_EXPECTED_RAW_BUFFERS);
    ASSERT_EQ(rawBuffers.size(), NUM_EXPECTED_RAW_BUFFERS);

    auto inputFormatterTask = createInputFormatterTask();
    std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> resultBuffers = std::make_shared<std::vector<std::vector<NES::Memory::TupleBuffer>>>(NUM_THREADS);
    std::shared_ptr<Memory::BufferManager> testBufferManager = Memory::BufferManager::create(schema->getSchemaSizeInBytes(), 1024);
    std::unique_ptr<TestTaskQueue> taskQueue = std::make_unique<TestTaskQueue>(NUM_THREADS, testBufferManager, resultBuffers);
    // Todo: create task vector from rawBuffers
    std::vector<TestablePipelineTask> pipelineTasks;
    pipelineTasks.reserve(NUM_EXPECTED_RAW_BUFFERS);
    for (size_t bufferIdx = 0; auto& rawBuffer : rawBuffers)
    {
        // Todo: assign different worker threads
        const auto currentSequenceNumber = SequenceNumber(bufferIdx + 1);
        rawBuffer.setSequenceNumber(currentSequenceNumber);
        auto pipelineTask = TestablePipelineTask(WorkerThreadId(0), currentSequenceNumber, std::move(rawBuffer), inputFormatterTask, {});
        pipelineTasks.emplace_back(std::move(pipelineTask));
        ++bufferIdx;
    }
    shuffleWithSeed<false>(pipelineTasks, std::nullopt);
    // shuffleWithSeed<false>(pipelineTasks, 1738768165034398667);
    taskQueue->processTasks(std::move(pipelineTasks));

    for (auto& resultBufferVector : *resultBuffers)
    {
        // Todo: fix setting sequence numbers in InputFormatterTask
        std::ranges::sort(resultBufferVector.begin(), resultBufferVector.end(), [](const Memory::TupleBuffer& left, const Memory::TupleBuffer& right)
        {
            if (left.getSequenceNumber() == right.getSequenceNumber())
            {
                return left.getChunkNumber() < right.getChunkNumber();
            }
            return left.getSequenceNumber() < right.getSequenceNumber();
        });

        for (const auto& buffer : resultBufferVector)
        {
            auto actualResultTestBuffer = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, schema);
            actualResultTestBuffer.setNumberOfTuples(buffer.getNumberOfTuples());
            std::cout << "buffer " << buffer.getSequenceNumber() << ": " << actualResultTestBuffer.toString(schema, false);
        }
    }
    /// Todo: make sure to destroy all data structures that own the BufferManager, before destroying the buffer manager
    taskQueue.release();
    ASSERT_TRUE(fileSource->stop());
}

}

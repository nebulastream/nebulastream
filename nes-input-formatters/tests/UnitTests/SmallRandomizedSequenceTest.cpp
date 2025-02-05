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
#include <BaseUnitTest.hpp>
#include <InputFormatterTestUtil.hpp>
#include <TestTaskQueue.hpp>


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

    std::unique_ptr<Sources::SourceHandle> createFileSource(const std::string& filePath, const size_t sizeOfBuffers, const size_t numberOfRequiredBuffers)
    {
        std::unordered_map<std::string, std::string> fileSourceConfiguration{{"filePath", filePath}};
        std::unordered_map<std::string, std::string> parserConfiguration{{"type", "CSV"}, {"tupleDelimiter", "\n"}, {"fieldDelimiter", "|"}};
        auto validatedSourceConfiguration = Sources::SourceValidationProvider::provide("File", std::move(fileSourceConfiguration));
        auto validatedParserConfiguration = validateAndFormatParserConfig(parserConfiguration);

        const auto schema = InputFormatterTestUtil::createSchema(
            {InputFormatterTestUtil::TestDataTypes::INT32, InputFormatterTestUtil::TestDataTypes::INT32});

        const auto sourceDescriptor = Sources::SourceDescriptor(std::move(schema), "TestSource", "File", std::move(validatedSourceConfiguration));

        std::shared_ptr<Memory::BufferManager> testBufferManager = Memory::BufferManager::create(sizeOfBuffers, numberOfRequiredBuffers);
        return Sources::SourceProvider::lower(NES::OriginId(1), sourceDescriptor, testBufferManager);
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
    constexpr size_t BUFFER_SIZE = 16;
    constexpr size_t NUM_EXPECTED_RAW_BUFFERS = 8;
    constexpr size_t NUM_REQUIRED_BUFFERS = 64; //min required to create fixed size pool for sources (numSourceLocalBuffers)

    const auto theFilePath = INPUT_FORMATTER_TEST_DATA + std::string("/twoIntegerColumns.csv");

    /// Create vector for result buffers and create emit function to collect buffers from source
    std::vector<NES::Memory::TupleBuffer> resultBuffers(NUM_EXPECTED_RAW_BUFFERS);
    const auto emitFunction = getEmitFunction(resultBuffers);

    /// Create file source, start it using the emit function, and wait for the file source to fill the result buffer vector
    const auto fileSource = createFileSource(theFilePath, BUFFER_SIZE, NUM_REQUIRED_BUFFERS);
    fileSource->start(std::move(emitFunction));
    waitForSource(resultBuffers, NUM_EXPECTED_RAW_BUFFERS);

    // Todo:
    // - create parser

    /// Format all result buffers
    for (size_t bufferIdx = 0; const auto& buffer : resultBuffers)
    {
        auto charBuffer = buffer.getBuffer();
        std::stringstream stringStream;
        stringStream << "Start Buffer: " << bufferIdx << std::endl;
        for (size_t i = 0; i < buffer.getNumberOfTuples(); i++)
        {
            stringStream << charBuffer[i];
        }
        stringStream << "\nEnd Buffer: " << bufferIdx++ << std::endl;
        std::cout << stringStream.str();
    }
    std::cout << std::endl;
}

}

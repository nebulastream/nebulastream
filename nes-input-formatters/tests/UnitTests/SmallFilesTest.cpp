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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <memory>
#include <numeric>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <MemoryLayout/RowLayoutField.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/TestUtil.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterTestUtil.hpp>
#include <TestTaskQueue.hpp>
#include "Common/DataTypes/VariableSizedDataType.hpp"
#include "Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp"
#include "Configuration/WorkerConfiguration.hpp"
#include "Util/Common.hpp"

/// NOLINTBEGIN(readability-magic-numbers)
namespace NES
{
class SmallFilesTest : public Testing::BaseUnitTest
{
    struct TestFile
    {
        std::string fileName;
        std::vector<InputFormatterTestUtil::TestDataTypes> schemaFieldTypes;
    };

    using enum InputFormatterTestUtil::TestDataTypes;
    std::unordered_map<std::string, TestFile> testFileMap{
        {"TwoIntegerColumns", TestFile{.fileName = "TwoIntegerColumns", .schemaFieldTypes = {INT32, INT32}}},
        {"Bimbo", /// https://github.com/cwida/public_bi_benchmark/blob/master/benchmark/Bimbo/
         TestFile{
             .fileName = "Bimbo",
             .schemaFieldTypes = {INT16, INT16, INT32, INT16, FLOAT64, INT32, INT16, INT32, INT16, INT16, FLOAT64, INT16}}},
        {"Food", /// https://github.com/cwida/public_bi_benchmark/blob/master/benchmark/Food/
         TestFile{.fileName = "Food", .schemaFieldTypes = {INT16, INT32, VARSIZED, VARSIZED, INT16, FLOAT64}}},
        {"Spacecraft_Telemetry", /// generated
         TestFile{.fileName = "Spacecraft_Telemetry", .schemaFieldTypes = {INT32, UINT32, BOOLEAN, CHAR, VARSIZED, FLOAT32, FLOAT64}}}};

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
        std::string formatterType;
        bool hasSpanningTuples;
        size_t numberOfIterations;
        size_t numberOfThreads;
        size_t sizeOfRawBuffers;
    };

    struct BinaryFileHeader
    {
        uint64_t numberOfBuffers = 0;
        uint64_t sizeOfBuffersInBytes = 4096; //Todo: get from buffer manager?
    };

    struct BinaryBufferHeader
    {
        uint64_t numberOfTuples = 0;
        uint64_t numberChildBuffers = 0;
        uint64_t sequenceNumber = 0;
        uint64_t chunkNumber = 0;
    };

    static std::ofstream createFile(const std::filesystem::path& filepath, bool append)
    {
        /// Ensure the directory exists
        if (const auto parentPath = filepath.parent_path(); not parentPath.empty())
        {
            create_directories(parentPath);
        }

        /// Set up file open mode flags
        std::ios_base::openmode openMode = std::ios::binary;
        openMode |= append ? std::ios::app : std::ios::trunc;

        /// Open the file with appropriate mode
        std::ofstream file(filepath, openMode);
        if (not file)
        {
            throw InvalidConfigParameter("Could not open file: {}", filepath.string());
        }
        return file;
    }

    static void writeFileHeaderToFile(const BinaryFileHeader& binaryFileHeader, const std::filesystem::path& filepath)
    {
        /// create new file and write file header
        auto file = createFile(filepath, false); // Todo: <-- add to enum
        file.write(reinterpret_cast<const char*>(&binaryFileHeader), sizeof(BinaryFileHeader));
        file.close();
    }

    static void writePagedSizeBufferChunkToFile(
        const BinaryBufferHeader& binaryHeader,
        const size_t sizeOfSchemaInBytes,
        const std::span<Memory::TupleBuffer> pagedSizeBufferChunk,
        std::ofstream& file)
    {
        file.write(reinterpret_cast<const char*>(&binaryHeader), sizeof(BinaryBufferHeader)); //Todo: use bit_cast

        for (const auto& buffer : pagedSizeBufferChunk)
        {
            const auto sizeOfBufferInBytes = buffer.getNumberOfTuples() * sizeOfSchemaInBytes;
            file.write(buffer.getBuffer<const char>(), static_cast<std::streamsize>(sizeOfBufferInBytes));
        }
    }

    static std::vector<Memory::TupleBuffer> loadTupleBuffersFromFile(
        Memory::AbstractBufferProvider& bufferProvider,
        const Schema& schema,
        const std::filesystem::path& filepath,
        const std::vector<size_t>& varSizedFieldOffsets)
    {
        const auto fileSize = std::filesystem::file_size(filepath);
        const auto sizeOfSchemaInBytes = schema.getSchemaSizeInBytes();
        if (std::ifstream file(filepath, std::ifstream::binary);
            file.is_open() && fileSize >= sizeof(BinaryBufferHeader)) // Todo: the check does not make sense
        {
            // second arg '_': sizeOfBuffersInBytes
            const auto [numberOfBuffers, _] = [](std::ifstream& file, const std::filesystem::path& filepath)
            {
                BinaryFileHeader tmpFileHeader;
                if (file.read(reinterpret_cast<char*>(&tmpFileHeader), sizeof(BinaryFileHeader)))
                {
                    return tmpFileHeader;
                }
                throw std::runtime_error("Failed to read file header from file: " + filepath.string());
            }(file, filepath);

            std::vector<Memory::TupleBuffer> expectedResultBuffers(numberOfBuffers);
            for (size_t bufferIdx = 0; bufferIdx < numberOfBuffers; ++bufferIdx)
            {
                BinaryBufferHeader bufferHeader; // Todo: combine with below
                file.read(reinterpret_cast<char*>(&bufferHeader), sizeof(BinaryBufferHeader));
                const auto numBytesInBuffer = bufferHeader.numberOfTuples * sizeOfSchemaInBytes;

                auto parentBuffer = bufferProvider.getBufferBlocking();
                file.read(parentBuffer.getBuffer<char>(), numBytesInBuffer);


                for (size_t childBufferIdx = 0; childBufferIdx < bufferHeader.numberChildBuffers; ++childBufferIdx)
                {
                    uint32_t childBufferSize = 0;
                    if (file.read(reinterpret_cast<char*>(&childBufferSize), sizeof(uint32_t)))
                    {
                        if (auto nextChildBuffer = bufferProvider.getUnpooledBuffer(childBufferSize + sizeof(uint32_t)))
                        {
                            nextChildBuffer.value().getBuffer<uint32_t>()[0] = childBufferSize;
                            file.read(nextChildBuffer.value().getBuffer<char>() + sizeof(uint32_t), childBufferSize);
                            const auto newChildBufferIdx = parentBuffer.storeChildBuffer(nextChildBuffer.value());
                            // Todo: childBufferIdx == idxInBuffer?
                            INVARIANT(newChildBufferIdx == childBufferIdx, "Child buffer index does not match");
                            const auto tupleIdx = newChildBufferIdx / varSizedFieldOffsets.size();
                            const size_t varSizedOffsetIdx = newChildBufferIdx % varSizedFieldOffsets.size();
                            const auto varSizedOffset = varSizedFieldOffsets.at(varSizedOffsetIdx) + (tupleIdx * sizeOfSchemaInBytes);
                            *reinterpret_cast<uint32_t*>(parentBuffer.getBuffer() + varSizedOffset) = newChildBufferIdx;
                            continue;
                        }
                        throw std::runtime_error("Failed to get unpooled buffer");
                    }
                    throw std::runtime_error("Failed to read header from file: " + filepath.string());
                }
                parentBuffer.setNumberOfTuples(bufferHeader.numberOfTuples);
                parentBuffer.setSequenceNumber(SequenceNumber(bufferHeader.sequenceNumber));
                parentBuffer.setChunkNumber(ChunkNumber(bufferHeader.chunkNumber));
                expectedResultBuffers.at(bufferIdx) = std::move(parentBuffer);
            }
            file.close();
            return expectedResultBuffers;
        }
        throw InvalidConfigParameter("Invalid filepath: {}", filepath.string());
    }

    static void writePagedSizeTupleBufferChunkToFile(
        std::span<Memory::TupleBuffer> pagedSizeBufferChunk,
        const SequenceNumber::Underlying sequenceNumber,
        const size_t numTuplesInChunk,
        std::ofstream& appendFile,
        const size_t sizeOfSchemaInBytes,
        const std::vector<size_t>& varSizedFieldOffsets)
    {
        const auto numChildBuffers = numTuplesInChunk * varSizedFieldOffsets.size();
        writePagedSizeBufferChunkToFile(
            {.numberOfTuples = numTuplesInChunk,
             .numberChildBuffers = numChildBuffers,
             .sequenceNumber = sequenceNumber,
             .chunkNumber = ChunkNumber::INITIAL},
            sizeOfSchemaInBytes,
            pagedSizeBufferChunk,
            appendFile);

        // Todo: iterate over all buffers, adding the

        if (not varSizedFieldOffsets.empty())
        {
            for (auto& buffer : pagedSizeBufferChunk)
            {
                for (size_t tupleIdx = 0; tupleIdx < buffer.getNumberOfTuples(); ++tupleIdx)
                {
                    for (const auto& varSizedFieldOffset : varSizedFieldOffsets)
                    {
                        const auto currentTupleOffset = tupleIdx * sizeOfSchemaInBytes;
                        const auto currentTupleVarSizedFieldOffset = currentTupleOffset + varSizedFieldOffset;
                        const auto childBufferIdx = *reinterpret_cast<uint32_t*>(buffer.getBuffer() + currentTupleVarSizedFieldOffset);
                        const auto childBufferSize = buffer.loadChildBuffer(childBufferIdx).getBuffer<uint32_t>()[0] + sizeof(uint32_t);

                        const auto childBufferSpan
                            = std::span(buffer.loadChildBuffer(childBufferIdx).getBuffer<const char>(), childBufferSize);
                        appendFile.write(childBufferSpan.data(), static_cast<std::streamsize>(childBufferSpan.size_bytes()));
                    }
                }
            }
        }
    }

    void writeTupleBuffersToFile(
        std::vector<Memory::TupleBuffer>& resultBufferVec,
        const Schema& schema,
        const std::filesystem::path& actualResultFilePath,
        const std::vector<size_t>& varSizedFieldOffsets) const
    {
        TestUtil::sortTupleBuffers(resultBufferVec);
        const auto sizeOfSchemaInBytes = schema.getSchemaSizeInBytes();

        // Todo: replace pair with struct (note that first/left is 1-based)
        const std::vector<std::pair<size_t, size_t>> pagedSizedChunkOffsets
            = [](const std::vector<Memory::TupleBuffer>& resultBufferVec, const size_t sizeOfSchemaInBytes)
        {
            size_t numBytesInNextChunk = 0;
            size_t numTuplesInNextChunk = 0;
            std::vector<std::pair<size_t, size_t>> offsets;
            for (const auto& [bufferIdx, buffer] : resultBufferVec | std::views::enumerate)
            {
                if (const auto sizeOfCurrentBufferInBytes = buffer.getNumberOfTuples() * sizeOfSchemaInBytes;
                    numBytesInNextChunk + sizeOfCurrentBufferInBytes > 4096)
                {
                    offsets.emplace_back(std::make_pair(bufferIdx, numTuplesInNextChunk));
                    numBytesInNextChunk = buffer.getNumberOfTuples() * sizeOfSchemaInBytes;
                    numTuplesInNextChunk = buffer.getNumberOfTuples();
                }
                else
                {
                    numBytesInNextChunk += sizeOfCurrentBufferInBytes;
                    numTuplesInNextChunk += buffer.getNumberOfTuples();
                }
            }
            offsets.emplace_back(resultBufferVec.size(), numTuplesInNextChunk);
            return offsets;
        }(resultBufferVec, sizeOfSchemaInBytes);

        writeFileHeaderToFile({.numberOfBuffers = pagedSizedChunkOffsets.size()}, actualResultFilePath);
        auto appendFile = createFile(actualResultFilePath, true);

        size_t nextChunkStart = 0;
        size_t nextChunkSequenceNumber = SequenceNumber::INITIAL;
        for (const auto& [lastBufferIdxInChunk, numTuplesInChunk] : pagedSizedChunkOffsets)
        {
            const auto nextChunk = std::span(resultBufferVec).subspan(nextChunkStart, lastBufferIdxInChunk - nextChunkStart);
            writePagedSizeTupleBufferChunkToFile(
                nextChunk, nextChunkSequenceNumber, numTuplesInChunk, appendFile, sizeOfSchemaInBytes, varSizedFieldOffsets);
            nextChunkStart = lastBufferIdxInChunk;
            ++nextChunkSequenceNumber;
        }
        appendFile.close();
    }

    std::vector<size_t> getVarSizedFieldOffsets(const Schema& schema) const
    {
        size_t priorFieldOffset = 0;
        std::vector<size_t> varSizedFieldOffsets;
        const auto defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
        for (const auto& field : schema)
        {
            if (Util::instanceOf<VariableSizedDataType>(field->getDataType()))
            {
                varSizedFieldOffsets.emplace_back(priorFieldOffset);
            }
            const auto physicalType = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
            priorFieldOffset += physicalType->size();
        }
        return varSizedFieldOffsets;
    }


    std::optional<std::filesystem::path>
    findFileByName(const std::string& fileNameWithoutExtension, const std::filesystem::path& directory) const
    {
        // Check if directory exists
        if (!std::filesystem::exists(directory) || !std::filesystem::is_directory(directory))
        {
            return std::nullopt;
        }

        // Iterate through directory entries
        for (const auto& entry : std::filesystem::directory_iterator(directory))
        {
            if (entry.is_regular_file())
            {
                // Get the filename without extension (stem)
                if (std::string stem = entry.path().stem().string(); stem == fileNameWithoutExtension)
                {
                    return entry.path();
                }
            }
        }

        return std::nullopt;
    }

    template <bool WriteResultsExpectedFile = false>
    void runTest(const TestConfig& testConfig) const
    {
        const size_t sizeOfFormattedBuffers = Configurations::WorkerConfiguration().bufferSizeInBytes.getValue();
        const auto currentTestFile = testFileMap.at(testConfig.testFileName);
        const auto schema = InputFormatterTestUtil::createSchema(currentTestFile.schemaFieldTypes);
        PRECONDITION(
            sizeOfFormattedBuffers >= schema->getSchemaSizeInBytes(),
            "The formatted buffer must be large enough to hold at least one tuple.");

        const auto testDirPath = std::filesystem::path(INPUT_FORMATTER_TEST_DATA) / testConfig.formatterType;
        const auto testFilePath = [this](TestFile currentTestFile, const std::filesystem::path& testDirPath, std::string_view formatterType)
        {
            if (const auto testFilePath = findFileByName(currentTestFile.fileName, testDirPath))
            {
                return testFilePath.value();
            }
            throw InvalidConfigParameter(
                "Could not find file test file: {}.<file_ending_of_{}>", testDirPath / currentTestFile.fileName, formatterType);
        }(currentTestFile, testDirPath, testConfig.formatterType);

        const auto fileSizeInBytes = file_size(testFilePath);
        const auto numberOfExpectedRawBuffers
            = (fileSizeInBytes / testConfig.sizeOfRawBuffers) + static_cast<uint64_t>(fileSizeInBytes % testConfig.sizeOfRawBuffers != 0);
        /// TODO #774: Sources sometimes need an extra buffer (reason currently unknown)
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
        const auto numberOfRequiredFormattedBuffers = rawBuffers.size() * 2
        const auto varSizedFieldOffsets = getVarSizedFieldOffsets(*schema);
        for (size_t i = 0; i < testConfig.numberOfIterations; ++i)
        {
            auto testBufferManager = Memory::BufferManager::create(sizeOfFormattedBuffers, numberOfRequiredFormattedBuffers);
            auto inputFormatterTask
                = InputFormatterTestUtil::createInputFormatterTask(schema, testConfig.formatterType, testConfig.hasSpanningTuples);
            auto resultBuffers = std::make_shared<std::vector<std::vector<NES::Memory::TupleBuffer>>>(testConfig.numberOfThreads);

            std::vector<Runtime::Execution::TestPipelineTask> pipelineTasks;
            pipelineTasks.reserve(numberOfExpectedRawBuffers);
            rawBuffers.modifyBuffer(
                [&](auto& rawBuffers)
                {
                    for (size_t bufferIdx = 0; auto& rawBuffer : rawBuffers)
                    {
                        const auto currentWorkerThreadId = bufferIdx % testConfig.numberOfThreads;
                        const auto currentSequenceNumber = SequenceNumber(bufferIdx + 1);
                        rawBuffer.setSequenceNumber(currentSequenceNumber);
                        auto pipelineTask
                            = Runtime::Execution::TestPipelineTask(WorkerThreadId(currentWorkerThreadId), rawBuffer, inputFormatterTask);
                        pipelineTasks.emplace_back(std::move(pipelineTask));
                        ++bufferIdx;
                    }
                });
            auto taskQueue = std::make_unique<Runtime::Execution::MultiThreadedTestTaskQueue>(
                testConfig.numberOfThreads, pipelineTasks, testBufferManager, resultBuffers);
            taskQueue->startProcessing();
            taskQueue->waitForCompletion();

            /// Create joined result buffer vector
            auto combinedThreadResults = std::ranges::views::join(*resultBuffers);
            std::vector resultBufferVec(combinedThreadResults.begin(), combinedThreadResults.end());

            /// Load expected buffers
            const auto expectedResultDirPath
                = std::filesystem::path(INPUT_FORMATTER_TEST_DATA) / std::format("Expected/{}.nes", currentTestFile.fileName);

            if constexpr (WriteResultsExpectedFile)
            {
                writeTupleBuffersToFile(resultBufferVec, *schema, expectedResultDirPath, varSizedFieldOffsets);
            }
            auto expectedBuffers = loadTupleBuffersFromFile(*testBufferManager, *schema, expectedResultDirPath, varSizedFieldOffsets);

            /// Compare result and expected buffers
            ASSERT_TRUE(TestUtil::compareTestTupleBuffersOrderSensitive(resultBufferVec, expectedBuffers, schema));

            resultBuffers->clear();
            resultBufferVec.clear();
        }
    }
};


TEST_F(SmallFilesTest, testTwoIntegerColumns)
{
    runTest(
        TestConfig{
            .testFileName = "TwoIntegerColumns",
            .formatterType = "CSV",
            .hasSpanningTuples = true,
            .numberOfIterations = 1,
            .numberOfThreads = 1,
            .sizeOfRawBuffers = 16});
}

TEST_F(SmallFilesTest, testBimboData)
{
    runTest(
        TestConfig{
            .testFileName = "Bimbo",
            .formatterType = "CSV",
            .hasSpanningTuples = true,
            .numberOfIterations = 1,
            .numberOfThreads = 8,
            .sizeOfRawBuffers = 16});
}

TEST_F(SmallFilesTest, testFoodData)
{
    runTest(
        TestConfig{
            .testFileName = "Food",
            .formatterType = "CSV",
            .hasSpanningTuples = true,
            .numberOfIterations = 1,
            .numberOfThreads = 8,
            .sizeOfRawBuffers = 16});
}

TEST_F(SmallFilesTest, testSpaceCraftTelemetryData)
{
    runTest<true>(
        {.testFileName = "Spacecraft_Telemetry",
         .formatterType = "CSV",
         .hasSpanningTuples = true,
         .numberOfIterations = 1,
         .numberOfThreads = 8,
         .sizeOfRawBuffers = 16});
}


/// Simple test that confirms that we forward already formatted buffers without spanning tuples correctly
TEST_F(SmallFilesTest, testTwoIntegerColumnsNoSpanningBinary)
{
    runTest(
        TestConfig{
            .testFileName = "TwoIntegerColumns",
            .formatterType = "Native",
            .hasSpanningTuples = false,
            /// Only one iteration possible, because the InputFormatterTask replaces the number of bytes with the number of tuples in a
            /// raw tuple buffer when the tuple buffer is in 'Native' format and has no spanning tuples
            .numberOfIterations = 1,
            .numberOfThreads = 8,
            .sizeOfRawBuffers = 4096});
}

TEST_F(SmallFilesTest, testBimboDataBinary)
{
    runTest(
        TestConfig{
            .testFileName = "Bimbo",
            .formatterType = "Native",
            .hasSpanningTuples = true,
            .numberOfIterations = 10,
            .numberOfThreads = 8,
            .sizeOfRawBuffers = 128});
}

/// TODO #802: disabled, because we don't handle writing VarSized data (in child buffers) to binary data yet
TEST_F(SmallFilesTest, DISABLED_testFoodDataBinary)
{
    runTest(
        TestConfig{
            .testFileName = "Food",
            .formatterType = "Native",
            .hasSpanningTuples = true,
            .numberOfIterations = 1,
            .numberOfThreads = 1,
            .sizeOfRawBuffers = 4096});
}

/// TODO #802: disabled, because we don't handle writing VarSized data (in child buffers) to binary data yet
TEST_F(SmallFilesTest, DISABLED_testSpaceCraftTelemetryDataBinary)
{
    runTest(
        TestConfig{
            .testFileName = "Spacecraft_Telemetry",
            .formatterType = "Native",
            .hasSpanningTuples = true,
            .numberOfIterations = 1,
            .numberOfThreads = 1,
            .sizeOfRawBuffers = 4096});
}

}
/// NOLINTEND(readability-magic-numbers)

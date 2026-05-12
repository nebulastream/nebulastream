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

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configuration/WorkerConfiguration.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/DataTypes/FixedSizedData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterTestUtil.hpp>
#include <TestTaskQueue.hpp>

#include "InputFormatterValidationProvider.hpp"

namespace NES
{

/// End-to-end test for FIXEDSIZED array support in the NestedJSON formatter.
///
/// Drives a single-threaded pipeline: write JSONL to a temp file -> file source ->
/// NestedJSON input formatter -> tuple buffer. Then reads each output record back
/// through `TupleIterator`, which exercises the FIXEDSIZED branch in
/// `TupleBufferRef::loadValue`. Element-by-element assertions are inlined in the
/// test so we don't depend on the binary `.nes` fixture format.
class FixedSizedArrayJSONTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("FixedSizedArrayJSONTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup FixedSizedArrayJSONTest test class.");
    }

    /// Writes `content` to a fresh temp file in the test fixture's working dir and
    /// returns the absolute path. The file is removed automatically when TearDown runs.
    std::string writeTempJsonFile(const std::string& fileName, const std::string& content) const
    {
        const auto path = std::filesystem::temp_directory_path() / fileName;
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out.write(content.data(), static_cast<std::streamsize>(content.size()));
        out.close();
        return path.string();
    }
};

/// Two records of `{timestamp UINT64, camera_id UINT32, image UINT16[16]}` from the
/// thermal_frames_4x4 fixture. The image arrays are loaded element-by-element via
/// `FixedSizedData::at(i)` and compared against the inlined expected values.
TEST_F(FixedSizedArrayJSONTest, ThermalFrames4x4ParsesAllElements)
{
    constexpr size_t imageElementCount = 16;
    using ImageRow = std::array<uint16_t, imageElementCount>;

    /// Match the content of nes-systests/testdata/small/thermal_frames_4x4.json so
    /// the test stays interpretable when read alongside that fixture, but keep the
    /// expected values inline so the test owns its truth.
    const std::vector<std::pair<uint64_t, ImageRow>> expectedRecords{
        {1714060800000ULL,
         {30100, 30180, 30220, 30150, 30210, 41900, 52400, 30260, 30230, 53100, 65535, 30290, 30170, 30240, 30205, 30130}},
        {1714060800040ULL,
         {30120, 30200, 30240, 30160, 30230, 42150, 52900, 30270, 30250, 53600, 65535, 30310, 30190, 30260, 30225, 30150}}};

    constexpr uint32_t cameraId = 1;

    const std::string jsonContent
        = R"({"timestamp": 1714060800000, "camera_id": 1, "image": [30100, 30180, 30220, 30150, 30210, 41900, 52400, 30260, 30230, 53100, 65535, 30290, 30170, 30240, 30205, 30130]})"
          "\n"
          R"({"timestamp": 1714060800040, "camera_id": 1, "image": [30120, 30200, 30240, 30160, 30230, 42150, 52900, 30270, 30250, 53600, 65535, 30310, 30190, 30260, 30225, 30150]})"
          "\n";

    const auto testFilePath = writeTempJsonFile("FixedSizedArrayJSONTest_thermal.json", jsonContent);

    /// Schema: scalar fields plus a FIXEDSIZED uint16[16] field. Element type and
    /// count are baked in via the new DataType constructor.
    Schema schema{};
    schema.addField("timestamp", DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE});
    schema.addField("camera_id", DataType{DataType::Type::UINT32, DataType::NULLABLE::NOT_NULLABLE});
    schema.addField(
        "image", DataType{DataType::Type::FIXEDSIZED, DataType::NULLABLE::NOT_NULLABLE, DataType::Type::UINT16, imageElementCount});

    /// Load JSONL through the file source. Buffer sized to comfortably hold the file.
    constexpr size_t sizeOfRawBuffers = 4096;
    InputFormatterTestUtil::ThreadSafeVector<TupleBuffer> rawBuffers;
    SourceCatalog sourceCatalog;
    constexpr size_t numberOfRequiredSourceBuffers = 4;
    auto sourceBufferPool = BufferManager::create(sizeOfRawBuffers, numberOfRequiredSourceBuffers);
    const auto [backpressureController, fileSource] = InputFormatterTestUtil::createFileSource(
        sourceCatalog, testFilePath, schema, std::move(sourceBufferPool), numberOfRequiredSourceBuffers);
    fileSource->start(InputFormatterTestUtil::getEmitFunction(rawBuffers));
    rawBuffers.waitForSize(1);
    ASSERT_EQ(fileSource->tryStop(std::chrono::milliseconds(1000)), SourceReturnType::TryStopResult::SUCCESS);

    /// Build a compiled NestedJSON pipeline stage with an Emit operator that pushes
    /// formatted buffers into `resultBuffers`.
    const auto sizeOfFormattedBuffers = WorkerConfiguration().defaultQueryExecution.operatorBufferSize.getValue();
    constexpr size_t numberOfRequiredFormattedBuffers = 8;
    auto testBufferManager = BufferManager::create(sizeOfFormattedBuffers, numberOfRequiredFormattedBuffers);
    const std::unordered_map<std::string, std::string> inputFormatterConfig{{"tuple_delimiter", "\n"}};
    const auto parserConfiguration = InputFormatterValidationProvider::provide("NestedJSON", inputFormatterConfig).value();
    auto testStage = InputFormatterTestUtil::createInputFormatter(
        parserConfiguration, schema, MemoryLayoutType::ROW_LAYOUT, sizeOfFormattedBuffers, /*isCompiled=*/false);

    /// One thread is enough for an in-order assertion; using the multithreaded queue
    /// with a single worker keeps us aligned with how SmallFilesTest drives the pipeline.
    constexpr size_t numberOfThreads = 1;
    auto resultBuffers = std::make_shared<std::vector<std::vector<TupleBuffer>>>(numberOfThreads);
    std::vector<TestPipelineTask> pipelineTasks;
    rawBuffers.modifyBuffer(
        [&pipelineTasks, &testStage](const auto& buffers)
        { std::ranges::for_each(buffers, [&](const auto& rawBuffer) { pipelineTasks.emplace_back(rawBuffer, testStage); }); });

    auto taskQueue = std::make_unique<MultiThreadedTestTaskQueue>(numberOfThreads, pipelineTasks, testBufferManager, resultBuffers);
    taskQueue->startProcessing();
    taskQueue->waitForCompletion();

    /// Flatten the per-thread result buffers into one ordered stream.
    auto combined = std::ranges::views::join(*resultBuffers);
    std::vector<TupleBuffer> resultBufferVec(combined.begin(), combined.end());
    InputFormatterTestUtil::sortTupleBuffers(resultBufferVec);
    ASSERT_FALSE(resultBufferVec.empty()) << "expected at least one result buffer";

    /// Read records back. `bufferRef->readRecord` dispatches to `loadValue`, which now
    /// has a FIXEDSIZED branch that resolves the indirect slot to a `FixedSizedData`.
    InputFormatterTestUtil::TupleIterator iterator(std::move(resultBufferVec), schema, MemoryLayoutType::ROW_LAYOUT);

    size_t recordIdx = 0;
    while (const auto recordOpt = iterator.getNextTuple())
    {
        ASSERT_LT(recordIdx, expectedRecords.size()) << "Got more records than expected";
        const auto& [expectedTimestamp, expectedImage] = expectedRecords[recordIdx];

        const auto timestampVal = recordOpt->read("timestamp");
        EXPECT_EQ(timestampVal.getRawValueAs<nautilus::val<uint64_t>>(), expectedTimestamp);

        const auto cameraIdVal = recordOpt->read("camera_id");
        EXPECT_EQ(cameraIdVal.getRawValueAs<nautilus::val<uint32_t>>(), cameraId);

        const auto imageVarVal = recordOpt->read("image");
        const auto imageArr = imageVarVal.getRawValueAs<FixedSizedData>();
        ASSERT_EQ(imageArr.getNumElements(), imageElementCount);
        ASSERT_EQ(imageArr.getElementType(), DataType::Type::UINT16);

        for (size_t i = 0; i < imageElementCount; ++i)
        {
            const auto element = imageArr.at(nautilus::val<uint64_t>(i));
            EXPECT_EQ(element.getRawValueAs<nautilus::val<uint16_t>>(), expectedImage[i])
                << "record " << recordIdx << " image[" << i << "] mismatch";
        }
        ++recordIdx;
    }
    EXPECT_EQ(recordIdx, expectedRecords.size()) << "Got fewer records than expected";

    std::filesystem::remove(testFilePath);
}

}

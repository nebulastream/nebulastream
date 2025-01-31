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
#include <API/Schema.hpp>
#include <InputFormatters/InputFormatterOperatorHandler.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <MemoryLayout/RowLayoutField.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/TestUtil.hpp>
#include <BaseUnitTest.hpp>
#include <TestTaskQueue.hpp>
#include "InputFormatterTestUtil.hpp" //Todo
#include <Sources/SourceProvider.hpp>
#include <Sources/SourceValidationProvider.hpp>


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
    std::cout << INPUT_FORMATTER_TEST_DATA << std::endl;
    constexpr size_t BUFFER_SIZE = 16;
    constexpr size_t NUM_REQUIRED_BUFFERS = 32;

    const auto theFilePath = INPUT_FORMATTER_TEST_DATA + std::string("smallRandomizedSequence.txt");
    std::unordered_map<std::string, std::string> fileSourceConfiguration {{"filePath", theFilePath}};
    const auto validatedConfiguration = Sources::SourceValidationProvider::provide("File", std::move(fileSourceConfiguration));
    // Todo: remove parser config, not needed anymore
    const auto sourceDescriptor = Sources::SourceDescriptor(
        std::move(schema), "TestSource", "File", std::move(validatedConfiguration), std::move(validSourceConfig));

    std::shared_ptr<Memory::BufferManager> testBufferManager
        = Memory::BufferManager::create(BUFFER_SIZE, NUM_REQUIRED_BUFFERS);
    // Sources::SourceDescriptor testDescriptor =
    /// OriginId originId, const SourceDescriptor& sourceDescriptor, std::shared_ptr<NES::Memory::AbstractPoolProvider> bufferPool
    auto csvSource = Sources::SourceProvider::lower(NES::OriginId(0), validatedConfiguration, testBufferManager);
//    using namespace InputFormatterTestUtil;
//    using enum TestDataTypes;
//    using TestTuple = std::tuple<int32_t, int32_t>;
//    runTest<TestTuple>(TestConfig<TestTuple>{
//        .numRequiredBuffers = 3, /// 2 buffer for raw data, 1 buffer for results
//        .numThreads = 1,
//        .bufferSize = 16,
//        .parserConfig = {.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","},
//        .testSchema = {INT32, INT32},
//        .expectedResults = {WorkerThreadResults<TestTuple>{0, {{TestTuple(123456789, 123456789)}}}},
//        .rawBytesPerThread = {/* buffer 1 */ {SequenceNumber(1), WorkerThreadId(0), "123456789,123456"},
//                              /* buffer 2 */ {SequenceNumber(2), WorkerThreadId(0), "789"}}});
}

}

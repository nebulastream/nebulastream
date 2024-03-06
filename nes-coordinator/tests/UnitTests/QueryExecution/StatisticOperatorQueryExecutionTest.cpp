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

#include <BaseIntegrationTest.hpp>
#include <Operators/LogicalOperators/Sinks/StatisticStorageSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Statistics/WindowStatisticDescriptorFactory.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <StatisticFieldIdentifiers.hpp>
#include <Statistics/CountMin.hpp>
#include <Statistics/Interval.hpp>
#include <Statistics/StatisticCollectorStorage.hpp>
#include <Statistics/StatisticManager/StatisticManager.hpp>
#include <Util/StatisticCollectorIdentifier.hpp>
#include <Util/StatisticUtil.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/TestSourceDescriptor.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <gmock/gmock-matchers.h>

namespace NES::Runtime::Execution {

constexpr auto queryCompilerDumpMode = NES::QueryCompilation::DumpMode::NONE;

class BuildStatisticQueryExecutionTest
    : public Testing::BaseUnitTest {
  public:
    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("BuildStatisticQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("QueryExecutionTest: Setup BuildStatisticQueryExecutionTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO("QueryExecutionTest: Setup BuildStatisticQueryExecutionTest test class.");
        Testing::BaseUnitTest::SetUp();
        const auto numWorkerThreads = 1;
        executionEngine = std::make_shared<Testing::TestExecutionEngine>(queryCompilerDumpMode, numWorkerThreads);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("QueryExecutionTest: Tear down BuildStatisticQueryExecutionTest test case.");
        EXPECT_TRUE(executionEngine->stop());
        Testing::BaseUnitTest::TearDown();
    }

    void runQueryWithCsvFiles(const std::vector<std::pair<SchemaPtr, std::string>>& inputs, const Query& query) {

        // Creating the input buffers
        auto bufferManager = executionEngine->getBufferManager();

        std::vector<std::vector<Runtime::TupleBuffer>> allInputBuffers;
        allInputBuffers.reserve(inputs.size());
        for (auto [inputSchema, fileNameInputBuffers] : inputs) {
            allInputBuffers.emplace_back(
                TestUtils::createExpectedBuffersFromCsv(fileNameInputBuffers, inputSchema, bufferManager));
        }

        // Creating query and submitting it to the execution engine
        NES_INFO("Submitting query: {}", query.getQueryPlan()->toString())
        auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());

        // Emitting the input buffers
        auto dataSourceCount = 0_u64;
        for (const auto& inputBuffers : allInputBuffers) {
            auto source = executionEngine->getDataSource(queryPlan, dataSourceCount);
            dataSourceCount++;
            for (auto buf : inputBuffers) {
                buf.setWatermark(6000);
                source->emitBuffer(buf);
            }
        }

        // Giving the execution engine time to process the tuples, so that we do not just test our terminate() implementation
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        // Stopping query and waiting until the test sink has received the expected number of tuples
        NES_INFO("Stopping query now!!!");
        EXPECT_TRUE(executionEngine->stopQuery(queryPlan, Runtime::QueryTerminationType::Graceful));
    }

    void runSingleStatisticBuildQuery(const std::string& logicalSourceName,
                                      const std::vector<std::string>& physicalSourceNames,
                                      const std::string& fieldName,
                                      const std::string& timestampFieldName,
                                      const uint64_t windowSize,
                                      const uint64_t slideFactor,
                                      const std::string& inputDataCsvFile,
                                      SchemaPtr inputSchema) {

        // Getting the expected output tuples
        auto bufferManager = executionEngine->getBufferManager();

        const auto testSourceDescriptor =
            executionEngine->createDataSource(inputSchema, logicalSourceName, physicalSourceNames[0]);
        auto windowStatisticDesc =
            NES::Experimental::Statistics::WindowStatisticDescriptorFactory::createCountMinDescriptor(logicalSourceName,
                                                                                                      fieldName,
                                                                                                      timestampFieldName,
                                                                                                      3,
                                                                                                      windowSize,
                                                                                                      slideFactor,
                                                                                                      8);

        auto statisticSinkDesc = Experimental::Statistics::StatisticStorageSinkDescriptor::create(
            Experimental::Statistics::StatisticCollectorType::COUNT_MIN,
            logicalSourceName);
        auto cmSinkDesc = std::dynamic_pointer_cast<Experimental::Statistics::StatisticStorageSinkDescriptor>(statisticSinkDesc);

        auto query = TestQuery::from(testSourceDescriptor).buildStatistic(windowStatisticDesc).sink(cmSinkDesc);

        inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator();

        // Running the query
        runQueryWithCsvFiles({{inputSchema, inputDataCsvFile}}, query);
    }
};

TEST_F(BuildStatisticQueryExecutionTest, buildCMSketchExecutionTestCsvFile) {

    std::string logicalSourceName = "defaultLogicalSourceName";
    std::vector<std::string> physicalSourceNames = {"physicalSourceName"};
    auto onField = "defaultLogicalSourceName$f1";
    auto timestampField = "defaultLogicalSourceName$ts";
    auto physicalSourceNameField = logicalSourceName + "$" + physicalSourceNames[0];
    const uint64_t depth = 3;
    const uint64_t width = 8;
    const uint64_t startTime = 0;
    const uint64_t endTime = 5000;
    const uint64_t windowSize = 5000;
    const uint64_t slideFactor = 5000;
    auto statisticCollectorType = NES::Experimental::Statistics::StatisticCollectorType::COUNT_MIN;
    auto observedTuples = 5;
    auto inputFileName = "countMinInput.csv";
    auto resultFileName = "countmin.csv";

    auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                           ->addField(onField, BasicType::UINT64)
                           ->addField(timestampField, BasicType::UINT64)
                           ->addField(physicalSourceNameField, BasicType::TEXT);

    auto outputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                            ->addField(NES::Experimental::Statistics::LOGICAL_SOURCE_NAME, BasicType::TEXT)
                            ->addField(NES::Experimental::Statistics::PHYSICAL_SOURCE_NAME, BasicType::TEXT)
                            ->addField(NES::Experimental::Statistics::FIELD_NAME, BasicType::TEXT)
                            ->addField(NES::Experimental::Statistics::OBSERVED_TUPLES, BasicType::UINT64)
                            ->addField(NES::Experimental::Statistics::DEPTH, BasicType::UINT64)
                            ->addField(NES::Experimental::Statistics::START_TIME, BasicType::UINT64)
                            ->addField(NES::Experimental::Statistics::END_TIME, BasicType::UINT64)
                            ->addField(NES::Experimental::Statistics::DATA, BasicType::TEXT)
                            ->addField(NES::Experimental::Statistics::WIDTH, BasicType::UINT64);
    // load cmInputData
    auto filepath = std::filesystem::path(TEST_DATA_DIRECTORY) / resultFileName;
    auto cmData = NES::Experimental::Statistics::StatisticUtil::readFlattenedVectorFromCsvFile(filepath);
    auto cmString = std::string(cmData.begin(), cmData.end());

    auto statisticCollectorIdentifier =
        std::make_shared<NES::Experimental::Statistics::StatisticCollectorIdentifier>(logicalSourceName,
                                                                                      physicalSourceNames[0],
                                                                                      onField,
                                                                                      startTime,
                                                                                      endTime,
                                                                                      statisticCollectorType);

    auto cmExpectedResult = std::make_shared<NES::Experimental::Statistics::CountMin>(depth,
                                                                                      cmData,
                                                                                      statisticCollectorIdentifier,
                                                                                      observedTuples,
                                                                                      width);
    //    expectedResult.setNumberOfTuples(1);

    // Running a single statistic query
    runSingleStatisticBuildQuery(logicalSourceName,
                                 physicalSourceNames,
                                 onField,
                                 timestampField,
                                 windowSize,
                                 slideFactor,
                                 inputFileName,
                                 inputSchema);

    auto rawStatisticCollectorIdentifier = *statisticCollectorIdentifier.get();
    auto resultStatistic = executionEngine->getNodeEngine()->getStatisticManager()->getStatisticCollectorStorage()->getStatistic(
        rawStatisticCollectorIdentifier);

    auto resultCM = std::dynamic_pointer_cast<NES::Experimental::Statistics::CountMin>(resultStatistic);

    EXPECT_EQ(resultCM->getDepth(), depth);

    //data
    EXPECT_EQ(resultCM->getData().size(), cmData.size());
    EXPECT_EQ(resultCM->getData(), cmData);

    //statisticCollectorIdentifier
    auto resultIdentifier = resultCM->getStatisticCollectorIdentifier();
    EXPECT_EQ(resultIdentifier->getLogicalSourceName(), logicalSourceName);

    EXPECT_EQ(resultCM->getObservedTuples(), observedTuples);
    EXPECT_EQ(resultCM->getWidth(), width);
}

}// namespace NES::Runtime::Execution
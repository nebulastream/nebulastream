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

#include <Operators/LogicalOperators/Sinks/StatisticStorageSinkDescriptor.hpp>
#include <Sinks/Mediums/StatisticSink.hpp>
#include <BaseIntegrationTest.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <QueryCompiler/Phases/Translations/ConvertLogicalToPhysicalSink.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/TestUtils.hpp>
#include <Util/StatisticUtil.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/WorkerContext.hpp>
#include <StatisticFieldIdentifiers.hpp>
#include <Statistics/CountMin.hpp>
#include <Statistics/StatisticCollectorStorage.hpp>
#include <Statistics/StatisticManager/StatisticManager.hpp>
#include <Util/TestExecutionEngine.hpp>

namespace NES {

class StatisticSinkTest : public Testing::BaseIntegrationTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StatisticSinkTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("StatisticSinkTest::SetUpTestCase()");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        NES_DEBUG("Setup StatisticSinkTest test case.");
        auto workerConfiguration = Configurations::WorkerConfiguration::create();
        bufferManager = std::make_shared<Runtime::BufferManager>();
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        NES_DEBUG("StatisticSinkTest::TearDown() Tear down StatisticSinkTest");
        Testing::BaseIntegrationTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_DEBUG("StatisticSinkTest::TearDownTestCases() Tear down StatisticSinkTest test class.");
    }

    Runtime::BufferManagerPtr bufferManager;
};

TEST_F(StatisticSinkTest, statisticSinkTest) {

    auto operatorId = 0;

    std::string logicalSourceName = "logicalSourceName1";
    std::string physicalSourceName = "physicalSourceName1";
    std::string fieldName = "f1";
    Experimental::Statistics::StatisticCollectorType statisticType = Experimental::Statistics::StatisticCollectorType::COUNT_MIN;
    std::string timestampFieldName = "ts";
    uint64_t observedTuples = 5;
    uint64_t depth = 3;
    uint64_t startTime = 0;
    uint64_t endTime = 5000;
    uint64_t width = 8;
    std::string fileName = "countmin.csv";
    auto logSrcWSep = logicalSourceName + "$";

    std::string filepath = std::filesystem::path(TEST_DATA_DIRECTORY) / fileName;
    auto cmData = Experimental::Statistics::StatisticUtil::readFlattenedVectorFromCsvFile(filepath);
    std::string cmString(reinterpret_cast<char*>(cmData.data()), cmData.size() * sizeof(uint64_t));

    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                      ->addField(logSrcWSep + NES::Experimental::Statistics::LOGICAL_SOURCE_NAME, BasicType::TEXT)
                      ->addField(logSrcWSep + NES::Experimental::Statistics::PHYSICAL_SOURCE_NAME, BasicType::TEXT)
                      ->addField(logSrcWSep + NES::Experimental::Statistics::FIELD_NAME, BasicType::TEXT)
                      ->addField(logSrcWSep + NES::Experimental::Statistics::OBSERVED_TUPLES, BasicType::UINT64)
                      ->addField(logSrcWSep + NES::Experimental::Statistics::DEPTH, BasicType::UINT64)
                      ->addField(logSrcWSep + NES::Experimental::Statistics::START_TIME, BasicType::UINT64)
                      ->addField(logSrcWSep + NES::Experimental::Statistics::END_TIME, BasicType::UINT64)
                      ->addField(logSrcWSep + NES::Experimental::Statistics::DATA, BasicType::TEXT)
                      ->addField(logSrcWSep + NES::Experimental::Statistics::WIDTH, BasicType::UINT64);

    auto buffer = bufferManager->getBufferBlocking();
    auto dynBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(buffer, schema);

    //fill buffer
    dynBuffer[0].writeVarSized(logSrcWSep + Experimental::Statistics::LOGICAL_SOURCE_NAME, logicalSourceName, bufferManager.get());
    dynBuffer[0].writeVarSized(logSrcWSep + Experimental::Statistics::PHYSICAL_SOURCE_NAME, physicalSourceName, bufferManager.get());
    dynBuffer[0].writeVarSized(logSrcWSep + Experimental::Statistics::FIELD_NAME, fieldName, bufferManager.get());

    dynBuffer[0][logSrcWSep + Experimental::Statistics::OBSERVED_TUPLES].write(observedTuples);
    dynBuffer[0][logSrcWSep + Experimental::Statistics::DEPTH].write(depth);
    dynBuffer[0][logSrcWSep + Experimental::Statistics::WIDTH].write(width);
    dynBuffer[0].writeVarSized(logSrcWSep + Experimental::Statistics::DATA, cmString, bufferManager.get());

    dynBuffer[0][logSrcWSep + Experimental::Statistics::START_TIME].write(startTime);
    dynBuffer[0][logSrcWSep + Experimental::Statistics::END_TIME].write(endTime);
    dynBuffer.setNumberOfTuples(1);

    auto synopsesText = Runtime::MemoryLayouts::readVarSizedData(
        dynBuffer.getBuffer(),
        dynBuffer[0][logSrcWSep + Experimental::Statistics::DATA].read<Runtime::TupleBuffer::NestedTupleBufferKey>());

    auto statisticSinkDesc = Experimental::Statistics::StatisticStorageSinkDescriptor::create(statisticType, logicalSourceName);

    auto testSink = std::make_shared<SinkLogicalOperatorNode>(statisticSinkDesc, operatorId);

    auto statManager = std::make_shared<Experimental::Statistics::StatisticManager>();
    auto nodeEngine = Testing::TestExecutionEngine().getNodeEngine();
    nodeEngine->setStatisticManager(statManager);

    uint64_t queryId = 0;
    uint64_t querySubPlanId = 0;
    uint64_t numOfProducers = 1;
    auto testPlan = QueryCompilation::PipelineQueryPlan::create(queryId, querySubPlanId);

    DataSinkPtr statisticStorageSink = ConvertLogicalToPhysicalSink::createDataSink(testSink->getId(),
                                                                                    statisticSinkDesc,
                                                                                    schema,
                                                                                    nodeEngine,
                                                                                    testPlan,
                                                                                    numOfProducers);

    auto workerCtx = Runtime::WorkerContext(0, bufferManager, 1000);

    statisticStorageSink->writeData(buffer, workerCtx);

    auto statisticStorage = statManager->getStatisticCollectorStorage();

    //create StatisticCollectorIdentifier
    auto statisticCollectorIdentifier = Experimental::Statistics::StatisticCollectorIdentifier(logicalSourceName,
                                                                                               physicalSourceName,
                                                                                               fieldName,
                                                                                               startTime,
                                                                                               endTime,
                                                                                               statisticType);

    auto statistic = statisticStorage->getStatistic(statisticCollectorIdentifier);

    std::shared_ptr<Experimental::Statistics::CountMin> countMinSketch =
        std::dynamic_pointer_cast<Experimental::Statistics::CountMin>(statistic);

    ASSERT_EQ(countMinSketch->getStatisticCollectorIdentifier()->getLogicalSourceName(), logicalSourceName);
    ASSERT_EQ(countMinSketch->getStatisticCollectorIdentifier()->getPhysicalSourceName(), physicalSourceName);
    ASSERT_EQ(countMinSketch->getStatisticCollectorIdentifier()->getFieldName(), fieldName);
    ASSERT_EQ(countMinSketch->getStatisticCollectorIdentifier()->getStatisticCollectorType(), statisticType);
    ASSERT_EQ(countMinSketch->getWidth(), width);
    ASSERT_EQ(countMinSketch->getDepth(), depth);
    ASSERT_EQ(countMinSketch->getStartTime(), startTime);
    ASSERT_EQ(countMinSketch->getEndTime(), endTime);
    ASSERT_EQ(countMinSketch->getObservedTuples(), observedTuples);

    auto resultData = countMinSketch->getData();
    for (auto i = 0UL; i < depth * width; ++i) {
        ASSERT_EQ(resultData[i], cmData.at(i));
    }

    ASSERT_TRUE(nodeEngine->stop());
}
}// namespace NES
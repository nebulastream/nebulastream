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

#include <API/Expressions/Expressions.hpp>
#include <API/QueryAPI.hpp>
#include <API/TestSchemas.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Expressions/FieldAssignmentExpressionNode.hpp>
#include <Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Metrics/Cardinality.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Metrics/Selectivity.hpp>
#include <Services/RequestHandlerService.hpp>
#include <StatisticCollection/Characteristic/DataCharacteristic.hpp>
#include <StatisticCollection/StatisticProbeHandling/StatisticProbeHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <unordered_map>

using namespace std;

namespace NES {

class StatisticsIntegrationTest : public Testing::BaseIntegrationTest,
                                  public ::testing::WithParamInterface<std::tuple<int, int, int, Statistic::MetricPtr>> {
  public:
    RequestHandlerServicePtr requestHandlerService;
    Statistic::StatisticProbeHandlerPtr probeHandler;
    std::vector<NesWorkerPtr> allWorkers;
    NesCoordinatorPtr nesCoordinator;
    SchemaPtr testInputSchema;
    static constexpr auto timeout = std::chrono::seconds(10);
    static constexpr auto fieldNameToTrack = "id";
    static constexpr auto timestampFieldName = "timestamp";
    static constexpr auto gatheringValue = 0;
    const std::string logicalSourceName = "defaultLogicalSourceName";
    const std::string basePhysicalSourceName = "basePhysicalSourceName";
    std::vector<std::string> physicalSourceNames;
    uint64_t numberOfWorkers;
    uint64_t numberOfBuffersToProduce;
    uint64_t windowSize;
    Statistic::MetricPtr metric;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("StatisticsIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup StatisticsIntegrationTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO("StatisticsIntegrationTest: Setup StatisticsIntegrationTest test class.");
        BaseIntegrationTest::SetUp();

        // Create test schema and store the field names that we are interested in
        testInputSchema = TestSchemas::getSchemaTemplate("id_time_u64");

        // Getting the test parameters
        numberOfWorkers = std::get<0>(GetParam());
        numberOfBuffersToProduce = std::get<1>(GetParam());
        windowSize = std::get<2>(GetParam());
        metric = std::get<3>(GetParam());

        // Create coordinator
        auto coordinatorConfig = CoordinatorConfiguration::createDefault();
        coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;
        NES_INFO("Start coordinator...");
        nesCoordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
        auto coordinatorRPCPort = nesCoordinator->startCoordinator(/**blocking**/ false);
        EXPECT_NE(coordinatorRPCPort, 0UL);
        requestHandlerService = nesCoordinator->getRequestHandlerService();
        probeHandler = nesCoordinator->getStatisticProbeHandler();
        nesCoordinator->getSourceCatalogService()->registerLogicalSource(logicalSourceName, testInputSchema);
        NES_DEBUG("Coordinator started successfully");

        // Create workers
        for (uint64_t i = 0; i < numberOfWorkers; ++i) {
            const auto physicalSourceName = basePhysicalSourceName + std::to_string(i);
            physicalSourceNames.emplace_back(physicalSourceName);
            auto worker = createWorker(logicalSourceName, physicalSourceName, coordinatorRPCPort, WorkerId(i + 2));
            allWorkers.emplace_back(worker);
        }

        // Waiting until all workers have connected to the coordinator
        auto topology = nesCoordinator->getTopology();
        auto start_timestamp = std::chrono::system_clock::now();
        for (const auto& worker : allWorkers) {
            while (!topology->nodeWithWorkerIdExists(worker->getWorkerId())) {
                if (std::chrono::system_clock::now() > start_timestamp + timeout) {
                    NES_THROW_RUNTIME_ERROR("TestHarness: Unable to find setup topology in given timeout.");
                }
            }
        }
        NES_INFO("Found topology!");
    }

    void TearDown() override {
        NES_INFO("StatisticsIntegrationTest: TearDown StatisticsIntegrationTest test class.");
        for (const auto& worker : allWorkers) {
            worker->stop(false);
        }
        nesCoordinator->stopCoordinator(false);
        BaseIntegrationTest::TearDown();
    }

    NesWorkerPtr createWorker(const string& defaultLogicalSourceName,
                              const std::string& physicalSourceName,
                              const uint64_t coordinatorRPCPort,
                              WorkerId workerId) {
        // Create the generator function that produces data id (rand() % 1000) and timestamp monotonic increasing
        auto createData = [this](Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
            static uint64_t curTimestamp = 0;
            auto dynamicBuffer = Runtime::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, testInputSchema);
            for (uint64_t i = 0; i < numberOfTuplesToProduce; i++) {
                dynamicBuffer[i][fieldNameToTrack].write<uint64_t>(rand() % 1000);
                dynamicBuffer[i][timestampFieldName].write<uint64_t>(i + curTimestamp);
            }
            buffer.setNumberOfTuples(numberOfTuplesToProduce);
            curTimestamp += numberOfTuplesToProduce;
        };

        // Create the worker configuration
        auto workerConfig = WorkerConfiguration::create();
        workerConfig->workerId = workerId;
        workerConfig->coordinatorPort = coordinatorRPCPort;
        workerConfig->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
        auto lambdaSourceType = LambdaSourceType::create(defaultLogicalSourceName,
                                                         physicalSourceName,
                                                         createData,
                                                         numberOfBuffersToProduce,
                                                         gatheringValue,
                                                         GatheringMode::INTERVAL_MODE);
        workerConfig->physicalSourceTypes.add(lambdaSourceType);

        // Start the worker
        NES_DEBUG("Start worker...")
        auto worker = std::make_shared<NesWorker>(std::move(workerConfig));
        auto retStart = worker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        NES_DEBUG("Worker started!");

        return worker;
    }
};

/**
 * @brief Test case for creating a statistic, probing it and deleting it for a cardinality
 */
TEST_P(StatisticsIntegrationTest, singleTrackAndProbeDataCharacteristic) {
    using namespace Statistic;
    auto expressionNodePtr = (Attribute(fieldNameToTrack) == 1);
    EXPECT_EQ(expressionNodePtr->instanceOf<EqualsExpressionNode>(), true);

    // Creating the trackStatistic request. Once we fix issue #4778 and #4776, we can call it for each physical source name
    auto characteristic = DataCharacteristic::create(metric, logicalSourceName, physicalSourceNames[0]);
    auto window = Windowing::TumblingWindow::of(EventTime(Attribute(timestampFieldName)), Milliseconds(windowSize));
    auto allStatisticKeys = requestHandlerService->trackStatisticRequest(characteristic, window);
    ASSERT_FALSE(allStatisticKeys.empty());

    // Waiting till all statistic queries are done creating the statistic. This works, as the lambda source is finite
    for (const auto& statisticKey : allStatisticKeys) {
        const auto queryId = probeHandler->getStatisticQueryId(statisticKey);
        if (!TestUtils::checkStoppedOrTimeout(queryId, nesCoordinator->getQueryCatalog())) {
            NES_ERROR("Statistic query did not stop in time.");
            ASSERT_FALSE(true);
        }
    }

    // Trying to probe the statistic for each statistic key
    std::vector<StatisticKey> statisticKeysWithProbeResult;
    for (const auto& statisticKey : allStatisticKeys) {
        // Trying to probe the statistics
        const StatisticProbeRequest probeRequest(statisticKey.hash(),
                                                 Milliseconds(windowSize),
                                                 ProbeExpression(expressionNodePtr));
        auto probeResult = probeHandler->probeStatistic(probeRequest);
        if (!probeResult.getProbeItems().empty()) {
            statisticKeysWithProbeResult.emplace_back(statisticKey);
        } else {
            NES_ERROR("No probe result for statistic key: {}", statisticKey.toString());
            ASSERT_FALSE(true);
        }
    }

    // Checking if we have received for each statistic key, at least one probe result
    EXPECT_EQ(statisticKeysWithProbeResult.size(), allStatisticKeys.size());
}

INSTANTIATE_TEST_CASE_P(
    testDefaultStatisticStore,
    StatisticsIntegrationTest,
    ::testing::Combine(::testing::Values(1, 2, 3),    // No. workers
                       ::testing::Values(1, 5, 10),   // No. buffers to produce
                       ::testing::Values(1, 10, 1000),// Window size
                       ::testing::Values(             // Different Metrics
                           Statistic::Cardinality::create(Statistic::Over(StatisticsIntegrationTest::fieldNameToTrack)),
                           Statistic::Selectivity::create(Statistic::Over(StatisticsIntegrationTest::fieldNameToTrack)))),
    [](const testing::TestParamInfo<StatisticsIntegrationTest::ParamType>& info) {
        return std::string(std::to_string(std::get<0>(info.param)) + "Workers_" + std::to_string(std::get<1>(info.param))
                           + "NumberOfBuffers_" + std::to_string(std::get<2>(info.param)) + "WindowSize_"
                           + std::to_string(info.index) + "Metric");
    });
}// namespace NES

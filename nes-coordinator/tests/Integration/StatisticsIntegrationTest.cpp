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
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

#include <Statistics/StatisticManager/StatisticManager.hpp>
#include <Statistics/Requests/StatisticCreateRequest.hpp>
#include <Statistics/Requests/StatisticDeleteRequest.hpp>
#include <Statistics/Requests/StatisticProbeRequest.hpp>
#include <Statistics/StatisticCoordinator/StatisticCoordinator.hpp>
#include <Statistics/StatisticManager/StatisticCollectorIdentifier.hpp>
#include <Statistics/StatisticManager/StatisticCollectorStorage.hpp>
#include <Statistics/StatisticCollectors/StatisticCollectorType.hpp>

#include <API/Schema.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>

using namespace std;

namespace NES {

class StatisticsIntegrationTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StatisticsIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup StatisticsIntegrationTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO("StatisticsIntegrationTest: Setup StatisticsIntegrationTest test class.");
        BaseIntegrationTest::SetUp();
    }
};

TEST_F(StatisticsIntegrationTest, createTest) {
    auto defaultLogicalSourceName = "defaultLogicalSourceName";
    std::vector<std::string> physicalSourceNames(1, "defaultPhysicalSourceName");
    auto defaultFieldName = "defaultFieldName";
    auto statisticCollectorType = Experimental::Statistics::StatisticCollectorType::COUNT_MIN;
    auto probeExpression = "x == 15";
    auto startTime = 100;
    auto endTime = 10;

    auto createObj = Experimental::Statistics::StatisticCreateRequest(defaultLogicalSourceName,
                                                                 defaultFieldName,
                                                                 Experimental::Statistics::StatisticCollectorType::COUNT_MIN);

    auto probeObj = Experimental::Statistics::StatisticProbeRequest(defaultLogicalSourceName,
                                                               defaultFieldName,
                                                               Experimental::Statistics::StatisticCollectorType::COUNT_MIN,
                                                               probeExpression,
                                                               physicalSourceNames,
                                                               startTime,
                                                               endTime);

    auto deleteObj = Experimental::Statistics::StatisticDeleteRequest(defaultLogicalSourceName,
                                                                      defaultFieldName,
                                                                      Experimental::Statistics::StatisticCollectorType::COUNT_MIN,
                                                                      endTime);

    // create coordinator
    auto coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("createProbeAndDeleteTest: Start coordinator");
    auto crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    auto port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    auto testSchema = Schema::create()->addField(createField(defaultFieldName, BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource(defaultLogicalSourceName, testSchema);
    NES_DEBUG("createProbeAndDeleteTest: Coordinator started successfully");

    NES_DEBUG("createProbeAndDeleteTest: Start worker 1");
    auto workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    auto defaultSourceType1 = DefaultSourceType::create(defaultLogicalSourceName, physicalSourceNames[0]);
    defaultSourceType1->setNumberOfBuffersToProduce(3);
    workerConfig1->physicalSourceTypes.add(defaultSourceType1);
    auto wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    auto retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("createProbeAndDeleteTest: Worker1 started successfully");

    auto statCoordinator = crd->getStatCoordinator();
    auto success = statCoordinator->createStatistic(createObj);

    // checks if the query ID is 1
    EXPECT_EQ(success, 1);

    auto statCollectorStorage = wrk1->getStatisticManager()->getStatisticCollectorStorage();
    double statistic = 1.0;
    for (auto physicalSourceName : physicalSourceNames) {
        auto statCollectorIdentifier = NES::Experimental::Statistics::StatisticCollectorIdentifier(defaultLogicalSourceName,
                                                                                                   physicalSourceName,
                                                                                                   wrk1->getTopologyNodeId(),
                                                                                                   defaultFieldName,
                                                                                                   statisticCollectorType);

        statCollectorStorage->createStatistic(statCollectorIdentifier, statistic);
        statistic += 1.0;
    }

    auto stats = statCoordinator->probeStatistic(probeObj);

    EXPECT_EQ(stats[0], 1);

    success = statCoordinator->deleteStatistic(deleteObj);

    EXPECT_EQ(success, true);
}
}// namespace NES

/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <gtest/gtest.h>

#include <Configurations/Sources/CSVSourceConfig.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <client/include/CPPClient.hpp>

#include <unistd.h>

using namespace std;

namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 1200;
uint64_t dataPort = 1400;
uint64_t restPort = 8000;

class CPPClientTest : public testing::Test {
  protected:
    static void SetUpTestCase() { NES::setupLogging("CPPClientTest.log", NES::LOG_DEBUG); }
    static void TearDownTestCase() { NES_INFO("Tear down CPPClientTest test class."); }

    void SetUp() override {
        rpcPort += 10;
        dataPort += 10;
        restPort += 10;
    }
};

/**
 * @brief Test if doploying a query over the REST api works properly
 * @result deployed query ID is valid
 */
TEST_F(CPPClientTest, DeployQueryTest) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("DeployQueryTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0U);
    NES_INFO("DeployQueryTest: Coordinator started successfully");

    NES_INFO("DeployQueryTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("DeployQueryTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    // Register query
    CSVSourceConfigPtr sourceConfig;
    sourceConfig = CSVSourceConfig::create();
    sourceConfig->setFilePath("");
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test2");
    sourceConfig->setLogicalStreamName("test_stream");

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);
    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(QueryParsingServicePtr());
    streamCatalog->addPhysicalStream("default_logical", sce);

    Query query = Query::from("default_logical");
    auto queryPlan = query.getQueryPlan();

    CPPClient client = CPPClient("localhost", std::to_string(restPort));
    int64_t queryId = client.submitQuery(queryPlan, "ButtomUp");

    EXPECT_TRUE(crd->getQueryCatalog()->queryExists(queryId));

    auto insertedQueryPlan = crd->getQueryCatalog()->getQueryCatalogEntry(queryId)->getInputQueryPlan();
    // Expect that the query id and query sub plan id from the deserialized query plan are valid
    EXPECT_FALSE(insertedQueryPlan->getQueryId() == INVALID_QUERY_ID);
    EXPECT_FALSE(insertedQueryPlan->getQuerySubPlanId() == INVALID_QUERY_SUB_PLAN_ID);
    // Since the deserialization acquires the next queryId and querySubPlanId from the PlanIdGenerator, the deserialized Id should not be the same with the original Id
    EXPECT_TRUE(insertedQueryPlan->getQueryId() != queryPlan->getQueryId());
    EXPECT_TRUE(insertedQueryPlan->getQuerySubPlanId() != queryPlan->getQuerySubPlanId());

    EXPECT_TRUE(wrk1->stop(true));
    EXPECT_TRUE(crd->stopCoordinator(true));
}

}// namespace NES

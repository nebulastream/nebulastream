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

#include <API/Query.hpp>
#include <Client/CPPClient.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
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

    CPPClient cppClient;
};

TEST_F(CPPClientTest, DeployQueryTest) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

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
    SourceConfigPtr sourceConfig;
    sourceConfig = SourceConfig::create();
    sourceConfig->setSourceConfig("");
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test2");
    sourceConfig->setLogicalStreamName("test_stream");

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);
    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(QueryParsingServicePtr());
    streamCatalog->addPhysicalStream("default_logical", sce);

    Query query = Query::from("default_logical").sink(PrintSinkDescriptor::create());

    web::json::value postJsonReturn = cppClient.deployQuery(query.getQueryPlan(), "localhost", std::to_string(restPort));

    EXPECT_TRUE(postJsonReturn.has_field("queryId"));
    EXPECT_TRUE(queryCatalog->queryExists(postJsonReturn.at("queryId").as_integer()));
    NES_INFO("RESTEndpointTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);
    NES_INFO("RESTEndpointTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("RESTEndpointTest: Test finished");
}

}// namespace NES
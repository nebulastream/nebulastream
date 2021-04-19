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
#include <Util/Logger.hpp>
#include <gtest/gtest.h>

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <Phases/QueryMigrationPhase.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
//
// Created by balint on 19.04.21.
//

namespace NES {


static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class QueryMigrationPhaseIntegrationTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("QueryMigrationPhaseIntegrationTest.log", NES::LOG_DEBUG);
        NES_DEBUG("Setup QueryMigrationPhaseIntegrationTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() { NES_DEBUG("TearDown QueryMigrationPhaseIntegrationTest class."); }
};
/**
 * tests findChild/ParentExecutionNodesAsTopologyNodes in QueryMigrationPhase
 */
TEST_F(QueryMigrationPhaseIntegrationTest, testFindChildAndParentExecutionNodesAsTopologyNodesFunctions) {
    CoordinatorConfigPtr coConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();
    coConf->setRpcPort(rpcPort);
    coConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);
    coConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();
    /**
     * Coordinator set up
     */
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("MaintenanceServiceIntegrationTest: Coordinator started successfully");
    uint64_t crdTopologyNodeId = crd->getTopology()->getRoot()->getId();
    std::vector<NesWorkerPtr> workers = {};

    for (int i = 2; i < 9; i++) {
        NES_DEBUG("MaintenanceServiceTest: Start worker " << std::to_string(i));
        wrkConf->resetWorkerOptions();
        wrkConf->setCoordinatorPort(port);
        wrkConf->setRpcPort(port + 10 * i);
        wrkConf->setDataPort(port + 10 * i + 1);
        NodeType type;
        if (i == 7) {
            type = NodeType::Sensor;
        } else {
            type = NodeType::Worker;
        }
        NesWorkerPtr wrk = std::make_shared<NesWorker>(wrkConf, type);
        bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        NES_DEBUG("MaintenanceServiceTest: Worker " << std::to_string(i) << " started successfully");
        TopologyNodeId wrkTopologyNodeId = wrk->getTopologyNodeId();
        NES_DEBUG("MaintenanceServiceTest: Worker " << std::to_string(i) << " has id: " << std::to_string(wrkTopologyNodeId));
        ASSERT_NE(wrkTopologyNodeId, INVALID_TOPOLOGY_NODE_ID);
        switch (i) {
            case 3:
                wrk->replaceParent(crdTopologyNodeId,2);
                break;
            case 4:
                wrk->replaceParent(crdTopologyNodeId,2);
                break;
            case 5:
                wrk->replaceParent(crdTopologyNodeId,2);
                break;

            case 6:
                wrk->replaceParent(crdTopologyNodeId,4);
                break;

            case 7:
                wrk->replaceParent(crdTopologyNodeId, 3);
                wrk->addParent(6);
                break;
            case 8:
                wrk->replaceParent(crdTopologyNodeId, 6);
                wrk->addParent(5);
                break;

        }
        workers.push_back(wrk);
    }

    std::string outputFilePath = "testDeployTwoWorkerJoinUsingTopDownOnSameSchema.out";
    remove(outputFilePath.c_str());

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    workers.at(1)->registerLogicalStream("window1", testSchemaFileName);

    //register logical stream qnv
    std::string window2 =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName2 = "window.hpp";
    std::ofstream out2(testSchemaFileName2);
    out2 << window2;
    out2.close();
    workers.at(1)->registerLogicalStream("window2", testSchemaFileName2);

    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/window.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->setNumberOfBuffersToProduce(2);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("window1");
    srcConf->setSkipHeader(true);
    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(srcConf);

    srcConf->setLogicalStreamName("window2");

    PhysicalStreamConfigPtr windowStream2 = PhysicalStreamConfig::create(srcConf);

    workers.at(4)->registerPhysicalStream(windowStream);
    workers.at(5)->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();



    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    crd->getTopology()->print();
    //auto queryMigrationPhase = QueryMigrationPhase::create(crd->getGlobalExecutionPlan(),crd->getTopology(), crd->getWorkerRPCClient());
    //auto parents = queryMigrationPhase->findParentExecutionNodesAsTopologyNodes(1,2);
    //EXPECT_TRUE(parents.size() == 1);
    //auto children = queryMigrationPhase->findChildExecutionNodesAsTopologyNodes(1,2);
    //EXPECT_TRUE(parents.size() == 2);
    string expectedContent = "window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$value:"
                             "INTEGER,window1$id:INTEGER,window1$timestamp:INTEGER,"
                             "window2$value:INTEGER,window2$id:INTEGER,window2$timestamp:INTEGER\n"
                             "1000,2000,4,1,4,1002,1,4,1002\n"
                             "1000,2000,12,1,12,1001,1,12,1001\n"
                             "2000,3000,1,2,1,2000,2,1,2000\n"
                             "2000,3000,11,2,11,2001,2,11,2001\n"
                             "2000,3000,16,2,16,2002,2,16,2002\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));



}

}//namepsace nes
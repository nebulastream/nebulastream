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
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Phases/QueryMigrationPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/MaintenanceService.hpp>
#include <Services/NESRequestProcessorService.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <iostream>

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

    bool compareDataToBaseline(std::string pathToFile, std::vector<uint64_t>& ids){
        //only applicable for data of the form in test_balint.csv
        //uses unique int identifier for each row. Uses to check if data was lost or duplicated
        std::vector<uint64_t> idsAfterMaintenance {};
        std::ifstream file;
        file.open(pathToFile);
        if(!file.good()){
            NES_DEBUG("QueryMigrationIntegrationtest: Error opening file fo reading");
            return false;
        }
        std::string headers;
        std::getline(file, headers);
        while(!file.eof()){
            std::string temp;
            std::getline(file,temp, '\n');
            if(temp.empty()){
                break;
            }

            uint64_t id = std::stoul(temp);
            idsAfterMaintenance.push_back(id);
        }
        if(ids.size() != idsAfterMaintenance.size()){
            NES_DEBUG("QueryMigrationPhaseIntegrationTest: Data has been lost!");
            NES_DEBUG(ids.size());
            NES_DEBUG(idsAfterMaintenance.size());
            file.close();
            return false;
        }
        std::sort(idsAfterMaintenance.begin(), idsAfterMaintenance.end());
        if(idsAfterMaintenance == ids){
            NES_DEBUG("QueryMigrationPhaseIntegrationTest: Vectors are equal. No data has been lost or added");
            file.close();
            return true;
        }
        NES_DEBUG("QueryMigrationPhaseIntegrationTest: Vectors are NOT equal.");
        file.close();
        return false;

    }
};
/**
 * tests findChild/ParentExecutionNodesAsTopologyNodes in QueryMigrationPhase
 */
TEST_F(QueryMigrationPhaseIntegrationTest, testPathIgnoresNodesMarkedForMaintenance) {
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
    EXPECT_NE(port, 0UL);
    NES_DEBUG("MaintenanceServiceIntegrationTest: Coordinator started successfully");
    uint64_t crdTopologyNodeId = crd->getTopology()->getRoot()->getId();

    /**
     * Building Topology made up of 14 nodes that has the following structure:
     *
     *
     * PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=65535, usedResource=0]
     * |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |--PhysicalNode[id=8, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |--PhysicalNode[id=12, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |  |--PhysicalNode[id=15, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |--PhysicalNode[id=11, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |  |--PhysicalNode[id=15, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |  |--PhysicalNode[id=14, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |--PhysicalNode[id=8, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |--PhysicalNode[id=12, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |  |--PhysicalNode[id=15, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |--PhysicalNode[id=11, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |  |--PhysicalNode[id=15, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |  |--PhysicalNode[id=14, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |--PhysicalNode[id=7, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |--PhysicalNode[id=11, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |  |--PhysicalNode[id=15, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |  |--PhysicalNode[id=14, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |--PhysicalNode[id=7, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |--PhysicalNode[id=11, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |  |--PhysicalNode[id=15, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |  |--PhysicalNode[id=14, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |--PhysicalNode[id=10, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |  |--PhysicalNode[id=14, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |  |--PhysicalNode[id=13, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |--PhysicalNode[id=9, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |  |--PhysicalNode[id=13, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |--PhysicalNode[id=9, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
     * |  |  |--PhysicalNode[id=13, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
    */
    for (int i = 2; i < 16; i++) {
        NES_DEBUG("MaintenanceServiceTest: Start worker " << std::to_string(i));
        wrkConf->resetWorkerOptions();
        wrkConf->setCoordinatorPort(port);
        wrkConf->setRpcPort(port + 10 * i);
        wrkConf->setDataPort(port + 10 * i + 1);
        NesNodeType type;
        if (i == 13 || i == 14 || i == 15) {
            type = NesNodeType::Sensor;
        } else {
            type = NesNodeType::Worker;
        }
        NesWorkerPtr wrk = std::make_shared<NesWorker>(wrkConf, type);
        bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        NES_DEBUG("MaintenanceServiceTest: Worker " << std::to_string(i) << " started successfully");
        TopologyNodeId wrkTopologyNodeId = wrk->getTopologyNodeId();
        NES_DEBUG("MaintenanceServiceTest: Worker " << std::to_string(i) << " has id: " << std::to_string(wrkTopologyNodeId));
        ASSERT_NE(wrkTopologyNodeId, INVALID_TOPOLOGY_NODE_ID);
        switch (i) {
            case 6: wrk->replaceParent(crdTopologyNodeId, 3); break;

            case 7:
                wrk->replaceParent(crdTopologyNodeId, 3);
                wrk->addParent(4);
                break;

            case 8:
                wrk->replaceParent(crdTopologyNodeId, 4);
                wrk->addParent(5);
                break;

            case 9:
                wrk->replaceParent(crdTopologyNodeId, 2);
                wrk->addParent(6);
                break;

            case 10: wrk->replaceParent(crdTopologyNodeId, 6); break;

            case 11:
                wrk->replaceParent(crdTopologyNodeId, 7);
                wrk->addParent(8);
                break;

            case 12: wrk->replaceParent(crdTopologyNodeId, 8); break;

            case 13:
                wrk->replaceParent(crdTopologyNodeId, 9);
                wrk->addParent(10);
                break;

            case 14:
                wrk->replaceParent(crdTopologyNodeId, 10);
                wrk->addParent(11);
                break;

            case 15:
                wrk->replaceParent(crdTopologyNodeId, 11);
                wrk->addParent(12);
                break;
        }
    }
    // Check if the topology matches the expected hierarchy
    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren().size(), 4UL);
    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[0]->getChildren().size(), 1UL);
    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[1]->getChildren().size(), 2UL);
    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[2]->getChildren().size(), 2UL);
    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[3]->getChildren().size(), 1UL);
    //mid level
    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[0]->getChildren()[0]->getChildren().size(), 1UL);

    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[1]->getChildren()[0]->getChildren().size(), 2UL);
    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[1]->getChildren()[1]->getChildren().size(), 1UL);

    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[2]->getChildren()[0]->getChildren().size(), 1UL);
    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[2]->getChildren()[1]->getChildren().size(), 2UL);

    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[3]->getChildren()[0]->getChildren().size(), 2UL);
    //bottom level
    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[0]->getChildren()[0]->getChildren().size(), 1UL);

    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[1]->getChildren()[0]->getChildren()[1]->getChildren().size(), 2UL);
    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[1]->getChildren()[1]->getChildren()[0]->getChildren().size(), 2UL);

    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[2]->getChildren()[1]->getChildren()[1]->getChildren().size(), 1UL);

    TopologyPtr topology = crd->getTopology();
    std::vector<TopologyNodePtr> sourceNodes{topology->findNodeWithId(13), topology->findNodeWithId(14),
                                             topology->findNodeWithId(15)};
    std::vector<TopologyNodePtr> destinationNodes{topology->findNodeWithId(1)};
    const std::vector<TopologyNodePtr> startNodes = topology->findPathBetween(sourceNodes, destinationNodes);

    EXPECT_TRUE(startNodes.size() == 3UL);
    EXPECT_TRUE(startNodes.size() == sourceNodes.size());
    const std::vector<TopologyNodePtr> StartNodes = crd->getTopology()->findPathBetween(sourceNodes, destinationNodes);

    //checks if Ids of source nodes are as expected
    EXPECT_TRUE(sourceNodes[0]->getId() == 13);
    EXPECT_TRUE(sourceNodes[1]->getId() == 14);
    EXPECT_TRUE(sourceNodes[2]->getId() == 15);
    //checks path from source node 12 to sink
    TopologyNodePtr firstStartNodeParent1 = startNodes[0]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(firstStartNodeParent1->getId() == 9);
    TopologyNodePtr firstStartNodeParent2 = firstStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(firstStartNodeParent2->getId() == 2);
    TopologyNodePtr firstStartNodeParent3 = firstStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(firstStartNodeParent3->getId() == 1);
    //checks path from source node 13 to sink
    TopologyNodePtr secondStartNodeParent1 = startNodes[1]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(secondStartNodeParent1->getId() == 11);
    TopologyNodePtr secondStartNodeParent2 = secondStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(secondStartNodeParent2->getId() == 7);
    TopologyNodePtr secondStartNodeParent3 = secondStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(secondStartNodeParent3->getId() == 3);
    TopologyNodePtr secondStartNodeParent4 = secondStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(secondStartNodeParent4->getId() == 1);
    //checks path from source node 14 to sink
    TopologyNodePtr thirdStartNodeParent1 = startNodes[2]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(thirdStartNodeParent1->getId() == 11);
    TopologyNodePtr thirdStartNodeParent2 = thirdStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(thirdStartNodeParent2->getId() == 7);
    TopologyNodePtr thirdStartNodeParent3 = thirdStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(thirdStartNodeParent3->getId() == 3);
    TopologyNodePtr thirdStartNodeParent4 = thirdStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(thirdStartNodeParent4->getId() == 1);
    //flags nodes currently on path for maintenance
    topology->findNodeWithId(2)->setMaintenanceFlag(true);
    topology->findNodeWithId(4)->setMaintenanceFlag(true);
    topology->findNodeWithId(11)->setMaintenanceFlag(true);
    //calculate Path again
    const std::vector<TopologyNodePtr> StartNodesAfterMaintenance = topology->findPathBetween(sourceNodes, destinationNodes);
    //checks path from source node 12 to sink
    TopologyNodePtr mFirstStartNodeParent1 = StartNodesAfterMaintenance[0]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mFirstStartNodeParent1->getId() == 10);
    TopologyNodePtr mFirstStartNodeParent2 = mFirstStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mFirstStartNodeParent2->getId() == 6);
    TopologyNodePtr mFirstStartNodeParent3 = mFirstStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mFirstStartNodeParent3->getId() == 3);
    TopologyNodePtr mFirstStartNodeParent4 = mFirstStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mFirstStartNodeParent4->getId() == 1);
    //checks path from source node 13 to sink
    TopologyNodePtr mSecondStartNodeParent1 = StartNodesAfterMaintenance[1]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mSecondStartNodeParent1->getId() == 10);
    TopologyNodePtr mSecondStartNodeParent2 = mSecondStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mSecondStartNodeParent2->getId() == 6);
    TopologyNodePtr mSecondStartNodeParent3 = mSecondStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mSecondStartNodeParent3->getId() == 3);
    TopologyNodePtr mSecondStartNodeParent4 = mSecondStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mSecondStartNodeParent4->getId() == 1);
    //checks path from source node 14 to sink
    TopologyNodePtr mThirdStartNodeParent1 = StartNodesAfterMaintenance[2]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mThirdStartNodeParent1->getId() == 12);
    TopologyNodePtr mThirdStartNodeParent2 = mThirdStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mThirdStartNodeParent2->getId() == 8);
    TopologyNodePtr mThirdStartNodeParent3 = mThirdStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mThirdStartNodeParent3->getId() == 5);
    TopologyNodePtr mThirdStartNodeParent4 = mThirdStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mThirdStartNodeParent4->getId() == 1);
    //TODO: move to Unit Tests
}
TEST_F(QueryMigrationPhaseIntegrationTest, DiamondTopologyWithOneQueryFirstStrategyTest) {
    CoordinatorConfigPtr coConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();
    coConf->setRpcPort(rpcPort);
    coConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("MaintenanceServiceIntegrationTest: Coordinator started successfully");
    //uint64_t crdTopologyNodeId = crd->getTopology()->getRoot()->getId();
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);

    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);

    wrkConf->setRpcPort(port + 30);
    wrkConf->setDataPort(port + 31);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    wrk3->addParent(3);

    TopologyPtr topo = crd->getTopology();
    ASSERT_EQ(topo->getRoot()->getId(), 1UL);
    ASSERT_EQ(topo->getRoot()->getChildren().size(), 2UL);
    ASSERT_EQ(topo->getRoot()->getChildren()[0]->getChildren().size(), 1UL);
    ASSERT_EQ(topo->getRoot()->getChildren()[1]->getChildren().size(), 1UL);
    NES_DEBUG(crd->getStreamCatalog()->getPhysicalStreamAndSchemaAsString());

    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/exdra.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("exdra");
    srcConf->setNumberOfBuffersToProduce(1000);
    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    wrk3->registerPhysicalStream(conf);
    std::string filePath = "contTestOut.csv";
    remove(filePath.c_str());
    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    MaintenanceServicePtr maintenanceService = crd->getMaintenanceService();

    NES_DEBUG("MaintenanceServiceTest: Submit query");
    //register query
    std::string queryString =
        R"(Query::from("exdra").sink(FileSinkDescriptor::create(")" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    auto globalExecutionPlan = crd->getGlobalExecutionPlan();
    ASSERT_TRUE(globalExecutionPlan->checkIfExecutionNodeIsARoot(1));
    ASSERT_TRUE(globalExecutionPlan->checkIfExecutionNodeExists(2));
    ASSERT_TRUE(globalExecutionPlan->checkIfExecutionNodeExists(4));
    maintenanceService->submitMaintenanceRequest(2, 1);

    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    auto exNodes = globalExecutionPlan->getAllExecutionNodes();
    ASSERT_TRUE(globalExecutionPlan->checkIfExecutionNodeIsARoot(1));
    ASSERT_TRUE(globalExecutionPlan->checkIfExecutionNodeExists(4));
    ASSERT_TRUE(globalExecutionPlan->checkIfExecutionNodeExists(3));
}
TEST_F(QueryMigrationPhaseIntegrationTest, DISABLED_DiamondTopologyWithTwoQueriesFirstStrategyTest) {
    CoordinatorConfigPtr coConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();
    coConf->setRpcPort(rpcPort);
    coConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);

    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);

    wrkConf->setRpcPort(port + 30);
    wrkConf->setDataPort(port + 31);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    wrk3->addParent(3);

    wrkConf->setRpcPort(port + 40);
    wrkConf->setDataPort(port + 41);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    wrk4->replaceParent(1, 2);
    wrk3->addParent(4);

    wrkConf->setRpcPort(port + 50);
    wrkConf->setDataPort(port + 51);
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart5);
    wrk4->replaceParent(1, 3);
    wrk3->addParent(4);

    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/exdra.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("exdra");
    srcConf->setNumberOfBuffersToProduce(1000);
    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    wrk4->registerPhysicalStream(conf);
    wrk5->registerPhysicalStream(conf);
    std::string filePath = "contTestOut.csv";
    remove(filePath.c_str());

    std::string query1 = R"(Query::from("exdra").filter(Attribute("id") < 42).sink(FileSinkDescriptor::create(")" + filePath
                         + R"(" , "CSV_FORMAT", "APPEND"));)";
    std::string query2 = R"(Query::from("exdra").filter(Attribute("id") < 10).sink(FileSinkDescriptor::create(")" + filePath
                         + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    MaintenanceServicePtr maintenanceService = crd->getMaintenanceService();

    NES_DEBUG("MaintenanceServiceTest: Submit query");

    QueryId queryId1 = queryService->validateAndQueueAddRequest(query1, "BottomUp");
    QueryId queryId2 = queryService->validateAndQueueAddRequest(query2, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalog));
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalog));

    auto globalExecutionPlan = crd->getGlobalExecutionPlan();
    auto executionNodes = globalExecutionPlan->getAllExecutionNodes();
    ASSERT_TRUE(executionNodes.size() == 4);


    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalog));
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalog));
    auto exNodes = globalExecutionPlan->getAllExecutionNodes();
    ASSERT_TRUE(globalExecutionPlan->checkIfExecutionNodeIsARoot(1));
    ASSERT_TRUE(globalExecutionPlan->checkIfExecutionNodeExists(4));
    ASSERT_TRUE(globalExecutionPlan->checkIfExecutionNodeExists(3));
}
TEST_F(QueryMigrationPhaseIntegrationTest, DiamondTopologySingleQueryWithBufferTest) {

    CoordinatorConfigPtr coConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();
    coConf->setRpcPort(rpcPort);
    coConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);
    wrkConf->setNumWorkerThreads(3);
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("MaintenanceServiceIntegrationTest: Coordinator started successfully");
    //uint64_t crdTopologyNodeId = crd->getTopology()->getRoot()->getId();
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);

    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);

    wrkConf->setRpcPort(port + 30);
    wrkConf->setDataPort(port + 31);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    wrk4->replaceParent(1,3);


    wrkConf->setRpcPort(port + 40);
    wrkConf->setDataPort(port + 41);
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>(wrkConf,NesNodeType::Sensor);
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart5);
    wrk5->replaceParent(1, 2);
    wrk5->addParent(4);

    TopologyPtr topo = crd->getTopology();
    NES_DEBUG(topo->toString());
    ASSERT_EQ(topo->getRoot()->getId(), 1UL);
    ASSERT_EQ(topo->getRoot()->getChildren().size(), 2UL);


    //register logical stream
    std::string testSchema = "Schema::create()->addField(createField(\"id\", UINT64));";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk5->registerLogicalStream("testStream", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    ASSERT_TRUE(std::filesystem::exists("../tests/test_data/test_balint.csv"));
    srcConf->setSourceConfig("../tests/test_data/test_balint.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(1); // when set to 0 it breaks
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("testStream");
    //srcConf->setSkipHeader(true);
    srcConf->setNumberOfBuffersToProduce(3000);
    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    wrk5->registerPhysicalStream(conf);

    std::string filePath = "withBuffer.csv";
    remove(filePath.c_str());
    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    MaintenanceServicePtr maintenanceService = crd->getMaintenanceService();

    NES_DEBUG("MaintenanceServiceTest: Submit query");
    //register query
    std::string queryString =
        R"(Query::from("testStream").sink(FileSinkDescriptor::create(")" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    auto globalQueryPlan = crd->getGlobalQueryPlan();

    ASSERT_TRUE(wrk2->getTopologyNodeId() == 2);
    ASSERT_TRUE(wrk3->getTopologyNodeId() == 3);
    ASSERT_TRUE(wrk4->getTopologyNodeId() == 4);

    auto globalExecutionPlan = crd->getGlobalExecutionPlan();
    ASSERT_TRUE(globalExecutionPlan->checkIfExecutionNodeIsARoot(1));
    ASSERT_TRUE(globalExecutionPlan->checkIfExecutionNodeExists(2));
    ASSERT_TRUE(!(globalExecutionPlan->checkIfExecutionNodeExists(3)));
    ASSERT_TRUE(!(globalExecutionPlan->checkIfExecutionNodeExists(4)));
    ASSERT_TRUE(globalExecutionPlan->checkIfExecutionNodeExists(5));

    maintenanceService->submitMaintenanceRequest(2,2); //doesnt work at all
    sleep(45);

//    ASSERT_TRUE(globalExecutionPlan->checkIfExecutionNodeExists(3));
//    ASSERT_TRUE(wrk3->getNodeEngine()->getDeployedQEP(3));
//    queryService->validateAndQueueStopRequest(queryId);
//    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

//    NES_INFO("QueryDeploymentTest: Stop worker 2");
//    bool retStopWrk2 = wrk2->stop(true);
//    EXPECT_TRUE(retStopWrk2);
//    NES_DEBUG("Worker2 stopped!");
//
//    NES_INFO("QueryDeploymentTest: Stop worker 3");
//    bool retStopWrk3 = wrk3->stop(true);
//    EXPECT_TRUE(retStopWrk3);
//    NES_DEBUG("Worker3 stopped!");
//
//    NES_INFO("QueryDeploymentTest: Stop worker 4");
//    bool retStopWrk4 = wrk4->stop(true);
//    EXPECT_TRUE(retStopWrk4);
//    NES_DEBUG("Worker4 stopped!");
//
//    NES_INFO("QueryDeploymentTest: Stop Coordinator");
//    bool retStopCord = crd->stopCoordinator(true);
//    EXPECT_TRUE(retStopCord);
//    NES_INFO("QueryDeploymentTest: Test finished");



    std::vector<uint64_t> ids (3000);
    std::iota(ids.begin(), ids.end(),1);

    bool success = compareDataToBaseline(filePath,ids);
    EXPECT_EQ(success , true);

}
TEST_F(QueryMigrationPhaseIntegrationTest, DiamondTopologySingleQueryNoBufferTest) {

    CoordinatorConfigPtr coConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();
    coConf->setRpcPort(rpcPort);
    coConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);
    wrkConf->setNumWorkerThreads(1); //breaks with only thread
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("MaintenanceServiceIntegrationTest: Coordinator started successfully");
    //uint64_t crdTopologyNodeId = crd->getTopology()->getRoot()->getId();
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);

    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);

    wrkConf->setRpcPort(port + 30);
    wrkConf->setDataPort(port + 31);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    wrk4->replaceParent(1,3);


    wrkConf->setRpcPort(port + 40);
    wrkConf->setDataPort(port + 41);
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>(wrkConf,NesNodeType::Sensor);
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart5);
    wrk5->replaceParent(1, 2);
    wrk5->addParent(4);

    TopologyPtr topo = crd->getTopology();
    NES_DEBUG(topo->toString());
    ASSERT_EQ(topo->getRoot()->getId(), 1UL);
    ASSERT_EQ(topo->getRoot()->getChildren().size(), 2UL);


    //register logical stream
    std::string testSchema = "Schema::create()->addField(createField(\"id\", UINT64));";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk5->registerLogicalStream("testStream", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    ASSERT_TRUE(std::filesystem::exists("../tests/test_data/test_balint.csv"));
    srcConf->setSourceConfig("../tests/test_data/test_balint.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(1); // when set to 0 it breaks
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("testStream");
    //srcConf->setSkipHeader(true);
    srcConf->setNumberOfBuffersToProduce(3000);
    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    wrk5->registerPhysicalStream(conf);

    std::string filePath = "withoutBuffer.csv";
    remove(filePath.c_str());
    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    MaintenanceServicePtr maintenanceService = crd->getMaintenanceService();

    NES_DEBUG("MaintenanceServiceTest: Submit query");
    //register query
    std::string queryString =
        R"(Query::from("testStream").sink(FileSinkDescriptor::create(")" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    auto globalQueryPlan = crd->getGlobalQueryPlan();

    ASSERT_TRUE(wrk2->getTopologyNodeId() == 2);
    ASSERT_TRUE(wrk3->getTopologyNodeId() == 3);
    ASSERT_TRUE(wrk4->getTopologyNodeId() == 4);

    auto globalExecutionPlan = crd->getGlobalExecutionPlan();
    ASSERT_TRUE(globalExecutionPlan->checkIfExecutionNodeIsARoot(1));
    ASSERT_TRUE(globalExecutionPlan->checkIfExecutionNodeExists(2));
    ASSERT_TRUE(!(globalExecutionPlan->checkIfExecutionNodeExists(3)));
    ASSERT_TRUE(!(globalExecutionPlan->checkIfExecutionNodeExists(4)));
    ASSERT_TRUE(globalExecutionPlan->checkIfExecutionNodeExists(5));

    maintenanceService->submitMaintenanceRequest(2,3);
    sleep(35);

//    ASSERT_TRUE(globalExecutionPlan->checkIfExecutionNodeExists(3));
//    ASSERT_TRUE(wrk3->getNodeEngine()->getDeployedQEP(3));
//    queryService->validateAndQueueStopRequest(queryId);
//    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

//    NES_INFO("QueryDeploymentTest: Stop worker 2");
//    bool retStopWrk2 = wrk2->stop(true);
//    EXPECT_TRUE(retStopWrk2);
//    NES_DEBUG("Worker2 stopped!");
//
//    NES_INFO("QueryDeploymentTest: Stop worker 3");
//    bool retStopWrk3 = wrk3->stop(true);
//    EXPECT_TRUE(retStopWrk3);
//    NES_DEBUG("Worker3 stopped!");
//
//    NES_INFO("QueryDeploymentTest: Stop worker 4");
//    bool retStopWrk4 = wrk4->stop(true);
//    EXPECT_TRUE(retStopWrk4);
//    NES_DEBUG("Worker4 stopped!");
//
//    NES_INFO("QueryDeploymentTest: Stop Coordinator");
//    bool retStopCord = crd->stopCoordinator(true);
//    EXPECT_TRUE(retStopCord);
//    NES_INFO("QueryDeploymentTest: Test finished");

    std::vector<uint64_t> ids (3000);
    std::iota(ids.begin(), ids.end(),1);

    bool success = compareDataToBaseline(filePath,ids);
    EXPECT_EQ(success , true);
}

TEST_F(QueryMigrationPhaseIntegrationTest, test) {

    CoordinatorConfigPtr coConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();
    coConf->setRpcPort(rpcPort);
    coConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("MaintenanceServiceIntegrationTest: Coordinator started successfully");
    //uint64_t crdTopologyNodeId = crd->getTopology()->getRoot()->getId();
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);

    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);

    wrkConf->setRpcPort(port + 30);
    wrkConf->setDataPort(port + 31);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    wrk3->addParent(3);

    TopologyPtr topo = crd->getTopology();
    ASSERT_EQ(topo->getRoot()->getId(), 1UL);
    ASSERT_EQ(topo->getRoot()->getChildren().size(), 2UL);
    ASSERT_EQ(topo->getRoot()->getChildren()[0]->getChildren().size(), 1UL);
    ASSERT_EQ(topo->getRoot()->getChildren()[1]->getChildren().size(), 1UL);
    NES_DEBUG(crd->getStreamCatalog()->getPhysicalStreamAndSchemaAsString());

    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/exdra.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("exdra");
    srcConf->setNumberOfBuffersToProduce(1000);
    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    wrk3->registerPhysicalStream(conf);
    std::string filePath = "withoutMigration.csv";
    remove(filePath.c_str());
    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();


    std::string queryString =
        R"(Query::from("exdra").sink(FileSinkDescriptor::create(")" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);
    NES_DEBUG("Worker1 stopped!");

    NES_INFO("QueryDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);
    NES_DEBUG("Worker2 stopped!");

    NES_INFO("QueryDeploymentTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);
    NES_DEBUG("Worker3 stopped!");

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");

}

TEST_F(QueryMigrationPhaseIntegrationTest, DetectionOfParentAndChildExecutionNodesOfAQuerySubPlanTest) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("JoinDeploymentTest: Worker2 started SUCCESSFULLY");

    std::string outputFilePath = "testDeployTwoWorkerJoinUsingTopDownOnSameSchema.out";
    remove(outputFilePath.c_str());

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window1", testSchemaFileName);

    //register logical stream qnv
    std::string window2 =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName2 = "window.hpp";
    std::ofstream out2(testSchemaFileName2);
    out2 << window2;
    out2.close();
    wrk1->registerLogicalStream("window2", testSchemaFileName2);

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

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent = "window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$value:"
                             "INTEGER,window1$id:INTEGER,window1$timestamp:INTEGER,"
                             "window2$value:INTEGER,window2$id:INTEGER,window2$timestamp:INTEGER\n"
                             "1000,2000,4,1,4,1002,1,4,1002\n"
                             "1000,2000,12,1,12,1001,1,12,1001\n"
                             "2000,3000,1,2,1,2000,2,1,2000\n"
                             "2000,3000,11,2,11,2001,2,11,2001\n"
                             "2000,3000,16,2,16,2002,2,16,2002\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    auto globalExecutionPlan = crd->getGlobalExecutionPlan();
    ASSERT_TRUE(globalExecutionPlan->checkIfExecutionNodeIsARoot(1));
    auto querySubPlansOnRoot = globalExecutionPlan->getExecutionNodeByNodeId(1)->getQuerySubPlans(1);
    ASSERT_TRUE(querySubPlansOnRoot.size() == 1);
    auto leafOperators = querySubPlansOnRoot[0]->getLeafOperators();
    EXPECT_TRUE(leafOperators.size() == 2);




    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("JoinDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("JoinDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("JoinDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("JoinDeploymentTest: Test finished");
}
}//namepsace nes
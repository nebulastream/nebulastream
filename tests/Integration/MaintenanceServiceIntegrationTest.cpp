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
//
// Created by balint on 05.03.21.
//
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/MaintenanceService.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>

namespace NES {

static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class MaintenanceServiceIntegrationTest : public testing::Test {
  public:
    CoordinatorConfigPtr coConf;
    WorkerConfigPtr wrkConf;
    SourceConfigPtr srcConf;
    NesCoordinatorPtr crd;

    static void SetUpTestCase() {
        NES::setupLogging("MaintenanceServiceTest.log", NES::LOG_DEBUG);
        NES_DEBUG("Setup MaintenanceService test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
        coConf = CoordinatorConfig::create();
        wrkConf = WorkerConfig::create();
        srcConf = SourceConfig::create();
        coConf->setRpcPort(rpcPort);
        coConf->setRestPort(restPort);
        wrkConf->setCoordinatorPort(rpcPort);
        coConf->resetCoordinatorOptions();
        wrkConf->resetWorkerOptions();
        /**
         * Coordinator set up
         */
        crd = std::make_shared<NesCoordinator>(coConf);
        uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
        EXPECT_NE(port, 0);
        NES_DEBUG("MaintenanceServiceTest: Coordinator started successfully");
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
        for(int i = 2; i<16; i++ ){
            NES_DEBUG("MaintenanceServiceTest: Start worker " <<std::to_string(i));
            wrkConf->resetWorkerOptions();
            wrkConf->setCoordinatorPort(port);
            wrkConf->setRpcPort(port + 10*i);
            wrkConf->setDataPort(port + 10*i+1);
            NodeType type;
            if(i == 13 || i == 14 || i == 15){
                type = NodeType::Sensor;
            }
            else{
                type = NodeType::Worker;
            }
            NesWorkerPtr wrk = std::make_shared<NesWorker>(wrkConf, type);
            bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
            EXPECT_TRUE(retStart);
            NES_DEBUG("MaintenanceServiceTest: Worker " << std::to_string(i) << " started successfully");
            TopologyNodeId wrkTopologyNodeId = wrk->getTopologyNodeId();
            NES_DEBUG("MaintenanceServiceTest: Worker " << std::to_string(i) <<" has id: " << std::to_string(wrkTopologyNodeId));
            ASSERT_NE(wrkTopologyNodeId, INVALID_TOPOLOGY_NODE_ID);
            switch (i){
                case 6:
                    wrk->replaceParent(crdTopologyNodeId, 3);
                    break;

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

                case 10:
                    wrk->replaceParent(crdTopologyNodeId, 6);
                    break;

                case 11:
                    wrk->replaceParent(crdTopologyNodeId, 7);
                    wrk->addParent(8);
                    break;

                case 12:
                    wrk->replaceParent(crdTopologyNodeId, 8);
                    break;

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
        ASSERT_EQ(crd->getTopology()->getRoot()->getChildren().size(), 4);
        ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[0]->getChildren().size(), 1);
        ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[1]->getChildren().size(), 2);
        ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[2]->getChildren().size(), 2);
        ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[3]->getChildren().size(), 1);
        //mid level
        ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[0]->getChildren()[0]->getChildren().size(), 1);

        ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[1]->getChildren()[0]->getChildren().size(), 2);
        ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[1]->getChildren()[1]->getChildren().size(), 1);

        ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[2]->getChildren()[0]->getChildren().size(), 1);
        ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[2]->getChildren()[1]->getChildren().size(), 2);

        ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[3]->getChildren()[0]->getChildren().size(), 2);
        //bottom level
        ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[0]->getChildren()[0]->getChildren().size(), 1);

        ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[1]->getChildren()[0]->getChildren()[1]->getChildren().size(), 2);
        ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[1]->getChildren()[1]->getChildren()[0]->getChildren().size(), 2);

        ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[2]->getChildren()[1]->getChildren()[1]->getChildren().size(), 1);
    }

    void TearDown() { NES_DEBUG("TearDown DeepTopologyHierarchyTest test class."); }
};
/**
 * compares path from source nodes to sink node with what is expected before and after maintenance
 */
TEST_F(MaintenanceServiceIntegrationTest, DISABLED_findPathIgnoresNodesMarkedForMaintenanceTest) {

    TopologyPtr topology = crd->getTopology();
    std::vector<TopologyNodePtr> sourceNodes{topology->findNodeWithId(13),topology->findNodeWithId(14),topology->findNodeWithId(15)};
    std::vector<TopologyNodePtr> destinationNodes{topology->findNodeWithId(1)};
    const std::vector<TopologyNodePtr> startNodes = topology->findPathBetween(sourceNodes, destinationNodes);

    EXPECT_TRUE(startNodes.size() == 3);
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

}

TEST_F(MaintenanceServiceIntegrationTest, DISABLED_defTest) {
    srcConf->setSourceConfig("");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setNumberOfBuffersToProduce(3);
    srcConf->setPhysicalStreamName("test");
    srcConf->setLogicalStreamName("default_logical");

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);

    StreamCatalogEntryPtr sce1 = std::make_shared<StreamCatalogEntry>(conf, crd->getTopology()->findNodeWithId(13));
    StreamCatalogEntryPtr sce2 = std::make_shared<StreamCatalogEntry>(conf, crd->getTopology()->findNodeWithId(14));
    StreamCatalogEntryPtr sce3 = std::make_shared<StreamCatalogEntry>(conf, crd->getTopology()->findNodeWithId(15));


    crd->getStreamCatalog()->addPhysicalStream("default_logical", sce1);
    auto streams = crd->getStreamCatalog()->getPhysicalStreams("default_logical");
    ASSERT_EQ(crd->getStreamCatalog()->getPhysicalStreams("default_logical").size(),4);//why is this 6?

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    std::string query = "Query::from(\"default_logical\").filter(Attribute(\"field_1\") <= 10).sink(PrintSinkDescriptor::create());";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    MaintenanceServicePtr maintenanceService = crd->getMaintenanceService();

    NES_DEBUG("MaintenanceServiceTest: Submit query");

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    auto parentQueryIds = maintenanceService->submitMaintenanceRequest(5,1);
    ASSERT_EQ(parentQueryIds.front(), queryId);

    }
TEST_F(MaintenanceServiceTest, testForDefaultLogicalStream) {
    auto allLogicalStreams = crd->getStreamCatalog()->getAllLogicalStream();
    ASSERT_EQ(allLogicalStreams.size(),0);
}

}//namespace NES

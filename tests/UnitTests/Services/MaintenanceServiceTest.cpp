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
// Created by balint on 10.03.21.
//
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>

#include <Util/Logger.hpp>
#include <Services/MaintenanceService.hpp>
#include <Catalogs/QueryCatalog.hpp>
#include <WorkQueues/QueryRequestQueue.hpp>
#include <GRPC/WorkerRPCClient.hpp>

#include <gtest/gtest.h>
namespace NES{
class MaintenanceServiceTest : public ::testing::Test {
  public:
    void SetUp(){
        NES::setupLogging("SerializationUtilTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup SerializationUtilTest test case.");
    }
    void TearDown(){}
};
TEST_F(MaintenanceServiceTest, findPathBetweenIgnoresNodesMakredForMaintenanceTest) {
    TopologyPtr topology = Topology::create();

    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    // create workers
    std::vector<TopologyNodePtr> topologyNodes;
    int resource = 4;
    for (uint32_t i = 0; i < 15; ++i) {
        topologyNodes.push_back(TopologyNode::create(i, "localhost", grpcPort, dataPort, resource));
        grpcPort = grpcPort + 2;
        dataPort = dataPort + 2;
    }

    topology->setAsRoot(topologyNodes.at(0));

    // link each worker with its neighbor
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(1));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(2));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(3));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(4));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(1), topologyNodes.at(8));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(2), topologyNodes.at(5));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(2), topologyNodes.at(6));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(3), topologyNodes.at(6));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(3), topologyNodes.at(7));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(4), topologyNodes.at(7));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(5), topologyNodes.at(8));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(5), topologyNodes.at(9));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(6), topologyNodes.at(10));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(7), topologyNodes.at(10));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(7), topologyNodes.at(11));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(8), topologyNodes.at(12));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(9), topologyNodes.at(12));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(9), topologyNodes.at(13));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(10), topologyNodes.at(13));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(10), topologyNodes.at(14));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(11), topologyNodes.at(14));

    std::vector<TopologyNodePtr> sourceNodes{topologyNodes.at(12),topologyNodes.at(13),topologyNodes.at(14)};

    std::vector<TopologyNodePtr> destinationNodes{topologyNodes.at(0)};

    const std::vector<TopologyNodePtr> startNodes = topology->findPathBetween(sourceNodes, destinationNodes);

    EXPECT_FALSE(startNodes.empty());
    EXPECT_TRUE(startNodes.size() == sourceNodes.size());

    //checks if Ids of source nodes are as expected
    EXPECT_TRUE(sourceNodes[0]->getId() == topologyNodes[12]->getId());
    EXPECT_TRUE(sourceNodes[1]->getId() == topologyNodes[13]->getId());
    EXPECT_TRUE(sourceNodes[2]->getId() == topologyNodes[14]->getId());
    //checks path from source node 12 to sink
    TopologyNodePtr firstStartNodeParent1 = startNodes[0]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(firstStartNodeParent1->getId() == topologyNodes[8]->getId());
    TopologyNodePtr firstStartNodeParent2 = firstStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(firstStartNodeParent2->getId() == topologyNodes[1]->getId());
    TopologyNodePtr firstStartNodeParent3 = firstStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(firstStartNodeParent3->getId() == topologyNodes[0]->getId());
    //checks path from source node 13 to sink
    TopologyNodePtr secondStartNodeParent1 = startNodes[1]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(secondStartNodeParent1->getId() == topologyNodes[10]->getId());
    TopologyNodePtr secondStartNodeParent2 = secondStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(secondStartNodeParent2->getId() == topologyNodes[6]->getId());
    TopologyNodePtr secondStartNodeParent3 = secondStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(secondStartNodeParent3->getId() == topologyNodes[2]->getId());
    TopologyNodePtr secondStartNodeParent4 = secondStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(secondStartNodeParent4->getId() == topologyNodes[0]->getId());
    //checks path from source node 14 to sink
    TopologyNodePtr thirdStartNodeParent1 = startNodes[2]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(thirdStartNodeParent1->getId() == topologyNodes[10]->getId());
    TopologyNodePtr thirdStartNodeParent2 = thirdStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(thirdStartNodeParent2->getId() == topologyNodes[6]->getId());
    TopologyNodePtr thirdStartNodeParent3 = thirdStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(thirdStartNodeParent3->getId() == topologyNodes[2]->getId());
    TopologyNodePtr thirdStartNodeParent4 = thirdStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(thirdStartNodeParent4->getId() == topologyNodes[0]->getId());
    //flags nodes currently on path for maintenance
    topologyNodes[1]->setMaintenanceFlag(true);
    topologyNodes[3]->setMaintenanceFlag(true);
    topologyNodes[10]->setMaintenanceFlag(true);
    //calculate Path again
    const std::vector<TopologyNodePtr> mStartNodes = topology->findPathBetween(sourceNodes, destinationNodes);
    //checks path from source node 12 to sink
    TopologyNodePtr mFirstStartNodeParent1 = mStartNodes[0]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mFirstStartNodeParent1->getId() == topologyNodes[9]->getId());
    TopologyNodePtr mFirstStartNodeParent2 = mFirstStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mFirstStartNodeParent2->getId() == topologyNodes[5]->getId());
    TopologyNodePtr mFirstStartNodeParent3 = mFirstStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mFirstStartNodeParent3->getId() == topologyNodes[2]->getId());
    TopologyNodePtr mFirstStartNodeParent4 = mFirstStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mFirstStartNodeParent4->getId() == topologyNodes[0]->getId());
    //checks path from source node 13 to sink
    TopologyNodePtr mSecondStartNodeParent1 = mStartNodes[1]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mSecondStartNodeParent1->getId() == topologyNodes[9]->getId());
    TopologyNodePtr mSecondStartNodeParent2 = mSecondStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mSecondStartNodeParent2->getId() == topologyNodes[5]->getId());
    TopologyNodePtr mSecondStartNodeParent3 = mSecondStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mSecondStartNodeParent3->getId() == topologyNodes[2]->getId());
    TopologyNodePtr mSecondStartNodeParent4 = mSecondStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mSecondStartNodeParent4->getId() == topologyNodes[0]->getId());
    //checks path from source node 14 to sink
    TopologyNodePtr mThirdStartNodeParent1 = mStartNodes[2]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mThirdStartNodeParent1->getId() == topologyNodes[11]->getId());
    TopologyNodePtr mThirdStartNodeParent2 = mThirdStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mThirdStartNodeParent2->getId() == topologyNodes[7]->getId());
    TopologyNodePtr mThirdStartNodeParent3 = mThirdStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mThirdStartNodeParent3->getId() == topologyNodes[4]->getId());
    TopologyNodePtr mThirdStartNodeParent4 = mThirdStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mThirdStartNodeParent4->getId() == topologyNodes[0]->getId());
}
TEST_F(MaintenanceServiceTest, sampleTest){

}
//
//TEST_F(MaintenanceServiceTest, PassingInvalidNodeIdTest){
//    auto workerRPCClient = std::make_shared<WorkerRPCClient>();
//    auto topology = Topology::create();
//    auto globalExecutionPlan = GlobalExecutionPlan::create();
//    auto queryCatalog = std::make_shared<QueryCatalog>();
//    auto queryRequestQueue = std::make_shared<QueryRequestQueue>(1);
//    auto maintenanceService = std::make_shared<MaintenanceService>(topology,queryCatalog,queryRequestQueue,globalExecutionPlan,workerRPCClient);
//
//    ASSERT_TRUE(true);
//}

}//namespace NES

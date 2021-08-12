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

#include <Catalogs/QueryCatalog.hpp>
#include <Services/MaintenanceService.hpp>
#include <Services/QueryParsingService.hpp>
#include <WorkQueues/NESRequestQueue.hpp>

#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Catalogs/StreamCatalogEntry.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gtest/gtest.h>
namespace NES{
class MaintenanceServiceTest : public ::testing::Test {
  public:
    TopologyPtr topology;
    std::shared_ptr<StreamCatalog> streamCatalog;

  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() {
      NES::setupLogging("StreamCatalogTest.log", NES::LOG_DEBUG);
      NES_INFO("Setup StreamCatalogTest test class.");
  }

  /* Will be called before a test is executed. */
  void TearDown() override { NES_INFO("Tear down StreamCatalogTest test case."); }

  void setupTopologyAndStreamCatalog() {

      auto cppCompiler = Compiler::CPPCompiler::create();
      auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
      auto queryParsingService = QueryParsingService::create(jitCompiler);
      streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);

        topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(1, "localhost", 123, 124, 4);
        topology->setAsRoot(rootNode);

        TopologyNodePtr node1 = TopologyNode::create(2, "localhost", 123, 124, 4);
        topology->addNewPhysicalNodeAsChild(rootNode, node1);

        TopologyNodePtr node2 = TopologyNode::create(3, "localhost", 123, 124, 4);
        topology->addNewPhysicalNodeAsChild(rootNode, node2);

        TopologyNodePtr node3 = TopologyNode::create(4, "localhost", 123, 124, 4);
        topology->addNewPhysicalNodeAsChild(rootNode, node3);

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string streamName = "car";

        streamCatalog->addLogicalStream(streamName, schema);

        SourceConfigPtr sourceConfig = SourceConfig::create();
        sourceConfig->setSourceFrequency(0);
        sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
        sourceConfig->setPhysicalStreamName("test2");
        sourceConfig->setLogicalStreamName("car");

        PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

        StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf, node3);

        streamCatalog->addPhysicalStream("car", streamCatalogEntry1);

    }


};

TEST_F(MaintenanceServiceTest, DISABLED_findPathBetweenIgnoresNodesMakredForMaintenanceTest) {
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

/**
 * Tests for response in case TopologyNode, Strategy or ExecutionNode doesnt exist
 */
TEST_F(MaintenanceServiceTest, InvalidTopologyNodeOrStrategyOrExecutionNodeTest){

    setupTopologyAndStreamCatalog();
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    NESRequestQueuePtr queryRequestQueue = std::make_shared<NESRequestQueue>(1);
    MaintenanceServicePtr maintenanceService = std::make_shared<MaintenanceService>(topology,queryCatalog,queryRequestQueue,globalExecutionPlan);
    MaintenanceService::QueryMigrationMessage invalidTopologyNodeMessage {0,false,"Topology with supplied ID doesnt exist"};
    MaintenanceService::QueryMigrationMessage invalidStrategyMessage {0,false,"Not a valid strategy"};
    MaintenanceService::QueryMigrationMessage noExecutionNodeMessage {0,true,"No queries deployed on Topology Node. Node can be taken down immediately"};
    auto response = maintenanceService->submitMaintenanceRequest(5,1).at(0);
    ASSERT_TRUE(response == invalidTopologyNodeMessage);
    response = maintenanceService->submitMaintenanceRequest(5,5).at(0);
    ASSERT_TRUE(response == invalidStrategyMessage);
    response = maintenanceService->submitMaintenanceRequest(1,1).at(0);
    ASSERT_TRUE(response == noExecutionNodeMessage);
    ASSERT_TRUE(topology->findNodeWithId(1)->getMaintenanceFlag() == true);
}
}//namespace NES

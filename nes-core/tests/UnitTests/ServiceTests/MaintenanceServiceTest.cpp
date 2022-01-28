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

#include "gtest/gtest.h"
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Topology/Topology.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Services/MaintenanceService.hpp>
#include <WorkQueues/RequestQueue.hpp>
#include <Phases/MigrationType.hpp>
#include <iostream>

using namespace NES;
class MaintenanceServiceTest : public testing::Test {
  public:
    Experimental::MaintenanceServicePtr maintenanceService;
    TopologyPtr topology;
    GlobalExecutionPlanPtr executionPlan;
    RequestQueuePtr nesRequestQueue;
    QueryCatalogPtr queryCatalog;


    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup MaintenanceService test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::setupLogging("MaintenanceService.log", NES::LOG_DEBUG);
        std::cout << "Setup MaintenanceService test case." << std::endl;
        topology = Topology::create();
        queryCatalog = std::make_shared<QueryCatalog>();
        executionPlan = GlobalExecutionPlan::create();
        nesRequestQueue = std::make_shared<RequestQueue>(1);
        maintenanceService = std::make_shared<Experimental::MaintenanceService>(topology, queryCatalog, nesRequestQueue, executionPlan);
    }


    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down MaintenanceService test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down MaintenanceService test class." << std::endl; }

    std::string ip = "127.0.0.1";
    uint32_t grpcPort = 1;
    uint16_t resources = 1;
    uint32_t dataPort = 1;
    uint64_t id = 1;

};

TEST_F(MaintenanceServiceTest, testMaintenanceService) {

    //Prepare
    TopologyNodePtr node = TopologyNode::create(id, ip, grpcPort, dataPort, resources);
    topology->setAsRoot(node);
    auto nonExistentType = Experimental::MigrationType::Value(4);
    //test no such Topology Node ID
    uint64_t nonExistentId = 0;
    auto [result1, info1] = maintenanceService->submitMaintenanceRequest(nonExistentId,nonExistentType);
    EXPECT_FALSE(result1);
    EXPECT_EQ(info1,"No Topology Node with ID 0");
    //test pass no such Execution Node
    auto [result2, info2] = maintenanceService->submitMaintenanceRequest(id,nonExistentType);
    EXPECT_TRUE(result2);
    EXPECT_EQ(info2,"No ExecutionNode for TopologyNode with ID: " + std::to_string(id) +". Node can be taken down for maintenance immediately");
    //add execution node
    executionPlan->addExecutionNode(ExecutionNode::createExecutionNode(node));
    //test no such MigrationType
    auto [result3, info3] = maintenanceService->submitMaintenanceRequest(id,nonExistentType);
    EXPECT_FALSE(result3);
    EXPECT_EQ(info3, "MigrationType: " + std::to_string(nonExistentType) + " not a valid type. Type must be either 1 (Restart), 2 (Migration with Buffering) or 3 (Migration without Buffering)");
    //pass valid TopologyNodeID with corresponding ExecutionNode and valid MigrationType
    auto [result4, info4] = maintenanceService->submitMaintenanceRequest(id, Experimental::MigrationType::Value::RESTART);
    EXPECT_TRUE(result4);
    EXPECT_EQ(info4, "Successfully submitted Query Migration Requests for all queries on Topology Node with ID: " + std::to_string(id));

}
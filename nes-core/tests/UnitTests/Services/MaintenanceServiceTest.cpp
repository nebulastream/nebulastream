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

#include "gtest/gtest.h"
#include <Catalogs/Query/QueryCatalog.hpp>
#include <NesBaseTest.hpp>
#include <Phases/MigrationType.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Services/MaintenanceService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <WorkQueues/RequestQueue.hpp>
#include <iostream>

using namespace NES;
class MaintenanceServiceTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    NES::Experimental::MaintenanceServicePtr maintenanceService;
    TopologyPtr topology;
    RequestQueuePtr nesRequestQueue;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MaintenanceService.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup MaintenanceService test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
        NES_DEBUG("Setup MaintenanceService test case.");
        topology = Topology::create();
        TopologyNodePtr root = TopologyNode::create(id, ip, grpcPort, dataPort, resources);
        topology->setAsRoot(root);
        nesRequestQueue = std::make_shared<RequestQueue>(1);
        maintenanceService = std::make_shared<NES::Experimental::MaintenanceService>(topology, nesRequestQueue);
    }

    std::string ip = "127.0.0.1";
    uint32_t grpcPort = 1;
    uint16_t resources = 1;
    uint32_t dataPort = 1;
    uint64_t id = 1;
};

TEST_F(MaintenanceServiceTest, testMaintenanceService) {

    //Prepare
    auto nonExistentType = NES::Experimental::MigrationType::Value(4);
    //test no such Topology Node ID
    uint64_t nonExistentId = 0;
    auto [result1, info1] = maintenanceService->submitMaintenanceRequest(nonExistentId, nonExistentType);
    EXPECT_FALSE(result1);
    EXPECT_EQ(info1, "No Topology Node with ID 0");
    //test pass no such Execution Node
    auto [result2, info2] = maintenanceService->submitMaintenanceRequest(id, nonExistentType);
    EXPECT_FALSE(result2);
    EXPECT_EQ(info2,
              "MigrationType: " + std::to_string(nonExistentType)
                  + " not a valid type. Type must be either 1 (Restart), 2 (Migration with Buffering) or 3 (Migration without "
                    "Buffering)");
    //test RESTART migration type behavior
    auto [result3, info3] = maintenanceService->submitMaintenanceRequest(id, NES::Experimental::MigrationType::Value::RESTART);
    EXPECT_FALSE(result3);
    EXPECT_EQ(info3, "RESTART currently not supported. Will be added in future");
    //test pass valid MigrationType and topology node
    auto [result4, info4] =
        maintenanceService->submitMaintenanceRequest(id, NES::Experimental::MigrationType::Value::MIGRATION_WITH_BUFFERING);
    EXPECT_TRUE(result4);
    EXPECT_EQ(info4, "Successfully submitted Query Migration Requests for Topology Node with ID: " + std::to_string(id));
}
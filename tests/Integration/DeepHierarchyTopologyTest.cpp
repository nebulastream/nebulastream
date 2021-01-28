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

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <iostream>

using namespace std;

namespace NES {
static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class DeepTopologyHierarchyTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("DeepTopologyHierarchyTest.log", NES::LOG_DEBUG);
        NES_DEBUG("Setup DeepTopologyHierarchyTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() { NES_DEBUG("TearDown DeepTopologyHierarchyTest test class."); }
    std::string ipAddress = "127.0.0.1";
};

/**
 * @brief This tests just outputs the default stream for a hierarchy with one relay which also produces data by itself
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0]
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(DeepTopologyHierarchyTest, DISABLED_testOutputAndAllSensors) {
    NES_DEBUG("DeepTopologyHierarchyTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("DeepTopologyHierarchyTest: Coordinator started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor, 1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=2
    EXPECT_TRUE(retStart1);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 1 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 2 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 3");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 30, port + 31, NodeType::Sensor);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 3 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 4");
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 40, port + 41, NodeType::Sensor);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    wrk4->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 2 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 5");
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 60, port + 61, NodeType::Sensor);
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart5);
    wrk5->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 6 started successfully");

    std::string outputFilePath = "testOutputAndAllSensors.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"" + outputFilePath
        + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "default_logical$id:INTEGER,default_logical$value:INTEGER\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("DeepTopologyHierarchyTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("DeepTopologyHierarchyTest: Test finished");
}

/**
 * @brief This tests just outputs the default stream for a hierarchy of two levels where each node produces data
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=7, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(DeepTopologyHierarchyTest, DISABLED_testSimpleQueryWithTwoLevelTreeWithDefaultSourceAndAllSensors) {
    NES_DEBUG("DeepTopologyHierarchyTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort, 12);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("DeepTopologyHierarchyTest: Coordinator started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor, 1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=2
    EXPECT_TRUE(retStart1);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 1 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor, 12);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 2 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 3");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 30, port + 31, NodeType::Sensor, 12);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 3 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 4");
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 40, port + 41, NodeType::Sensor, 12);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 4 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 5");
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 60, port + 61, NodeType::Sensor, 12);
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart5);
    wrk5->replaceParent(1, 5);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 5 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 6");
    NesWorkerPtr wrk6 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 70, port + 71, NodeType::Sensor, 12);
    bool retStart6 = wrk6->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart6);
    wrk6->replaceParent(1, 5);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 6 started successfully");

    std::string outputFilePath = "testOutput.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"" + outputFilePath
        + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "default_logical$id:INTEGER,default_logical$value:INTEGER\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("DeepTopologyHierarchyTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 6");
    bool retStopWrk6 = wrk6->stop(true);
    EXPECT_TRUE(retStopWrk6);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("DeepTopologyHierarchyTest: Test finished");
}

/**
 * @brief  * @brief This tests just outputs the default stream for a hierarchy with one relay which does not produce data
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0]
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(DeepTopologyHierarchyTest, DISABLED_testOutputAndNoSensors) {
    NES_DEBUG("DeepTopologyHierarchyTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("DeepTopologyHierarchyTest: Coordinator started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor, 1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=2
    EXPECT_TRUE(retStart1);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 1 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Worker);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 2 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 3");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 30, port + 31, NodeType::Sensor);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 3 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 4");
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 40, port + 41, NodeType::Sensor);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    wrk4->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 2 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 5");
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 60, port + 61, NodeType::Sensor);
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart5);
    wrk5->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 6 started successfully");

    std::string outputFilePath = "testOutputAndAllSensors.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"" + outputFilePath
        + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "default_logical$id:INTEGER,default_logical$value:INTEGER\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("DeepTopologyHierarchyTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("DeepTopologyHierarchyTest: Test finished");
}

/**
 * @brief This tests just outputs the default stream for a hierarchy of two levels where only leaves produce data
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=7, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(DeepTopologyHierarchyTest, DISABLED_testSimpleQueryWithTwoLevelTreeWithDefaultSourceAndWorker) {
    NES_DEBUG("DeepTopologyHierarchyTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("DeepTopologyHierarchyTest: Coordinator started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor, 1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=2
    EXPECT_TRUE(retStart1);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 1 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 2 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 3");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 30, port + 31, NodeType::Sensor);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 3 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 4");
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 40, port + 41, NodeType::Worker);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 4 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 5");
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 60, port + 61, NodeType::Sensor);
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart5);
    wrk5->replaceParent(1, 5);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 5 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 6");
    NesWorkerPtr wrk6 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 70, port + 71, NodeType::Sensor);
    bool retStart6 = wrk6->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart6);
    wrk6->replaceParent(1, 5);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 6 started successfully");

    std::string outputFilePath = "testOutput.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"" + outputFilePath
        + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "default_logical$id:INTEGER,default_logical$value:INTEGER\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("DeepTopologyHierarchyTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 6");
    bool retStopWrk6 = wrk6->stop(true);
    EXPECT_TRUE(retStopWrk6);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("DeepTopologyHierarchyTest: Test finished");
}

/**
 * @brief This tests just outputs the default stream for a hierarchy of three levels where only leaves produce data
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=7, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=9, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=8, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=11, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=10, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(DeepTopologyHierarchyTest, DISABLED_testSimpleQueryWithThreeLevelTreeWithDefaultSourceAndWorker) {
    NES_DEBUG("DeepTopologyHierarchyTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort, 12);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("DeepTopologyHierarchyTest: Coordinator started successfully");

    /**
     * Worker
     */
    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor, 1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=2
    EXPECT_TRUE(retStart1);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 1 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Worker, 12);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 2 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 3");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 30, port + 31, NodeType::Worker, 12);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 3 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 4");
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 40, port + 41, NodeType::Worker, 12);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 4 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 5");
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 60, port + 61, NodeType::Worker, 12);
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart5);
    wrk5->replaceParent(1, 5);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 5 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 6");
    NesWorkerPtr wrk6 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 70, port + 71, NodeType::Worker, 12);
    bool retStart6 = wrk6->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart6);
    wrk6->replaceParent(1, 5);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 6 started successfully");

    /**
     * Sensors
     */
    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 7");
    NesWorkerPtr wrk7 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 80, port + 81, NodeType::Sensor, 12);
    bool retStart7 = wrk7->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart7);
    wrk7->replaceParent(1, 6);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 7 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 8");
    NesWorkerPtr wrk8 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 90, port + 91, NodeType::Sensor, 12);
    bool retStart8 = wrk8->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart8);
    wrk8->replaceParent(1, 7);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 8 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 9");
    NesWorkerPtr wrk9 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 100, port + 101, NodeType::Sensor, 12);
    bool retStart9 = wrk9->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart9);
    wrk9->replaceParent(1, 3);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 9 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 10");
    NesWorkerPtr wrk10 =
        std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 110, port + 111, NodeType::Sensor, 12);
    bool retStart10 = wrk10->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart10);
    wrk10->replaceParent(1, 4);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 10 started successfully");

    std::string outputFilePath = "testOutput.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\"" + outputFilePath
        + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "default_logical$id:INTEGER,default_logical$value:INTEGER\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("DeepTopologyHierarchyTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 6");
    bool retStopWrk6 = wrk6->stop(true);
    EXPECT_TRUE(retStopWrk6);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 7");
    bool retStopWrk7 = wrk7->stop(true);
    EXPECT_TRUE(retStopWrk7);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 8");
    bool retStopWrk8 = wrk8->stop(true);
    EXPECT_TRUE(retStopWrk8);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 9");
    bool retStopWrk9 = wrk9->stop(true);
    EXPECT_TRUE(retStopWrk9);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 10");
    bool retStopWrk10 = wrk10->stop(true);
    EXPECT_TRUE(retStopWrk10);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("DeepTopologyHierarchyTest: Test finished");
}

/**
 * @brief This tests applies project and selection on a three level hierarchy
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=7, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=9, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=8, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=11, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=10, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(DeepTopologyHierarchyTest, DISABLED_testSelectProjectThreeLevel) {
    NES_DEBUG("DeepTopologyHierarchyTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("DeepTopologyHierarchyTest: Coordinator started successfully");

    /**
     * Worker
     */
    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor, 1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=2
    EXPECT_TRUE(retStart1);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 1 started successfully");

    //register logical stream
    std::string testSchema = "Schema::create()->addField(createField(\"val1\", UINT64))->"
                             "addField(createField(\"val2\", UINT64))->"
                             "addField(createField(\"val3\", UINT64));";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("testStream", testSchemaFileName);

    PhysicalStreamConfigPtr conf =
        PhysicalStreamConfig::create(/**Source Type**/ "CSVSource", /**Source Config**/ "../tests/test_data/testCSV.csv",
                                     /**Source Frequence**/ 1, /**Number Of Tuples To Produce Per Buffer**/ 3,
                                     /**Number of Buffers To Produce**/ 1, /**Physical Stream Name**/ "test_stream",
                                     /**Logical Stream Name**/ "testStream", false);

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Worker);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 2 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 3");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 30, port + 31, NodeType::Worker);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 3 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 4");
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 40, port + 41, NodeType::Worker);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 4 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 5");
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 60, port + 61, NodeType::Worker);
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart5);
    wrk5->replaceParent(1, 5);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 5 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 6");
    NesWorkerPtr wrk6 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 70, port + 71, NodeType::Worker);
    bool retStart6 = wrk6->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart6);
    wrk6->replaceParent(1, 5);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 6 started successfully");

    /**
     * Sensors
     */
    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 7");
    NesWorkerPtr wrk7 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 80, port + 81, NodeType::Sensor);
    bool retStart7 = wrk7->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart7);
    wrk7->replaceParent(1, 6);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 7 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 8");
    NesWorkerPtr wrk8 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 90, port + 91, NodeType::Sensor);
    bool retStart8 = wrk8->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart8);
    wrk8->replaceParent(1, 7);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 8 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 9");
    NesWorkerPtr wrk9 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 100, port + 101, NodeType::Sensor);
    bool retStart9 = wrk9->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart9);
    wrk9->replaceParent(1, 3);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 9 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 10");
    NesWorkerPtr wrk10 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 110, port + 111, NodeType::Sensor);
    bool retStart10 = wrk10->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart10);
    wrk10->replaceParent(1, 4);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 10 started successfully");

    wrk7->registerPhysicalStream(conf);
    wrk8->registerPhysicalStream(conf);
    wrk9->registerPhysicalStream(conf);
    wrk10->registerPhysicalStream(conf);

    std::string outputFilePath = "testOutput.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");
    string query = "Query::from(\"testStream\").filter(Attribute(\"val1\") < "
                   "3).project(Attribute(\"val3\")).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "testStream$val3:INTEGER\n"
                             "3\n"
                             "4\n"
                             "3\n"
                             "4\n"
                             "3\n"
                             "4\n"
                             "3\n"
                             "4\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("DeepTopologyHierarchyTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 6");
    bool retStopWrk6 = wrk6->stop(true);
    EXPECT_TRUE(retStopWrk6);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 7");
    bool retStopWrk7 = wrk7->stop(true);
    EXPECT_TRUE(retStopWrk7);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 8");
    bool retStopWrk8 = wrk8->stop(true);
    EXPECT_TRUE(retStopWrk8);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 9");
    bool retStopWrk9 = wrk9->stop(true);
    EXPECT_TRUE(retStopWrk9);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 10");
    bool retStopWrk10 = wrk10->stop(true);
    EXPECT_TRUE(retStopWrk10);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("DeepTopologyHierarchyTest: Test finished");
}

/**
 * @brief This tests applies a window on a three level hierarchy
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=7, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=9, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=8, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=11, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=10, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(DeepTopologyHierarchyTest, DISABLED_testWindowThreeLevel) {
    NES_DEBUG("DeepTopologyHierarchyTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("DeepTopologyHierarchyTest: Coordinator started successfully");

    /**
     * Worker
     */
    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor, 1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=2
    EXPECT_TRUE(retStart1);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 1 started successfully");

    //register logical stream
    std::string testSchema =
        R"(Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64)->addField("ts", BasicType::UINT64);)";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Worker);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 2 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 3");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 30, port + 31, NodeType::Worker);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 3 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 4");
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 40, port + 41, NodeType::Worker);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 4 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 5");
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 60, port + 61, NodeType::Worker);
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart5);
    wrk5->replaceParent(1, 5);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 5 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 6");
    NesWorkerPtr wrk6 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 70, port + 71, NodeType::Worker);
    bool retStart6 = wrk6->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart6);
    wrk6->replaceParent(1, 5);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 6 started successfully");

    /**
     * Sensors
     */
    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 7");
    NesWorkerPtr wrk7 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 80, port + 81, NodeType::Sensor);
    bool retStart7 = wrk7->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart7);
    wrk7->replaceParent(1, 6);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 7 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 8");
    NesWorkerPtr wrk8 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 90, port + 91, NodeType::Sensor);
    bool retStart8 = wrk8->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart8);
    wrk8->replaceParent(1, 7);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 8 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 9");
    NesWorkerPtr wrk9 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 100, port + 101, NodeType::Sensor);
    bool retStart9 = wrk9->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart9);
    wrk9->replaceParent(1, 3);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 9 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 10");
    NesWorkerPtr wrk10 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 110, port + 111, NodeType::Sensor);
    bool retStart10 = wrk10->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart10);
    wrk10->replaceParent(1, 4);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 10 started successfully");

    PhysicalStreamConfigPtr conf =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 3, "test_stream", "window", false);
    wrk7->registerPhysicalStream(conf);
    wrk8->registerPhysicalStream(conf);
    wrk9->registerPhysicalStream(conf);
    wrk10->registerPhysicalStream(conf);

    std::string outputFilePath = "testOutput.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"ts\")), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "_$start:INTEGER,_$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "1000,2000,1,68\n"
                             "2000,3000,2,112\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("DeepTopologyHierarchyTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 6");
    bool retStopWrk6 = wrk6->stop(true);
    EXPECT_TRUE(retStopWrk6);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 7");
    bool retStopWrk7 = wrk7->stop(true);
    EXPECT_TRUE(retStopWrk7);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 8");
    bool retStopWrk8 = wrk8->stop(true);
    EXPECT_TRUE(retStopWrk8);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 9");
    bool retStopWrk9 = wrk9->stop(true);
    EXPECT_TRUE(retStopWrk9);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 10");
    bool retStopWrk10 = wrk10->stop(true);
    EXPECT_TRUE(retStopWrk10);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("DeepTopologyHierarchyTest: Test finished");
}

/**
 * @brief This tests applies a merge on a three level hierarchy
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=7, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=9, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=8, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=11, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=10, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(DeepTopologyHierarchyTest, DISABLED_testMergeThreeLevel) {
    NES_DEBUG("DeepTopologyHierarchyTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("DeepTopologyHierarchyTest: Coordinator started successfully");

    /**
     * Worker
     */
    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor, 1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=2
    EXPECT_TRUE(retStart1);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 1 started successfully");

    //register logical stream
    std::string testSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)->addField(\"value\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("car", testSchemaFileName);
    wrk1->registerLogicalStream("truck", testSchemaFileName);

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Worker);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 2 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 3");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 30, port + 31, NodeType::Worker);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 3 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 4");
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 40, port + 41, NodeType::Worker);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 4 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 5");
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 60, port + 61, NodeType::Worker);
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart5);
    wrk5->replaceParent(1, 5);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 5 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 6");
    NesWorkerPtr wrk6 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 70, port + 71, NodeType::Worker);
    bool retStart6 = wrk6->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart6);
    wrk6->replaceParent(1, 5);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 6 started successfully");

    /**
     * Sensors
     */

    //register physical stream
    PhysicalStreamConfigPtr confCar = PhysicalStreamConfig::create("DefaultSource", "", 1, 0, 1, "physical_car", "car");
    PhysicalStreamConfigPtr confTruck = PhysicalStreamConfig::create("DefaultSource", "", 1, 0, 1, "physical_truck", "truck");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 7");
    NesWorkerPtr wrk7 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 80, port + 81, NodeType::Sensor);
    bool retStart7 = wrk7->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart7);
    wrk7->replaceParent(1, 6);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 7 started successfully");
    wrk7->registerPhysicalStream(confCar);

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 8");
    NesWorkerPtr wrk8 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 90, port + 91, NodeType::Sensor);
    bool retStart8 = wrk8->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart8);
    wrk8->replaceParent(1, 7);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 8 started successfully");
    wrk8->registerPhysicalStream(confTruck);

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 9");
    NesWorkerPtr wrk9 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 100, port + 101, NodeType::Sensor);
    bool retStart9 = wrk9->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart9);
    wrk9->replaceParent(1, 3);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 9 started successfully");
    wrk9->registerPhysicalStream(confCar);

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 10");
    NesWorkerPtr wrk10 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 110, port + 111, NodeType::Sensor);
    bool retStart10 = wrk10->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart10);
    wrk10->replaceParent(1, 4);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 10 started successfully");
    wrk10->registerPhysicalStream(confTruck);

    std::string outputFilePath = "testOutput.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");
    string query = "Query::from(\"car\").merge(Query::from(\"truck\")).sink(FileSinkDescriptor::create(\"" + outputFilePath
        + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "car$id:INTEGER,car$value:INTEGER\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("DeepTopologyHierarchyTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 6");
    bool retStopWrk6 = wrk6->stop(true);
    EXPECT_TRUE(retStopWrk6);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 7");
    bool retStopWrk7 = wrk7->stop(true);
    EXPECT_TRUE(retStopWrk7);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 8");
    bool retStopWrk8 = wrk8->stop(true);
    EXPECT_TRUE(retStopWrk8);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 9");
    bool retStopWrk9 = wrk9->stop(true);
    EXPECT_TRUE(retStopWrk9);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 10");
    bool retStopWrk10 = wrk10->stop(true);
    EXPECT_TRUE(retStopWrk10);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("DeepTopologyHierarchyTest: Test finished");
}

/**
 * @brief This tests just outputs the default stream for a hierarchy of two levels where only leaves produce data
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=7, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(DeepTopologyHierarchyTest, DISABLED_testSimpleQueryWithThreeLevelTreeWithWindowDataAndWorker) {
    NES_DEBUG("DeepTopologyHierarchyTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("DeepTopologyHierarchyTest: Coordinator started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor, 1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=2
    EXPECT_TRUE(retStart1);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 1 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 2 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 3");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 30, port + 31, NodeType::Sensor);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 3 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 4");
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 40, port + 41, NodeType::Worker);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 4 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 5");
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 60, port + 61, NodeType::Sensor);
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart5);
    wrk5->replaceParent(1, 5);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 5 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 6");
    NesWorkerPtr wrk6 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 70, port + 71, NodeType::Sensor);
    bool retStart6 = wrk6->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart6);
    wrk6->replaceParent(1, 5);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 6 started successfully");

    std::string window =
        R"(Schema::create()->addField(createField("win2", INT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    PhysicalStreamConfigPtr conf =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 5, 3, "test_stream", "window", false);
    wrk2->registerPhysicalStream(conf);
    wrk3->registerPhysicalStream(conf);
    wrk5->registerPhysicalStream(conf);
    wrk6->registerPhysicalStream(conf);

    std::string outputFilePath = "testOutput.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");
    string query = R"(Query::from("window")
        .filter(Attribute("id") < 15)
        .windowByKey(Attribute("id"), SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)), Sum(Attribute("value")))
        .windowByKey(Attribute("id"), TumblingWindow::of(EventTime(Attribute("start")), Seconds(1)), Sum(Attribute("value")))
        .filter(Attribute("id") < 10)
        .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(2)), Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "_$start:INTEGER,_$end:INTEGER,window$value:INTEGER\n"
                             "0,2000,96\n"
                             "2000,4000,256\n"
                             "4000,6000,168\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("DeepTopologyHierarchyTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 6");
    bool retStopWrk6 = wrk6->stop(true);
    EXPECT_TRUE(retStopWrk6);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("DeepTopologyHierarchyTest: Test finished");
}

//TODO:add join once it is implemented correctly
}// namespace NES

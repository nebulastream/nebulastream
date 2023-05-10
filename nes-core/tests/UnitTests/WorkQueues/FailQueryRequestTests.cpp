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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Common/Identifiers.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <NesBaseTest.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Services/QueryService.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <WorkQueues/RequestTypes/Experimental/FailQueryRequest.hpp>
#include <WorkQueues/StorageHandles/SerialStorageHandler.hpp>
#include <gtest/gtest.h>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

class FailQueryRequestTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryFailureTest.log", NES::LogLevel::LOG_DEBUG);
        auto inputSequence = std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv";
        std::ofstream inputSequenceStream(inputSequence);
        for (int i = 1; i < 100000; ++i) {
            inputSequenceStream << std::to_string(i) << std::endl;
        }
        inputSequenceStream.close();
        ASSERT_FALSE(inputSequenceStream.fail());
    }
    static void TearDownTestCase() {
        auto inputSequence = std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv";
        remove(inputSequence.c_str());
    }
};

TEST_F(FailQueryRequestTest, testQueryFailureForFaultySource) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");
    crd->getSourceCatalog()->addLogicalSource("test", "Schema::create()->addField(createField(\"value\", BasicType::UINT64));");
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::string(TEST_DATA_DIRECTORY) + "/sequence_long.csv");
    csvSourceType->setGatheringInterval(1);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(2);
    csvSourceType->setNumberOfBuffersToProduce(100000);
    csvSourceType->setSkipHeader(false);

    workerConfig1->coordinatorPort = port;
    auto physicalSource1 = PhysicalSource::create("test", "physical_test", csvSourceType);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testDeployTwoWorkerMergeUsingBottomUp.out";

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = R"(Query::from("test").filter(Attribute("value")>2).sink(FileSinkDescriptor::create(")" + outputFilePath
        + R"(", "CSV_FORMAT", "APPEND"));)";
    NES_DEBUG("query=" << query);
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    //wait until query has been succesfully deployed on worker 1
    while (wrk1->getNodeEngine()->getQueryStatus(queryId) != NES::Runtime::Execution::ExecutableQueryPlanStatus::Running) {
        NES_DEBUG2("query not running yet")
        sleep(1);
    }
    EXPECT_EQ(wrk1->getNodeEngine()->getQueryStatus(queryId), NES::Runtime::Execution::ExecutableQueryPlanStatus::Running);

    //wait until query has been succesfully deployed on worker at coordinator
    while (wrk1->getNodeEngine()->getQueryStatus(queryId) != NES::Runtime::Execution::ExecutableQueryPlanStatus::Running) {
        NES_DEBUG2("query not running yet")
        sleep(1);
    }
    EXPECT_EQ(wrk1->getNodeEngine()->getQueryStatus(queryId), NES::Runtime::Execution::ExecutableQueryPlanStatus::Running);

    //check status of query in the query catalog
    EXPECT_EQ(crd->getQueryCatalogService()->getEntryForQuery(queryId)->getQueryStatus(), QueryStatus::RUNNING);

    //check status of query in the global query plan
    auto sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    EXPECT_NE(sharedQueryId, INVALID_SHARED_QUERY_ID);
    EXPECT_EQ(crd->getGlobalQueryPlan()->getSharedQueryPlan(sharedQueryId)->getStatus(), SharedQueryPlanStatus::Deployed);

    //execute fail query request
    auto subPlanId = wrk1->getNodeEngine()->getSubQueryIds(queryId).front();
    auto workerRpcClient = std::make_shared<WorkerRPCClient>();
    Experimental::FailQueryRequest failQueryRequest(queryId, subPlanId, 1, workerRpcClient);
    auto serialStorageHandler  = SerialStorageHandler(crd->getGlobalExecutionPlan(), crd->getTopology(), crd->getQueryCatalogService(), crd->getGlobalQueryPlan(), crd->getSourceCatalog(), crd->getUDFCatalog());
    failQueryRequest.execute(serialStorageHandler);

    //check status of query in the query catalog
    EXPECT_EQ(crd->getQueryCatalogService()->getEntryForQuery(queryId)->getQueryStatus(), QueryStatus::FAILED);

    //check status of query in the global query plan
    EXPECT_EQ(crd->getGlobalQueryPlan()->getSharedQueryPlan(sharedQueryId)->getStatus(), SharedQueryPlanStatus::Failed);

    //wait until query has been succesfully deployed on worker 1
    while (wrk1->getNodeEngine()->getQueryStatus(queryId) == NES::Runtime::Execution::ExecutableQueryPlanStatus::Running) {
        NES_DEBUG2("query still running")
        sleep(1);
    }
    EXPECT_EQ(wrk1->getNodeEngine()->getQueryStatus(queryId), NES::Runtime::Execution::ExecutableQueryPlanStatus::Invalid);

    //wait until query has been succesfully deployed on worker at coordinator
    while (crd->getNesWorker()->getNodeEngine()->getQueryStatus(queryId) == NES::Runtime::Execution::ExecutableQueryPlanStatus::Running) {
        NES_DEBUG2("query still running")
        sleep(1);
    }
    EXPECT_EQ(crd->getNesWorker()->getNodeEngine()->getQueryStatus(queryId), NES::Runtime::Execution::ExecutableQueryPlanStatus::Invalid);
}
}// namespace NES
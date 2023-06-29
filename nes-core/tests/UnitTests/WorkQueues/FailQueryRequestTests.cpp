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

#include <Catalogs/Query/QuerySubPlanMetaData.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Common/Identifiers.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <Exceptions/QueryUndeploymentRpcException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <NesBaseTest.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Services/QueryService.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <WorkQueues/RequestTypes/Experimental/FailQueryRequest.hpp>
#include <WorkQueues/StorageHandles/LockManager.hpp>
#include <WorkQueues/StorageHandles/SerialStorageHandler.hpp>
#include <WorkQueues/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
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
    static constexpr auto sleepDuration = std::chrono::milliseconds(250);
    static constexpr auto defaultTimeout = std::chrono::seconds(60);
};

//test successful execution of fail query request for a single query
TEST_F(FailQueryRequestTest, testValidFailRequest) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("Fail Query Request Test: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    ASSERT_NE(port, 0UL);
    NES_DEBUG("Fail Query Request Test: Coordinator started successfully");
    crd->getSourceCatalog()->addLogicalSource("test", "Schema::create()->addField(createField(\"value\", BasicType::UINT64));");
    NES_DEBUG("Fail Query Request Test: Coordinator started successfully");

    NES_DEBUG("Fail Query Request Test: Start worker 1");
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
    ASSERT_TRUE(retStart1);
    NES_INFO("Fail Query Request Test: Worker1 started successfully");

    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, 100, 1));

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testDeployTwoWorkerMergeUsingBottomUp.out";

    NES_INFO("Fail Query Request Test: Submit query");
    string query = R"(Query::from("test").filter(Attribute("value")>2).sink(FileSinkDescriptor::create(")" + outputFilePath
        + R"(", "CSV_FORMAT", "APPEND"));)";
    NES_DEBUG("query= {}", query);
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    //wait until query has been succesfully deployed on worker 1
    auto timeoutInSec = std::chrono::seconds(defaultTimeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (wrk1->getNodeEngine()->getQueryStatus(queryId) != NES::Runtime::Execution::ExecutableQueryPlanStatus::Running
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
    }
    ASSERT_EQ(wrk1->getNodeEngine()->getQueryStatus(queryId), NES::Runtime::Execution::ExecutableQueryPlanStatus::Running);

    //wait until query has been succesfully deployed on worker at coordinator
    start_timestamp = std::chrono::system_clock::now();
    while (crd->getNodeEngine()->getQueryStatus(queryId) != NES::Runtime::Execution::ExecutableQueryPlanStatus::Running
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
    }
    ASSERT_EQ(crd->getNodeEngine()->getQueryStatus(queryId), NES::Runtime::Execution::ExecutableQueryPlanStatus::Running);

    //check status of query in the query catalog
    start_timestamp = std::chrono::system_clock::now();
    while (crd->getQueryCatalogService()->getEntryForQuery(queryId)->getQueryStatus() != QueryStatus::RUNNING
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
    }
    ASSERT_EQ(crd->getQueryCatalogService()->getEntryForQuery(queryId)->getQueryStatus(), QueryStatus::RUNNING);

    //check status of query in the global query plan
    auto sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    start_timestamp = std::chrono::system_clock::now();
    while (sharedQueryId == INVALID_SHARED_QUERY_ID && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
        sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    }
    ASSERT_NE(sharedQueryId, INVALID_SHARED_QUERY_ID);

    start_timestamp = std::chrono::system_clock::now();
    while (crd->getGlobalQueryPlan()->getSharedQueryPlan(sharedQueryId)->getStatus() != SharedQueryPlanStatus::Deployed
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
    }
    ASSERT_EQ(crd->getGlobalQueryPlan()->getSharedQueryPlan(sharedQueryId)->getStatus(), SharedQueryPlanStatus::Deployed);

    //execute fail query request
    auto subPlanId = wrk1->getNodeEngine()->getSubQueryIds(queryId).front();
    auto workerRpcClient = std::make_shared<WorkerRPCClient>();
    Experimental::FailQueryRequest failQueryRequest(queryId, subPlanId, 1, workerRpcClient);
    auto lockManager = std::make_shared<LockManager>(crd->getGlobalExecutionPlan(),
                                                     crd->getTopology(),
                                                     crd->getQueryCatalogService(),
                                                     crd->getGlobalQueryPlan(),
                                                     crd->getSourceCatalog(),
                                                     crd->getUDFCatalog());
    auto storageHandler = TwoPhaseLockingStorageHandler(lockManager);
    failQueryRequest.execute(storageHandler);

    //check status of query in the query catalog
    start_timestamp = std::chrono::system_clock::now();
    while (crd->getQueryCatalogService()->getEntryForQuery(queryId)->getQueryStatus() != QueryStatus::FAILED
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not failed yet")
        std::this_thread::sleep_for(sleepDuration);
    }
    EXPECT_EQ(crd->getQueryCatalogService()->getEntryForQuery(queryId)->getQueryStatus(), QueryStatus::FAILED);
    auto entry = crd->getQueryCatalogService()->getAllQueryCatalogEntries()[queryId];
    EXPECT_EQ(entry->getQueryStatus(), QueryStatus::FAILED);
    for (const auto& subQueryPlanMetaData : entry->getAllSubQueryPlanMetaData()) {
        EXPECT_EQ(subQueryPlanMetaData->getSubQueryStatus(), QueryStatus::FAILED);
    }

    //check status of query in the global query plan
    start_timestamp = std::chrono::system_clock::now();
    while (crd->getGlobalQueryPlan()->getSharedQueryPlan(sharedQueryId)->getStatus() != SharedQueryPlanStatus::Failed
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not stopped yet")
        std::this_thread::sleep_for(sleepDuration);
    }
    EXPECT_EQ(crd->getGlobalQueryPlan()->getSharedQueryPlan(sharedQueryId)->getStatus(), SharedQueryPlanStatus::Failed);

    //wait until query has been stopped running on worker 1
    start_timestamp = std::chrono::system_clock::now();
    while (wrk1->getNodeEngine()->getQueryStatus(queryId) == NES::Runtime::Execution::ExecutableQueryPlanStatus::Running
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query still running")
        std::this_thread::sleep_for(sleepDuration);
    }
    EXPECT_EQ(wrk1->getNodeEngine()->getQueryStatus(queryId), NES::Runtime::Execution::ExecutableQueryPlanStatus::Invalid);

    //wait until query has been stopped on worker at coordinator
    start_timestamp = std::chrono::system_clock::now();
    while (crd->getNesWorker()->getNodeEngine()->getQueryStatus(queryId)
               == NES::Runtime::Execution::ExecutableQueryPlanStatus::Running
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query still running")
        std::this_thread::sleep_for(sleepDuration);
    }
    EXPECT_TRUE(TestUtils::checkFailedOrTimeout(queryId, queryCatalogService, std::chrono::seconds(5)));
    EXPECT_EQ(crd->getNesWorker()->getNodeEngine()->getQueryStatus(queryId),
              NES::Runtime::Execution::ExecutableQueryPlanStatus::Invalid);
    bool stopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(stopWrk1);
    bool stopCrd = crd->stopCoordinator(true);
    EXPECT_TRUE(stopCrd);
}

//test error handling if a fail query request is executed but no query with the supplied id exists
TEST_F(FailQueryRequestTest, testInvalidQueryId) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("Fail Query Request Test: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    ASSERT_NE(port, 0UL);
    NES_DEBUG("Fail Query Request Test: Coordinator started successfully");
    crd->getSourceCatalog()->addLogicalSource("test", "Schema::create()->addField(createField(\"value\", BasicType::UINT64));");
    NES_DEBUG("Fail Query Request Test: Coordinator started successfully");

    NES_DEBUG("Fail Query Request Test: Start worker 1");
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
    ASSERT_TRUE(retStart1);
    NES_INFO("Fail Query Request Test: Worker1 started successfully");

    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, 100, 1));

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testDeployTwoWorkerMergeUsingBottomUp.out";

    NES_INFO("Fail Query Request Test: Submit query");
    string query = R"(Query::from("test").filter(Attribute("value")>2).sink(FileSinkDescriptor::create(")" + outputFilePath
        + R"(", "CSV_FORMAT", "APPEND"));)";
    NES_DEBUG("query= {}", query);
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    //wait until query has been succesfully deployed on worker 1
    auto timeoutInSec = std::chrono::seconds(defaultTimeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (wrk1->getNodeEngine()->getQueryStatus(queryId) != NES::Runtime::Execution::ExecutableQueryPlanStatus::Running
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
    }
    ASSERT_EQ(wrk1->getNodeEngine()->getQueryStatus(queryId), NES::Runtime::Execution::ExecutableQueryPlanStatus::Running);

    //wait until query has been succesfully deployed on worker at coordinator
    start_timestamp = std::chrono::system_clock::now();
    while (crd->getNodeEngine()->getQueryStatus(queryId) != NES::Runtime::Execution::ExecutableQueryPlanStatus::Running
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
    }
    ASSERT_EQ(crd->getNodeEngine()->getQueryStatus(queryId), NES::Runtime::Execution::ExecutableQueryPlanStatus::Running);

    //check status of query in the query catalog
    start_timestamp = std::chrono::system_clock::now();
    while (crd->getQueryCatalogService()->getEntryForQuery(queryId)->getQueryStatus() != QueryStatus::RUNNING
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
    }
    ASSERT_EQ(crd->getQueryCatalogService()->getEntryForQuery(queryId)->getQueryStatus(), QueryStatus::RUNNING);

    //check status of query in the global query plan
    auto sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    start_timestamp = std::chrono::system_clock::now();
    while (sharedQueryId == INVALID_SHARED_QUERY_ID && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
        sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    }
    ASSERT_NE(sharedQueryId, INVALID_SHARED_QUERY_ID);

    start_timestamp = std::chrono::system_clock::now();
    while (crd->getGlobalQueryPlan()->getSharedQueryPlan(sharedQueryId)->getStatus() != SharedQueryPlanStatus::Deployed
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
    }
    ASSERT_EQ(crd->getGlobalQueryPlan()->getSharedQueryPlan(sharedQueryId)->getStatus(), SharedQueryPlanStatus::Deployed);

    //execute fail query request
    auto subPlanId = wrk1->getNodeEngine()->getSubQueryIds(queryId).front();
    auto workerRpcClient = std::make_shared<WorkerRPCClient>();

    //creating request with wrong query id
    Experimental::FailQueryRequest failQueryRequest(queryId + 1, subPlanId, 1, workerRpcClient);

    auto lockManager = std::make_shared<LockManager>(crd->getGlobalExecutionPlan(),
                                                     crd->getTopology(),
                                                     crd->getQueryCatalogService(),
                                                     crd->getGlobalQueryPlan(),
                                                     crd->getSourceCatalog(),
                                                     crd->getUDFCatalog());
    auto storageHandler = TwoPhaseLockingStorageHandler(lockManager);
    EXPECT_THROW(failQueryRequest.execute(storageHandler), QueryNotFoundException);
    bool stopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(stopWrk1);
    bool stopCrd = crd->stopCoordinator(true);
    EXPECT_TRUE(stopCrd);
}

//test error handling when trying to let a query fail after it has already been set to the status STOPPED
TEST_F(FailQueryRequestTest, testWrongQueryStatus) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("Fail Query Request Test: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    ASSERT_NE(port, 0UL);
    NES_DEBUG("Fail Query Request Test: Coordinator started successfully");
    crd->getSourceCatalog()->addLogicalSource("test", "Schema::create()->addField(createField(\"value\", BasicType::UINT64));");
    NES_DEBUG("Fail Query Request Test: Coordinator started successfully");

    NES_DEBUG("Fail Query Request Test: Start worker 1");
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
    ASSERT_TRUE(retStart1);
    NES_INFO("Fail Query Request Test: Worker1 started successfully");

    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, 100, 1));

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testDeployTwoWorkerMergeUsingBottomUp.out";

    NES_INFO("Fail Query Request Test: Submit query");
    string query = R"(Query::from("test").filter(Attribute("value")>2).sink(FileSinkDescriptor::create(")" + outputFilePath
        + R"(", "CSV_FORMAT", "APPEND"));)";
    NES_DEBUG("query= {}", query);
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    //wait until query has been succesfully deployed on worker 1
    auto timeoutInSec = std::chrono::seconds(defaultTimeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (wrk1->getNodeEngine()->getQueryStatus(queryId) != NES::Runtime::Execution::ExecutableQueryPlanStatus::Running
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
    }
    ASSERT_EQ(wrk1->getNodeEngine()->getQueryStatus(queryId), NES::Runtime::Execution::ExecutableQueryPlanStatus::Running);

    //wait until query has been succesfully deployed on worker at coordinator
    start_timestamp = std::chrono::system_clock::now();
    while (crd->getNodeEngine()->getQueryStatus(queryId) != NES::Runtime::Execution::ExecutableQueryPlanStatus::Running
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
    }
    ASSERT_EQ(crd->getNodeEngine()->getQueryStatus(queryId), NES::Runtime::Execution::ExecutableQueryPlanStatus::Running);

    //check status of query in the query catalog
    start_timestamp = std::chrono::system_clock::now();
    while (crd->getQueryCatalogService()->getEntryForQuery(queryId)->getQueryStatus() != QueryStatus::RUNNING
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
    }
    ASSERT_EQ(crd->getQueryCatalogService()->getEntryForQuery(queryId)->getQueryStatus(), QueryStatus::RUNNING);

    //check status of query in the global query plan
    auto sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    start_timestamp = std::chrono::system_clock::now();
    while (sharedQueryId == INVALID_SHARED_QUERY_ID && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
        sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    }
    ASSERT_NE(sharedQueryId, INVALID_SHARED_QUERY_ID);

    start_timestamp = std::chrono::system_clock::now();
    while (crd->getGlobalQueryPlan()->getSharedQueryPlan(sharedQueryId)->getStatus() != SharedQueryPlanStatus::Deployed
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
    }
    ASSERT_EQ(crd->getGlobalQueryPlan()->getSharedQueryPlan(sharedQueryId)->getStatus(), SharedQueryPlanStatus::Deployed);

    crd->getQueryCatalogService()->getEntryForQuery(queryId)->setQueryStatus(QueryStatus::STOPPED);

    //execute fail query request
    auto subPlanId = wrk1->getNodeEngine()->getSubQueryIds(queryId).front();
    auto workerRpcClient = std::make_shared<WorkerRPCClient>();
    Experimental::FailQueryRequest failQueryRequest(queryId, subPlanId, 1, workerRpcClient);
    auto lockManager = std::make_shared<LockManager>(crd->getGlobalExecutionPlan(),
                                                     crd->getTopology(),
                                                     crd->getQueryCatalogService(),
                                                     crd->getGlobalQueryPlan(),
                                                     crd->getSourceCatalog(),
                                                     crd->getUDFCatalog());
    auto storageHandler = TwoPhaseLockingStorageHandler(lockManager);

    EXPECT_THROW(failQueryRequest.execute(storageHandler), InvalidQueryStatusException);

    //make sure that query is stopped before shutting down node engine to avoid errors in shutdown
    EXPECT_TRUE(wrk1->getNodeEngine()->stopQuery(queryId));
    bool stopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(stopWrk1);
    bool stopCrd = crd->stopCoordinator(true);
    EXPECT_TRUE(stopCrd);
}

//test if exception is thrown when the undeployment did not succeed
TEST_F(FailQueryRequestTest, testUndeploymentFailure) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("Fail Query Request Test: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    ASSERT_NE(port, 0UL);
    NES_DEBUG("Fail Query Request Test: Coordinator started successfully");
    crd->getSourceCatalog()->addLogicalSource("test", "Schema::create()->addField(createField(\"value\", BasicType::UINT64));");
    NES_DEBUG("Fail Query Request Test: Coordinator started successfully");

    NES_DEBUG("Fail Query Request Test: Start worker 1");
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
    ASSERT_TRUE(retStart1);
    NES_INFO("Fail Query Request Test: Worker1 started successfully");

    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, 100, 1));

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testDeployTwoWorkerMergeUsingBottomUp.out";

    NES_INFO("Fail Query Request Test: Submit query");
    string query = R"(Query::from("test").filter(Attribute("value")>2).sink(FileSinkDescriptor::create(")" + outputFilePath
        + R"(", "CSV_FORMAT", "APPEND"));)";
    NES_DEBUG("query= {}", query);
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    //wait until query has been succesfully deployed on worker 1
    auto timeoutInSec = std::chrono::seconds(defaultTimeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (wrk1->getNodeEngine()->getQueryStatus(queryId) != NES::Runtime::Execution::ExecutableQueryPlanStatus::Running
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
    }
    ASSERT_EQ(wrk1->getNodeEngine()->getQueryStatus(queryId), NES::Runtime::Execution::ExecutableQueryPlanStatus::Running);

    //wait until query has been succesfully deployed on worker at coordinator
    start_timestamp = std::chrono::system_clock::now();
    while (crd->getNodeEngine()->getQueryStatus(queryId) != NES::Runtime::Execution::ExecutableQueryPlanStatus::Running
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
    }
    ASSERT_EQ(crd->getNodeEngine()->getQueryStatus(queryId), NES::Runtime::Execution::ExecutableQueryPlanStatus::Running);

    //check status of query in the query catalog
    start_timestamp = std::chrono::system_clock::now();
    while (crd->getQueryCatalogService()->getEntryForQuery(queryId)->getQueryStatus() != QueryStatus::RUNNING
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
    }
    ASSERT_EQ(crd->getQueryCatalogService()->getEntryForQuery(queryId)->getQueryStatus(), QueryStatus::RUNNING);

    //check status of query in the global query plan
    auto sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    start_timestamp = std::chrono::system_clock::now();
    while (sharedQueryId == INVALID_SHARED_QUERY_ID && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
        sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    }
    ASSERT_NE(sharedQueryId, INVALID_SHARED_QUERY_ID);

    start_timestamp = std::chrono::system_clock::now();
    while (crd->getGlobalQueryPlan()->getSharedQueryPlan(sharedQueryId)->getStatus() != SharedQueryPlanStatus::Deployed
           && std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("query not running yet")
        std::this_thread::sleep_for(sleepDuration);
    }
    ASSERT_EQ(crd->getGlobalQueryPlan()->getSharedQueryPlan(sharedQueryId)->getStatus(), SharedQueryPlanStatus::Deployed);

    //execute fail query request
    auto subPlanId = wrk1->getNodeEngine()->getSubQueryIds(queryId).front();
    auto workerRpcClient = std::make_shared<WorkerRPCClient>();
    Experimental::FailQueryRequest failQueryRequest(queryId, subPlanId, 1, workerRpcClient);
    auto lockManager = std::make_shared<LockManager>(crd->getGlobalExecutionPlan(),
                                                     crd->getTopology(),
                                                     crd->getQueryCatalogService(),
                                                     crd->getGlobalQueryPlan(),
                                                     crd->getSourceCatalog(),
                                                     crd->getUDFCatalog());
    auto storageHandler = TwoPhaseLockingStorageHandler(lockManager);

    auto workerId = wrk1->getWorkerId();

    //stop worker to provoke failure of undeployment
    EXPECT_TRUE(wrk1->stop(true));

    try {
        failQueryRequest.execute(storageHandler);
        std::this_thread::sleep_for(std::chrono::seconds(defaultTimeout));
        //fail the test if exception was not thrown within timeout
        FAIL();
    } catch (Exceptions::QueryUndeploymentRpcException& e) {
        NES_DEBUG2("Caught query undeployment exception: {}", e.what());
        ASSERT_EQ(e.getMode(), RpcClientModes::Stop);
        ASSERT_EQ(e.getFailedExecutionNodeIds(), std::vector {workerId});
    }
    bool stopCrd = crd->stopCoordinator(true);
    EXPECT_TRUE(stopCrd);
}
}// namespace NES
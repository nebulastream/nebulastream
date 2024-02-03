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
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Query/QuerySubPlanMetaData.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <Mobility/WorkerMobilityHandler.hpp>
#include <Network/NetworkSink.hpp>
#include <Network/PartitionManager.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LambdaSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <RequestProcessor/RequestTypes/TopologyNodeRelocationRequest.hpp>
#include <RequestProcessor/StorageHandles/SerialStorageHandler.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/TestUtils.hpp>
#include <atomic>
#include <filesystem>
#include <gtest/gtest.h>

namespace NES {

class QueryRedeploymentIntegrationTest : public Testing::BaseIntegrationTest, public testing::WithParamInterface<uint32_t> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryRedeploymentIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Set up QueryRedeploymentIntegrationTest test class");
    }

    /**
     * @brief wait until the topology contains the expected number of nodes so we can rely on these nodes being present for
     * the rest of the test
     * @param timeoutSeconds time to wait before aborting
     * @param nodes expected number of nodes
     * @param topology  the topology object to query
     * @return true if expected number of nodes was reached. false in case of timeout before number was reached
     */
    static bool waitForNodes(int timeoutSeconds, size_t nodes, TopologyPtr topology) {
        size_t numberOfNodes = 0;
        for (int i = 0; i < timeoutSeconds; ++i) {
            auto topoString = topology->toString();
            numberOfNodes = std::count(topoString.begin(), topoString.end(), '\n');
            numberOfNodes -= 1;
            if (numberOfNodes == nodes) {
                break;
            }
        }
        return numberOfNodes == nodes;
    }
};

constexpr std::chrono::duration<int64_t, std::milli> defaultTimeoutInSec = std::chrono::seconds(TestUtils::defaultTimeout);
constexpr uint32_t DATA_CHANNEL_RETRY_TIMES = 10;
//todo #4441: increase number of event channel retries and add test for successful connection
constexpr uint32_t EVENT_CHANNEL_RETRY_TIMES = 1;
constexpr uint32_t DEFAULT_NUMBER_OF_ORIGINS = 0;
constexpr std::chrono::milliseconds WAIT_TIME = std::chrono::milliseconds(1000);

/**
 * @brief This tests the asynchronous connection establishment, where the sink buffers incoming tuples while waiting for the
 * network channel to become available
 */
TEST_P(QueryRedeploymentIntegrationTest, testAsyncConnectingSink) {
    const uint64_t numBuffersToProduceBeforeCount = 20;
    const uint64_t numBuffersToProduceAfterCount = 20;
    const uint64_t numBuffersToProduce = numBuffersToProduceBeforeCount + numBuffersToProduceAfterCount;
    uint64_t tuplesPerBuffer = 10;
    uint8_t bytesPerTuple = sizeof(uint64_t);
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";
    remove(testFile.c_str());

    std::string compareStringBeforeCount;
    std::ostringstream ossBeforeCount;
    ossBeforeCount << "seq$value:INTEGER(64 bits)" << std::endl;
    for (uint64_t i = 0; i < numBuffersToProduceBeforeCount * tuplesPerBuffer; ++i) {
        ossBeforeCount << std::to_string(i) << std::endl;
        compareStringBeforeCount = ossBeforeCount.str();
    }

    std::string compareString;
    std::ostringstream oss;
    oss << "seq$value:INTEGER(64 bits)" << std::endl;
    for (uint64_t i = 0; i < numBuffersToProduce * tuplesPerBuffer; ++i) {
        oss << std::to_string(i) << std::endl;
        compareString = oss.str();
    }

    NES_INFO("rest port = {}", *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    coordinatorConfig->worker.connectSourceEventChannelsAsync.setValue(true);

    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    crd->getSourceCatalog()->addLogicalSource("seq", Schema::create()->addField(createField("value", BasicType::UINT64)));

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    wrkConf1->numWorkerThreads.setValue(GetParam());
    wrkConf1->connectSinksAsync.setValue(true);
    wrkConf1->connectSourceEventChannelsAsync.setValue(true);
    wrkConf1->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);

    std::atomic<uint64_t> bufferCount = 0;
    std::atomic<bool> countReached = false;
    auto lambdaSourceFunction = [&countReached, &bufferCount](NES::Runtime::TupleBuffer& buffer,
                                                              uint64_t numberOfTuplesToProduce) {
        struct Record {
            uint64_t value;
        };
        auto currentCount = ++bufferCount;
        if (currentCount > numBuffersToProduceBeforeCount) {
            //after sending the specified amount of tuples, wait until some tuples actually arrived
            while (!countReached)
                ;
        }
        auto valCount = (currentCount - 1) * (numberOfTuplesToProduce);
        auto* records = buffer.getBuffer<Record>();
        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            records[u].value = valCount + u;
        }
    };
    auto gatheringValue = 10;
    auto lambdaSourceType = LambdaSourceType::create("seq",
                                                     "test_stream",
                                                     std::move(lambdaSourceFunction),
                                                     numBuffersToProduce,
                                                     gatheringValue,
                                                     GatheringMode::INTERVAL_MODE);
    wrkConf1->physicalSourceTypes.add(lambdaSourceType);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(waitForNodes(5, 2, topology));

    QueryId queryId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(
        R"(Query::from("seq").sink(FileSinkDescriptor::create(")" + testFile + R"(", "CSV_FORMAT", "APPEND"));)",
        Optimizer::PlacementStrategy::BottomUp);

    NES_INFO("Query ID: {}", queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringBeforeCount, testFile));
    countReached = true;
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareString, testFile));

    int response = remove(testFile.c_str());
    ASSERT_TRUE(response == 0);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/**
 * @brief This tests multiple iterations of inserting VersionDrain events to trigger the reconfiguration of a network sink to point to a new source.
 */
TEST_P(QueryRedeploymentIntegrationTest, testMultiplePlannedReconnects) {
    const uint64_t numberOfReconnectsToPerform = 3;
    const uint64_t numBuffersToProduceBeforeReconnect = 10;
    const uint64_t numBuffersToProduceWhileBuffering = 10;
    const uint64_t numBuffersToProduceAfterReconnect = 10;
    const uint64_t buffersToProducePerReconnectCycle =
        (numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect + numBuffersToProduceWhileBuffering);
    //const uint64_t totalBuffersToProduce = numberOfReconnectsToPerform * buffersToProducePerReconnectCycle;
    const uint64_t totalBuffersToProduce = (numberOfReconnectsToPerform + 1) * buffersToProducePerReconnectCycle;
    const uint64_t gatheringValue = 10;
    const std::chrono::seconds waitTime(10);
    uint64_t tuplesPerBuffer = 10;
    uint8_t bytesPerTuple = sizeof(uint64_t);
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";
    remove(testFile.c_str());

    std::atomic<uint64_t> bufferCount = 0;
    std::atomic<uint64_t> actualReconnects = 0;
    std::atomic<bool> waitForReconfig = false;
    std::atomic<bool> waitForReconnect = false;
    std::atomic<bool> waitForFinalCount = false;
    auto lambdaSourceFunction = [&bufferCount, &waitForReconfig, &waitForReconnect, &waitForFinalCount, &actualReconnects](
                                    NES::Runtime::TupleBuffer& buffer,
                                    uint64_t numberOfTuplesToProduce) {
        struct Record {
            uint64_t value;
        };
        auto currentCount = ++bufferCount;
        if (currentCount > numBuffersToProduceBeforeReconnect + (actualReconnects * buffersToProducePerReconnectCycle)) {
            //after sending the specified amount of tuples, wait until the reconfiguration has been triggered, subsequent tuples will be buffered
            while (!waitForReconfig)
                ;
        }
        if (currentCount > numBuffersToProduceBeforeReconnect + numBuffersToProduceWhileBuffering
                + (actualReconnects * buffersToProducePerReconnectCycle)) {
            //after writing some tuples into the buffer, give signal to start the new operators to finish the reconnect, tuples will be unbuffered to new destination
            waitForReconnect = true;
        }
        if (currentCount > numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect
                + numBuffersToProduceWhileBuffering + (actualReconnects * buffersToProducePerReconnectCycle)) {
            while (!waitForFinalCount)
                ;
        }
        auto valCount = (currentCount - 1) * (numberOfTuplesToProduce);
        auto* records = buffer.getBuffer<Record>();
        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            records[u].value = valCount + u;
        }
    };
    auto lambdaSourceType = LambdaSourceType::create("seq",
                                                     "test_stream",
                                                     std::move(lambdaSourceFunction),
                                                     totalBuffersToProduce,
                                                     gatheringValue,
                                                     GatheringMode::INTERVAL_MODE);

    NES_INFO("rest port = {}", *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    coordinatorConfig->enableQueryReconfiguration.setValue(true);
    auto crdWorkerDataPort = getAvailablePort();
    coordinatorConfig->worker.dataPort = *crdWorkerDataPort;
    coordinatorConfig->worker.connectSourceEventChannelsAsync.setValue(true);
    coordinatorConfig->worker.timestampFileSinkAndWriteToTCP.setValue(false);

    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    auto schema = Schema::create()->addField(createField("value", BasicType::UINT64));
    crd->getSourceCatalog()->addLogicalSource("seq", schema);

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    wrkConf1->numWorkerThreads.setValue(GetParam());
    wrkConf1->connectSinksAsync.setValue(true);
    wrkConf1->connectSourceEventChannelsAsync.setValue(true);
    wrkConf1->timestampFileSinkAndWriteToTCP.setValue(false);
    wrkConf1->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
    wrkConf1->numberOfSlots.setValue(1);

    wrkConf1->physicalSourceTypes.add(lambdaSourceType);

    auto wrk1DataPort = getAvailablePort();
    wrkConf1->dataPort = *wrk1DataPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(waitForNodes(5, 2, topology));

    NES_INFO("start worker 2");
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort.setValue(*rpcCoordinatorPort);
    auto wrk2DataPort = getAvailablePort();
    wrkConf2->dataPort = *wrk2DataPort;
    wrkConf2->numWorkerThreads.setValue(GetParam());
    wrkConf2->connectSinksAsync.setValue(true);
    wrkConf2->connectSourceEventChannelsAsync.setValue(true);
    wrkConf2->timestampFileSinkAndWriteToTCP.setValue(false);
    wrkConf2->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
    wrkConf2->numberOfSlots.setValue(1);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart2);
    ASSERT_TRUE(waitForNodes(5, 3, topology));

    NES_INFO("start worker below crd");
    WorkerConfigurationPtr wrkConfBelowCrd = WorkerConfiguration::create();
    wrkConfBelowCrd->coordinatorPort.setValue(*rpcCoordinatorPort);
    auto wrkBelowCrdDataPort = getAvailablePort();
    wrkConfBelowCrd->dataPort = *wrkBelowCrdDataPort;
    wrkConfBelowCrd->numWorkerThreads.setValue(GetParam());
    wrkConfBelowCrd->connectSinksAsync.setValue(true);
    wrkConfBelowCrd->connectSourceEventChannelsAsync.setValue(true);
    wrkConfBelowCrd->timestampFileSinkAndWriteToTCP.setValue(false);
    wrkConfBelowCrd->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
    NesWorkerPtr wrkBelowCrd = std::make_shared<NesWorker>(std::move(wrkConfBelowCrd));
    bool retStartBelowCrd = wrkBelowCrd->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStartBelowCrd);
    ASSERT_TRUE(waitForNodes(5, 4, topology));

    wrk1->replaceParent(crd->getNesWorker()->getWorkerId(), wrk2->getWorkerId());
    wrk2->replaceParent(crd->getNesWorker()->getWorkerId(), wrkBelowCrd->getWorkerId());

    //start query
    QueryId queryId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(
        R"(Query::from("seq").map(Attribute("value") = Attribute("value") * 1).map(Attribute("value") = Attribute("value") * 1).sink(FileSinkDescriptor::create(")" + testFile + R"(", "CSV_FORMAT", "APPEND"));)",
        Optimizer::PlacementStrategy::BottomUp);
    auto networkSinkWrk3Id = 31;
    auto networkSrcWrk3Id = 32;

    auto sinkLocationWrk1 = NES::Network::NodeLocation(wrk1->getWorkerId(), "localhost", *wrk1DataPort);
    auto networkSourceCrdLocation =
        NES::Network::NodeLocation(crd->getNesWorker()->getWorkerId(), "localhost", *crdWorkerDataPort);

    std::vector<NesWorkerPtr> reconnectParents;

    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, crd->getQueryCatalogService()));
    SharedQueryId sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    ASSERT_NE(sharedQueryId, INVALID_SHARED_QUERY_ID);
    //reconfiguration
    auto subPlanIdWrk3 = 30;
    ASSERT_EQ(wrk2->getNodeEngine()->getDecomposedQueryIds(queryId).size(), 1);
    auto oldSubplanId = wrk2->getNodeEngine()->getDecomposedQueryIds(queryId).front();
    auto wrk2Source = wrk2->getNodeEngine()->getExecutableQueryPlan(oldSubplanId)->getSources().front();
    Network::NesPartition currentWrk1TargetPartition(queryId, wrk2Source->getOperatorId(), 0, 0);

    ASSERT_EQ(crd->getNesWorker()->getNodeEngine()->getDecomposedQueryIds(queryId).size(), 1);
    auto coordinatorSubplanId = crd->getNesWorker()->getNodeEngine()->getDecomposedQueryIds(queryId).front();
    auto coordinatorSource =
        crd->getNesWorker()->getNodeEngine()->getExecutableQueryPlan(coordinatorSubplanId)->getSources().front();
    Network::NesPartition networkSourceCrdPartition(queryId, coordinatorSource->getOperatorId(), 0, 0);

    auto oldWorker = wrk2;
    while (actualReconnects < numberOfReconnectsToPerform) {
        subPlanIdWrk3++;

        //wait for data to be written
        std::string compareStringBefore;
        std::ostringstream oss;
        oss << "seq$value:INTEGER(64 bits)" << std::endl;
        for (uint64_t i = 0; i < (numBuffersToProduceBeforeReconnect
                                  + (actualReconnects
                                     * (numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect
                                        + numBuffersToProduceWhileBuffering)))
                 * tuplesPerBuffer;
             ++i) {
            oss << std::to_string(i) << std::endl;
        }
        compareStringBefore = oss.str();
        ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringBefore, testFile));
        waitForFinalCount = false;
        NES_INFO("start reconnect parent {}", actualReconnects);
        WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
        wrkConf3->coordinatorPort.setValue(*rpcCoordinatorPort);
        auto wrk3DataPort = getAvailablePort();
        wrkConf3->dataPort = *wrk3DataPort;
        wrkConf3->numWorkerThreads.setValue(GetParam());
        wrkConf3->connectSinksAsync.setValue(true);
        wrkConf3->connectSourceEventChannelsAsync.setValue(true);
        wrkConf3->timestampFileSinkAndWriteToTCP.setValue(false);
        wrkConf3->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
        wrkConf3->numberOfSlots.setValue(1);
        NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
        bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
        ASSERT_TRUE(retStart3);
        ASSERT_TRUE(waitForNodes(5, 5 + actualReconnects, topology));
        reconnectParents.push_back(wrk3);
        wrk3->replaceParent(crd->getNesWorker()->getWorkerId(), wrkBelowCrd->getWorkerId());

        std::string compareStringAfter;
        std::ostringstream ossAfter;
        ossAfter << "seq$value:INTEGER(64 bits)" << std::endl;
        for (uint64_t i = 0;
             i < (numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect + numBuffersToProduceWhileBuffering)
                 * tuplesPerBuffer * (actualReconnects + 1);
             ++i) {
            ossAfter << std::to_string(i) << std::endl;
        }
        compareStringAfter = ossAfter.str();

        int noOfMigratingPlans = 0;
        int noOfCompletedMigrations = 0;
        int noOfRunningPlans = 0;

        RequestProcessor::StorageDataStructures storageDataStructures(coordinatorConfig,
                                                                      topology,
                                                                      crd->getGlobalExecutionPlan(),
                                                                      crd->getQueryCatalogService(),
                                                                      crd->getGlobalQueryPlan(),
                                                                      crd->getSourceCatalog(),
                                                                      crd->getUDFCatalog());
        auto storageHandler = RequestProcessor::SerialStorageHandler::create(storageDataStructures);
        std::vector<TopologyLinkInformation> removedLinks = {{wrk1->getWorkerId(), oldWorker->getWorkerId()}};
        std::vector<TopologyLinkInformation> addedLinks = {{wrk1->getWorkerId(), wrk3->getWorkerId()}};
        std::string coordinatorAddress = coordinatorConfig->coordinatorIp.getValue() + ":" + std::to_string(*rpcCoordinatorPort);
        auto coordinatorRPCClient = CoordinatorRPCClient(coordinatorAddress);
        coordinatorRPCClient.relocateTopologyNode(removedLinks, addedLinks);

        //notify lambda source that reconfig happened and make it release more tuples into the buffer
        waitForFinalCount = false;
        waitForReconfig = true;
        //wait for tuples in order to make sure that the buffer is actually tested
        while (!waitForReconnect) {
        }

        //verify that the old partition gets unregistered
        auto timeoutInSec = std::chrono::seconds(TestUtils::defaultTimeout);
        auto start_timestamp = std::chrono::system_clock::now();
        while (wrk1->getNodeEngine()->getPartitionManager()->getProducerRegistrationStatus(currentWrk1TargetPartition)
               == Network::PartitionRegistrationStatus::Registered) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            NES_DEBUG("Partition {} has not yet been unregistered", currentWrk1TargetPartition);
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_NE(wrk1->getNodeEngine()->getPartitionManager()->getProducerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
        auto networkSourceWrk3Partition =
            std::dynamic_pointer_cast<Network::NetworkSourceDescriptor>(crd->getGlobalExecutionPlan()
                                                                            ->getExecutionNodeById(wrk3->getWorkerId())
                                                                            ->getAllDecomposedQueryPlans(sharedQueryId)
                                                                            .front()
                                                                            ->getSourceOperators()
                                                                            .front()
                                                                            ->getSourceDescriptor())
                ->getNesPartition();
        ASSERT_EQ(wrk1->getNodeEngine()->getPartitionManager()->getProducerRegistrationStatus(networkSourceWrk3Partition),
                  Network::PartitionRegistrationStatus::Registered);
        EXPECT_NE(oldWorker->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
        currentWrk1TargetPartition = networkSourceWrk3Partition;

        //verify that query has been undeployed from old parent
        while (oldWorker->getNodeEngine()->getQueryStatus(queryId) != Runtime::Execution::ExecutableQueryPlanStatus::Finished) {
            NES_DEBUG("Query has not yet stopped on worker {}", oldWorker->getWorkerId());
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_EQ(oldWorker->getNodeEngine()->getQueryStatus(queryId), Runtime::Execution::ExecutableQueryPlanStatus::Finished);

        oldWorker = wrk3;
        EXPECT_EQ(oldWorker->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
        oldSubplanId = oldWorker->getNodeEngine()->getDecomposedQueryIds(queryId).front();

        //check that query has left migrating state and is running again
        ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, crd->getQueryCatalogService()));
        auto entries = crd->getQueryCatalogService()->getAllEntriesInStatus("MIGRATING");
        ASSERT_TRUE(entries.empty());
        auto subplansAfterMigration = crd->getQueryCatalogService()->getEntryForQuery(queryId)->getAllSubQueryPlanMetaData();
        ASSERT_FALSE(subplansAfterMigration.empty());

        //subplans should not include any migrating plans anymore but one more entry in marked as migration completed
        noOfMigratingPlans = 0;
        noOfCompletedMigrations = 0;
        noOfRunningPlans = 0;
        for (auto& plan : subplansAfterMigration) {
            switch (plan->getSubQueryStatus()) {
                case QueryState::MIGRATING: {
                    noOfMigratingPlans++;
                    break;
                }
                case QueryState::MIGRATION_COMPLETED: {
                    noOfCompletedMigrations++;
                    continue;
                }
                case QueryState::RUNNING: {
                    noOfRunningPlans++;
                    continue;
                }
                default: {
                    NES_DEBUG("Found subplan in unexpected state");
                    FAIL();
                }
            }
        }
        ASSERT_EQ(noOfMigratingPlans, 0);
        //ASSERT_EQ(noOfCompletedMigrations, actualReconnects + 2);
        ASSERT_EQ(noOfRunningPlans, 4);
        ASSERT_EQ(topology->getParentTopologyNodeIds(wrk1->getWorkerId()), std::vector<WorkerId>{wrk3->getWorkerId()});

        //check that all tuples arrived
        ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringAfter, testFile));

        waitForReconfig = false;
        waitForReconnect = false;
        actualReconnects++;
        waitForFinalCount = true;
    }

    waitForReconfig = true;
    waitForReconnect = true;
    actualReconnects++;
    waitForFinalCount = true;
    //send the last tuples, after which the lambda source shuts down
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk1));

    auto lastReconnectParent = reconnectParents.back();
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, lastReconnectParent));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk2));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, crd->getNesWorker()));
    cout << "stopping worker" << endl;
    bool retStopLastParent = lastReconnectParent->stop(false);
    ASSERT_TRUE(retStopLastParent);
    reconnectParents.pop_back();
    for (auto parent : reconnectParents) {
        NES_DEBUG("stopping parent node")
        ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, parent));
        cout << "stopping worker" << endl;
        bool stopParent = parent->stop(false);
        ASSERT_TRUE(stopParent);
    }

    int response = remove(testFile.c_str());
    ASSERT_TRUE(response == 0);

    auto stopSuccess = wrk1->getNodeEngine()->stopQuery(sharedQueryId);
    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    auto stopSuccess2 = wrk2->getNodeEngine()->stopQuery(sharedQueryId);
    cout << "stopping worker" << endl;
    bool retStopWrk2 = wrk2->stop(false);
    ASSERT_TRUE(retStopWrk2);

    auto stopSuccessBelowCrd = wrkBelowCrd->getNodeEngine()->stopQuery(sharedQueryId);
    cout << "stopping worker" << endl;
    bool retStopBelowCrd = wrkBelowCrd->stop(false);
    ASSERT_TRUE(retStopBelowCrd);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/**
 * @brief This tests multiple iterations of inserting VersionDrain events to trigger the reconfiguration of a network sink to point to a new source.
 */
TEST_P(QueryRedeploymentIntegrationTest, testMultipleUnplannedReconnects) {
    const uint64_t numberOfReconnectsToPerform = 3;
    const uint64_t numBuffersToProduceBeforeReconnect = 10;
    const uint64_t numBuffersToProduceWhileBuffering = 10;
    const uint64_t numBuffersToProduceAfterReconnect = 10;
    const uint64_t buffersToProducePerReconnectCycle =
        (numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect + numBuffersToProduceWhileBuffering);
    const uint64_t totalBuffersToProduce = numberOfReconnectsToPerform * buffersToProducePerReconnectCycle;
    const uint64_t gatheringValue = 10;
    const std::chrono::seconds waitTime(10);
    uint64_t tuplesPerBuffer = 10;
    uint8_t bytesPerTuple = sizeof(uint64_t);
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";
    remove(testFile.c_str());

    std::atomic<uint64_t> bufferCount = 0;
    std::atomic<uint64_t> actualReconnects = 0;
    std::atomic<bool> waitForReconfig = false;
    std::atomic<bool> waitForReconnect = false;
    std::atomic<bool> waitForFinalCount = false;
    auto lambdaSourceFunction = [&bufferCount, &waitForReconfig, &waitForReconnect, &waitForFinalCount, &actualReconnects](
                                    NES::Runtime::TupleBuffer& buffer,
                                    uint64_t numberOfTuplesToProduce) {
        struct Record {
            uint64_t value;
        };
        auto currentCount = ++bufferCount;
        if (currentCount > numBuffersToProduceBeforeReconnect + (actualReconnects * buffersToProducePerReconnectCycle)) {
            //after sending the specified amount of tuples, wait until the reconfiguration has been triggered, subsequent tuples will be buffered
            while (!waitForReconfig)
                ;
        }
        if (currentCount > numBuffersToProduceBeforeReconnect + numBuffersToProduceWhileBuffering
                + (actualReconnects * buffersToProducePerReconnectCycle)) {
            //after writing some tuples into the buffer, give signal to start the new operators to finish the reconnect, tuples will be unbuffered to new destination
            waitForReconnect = true;
        }
        if (currentCount > numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect
                + numBuffersToProduceWhileBuffering + (actualReconnects * buffersToProducePerReconnectCycle)) {
            while (!waitForFinalCount)
                ;
        }
        auto valCount = (currentCount - 1) * (numberOfTuplesToProduce);
        auto* records = buffer.getBuffer<Record>();
        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            records[u].value = valCount + u;
        }
    };
    auto lambdaSourceType = LambdaSourceType::create("seq",
                                                     "test_stream",
                                                     std::move(lambdaSourceFunction),
                                                     totalBuffersToProduce,
                                                     gatheringValue,
                                                     GatheringMode::INTERVAL_MODE);

    NES_INFO("rest port = {}", *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    coordinatorConfig->enableQueryReconfiguration.setValue(true);
    auto crdWorkerDataPort = getAvailablePort();
    coordinatorConfig->worker.dataPort = *crdWorkerDataPort;
    coordinatorConfig->worker.connectSourceEventChannelsAsync.setValue(true);
    coordinatorConfig->worker.timestampFileSinkAndWriteToTCP.setValue(false);

    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    auto schema = Schema::create()->addField(createField("value", BasicType::UINT64));
    crd->getSourceCatalog()->addLogicalSource("seq", schema);

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    wrkConf1->numWorkerThreads.setValue(GetParam());
    wrkConf1->connectSinksAsync.setValue(true);
    wrkConf1->connectSourceEventChannelsAsync.setValue(true);
    wrkConf1->timestampFileSinkAndWriteToTCP.setValue(false);
    wrkConf1->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);

    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    //wrkConf1->mobilityConfiguration.locationProviderType.setValue(
    // NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    //    wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(std::filesystem::path(TEST_DATA_DIRECTORY)
    //                                                                    / "singleLocation.csv");

    wrkConf1->physicalSourceTypes.add(lambdaSourceType);

    auto wrk1DataPort = getAvailablePort();
    wrkConf1->dataPort = *wrk1DataPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(waitForNodes(5, 2, topology));

    NES_INFO("start worker 2");
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort.setValue(*rpcCoordinatorPort);
    auto wrk2DataPort = getAvailablePort();
    wrkConf2->dataPort = *wrk2DataPort;
    wrkConf2->numWorkerThreads.setValue(GetParam());
    wrkConf2->connectSinksAsync.setValue(true);
    wrkConf2->connectSourceEventChannelsAsync.setValue(true);
    wrkConf2->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart2);
    ASSERT_TRUE(waitForNodes(5, 3, topology));

    wrk1->replaceParent(crd->getNesWorker()->getWorkerId(), wrk2->getWorkerId());

    //start query
    QueryId queryId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(
        R"(Query::from("seq").sink(FileSinkDescriptor::create(")" + testFile + R"(", "CSV_FORMAT", "APPEND"));)",
        Optimizer::PlacementStrategy::BottomUp);
    auto networkSinkWrk3Id = 31;
    auto networkSrcWrk3Id = 32;

    auto sinkLocationWrk1 = NES::Network::NodeLocation(wrk1->getWorkerId(), "localhost", *wrk1DataPort);
    auto networkSourceCrdLocation =
        NES::Network::NodeLocation(crd->getNesWorker()->getWorkerId(), "localhost", *crdWorkerDataPort);

    std::vector<NesWorkerPtr> reconnectParents;

    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, crd->getQueryCatalogService()));
    SharedQueryId sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    ASSERT_NE(sharedQueryId, INVALID_SHARED_QUERY_ID);
    //reconfiguration
    auto subPlanIdWrk3 = 30;
    ASSERT_EQ(wrk2->getNodeEngine()->getDecomposedQueryIds(queryId).size(), 1);
    auto oldSubplanId = wrk2->getNodeEngine()->getDecomposedQueryIds(queryId).front();
    auto wrk2Source = wrk2->getNodeEngine()->getExecutableQueryPlan(oldSubplanId)->getSources().front();
    Network::NesPartition currentWrk1TargetPartition(queryId, wrk2Source->getOperatorId(), 0, 0);

    ASSERT_EQ(crd->getNesWorker()->getNodeEngine()->getDecomposedQueryIds(queryId).size(), 1);
    auto coordinatorSubplanId = crd->getNesWorker()->getNodeEngine()->getDecomposedQueryIds(queryId).front();
    auto coordinatorSource =
        crd->getNesWorker()->getNodeEngine()->getExecutableQueryPlan(coordinatorSubplanId)->getSources().front();
    Network::NesPartition networkSourceCrdPartition(queryId, coordinatorSource->getOperatorId(), 0, 0);

    auto oldWorker = wrk2;
    while (actualReconnects < numberOfReconnectsToPerform) {
        subPlanIdWrk3++;

        //wait for data to be written
        std::string compareStringBefore;
        std::ostringstream oss;
        oss << "seq$value:INTEGER(64 bits)" << std::endl;
        for (uint64_t i = 0; i < (numBuffersToProduceBeforeReconnect
                                  + (actualReconnects
                                     * (numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect
                                        + numBuffersToProduceWhileBuffering)))
                 * tuplesPerBuffer;
             ++i) {
            oss << std::to_string(i) << std::endl;
        }
        compareStringBefore = oss.str();
        ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringBefore, testFile));
        waitForFinalCount = false;
        NES_INFO("start reconnect parent {}", actualReconnects);
        WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
        wrkConf3->coordinatorPort.setValue(*rpcCoordinatorPort);
        auto wrk3DataPort = getAvailablePort();
        wrkConf3->dataPort = *wrk3DataPort;
        wrkConf3->numWorkerThreads.setValue(GetParam());
        wrkConf3->connectSinksAsync.setValue(true);
        wrkConf3->connectSourceEventChannelsAsync.setValue(true);
        wrkConf3->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
        NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
        bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
        ASSERT_TRUE(retStart3);
        ASSERT_TRUE(waitForNodes(5, 4 + actualReconnects, topology));
        reconnectParents.push_back(wrk3);

        std::string compareStringAfter;
        std::ostringstream ossAfter;
        ossAfter << "seq$value:INTEGER(64 bits)" << std::endl;
        for (uint64_t i = 0;
             i < (numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect + numBuffersToProduceWhileBuffering)
                 * tuplesPerBuffer * (actualReconnects + 1);
             ++i) {
            ossAfter << std::to_string(i) << std::endl;
        }
        compareStringAfter = ossAfter.str();

        int noOfMigratingPlans = 0;
        int noOfCompletedMigrations = 0;
        int noOfRunningPlans = 0;

        RequestProcessor::StorageDataStructures storageDataStructures(coordinatorConfig,
                                                                      topology,
                                                                      crd->getGlobalExecutionPlan(),
                                                                      crd->getQueryCatalogService(),
                                                                      crd->getGlobalQueryPlan(),
                                                                      crd->getSourceCatalog(),
                                                                      crd->getUDFCatalog());
        auto storageHandler = RequestProcessor::SerialStorageHandler::create(storageDataStructures);
        std::vector<TopologyLinkInformation> removedLinks = {{wrk1->getWorkerId(), oldWorker->getWorkerId()}};
        std::vector<TopologyLinkInformation> addedLinks = {{wrk1->getWorkerId(), wrk3->getWorkerId()}};
        // std::string coordinatorAddress = coordinatorConfig->coordinatorIp.getValue() + ":" + std::to_string(*rpcCoordinatorPort);
        // auto coordinatorRPCClient = CoordinatorRPCClient(coordinatorAddress);
        // coordinatorRPCClient.relocateTopologyNode(removedLinks, addedLinks);
        auto currentParent = oldWorker->getWorkerId();
        wrk1->getMobilityHandler()->triggerReconnectionRoutine(currentParent, wrk3->getWorkerId());
        ASSERT_EQ(currentParent, wrk3->getWorkerId());

        //notify lambda source that reconfig happened and make it release more tuples into the buffer
        waitForFinalCount = false;
        waitForReconfig = true;
        //wait for tuples in order to make sure that the buffer is actually tested
        while (!waitForReconnect) {
        }

        //verify that the old partition gets unregistered
        auto timeoutInSec = std::chrono::seconds(TestUtils::defaultTimeout);
        auto start_timestamp = std::chrono::system_clock::now();
        while (wrk1->getNodeEngine()->getPartitionManager()->getProducerRegistrationStatus(currentWrk1TargetPartition)
               == Network::PartitionRegistrationStatus::Registered) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            NES_DEBUG("Partition {} has not yet been unregistered", currentWrk1TargetPartition);
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_NE(wrk1->getNodeEngine()->getPartitionManager()->getProducerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
        auto networkSourceWrk3Partition =
            std::dynamic_pointer_cast<Network::NetworkSourceDescriptor>(crd->getGlobalExecutionPlan()
                                                                            ->getExecutionNodeById(wrk3->getWorkerId())
                                                                            ->getAllDecomposedQueryPlans(sharedQueryId)
                                                                            .front()
                                                                            ->getSourceOperators()
                                                                            .front()
                                                                            ->getSourceDescriptor())
                ->getNesPartition();
        ASSERT_EQ(wrk1->getNodeEngine()->getPartitionManager()->getProducerRegistrationStatus(networkSourceWrk3Partition),
                  Network::PartitionRegistrationStatus::Registered);
        EXPECT_NE(oldWorker->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
        currentWrk1TargetPartition = networkSourceWrk3Partition;

        //verify that query has been undeployed from old parent
        while (oldWorker->getNodeEngine()->getQueryStatus(queryId) != Runtime::Execution::ExecutableQueryPlanStatus::Finished) {
            NES_DEBUG("Query has not yet stopped on worker {}", oldWorker->getWorkerId());
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_EQ(oldWorker->getNodeEngine()->getQueryStatus(queryId), Runtime::Execution::ExecutableQueryPlanStatus::Finished);

        oldWorker = wrk3;
        EXPECT_EQ(oldWorker->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
        oldSubplanId = oldWorker->getNodeEngine()->getDecomposedQueryIds(queryId).front();

        //check that query has left migrating state and is running again
        ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, crd->getQueryCatalogService()));
        auto entries = crd->getQueryCatalogService()->getAllEntriesInStatus("MIGRATING");
        ASSERT_TRUE(entries.empty());
        auto subplansAfterMigration = crd->getQueryCatalogService()->getEntryForQuery(queryId)->getAllSubQueryPlanMetaData();
        ASSERT_FALSE(subplansAfterMigration.empty());

        //subplans should not include any migrating plans anymore but one more entry in marked as migration completed
        noOfMigratingPlans = 0;
        noOfCompletedMigrations = 0;
        noOfRunningPlans = 0;
        for (auto& plan : subplansAfterMigration) {
            switch (plan->getSubQueryStatus()) {
                case QueryState::MIGRATING: {
                    noOfMigratingPlans++;
                    break;
                }
                case QueryState::MIGRATION_COMPLETED: {
                    noOfCompletedMigrations++;
                    continue;
                }
                case QueryState::RUNNING: {
                    noOfRunningPlans++;
                    continue;
                }
                default: {
                    NES_DEBUG("Found subplan in unexpected state");
                    FAIL();
                }
            }
        }
        ASSERT_EQ(noOfMigratingPlans, 0);
        ASSERT_EQ(noOfCompletedMigrations, actualReconnects + 1);
        ASSERT_EQ(noOfRunningPlans, 3);
        ASSERT_EQ(topology->getParentTopologyNodeIds(wrk1->getWorkerId()), std::vector<WorkerId>{wrk3->getWorkerId()});

        //check that all tuples arrived
        ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringAfter, testFile));

        waitForReconfig = false;
        waitForReconnect = false;
        actualReconnects++;
        waitForFinalCount = true;
    }

    //send the last tuples, after which the lambda source shuts down
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk1));

    auto lastReconnectParent = reconnectParents.back();
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, lastReconnectParent));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk2));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, crd->getNesWorker()));
    cout << "stopping worker" << endl;
    bool retStopLastParent = lastReconnectParent->stop(false);
    ASSERT_TRUE(retStopLastParent);
    reconnectParents.pop_back();
    for (auto parent : reconnectParents) {
        NES_DEBUG("stopping parent node")
        ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, parent));
        cout << "stopping worker" << endl;
        bool stopParent = parent->stop(false);
        ASSERT_TRUE(stopParent);
    }

    int response = remove(testFile.c_str());
    ASSERT_TRUE(response == 0);

    auto stopSuccess = wrk1->getNodeEngine()->stopQuery(sharedQueryId);
    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    auto stopSuccess2 = wrk2->getNodeEngine()->stopQuery(sharedQueryId);
    cout << "stopping worker" << endl;
    bool retStopWrk2 = wrk2->stop(false);
    ASSERT_TRUE(retStopWrk2);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

#ifdef S2DEF
TEST_F(QueryRedeploymentIntegrationTest, testSequenceWithReconnecting) {
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_reconnecting_out.csv";

    std::string compareString;
    std::ostringstream oss;
    oss << "seq$value:INTEGER(64 bits)" << std::endl;
    for (int i = 1; i <= 10000; ++i) {
        oss << std::to_string(i) << std::endl;
        compareString = oss.str();
    }

    NES_INFO("rest port = {}", *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully")

    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    //TopologyNodePtr node = topology->getRootTopologyNodeId();
    std::vector<NES::Spatial::DataTypes::Experimental::GeoLocation> locVec = {
        {52.53024925374664, 13.440408001670573},  {52.44959193751221, 12.994693532702838},
        {52.58394737653231, 13.404557656002641},  {52.48534029037908, 12.984138457171484},
        {52.37433823627218, 13.558651957244951},  {52.51533875315059, 13.241771507925069},
        {52.55973107205912, 13.015653271890772},  {52.63119966549814, 13.441159505328082},
        {52.52554704888443, 13.140415389311752},  {52.482596286130494, 13.292443465145574},
        {52.54298642356826, 13.73191525503437},   {52.42678133005856, 13.253118169911525},
        {52.49621174869779, 13.660943763979146},  {52.45590365225229, 13.683553731893118},
        {52.62859441558, 13.135969230535936},     {52.49564618880393, 13.333672868668472},
        {52.58790396655713, 13.283405589901832},  {52.43730546215479, 13.288472865017477},
        {52.452625895558846, 13.609715377620118}, {52.604381034747234, 13.236153100778251},
        {52.52406858008703, 13.202905224067974},  {52.48532771063918, 13.248322218507269},
        {52.50023010173765, 13.35516100143647},   {52.5655774963026, 13.416236069617133},
        {52.56839177666675, 13.311990021109548},  {52.42881523569258, 13.539510531504995},
        {52.55745803205775, 13.521177091034348},  {52.378590211721814, 13.39387224077735},
        {52.45968932886132, 13.466172426273232},  {52.60131778672673, 13.6759151640276},
        {52.59382248148305, 13.17751716953493},   {52.51690603363213, 13.627430091500505},
        {52.40035318355461, 13.386405495784041},  {52.49369404130713, 13.503477002208028},
        {52.52102316662499, 13.231109595273479},  {52.6264057419334, 13.239482930461145},
        {52.45997462557177, 13.038370380285766},  {52.405581430754694, 12.994506535621692},
        {52.5165220102255, 13.287867202522792},   {52.61937748717004, 13.607622490869543},
        {52.620153404197254, 13.236774758123099}, {52.53095039302521, 13.150218024942914},
        {52.60042748492653, 13.591960614892749},  {52.44688258081577, 13.091132219453291},
        {52.44810624782493, 13.189186365976528},  {52.631904019035325, 13.099599387131189},
        {52.51607843891218, 13.361003233097668},  {52.63920358795863, 13.365640690678045},
        {52.51050545031392, 13.687455299147123},  {52.42516226249599, 13.597154340475155},
        {52.585620728658185, 13.177440252255762}, {52.54251642039891, 13.270687079693818},
        {52.62589583837628, 13.58922212327232},   {52.63840628658707, 13.336777486335386},
        {52.382935034604074, 13.54689828854007},  {52.46173261319607, 13.637993027984113},
        {52.45558349451082, 13.774558360650097},  {52.50660545385822, 13.171564805090318},
        {52.38586011054127, 13.772290920473052},  {52.4010561708298, 13.426889487526187}};

    std::vector<NesWorkerPtr> fieldNodes;
    for (auto elem : locVec) {
        WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
        wrkConf->coordinatorPort.setValue(*rpcCoordinatorPort);
        wrkConf->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::FIXED_LOCATION);
        wrkConf->locationCoordinates.setValue(elem);
        NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
        fieldNodes.push_back(wrk);
        bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
        ASSERT_TRUE(retStart);
    }
    ASSERT_TRUE(waitForNodes(5, 61, topology));
    string singleLocStart = "52.55227464714949, 13.351743136322877";
    crd->getSourceCatalog()->addLogicalSource("seq", Schema::create()->addField(createField("value", BasicType::UINT64)));

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);

    auto stype = CSVSourceType::create("seq", "test_stream");
    //stype->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "sequence_long.csv");
    stype->setFilePath("/home/x/.local/share/Cryptomator/mnt/tubCloudCr/old_test_data/sequence_long.csv");
    stype->setNumberOfBuffersToProduce(1000);
    stype->setNumberOfTuplesToProducePerBuffer(10);
    stype->setGatheringInterval(1);
    wrkConf1->physicalSourceTypes.add(stype);

    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf1->mobilityConfiguration.nodeInfoDownloadRadius.setValue(20000);
    wrkConf1->mobilityConfiguration.nodeIndexUpdateThreshold.setValue(5000);
    wrkConf1->mobilityConfiguration.mobilityHandlerUpdateInterval.setValue(10);
    wrkConf1->mobilityConfiguration.locationBufferSaveRate.setValue(1);
    wrkConf1->mobilityConfiguration.pathPredictionLength.setValue(40000);
    wrkConf1->mobilityConfiguration.defaultCoverageRadius.setValue(5000);
    wrkConf1->mobilityConfiguration.mobilityHandlerUpdateInterval.setValue(1000);
    wrkConf1->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    //wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(std::filesystem::path(TEST_DATA_DIRECTORY) / "path1.csv");
    wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(
        "/home/x/.local/share/Cryptomator/mnt/tubCloudCr/old_test_data/path1.csv");

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(waitForNodes(5, 62, topology));

    QueryId queryId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(
        R"(Query::from("seq").sink(FileSinkDescriptor::create(")" + testFile + R"(", "CSV_FORMAT", "APPEND"));)",
        Optimizer::PlacementStrategy::BottomUp);

    std::stringstream ss;
    ss << "google-chrome \"http://localhost:3000/?host=localhost&port=";
    ss << std::to_string(*restPort);
    ss << "\"";
    std::system(ss.str().c_str());

    NES_INFO("Query ID: {}", queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);
    size_t recv_tuples = 0;
    auto startTimestamp = std::chrono::system_clock::now();
    while (recv_tuples < 10000 && std::chrono::system_clock::now() < startTimestamp + defaultTimeoutInSec) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile), std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("received: {}", recv_tuples)
        sleep(1);
    }

    ASSERT_EQ(recv_tuples, 10001);
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareString, testFile));

    //std::cin.get();

    int response = remove(testFile.c_str());
    ASSERT_TRUE(response == 0);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    for (const auto& w : fieldNodes) {
        bool stop = w->stop(false);
        ASSERT_TRUE(stop);
    }

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

void writeReconnectCSV(const std::vector<uint64_t>& integers,
                       const std::vector<std::chrono::duration<double>>& durations,
                       const std::string& filename) {
    // Open the file for writing
    std::ofstream outputFile(filename);

    // Check if the file opened successfully
    if (!outputFile.is_open()) {
        std::cerr << "Error: Unable to open file " << filename << " for writing." << std::endl;
        return;
    }

    // Write the values to the CSV file
    for (size_t i = 0; i < integers.size(); ++i) {
        auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(durations[i]);
        outputFile << integers[i] << "," << nanoseconds.count() << std::endl;
    }

    // Close the file
    outputFile.close();
}

TEST_F(QueryRedeploymentIntegrationTest, testSequenceWithReconnectingPrecalculated) {
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_reconnecting_out.csv";

    std::string compareString;
    std::ostringstream oss;
    oss << "seq$value:INTEGER(64 bits)" << std::endl;
    for (int i = 1; i <= 10000; ++i) {
        oss << std::to_string(i) << std::endl;
        compareString = oss.str();
    }

    NES_INFO("rest port = {}", *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully")

    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    //TopologyNodePtr node = topology->getRootTopologyNodeId();
    std::vector<NES::Spatial::DataTypes::Experimental::GeoLocation> locVec = {
        {52.53024925374664, 13.440408001670573},  {52.44959193751221, 12.994693532702838},
        {52.58394737653231, 13.404557656002641},  {52.48534029037908, 12.984138457171484},
        {52.37433823627218, 13.558651957244951},  {52.51533875315059, 13.241771507925069},
        {52.55973107205912, 13.015653271890772},  {52.63119966549814, 13.441159505328082},
        {52.52554704888443, 13.140415389311752},  {52.482596286130494, 13.292443465145574},
        {52.54298642356826, 13.73191525503437},   {52.42678133005856, 13.253118169911525},
        {52.49621174869779, 13.660943763979146},  {52.45590365225229, 13.683553731893118},
        {52.62859441558, 13.135969230535936},     {52.49564618880393, 13.333672868668472},
        {52.58790396655713, 13.283405589901832},  {52.43730546215479, 13.288472865017477},
        {52.452625895558846, 13.609715377620118}, {52.604381034747234, 13.236153100778251},
        {52.52406858008703, 13.202905224067974},  {52.48532771063918, 13.248322218507269},
        {52.50023010173765, 13.35516100143647},   {52.5655774963026, 13.416236069617133},
        {52.56839177666675, 13.311990021109548},  {52.42881523569258, 13.539510531504995},
        {52.55745803205775, 13.521177091034348},  {52.378590211721814, 13.39387224077735},
        {52.45968932886132, 13.466172426273232},  {52.60131778672673, 13.6759151640276},
        {52.59382248148305, 13.17751716953493},   {52.51690603363213, 13.627430091500505},
        {52.40035318355461, 13.386405495784041},  {52.49369404130713, 13.503477002208028},
        {52.52102316662499, 13.231109595273479},  {52.6264057419334, 13.239482930461145},
        {52.45997462557177, 13.038370380285766},  {52.405581430754694, 12.994506535621692},
        {52.5165220102255, 13.287867202522792},   {52.61937748717004, 13.607622490869543},
        {52.620153404197254, 13.236774758123099}, {52.53095039302521, 13.150218024942914},
        {52.60042748492653, 13.591960614892749},  {52.44688258081577, 13.091132219453291},
        {52.44810624782493, 13.189186365976528},  {52.631904019035325, 13.099599387131189},
        {52.51607843891218, 13.361003233097668},  {52.63920358795863, 13.365640690678045},
        {52.51050545031392, 13.687455299147123},  {52.42516226249599, 13.597154340475155},
        {52.585620728658185, 13.177440252255762}, {52.54251642039891, 13.270687079693818},
        {52.62589583837628, 13.58922212327232},   {52.63840628658707, 13.336777486335386},
        {52.382935034604074, 13.54689828854007},  {52.46173261319607, 13.637993027984113},
        {52.45558349451082, 13.774558360650097},  {52.50660545385822, 13.171564805090318},
        {52.38586011054127, 13.772290920473052},  {52.4010561708298, 13.426889487526187}};

    std::vector<NesWorkerPtr> fieldNodes;
    for (auto elem : locVec) {
        WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
        wrkConf->coordinatorPort.setValue(*rpcCoordinatorPort);
        wrkConf->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::FIXED_LOCATION);
        wrkConf->locationCoordinates.setValue(elem);
        NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
        fieldNodes.push_back(wrk);
        bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
        ASSERT_TRUE(retStart);
    }
    ASSERT_TRUE(waitForNodes(5, 61, topology));
    string singleLocStart = "52.55227464714949, 13.351743136322877";
    crd->getSourceCatalog()->addLogicalSource("seq", Schema::create()->addField(createField("value", BasicType::UINT64)));

    //write reconnect file
    std::string precalculated = getTestResourceFolder() / "precalculated_reconnects.csv";
    std::vector<uint64_t> parentIds = {3, 4, 5, 6, 7};
    std::vector<std::chrono::duration<double>> durations = {std::chrono::seconds(0),
                                                            std::chrono::seconds(1),
                                                            std::chrono::seconds(2),
                                                            std::chrono::seconds(3),
                                                            std::chrono::seconds(4)};

    writeReconnectCSV(parentIds, durations, precalculated);

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);

    auto stype = CSVSourceType::create("seq", "test_stream");
    //stype->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "sequence_long.csv");
    stype->setFilePath("/home/x/.local/share/Cryptomator/mnt/tubCloudCr/old_test_data/sequence_long.csv");
    stype->setNumberOfBuffersToProduce(1000);
    stype->setNumberOfTuplesToProducePerBuffer(10);
    stype->setGatheringInterval(1);
    wrkConf1->physicalSourceTypes.add(stype);

    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf1->mobilityConfiguration.nodeInfoDownloadRadius.setValue(20000);
    wrkConf1->mobilityConfiguration.nodeIndexUpdateThreshold.setValue(5000);
    wrkConf1->mobilityConfiguration.mobilityHandlerUpdateInterval.setValue(10);
    wrkConf1->mobilityConfiguration.locationBufferSaveRate.setValue(1);
    wrkConf1->mobilityConfiguration.pathPredictionLength.setValue(40000);
    wrkConf1->mobilityConfiguration.defaultCoverageRadius.setValue(5000);
    wrkConf1->mobilityConfiguration.mobilityHandlerUpdateInterval.setValue(1000);
    wrkConf1->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf1->mobilityConfiguration.reconnectPredictorType.setValue(
        NES::Spatial::Mobility::Experimental::ReconnectPredictorType::PRECALCULATED);
    //wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(std::filesystem::path(TEST_DATA_DIRECTORY) / "path1.csv");
    wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(
        "/home/x/.local/share/Cryptomator/mnt/tubCloudCr/old_test_data/path1.csv");
    wrkConf1->mobilityConfiguration.precalculatedReconnectPath.setValue(precalculated);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(waitForNodes(5, 62, topology));

    //    QueryId queryId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(
    //        R"(Query::from("seq").sink(FileSinkDescriptor::create(")" + testFile + R"(", "CSV_FORMAT", "APPEND"));)",
    //        Optimizer::PlacementStrategy::BottomUp);

    auto actualParentId = topology->getParentTopologyNodeIds(wrk1->getWorkerId()).front();
    while (actualParentId != parentIds.back()) {
        actualParentId = topology->getParentTopologyNodeIds(wrk1->getWorkerId()).front();
        NES_DEBUG("current parent {}", actualParentId);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::stringstream ss;
    ss << "google-chrome \"http://localhost:3000/?host=localhost&port=";
    ss << std::to_string(*restPort);
    ss << "\"";
    std::system(ss.str().c_str());

    int response = remove(testFile.c_str());
    //    ASSERT_TRUE(response == 0);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    for (const auto& w : fieldNodes) {
        bool stop = w->stop(false);
        ASSERT_TRUE(stop);
    }

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

TEST_F(QueryRedeploymentIntegrationTest, debugDublinBus) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully")

    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    auto schema = Schema::create()
                      ->addField(createField("id", BasicType::UINT64))
                      ->addField(createField("value", BasicType::UINT64))
                      ->addField(createField("ingestion_timestamp", BasicType::UINT64))
                      ->addField(createField("processing_timestamp"
                                             "",
                                             BasicType::UINT64));
    crd->getSourceCatalog()->addLogicalSource("values", schema);

    auto topologyJsonPath = "/home/x/rustProjects/nes_simulation_starter_rs/stuff/3_layer_topology.json";
    auto stream = std::ifstream(topologyJsonPath);
    std::stringstream buffer;
    buffer << stream.rdbuf();
    auto topologyJson = nlohmann::json::parse(buffer.str());
    //auto nodes = topologyJson["nodes"];
    auto nodes = topologyJson["nodes"].get<std::map<std::string, std::vector<double>>>();
    //std::cout << nodes.size() << std::endl;

    std::map<std::string, NesWorkerPtr> fieldNodes;
    std::vector<std::shared_ptr<Testing::BorrowedPort>> ports;
    for (auto elem : nodes) {
        //for (auto [lat, lng] : nodes.get<std::vector<std::pair<double, double>>>()) {
        auto dataPort = getAvailablePort();
        ports.push_back(dataPort);
        auto rpcPort = getAvailablePort();
        ports.push_back(rpcPort);
        WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
        wrkConf->coordinatorPort.setValue(*rpcCoordinatorPort);
        wrkConf->dataPort.setValue(*dataPort);
        wrkConf->rpcPort.setValue(*rpcPort);
        wrkConf->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::FIXED_LOCATION);
        wrkConf->locationCoordinates.setValue({elem.second[0], elem.second[1]});
        NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
        fieldNodes[elem.first] = wrk;
        bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
        ASSERT_TRUE(retStart);
    }

    //auto childLists = topologyJson["children"];
    auto childLists = topologyJson["children"].get<std::map<std::string, std::vector<uint64_t>>>();
    //auto childLists = topologyJson["children"].get<std::map<std::string, std::vector<std::string>>>();
    auto coordinatorId = crd->getNesWorker()->getWorkerId();

    for (auto parent : fieldNodes) {
        auto parentId = parent.second->getWorkerId();
        for (auto child : childLists[parent.first]) {
            auto childId = fieldNodes[std::to_string(child)]->getWorkerId();
            topology->addTopologyNodeAsChild(parentId, childId);
            topology->removeTopologyNodeAsChild(coordinatorId, childId);
        }
    }

    auto stype = CSVSourceType::create("values", "values");
    //stype->setFilePath("/home/x/sequence2.csv");
    //stype->setFilePath("/home/x/sequence3.csv");
    stype->setFilePath("54321");
    stype->setNumberOfBuffersToProduce(1000);
    stype->setNumberOfTuplesToProducePerBuffer(0);
    stype->setGatheringInterval(1000);

    auto mobileWorkerConfigDir = "/home/x/rustProjects/nes_simulation_starter_rs/stuff/1h_dublin_bus_nanosec";
    std::vector<NesWorkerPtr> mobileWorkers;
    for (const auto& configFile : std::filesystem::recursive_directory_iterator(mobileWorkerConfigDir)) {
        std::cout << configFile << std::endl;
        auto dataPort = getAvailablePort();
        ports.push_back(dataPort);
        auto rpcPort = getAvailablePort();
        ports.push_back(rpcPort);
        WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
        wrkConf->coordinatorPort.setValue(*rpcCoordinatorPort);
        wrkConf->dataPort.setValue(*dataPort);
        wrkConf->rpcPort.setValue(*rpcPort);
        wrkConf->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
        wrkConf->mobilityConfiguration.locationProviderConfig.setValue(configFile.path());
        wrkConf->mobilityConfiguration.locationProviderType.setValue(
            NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
        wrkConf->physicalSourceTypes.add(stype);
        wrkConf->numberOfSlots.setValue(1);
        //wrkConf->locationCoordinates.setValue({elem.second[0], elem.second[1]});
        NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
        //fieldNodes[elem.first] = wrk;
        mobileWorkers.push_back(wrk);
        bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
        ASSERT_TRUE(retStart);
    }

    std::string testFile = "/tmp/test_sink";

    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    //    QueryId queryId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(
    //        R"(Query::from("values").sink(FileSinkDescriptor::create(")" + testFile + R"(", "CSV_FORMAT", "APPEND"));)",
    //        Optimizer::PlacementStrategy::BottomUp);
    QueryId queryId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(
        R"(Query::from("values").map(Attribute("value") = Attribute("value") * 2).sink(FileSinkDescriptor::create(")" + testFile
            + R"(", "CSV_FORMAT", "APPEND"));)",
        Optimizer::PlacementStrategy::BottomUp);

    //    std::stringstream ss;
    //    ss << "google-chrome \"http://localhost:3000/?host=localhost&port=";
    //    ss << std::to_string(*restPort);
    //    ss << "\"";
    //    std::system(ss.str().c_str());

    std::cin.get();
    //auto nodeMap = nodes.get<std::map<uint64, std::pair<double, double>>>();
    std::map<uint64_t, uint64_t> workerIdToJsonId;

    //    auto count = 0;
    //    for (auto node : nodes) {
    //        std::cout << node << std::endl;
    //
    //    }
    //auto nodeMap = nodes.get<std::map<uint64, std::pair<double, double>>>();
    //std::map<uint64, std::vector<double>> nodeMap;
    //nodes.get_to(nodeMap);
    //auto nodes = topologyJson["nodes"];
    //nlohmann::json::object_t topologyObject = topologyJson.parse();
    //auto nodes = topologyObject["nodes"];
}

TEST_F(QueryRedeploymentIntegrationTest, testSingleWorkerDublin) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully")

    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    auto schema = Schema::create()
                      ->addField(createField("id", BasicType::UINT64))
                      ->addField(createField("value", BasicType::UINT64))
                      ->addField(createField("ingestion_timestamp", BasicType::UINT64))
                      ->addField(createField("processing_timestamp"
                                             "",
                                             BasicType::UINT64));
    crd->getSourceCatalog()->addLogicalSource("values", schema);

    auto topologyJsonPath = "/home/x/rustProjects/nes_simulation_starter_rs/stuff/3_layer_topology.json";
    auto stream = std::ifstream(topologyJsonPath);
    std::stringstream buffer;
    buffer << stream.rdbuf();
    auto topologyJson = nlohmann::json::parse(buffer.str());
    //auto nodes = topologyJson["nodes"];
    auto nodes = topologyJson["nodes"].get<std::map<std::string, std::vector<double>>>();
    //std::cout << nodes.size() << std::endl;

    std::map<std::string, NesWorkerPtr> fieldNodes;
    std::vector<std::shared_ptr<Testing::BorrowedPort>> ports;
    for (auto elem : nodes) {
        //for (auto [lat, lng] : nodes.get<std::vector<std::pair<double, double>>>()) {
        auto dataPort = getAvailablePort();
        ports.push_back(dataPort);
        auto rpcPort = getAvailablePort();
        ports.push_back(rpcPort);
        WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
        wrkConf->coordinatorPort.setValue(*rpcCoordinatorPort);
        wrkConf->dataPort.setValue(*dataPort);
        wrkConf->rpcPort.setValue(*rpcPort);
        wrkConf->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::FIXED_LOCATION);
        wrkConf->locationCoordinates.setValue({elem.second[0], elem.second[1]});
        NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
        fieldNodes[elem.first] = wrk;
        bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
        ASSERT_TRUE(retStart);
    }

    //auto childLists = topologyJson["children"];
    auto childLists = topologyJson["children"].get<std::map<std::string, std::vector<uint64_t>>>();
    //auto childLists = topologyJson["children"].get<std::map<std::string, std::vector<std::string>>>();
    auto coordinatorId = crd->getNesWorker()->getWorkerId();

    for (auto parent : fieldNodes) {
        auto parentId = parent.second->getWorkerId();
        for (auto child : childLists[parent.first]) {
            auto childId = fieldNodes[std::to_string(child)]->getWorkerId();
            topology->addTopologyNodeAsChild(parentId, childId);
            topology->removeTopologyNodeAsChild(coordinatorId, childId);
        }
    }

    auto stype = CSVSourceType::create("values", "values");
    //stype->setFilePath("/home/x/sequence2.csv");
    //stype->setFilePath("/home/x/sequence3.csv");
    stype->setFilePath("54321");
    stype->setNumberOfBuffersToProduce(1000);
    stype->setNumberOfTuplesToProducePerBuffer(0);
    stype->setGatheringInterval(1000);

    //    auto mobileWorkerConfigDir = "/home/x/rustProjects/nes_simulation_starter_rs/stuff/1h_dublin_bus_nanosec";
    std::vector<NesWorkerPtr> mobileWorkers;
    //    for (const auto& configFile : std::filesystem::recursive_directory_iterator(mobileWorkerConfigDir)) {
    //        std::cout << configFile << std::endl;
    //        auto dataPort = getAvailablePort();
    //        ports.push_back(dataPort);
    //        auto rpcPort = getAvailablePort();
    //        ports.push_back(rpcPort);
    //        WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    //        wrkConf->coordinatorPort.setValue(*rpcCoordinatorPort);
    //        wrkConf->dataPort.setValue(*dataPort);
    //        wrkConf->rpcPort.setValue(*rpcPort);
    //        wrkConf->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    //        wrkConf->mobilityConfiguration.locationProviderConfig.setValue(configFile.path());
    //        wrkConf->mobilityConfiguration.locationProviderType.setValue(NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    //        wrkConf->physicalSourceTypes.add(stype);
    //        wrkConf->numberOfSlots.setValue(1);
    //        //wrkConf->locationCoordinates.setValue({elem.second[0], elem.second[1]});
    //        NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
    //        //fieldNodes[elem.first] = wrk;
    //        mobileWorkers.push_back(wrk);
    //        bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    //        ASSERT_TRUE(retStart);
    //    }
    auto dataPort = getAvailablePort();
    ports.push_back(dataPort);
    auto rpcPort = getAvailablePort();
    ports.push_back(rpcPort);
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort.setValue(*rpcCoordinatorPort);
    wrkConf->dataPort.setValue(*dataPort);
    wrkConf->rpcPort.setValue(*rpcPort);
    wrkConf->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf->mobilityConfiguration.locationProviderConfig.setValue(
        "/home/x/uni/ba/experiments/nes_experiment_input/1h_dublin_bus_nanosec/"
        "vehicle33119_from1357128000000000_to1357131600000000.csv");
    wrkConf->mobilityConfiguration.locationProviderType.setValue(NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf->mobilityConfiguration.reconnectPredictorType.setValue(
        NES::Spatial::Mobility::Experimental::ReconnectPredictorType::PRECALCULATED);
    wrkConf->mobilityConfiguration.precalculatedReconnectPath.setValue(
        "/home/x/uni/ba/experiments/nes_experiment_input/1_mobile_worker_explicit_reconnects/reconnects.csv");
    wrkConf->physicalSourceTypes.add(stype);
    wrkConf->numberOfSlots.setValue(1);
    //wrkConf->locationCoordinates.setValue({elem.second[0], elem.second[1]});
    NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
    //fieldNodes[elem.first] = wrk;
    mobileWorkers.push_back(wrk);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart);

    std::string testFile = "/tmp/test_sink";

    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    //    QueryId queryId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(
    //        R"(Query::from("values").sink(FileSinkDescriptor::create(")" + testFile + R"(", "CSV_FORMAT", "APPEND"));)",
    //        Optimizer::PlacementStrategy::BottomUp);
    QueryId queryId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(
        R"(Query::from("values").map(Attribute("value") = Attribute("value") * 2).sink(FileSinkDescriptor::create(")" + testFile
            + R"(", "CSV_FORMAT", "APPEND"));)",
        Optimizer::PlacementStrategy::BottomUp);

    //    std::stringstream ss;
    //    ss << "google-chrome \"http://localhost:3000/?host=localhost&port=";
    //    ss << std::to_string(*restPort);
    //    ss << "\"";
    //    std::system(ss.str().c_str());

    std::cin.get();
    //auto nodeMap = nodes.get<std::map<uint64, std::pair<double, double>>>();
    std::map<uint64_t, uint64_t> workerIdToJsonId;

    //    auto count = 0;
    //    for (auto node : nodes) {
    //        std::cout << node << std::endl;
    //
    //    }
    //auto nodeMap = nodes.get<std::map<uint64, std::pair<double, double>>>();
    //std::map<uint64, std::vector<double>> nodeMap;
    //nodes.get_to(nodeMap);
    //auto nodes = topologyJson["nodes"];
    //nlohmann::json::object_t topologyObject = topologyJson.parse();
    //auto nodes = topologyObject["nodes"];
}

#endif

INSTANTIATE_TEST_CASE_P(QueryRedeploymentIntegrationTestParam, QueryRedeploymentIntegrationTest, ::testing::Values(1, 4));
}// namespace NES

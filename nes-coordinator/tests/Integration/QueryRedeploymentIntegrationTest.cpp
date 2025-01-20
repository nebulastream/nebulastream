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
#include <Network/NetworkSource.hpp>
#include <Network/PartitionManager.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Reconfiguration/Metadata/DrainQueryMetadata.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddLinkEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPRemoveLinkEvent.hpp>
#include <RequestProcessor/StorageHandles/SerialStorageHandler.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/TestUtils.hpp>
#include <Util/TopologyLinkInformation.hpp>
#include <atomic>
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

constexpr uint32_t DEFAULT_NUMBER_OF_ORIGINS = 0;
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
    const uint32_t retryTimes = 10;
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
        if (currentCount > totalBuffersToProduce) {
            return;
        }//avoid eos
        auto valCount = (currentCount - 1) * (numberOfTuplesToProduce);
        auto* records = buffer.getBuffer<Record>();
        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            records[u].value = valCount + u;
        }
    };
    auto lambdaSourceType = LambdaSourceType::create("seq",
                                                     "test_stream",
                                                     std::move(lambdaSourceFunction),
                                                     // totalBuffersToProduce,
                                                     totalBuffersToProduce + 1000,//avoid eos
                                                     gatheringValue,
                                                     GatheringMode::INTERVAL_MODE);

    NES_INFO("rest port = {}", *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    auto crdWorkerDataPort = getAvailablePort();
    coordinatorConfig->worker.dataPort = *crdWorkerDataPort;
    coordinatorConfig->worker.connectSourceEventChannelsAsync.setValue(true);

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
    wrkConf1->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);

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

    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, crd->getQueryCatalog()));
    SharedQueryId sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    ASSERT_NE(sharedQueryId, INVALID_SHARED_QUERY_ID);
    //reconfiguration
    auto subPlanIdWrk3 = 300;
    ASSERT_EQ(wrk2->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).size(), 1);
    auto oldDecomposedQueryId = wrk2->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();
    auto wrk2Source = wrk2->getNodeEngine()
                          ->getExecutableQueryPlan(oldDecomposedQueryId.id, oldDecomposedQueryId.version)
                          ->getSources()
                          .front();
    Network::NesPartition currentWrk1TargetPartition(sharedQueryId,
                                                     wrk2Source->getOperatorId(),
                                                     PartitionId(0),
                                                     SubpartitionId(0));

    ASSERT_EQ(crd->getNesWorker()->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).size(), 1);
    auto coordinatorSubplanId = crd->getNesWorker()->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();
    auto coordinatorSource = crd->getNesWorker()
                                 ->getNodeEngine()
                                 ->getExecutableQueryPlan(oldDecomposedQueryId.id, oldDecomposedQueryId.version)
                                 ->getSources()
                                 .front();
    Network::NesPartition networkSourceCrdPartition(sharedQueryId,
                                                    coordinatorSource->getOperatorId(),
                                                    PartitionId(0),
                                                    SubpartitionId(0));

    auto oldWorker = wrk2;
    while (actualReconnects < numberOfReconnectsToPerform) {
        ASSERT_EQ(wrk1->getNodeEngine()->getPartitionManager()->getProducerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
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

        auto subQueryIds = wrk1->getNodeEngine()->getDecomposedQueryIds(sharedQueryId);
        EXPECT_EQ(subQueryIds.size(), 1);
        //retrieve data about running network sink at wrk1
        auto networkSink = std::dynamic_pointer_cast<Network::NetworkSink>(
            wrk1->getNodeEngine()->getExecutableQueryPlan(subQueryIds.front())->getSinks().front());
        auto uniqueNetworkSinkDescriptorId = networkSink->getUniqueNetworkSinkDescriptorId();
        //trigger sink reconnection to new source on wrk2
        auto networkSourceWrk3Partition =
            NES::Network::NesPartition(sharedQueryId, OperatorId(networkSrcWrk3Id), PartitionId(0), SubpartitionId(0));
        //start query on wrk3
        DecomposedQueryId decomposedId(subPlanIdWrk3);
        auto queryPlan3 = DecomposedQueryPlan::create(decomposedId, sharedQueryId, wrk3->getWorkerId());
        //create network source getting data from sink at wrk1
        auto worker3Location = NES::Network::NodeLocation(wrk3->getWorkerId(), "localhost", *wrk3DataPort);
        auto networkSourceDescriptorWrk3 = Network::NetworkSourceDescriptor::create(schema,
                                                                                    networkSourceWrk3Partition,
                                                                                    worker3Location,
                                                                                    std::chrono::milliseconds(1000),
                                                                                    5,
                                                                                    0,
                                                                                    OperatorId(networkSrcWrk3Id));
        auto sourceOperatorNodeWrk3 =
            std::make_shared<SourceLogicalOperator>(networkSourceDescriptorWrk3, OperatorId(networkSrcWrk3Id));
        queryPlan3->addRootOperator(sourceOperatorNodeWrk3);

        auto crdDecomposedId = crd->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();
        auto crdDecomposedPlan = crd->getNodeEngine()->getExecutableQueryPlan(crdDecomposedId);
        auto networkSourceCrd = crdDecomposedPlan->getSources().front();
        auto networkSinkDescriptorWrk3 = Network::NetworkSinkDescriptor::create(networkSourceCrdLocation,
                                                                                networkSourceCrdPartition,
                                                                                waitTime,
                                                                                retryTimes,
                                                                                0,
                                                                                0,
                                                                                OperatorId(networkSinkWrk3Id));
        auto networkSinkOperatorNodeWrk3 =
            std::make_shared<SinkLogicalOperator>(networkSinkDescriptorWrk3, OperatorId(networkSinkWrk3Id));
        queryPlan3->appendOperatorAsNewRoot(networkSinkOperatorNodeWrk3);
        queryPlan3->getSinkOperators().front()->inferSchema();
        //register and start query on worker 3
        auto success_register_wrk3 = wrk3->getNodeEngine()->registerDecomposableQueryPlan(queryPlan3);
        ASSERT_TRUE(success_register_wrk3);
        auto success_start_wrk3 =
            wrk3->getNodeEngine()->startDecomposedQueryPlan(sharedQueryId, queryPlan3->getDecomposedQueryId());
        ASSERT_TRUE(success_start_wrk3);

        auto networkSourceWrk3Descriptor =
            std::dynamic_pointer_cast<Network::NetworkSourceDescriptor>(sourceOperatorNodeWrk3->getSourceDescriptor());
        auto newSinkDescriptor = Network::NetworkSinkDescriptor::create(networkSourceWrk3Descriptor->getNodeLocation(),
                                                                        networkSourceWrk3Descriptor->getNesPartition(),
                                                                        waitTime,
                                                                        retryTimes,
                                                                        0,
                                                                        0,
                                                                        OperatorId(0));
        auto newNetworkSinkDescriptor = std::dynamic_pointer_cast<Network::NetworkSinkDescriptor>(newSinkDescriptor);

        //create marker with events to undeploy obsolete plans
        auto reconfigMarker = ReconfigurationMarker::create();
        auto metadata = std::make_shared<DrainQueryMetadata>(1);
        auto event = ReconfigurationMarkerEvent::create(QueryState::RUNNING, metadata);
        reconfigMarker->addReconfigurationEvent(oldSubplanId, event);

        //insert reconfiguration at sink
        auto sinkToReconfigure = wrk1->getNodeEngine()->getExecutableQueryPlan(subQueryIds.front())->getSinks().front();
        auto networkSinkToReconfigure = std::dynamic_pointer_cast<Network::NetworkSink>(sinkToReconfigure);
        networkSinkToReconfigure->configureNewSinkDescriptor(*newNetworkSinkDescriptor, reconfigMarker);

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

        //coordinator side checks
        //todo #5133: reactivate check after logic on query catalog is updated query catalog is updated
        // auto lockedExecutionNode = crd->getGlobalExecutionPlan()->getLockedExecutionNode(wrk3->getWorkerId());
        // auto updatedPartition =
        //     std::dynamic_pointer_cast<Network::NetworkSourceDescriptor>(lockedExecutionNode->operator*()
        //                                                                     ->getAllDecomposedQueryPlans(sharedQueryId)
        //                                                                     .front()
        //                                                                     ->getSourceOperators()
        //                                                                     .front()
        //                                                                     ->getSourceDescriptor())
        //         ->getNesPartition();

        ASSERT_EQ(wrk1->getNodeEngine()->getPartitionManager()->getProducerRegistrationStatus(
                      newNetworkSinkDescriptor->getNesPartition()),
                  Network::PartitionRegistrationStatus::Registered);
        EXPECT_NE(oldWorker->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
        currentWrk1TargetPartition = networkSourceWrk3Partition;

        //verify that query has been undeployed from old parent
        while (oldWorker->getNodeEngine()->getQueryStatus(sharedQueryId)
               != Runtime::Execution::ExecutableQueryPlanStatus::Finished) {
            NES_DEBUG("Query has not yet stopped on worker {}", oldWorker->getWorkerId());
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_EQ(oldWorker->getNodeEngine()->getQueryStatus(sharedQueryId),
                  Runtime::Execution::ExecutableQueryPlanStatus::Finished);

        oldWorker = wrk3;
        EXPECT_EQ(oldWorker->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
        oldDecomposedQueryId = oldWorker->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();

        //check that query has left migrating state and is running again
        ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, crd->getQueryCatalog()));
        auto entries = crd->getQueryCatalog()->getQueryEntriesWithStatus("MIGRATING");
        ASSERT_TRUE(entries.empty());

        //check that all tuples arrived
        ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringAfter, testFile));

        waitForReconfig = false;
        waitForReconnect = false;
        actualReconnects++;
        waitForFinalCount = true;

        networkSinkWrk3Id += 10;
        networkSrcWrk3Id += 10;
    }

    //use marker to shut down to not cause coordinator side errors
    //create marker
    auto reconfigMarker = ReconfigurationMarker::create();
    auto metadata = std::make_shared<DrainQueryMetadata>(1);
    auto event = ReconfigurationMarkerEvent::create(QueryState::RUNNING, metadata);
    reconfigMarker->addReconfigurationEvent(wrk1->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);
    reconfigMarker->addReconfigurationEvent(oldWorker->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);
    reconfigMarker->addReconfigurationEvent(crd->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);

    //insert reconfig marker at the source
    auto dataSource = wrk1->getNodeEngine()
                          ->getExecutableQueryPlan(wrk1->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front())
                          ->getSources()
                          .front();
    dataSource->handleReconfigurationMarker(reconfigMarker);

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

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    cout << "stopping worker" << endl;
    bool retStopWrk2 = wrk2->stop(false);
    ASSERT_TRUE(retStopWrk2);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/**
 * @brief This tests multiple iterations of inserting VersionDrain events to trigger the reconfiguration of a network sink to point to a new source.
 */
TEST_P(QueryRedeploymentIntegrationTest, testSourceReuse) {
    const uint32_t retryTimes = 10;
    const bool performReconnects = true;
    const uint64_t addToSrcTwo = 10000;
    const uint64_t addToSrcThree = 20000;
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

    std::atomic<uint64_t> actualReconnects = 0;
    std::atomic<bool> waitForReconfig = false;
    std::atomic<bool> waitForReconnect = false;
    std::atomic<bool> waitForFinalCount = false;
    std::atomic<uint64_t> bufferCount = 0;
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
        if (currentCount > totalBuffersToProduce) {
            return;
        }//avoid eos
        auto valCount = (currentCount - 1) * (numberOfTuplesToProduce);
        auto* records = buffer.getBuffer<Record>();
        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            records[u].value = valCount + u;
        }
    };
    std::atomic<uint64_t> bufferCount2 = 0;
    auto lambdaSourceFunction2 =
        [&bufferCount2, &waitForReconfig, &waitForFinalCount, &actualReconnects](NES::Runtime::TupleBuffer& buffer,
                                                                                 uint64_t numberOfTuplesToProduce) {
            struct Record {
                uint64_t value;
            };
            auto currentCount = ++bufferCount2;
            if (currentCount > numBuffersToProduceBeforeReconnect + (actualReconnects * buffersToProducePerReconnectCycle)) {
                //after sending the specified amount of tuples, wait until the reconfiguration has been triggered, subsequent tuples will be buffered
                while (!waitForReconfig)
                    ;
            }
            if (currentCount > numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect
                    + numBuffersToProduceWhileBuffering + (actualReconnects * buffersToProducePerReconnectCycle)) {
                while (!waitForFinalCount)
                    ;
            }
            if (currentCount > totalBuffersToProduce) {
                return;
            }//avoid eos
            auto valCount = (currentCount - 1) * (numberOfTuplesToProduce);
            auto* records = buffer.getBuffer<Record>();
            for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
                records[u].value = valCount + u + addToSrcTwo;
            }
        };
    std::atomic<uint64_t> bufferCount3 = 0;
    auto lambdaSourceFunction3 =
        [&bufferCount3, &waitForReconfig, &waitForFinalCount, &actualReconnects](NES::Runtime::TupleBuffer& buffer,
                                                                                 uint64_t numberOfTuplesToProduce) {
          struct Record {
              uint64_t value;
          };
          auto currentCount = ++bufferCount3;
          if (currentCount > numBuffersToProduceBeforeReconnect + (actualReconnects * buffersToProducePerReconnectCycle)) {
              //after sending the specified amount of tuples, wait until the reconfiguration has been triggered, subsequent tuples will be buffered
              while (!waitForReconfig)
                  ;
          }
          if (currentCount > numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect
                             + numBuffersToProduceWhileBuffering + (actualReconnects * buffersToProducePerReconnectCycle)) {
              while (!waitForFinalCount)
                  ;
          }
          if (currentCount > totalBuffersToProduce) {
              return;
          }//avoid eos
          auto valCount = (currentCount - 1) * (numberOfTuplesToProduce);
          auto* records = buffer.getBuffer<Record>();
          for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
              records[u].value = valCount + u + addToSrcThree;
          }
        };
    auto lambdaSourceType = LambdaSourceType::create("seq",
                                                     "test_stream",
                                                     std::move(lambdaSourceFunction),
                                                     // totalBuffersToProduce,
                                                     totalBuffersToProduce + 1000,//avoid eos
                                                     gatheringValue,
                                                     GatheringMode::INTERVAL_MODE);

    auto lambdaSourceType2 = LambdaSourceType::create("seq",
                                                      "test_stream2",
                                                      std::move(lambdaSourceFunction2),
                                                      // totalBuffersToProduce,
                                                      totalBuffersToProduce + 1000,//avoid eos
                                                      gatheringValue,
                                                      GatheringMode::INTERVAL_MODE);
    auto lambdaSourceType3 = LambdaSourceType::create("seq",
                                                      "test_stream3",
                                                      std::move(lambdaSourceFunction3),
                                                      totalBuffersToProduce + 1000,//avoid eos
                                                      gatheringValue,
                                                      GatheringMode::INTERVAL_MODE);

    NES_INFO("rest port = {}", *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    auto crdWorkerDataPort = getAvailablePort();
    coordinatorConfig->worker.dataPort = *crdWorkerDataPort;
    coordinatorConfig->worker.connectSourceEventChannelsAsync.setValue(true);
    coordinatorConfig->worker.connectSinksAsync.setValue(true);
    coordinatorConfig->optimizer.enableIncrementalPlacement.setValue(true);

    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    TopologyPtr topology = crd->getTopology();
    auto nodeCount = 1;
    ASSERT_TRUE(waitForNodes(5, nodeCount, topology));
    nodeCount++;

    auto schema = Schema::create()->addField(createField("value", BasicType::UINT64));
    crd->getSourceCatalog()->addLogicalSource("seq", schema);

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    wrkConf1->numWorkerThreads.setValue(GetParam());
    wrkConf1->connectSinksAsync.setValue(true);
    wrkConf1->connectSourceEventChannelsAsync.setValue(true);
    wrkConf1->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);

    wrkConf1->physicalSourceTypes.add(lambdaSourceType);

    auto wrk1DataPort = getAvailablePort();
    wrkConf1->dataPort = *wrk1DataPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(waitForNodes(5, nodeCount, topology));
    nodeCount++;

    NES_INFO("start worker 4");
    WorkerConfigurationPtr wrkConf4 = WorkerConfiguration::create();
    wrkConf4->coordinatorPort.setValue(*rpcCoordinatorPort);
    wrkConf4->numWorkerThreads.setValue(GetParam());
    wrkConf4->connectSinksAsync.setValue(true);
    wrkConf4->connectSourceEventChannelsAsync.setValue(true);
    wrkConf4->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
    wrkConf4->numberOfSlots.setValue(4);

    wrkConf4->physicalSourceTypes.add(lambdaSourceType2);
    wrkConf4->physicalSourceTypes.add(lambdaSourceType3);

    auto wrk4DataPort = getAvailablePort();
    wrkConf4->dataPort = *wrk4DataPort;
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(wrkConf4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart4);

    ASSERT_TRUE(waitForNodes(5, nodeCount, topology));
    nodeCount++;

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
    ASSERT_TRUE(waitForNodes(5, nodeCount, topology));
    nodeCount++;

    wrk1->replaceParent(crd->getNesWorker()->getWorkerId(), wrk2->getWorkerId());

    //start query
    QueryId queryId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(
                    R"(Query::from("seq").map(Attribute("value") = Attribute("value")).map(Attribute("value") = Attribute("value")).sink(FileSinkDescriptor::create(")"
            + testFile + R"(", "CSV_FORMAT", "APPEND"));)",
        Optimizer::PlacementStrategy::BottomUp);
    auto networkSinkWrk3Id = 31;
    auto networkSrcWrk3Id = 32;
    auto networkSourceCrdIncId = 900;
    auto fileSinkId = 800;

    auto sinkLocationWrk1 = NES::Network::NodeLocation(wrk1->getWorkerId(), "localhost", *wrk1DataPort);
    auto networkSourceCrdLocation =
        NES::Network::NodeLocation(crd->getNesWorker()->getWorkerId(), "localhost", *crdWorkerDataPort);

    std::vector<NesWorkerPtr> reconnectParents;

    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, crd->getQueryCatalog()));
    SharedQueryId sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    ASSERT_NE(sharedQueryId, INVALID_SHARED_QUERY_ID);
    //reconfiguration
    auto subPlanIdWrk3 = 300;
    auto subPlanIdCrd = 1900;
    ASSERT_EQ(wrk2->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).size(), 1);
    auto oldSubplanId = wrk2->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();
    auto wrk2Source = wrk2->getNodeEngine()->getExecutableQueryPlan(oldSubplanId)->getSources().front();
    Network::NesPartition currentWrk1TargetPartition(sharedQueryId,
                                                     wrk2Source->getOperatorId(),
                                                     PartitionId(0),
                                                     SubpartitionId(0));

    ASSERT_EQ(crd->getNesWorker()->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).size(), 1);

    auto coordinatorSubplanId = crd->getNesWorker()->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();
    //todo: make sure we get the right source here
    auto coordinatorSources = crd->getNesWorker()->getNodeEngine()->getExecutableQueryPlan(coordinatorSubplanId)->getSources();
    auto mobileWorkerParentSrcCrdIt =
        crd->getNesWorker()->getNodeEngine()->getExecutableQueryPlan(coordinatorSubplanId)->getSources().cbegin();

    while (std::static_pointer_cast<Network::NetworkSource>(*mobileWorkerParentSrcCrdIt)->getSenderLocation().getNodeId()
           != wrk2->getWorkerId()) {
        mobileWorkerParentSrcCrdIt++;
    }
    auto mobileCoordinatorSource = *mobileWorkerParentSrcCrdIt;
    auto fixedWorkerParentSrcCrdIt =
        crd->getNesWorker()->getNodeEngine()->getExecutableQueryPlan(coordinatorSubplanId)->getSources().cbegin();
    while (std::static_pointer_cast<Network::NetworkSource>(*fixedWorkerParentSrcCrdIt)->getSenderLocation().getNodeId()
           != wrk4->getWorkerId()) {
        fixedWorkerParentSrcCrdIt++;
    }
    auto fixedCoordinatorSource = *fixedWorkerParentSrcCrdIt;
    auto fixedWorkerParentSrcCrdIt2 =
        crd->getNesWorker()->getNodeEngine()->getExecutableQueryPlan(coordinatorSubplanId)->getSources().cbegin();
    while (std::static_pointer_cast<Network::NetworkSource>(*fixedWorkerParentSrcCrdIt2)->getSenderLocation().getNodeId()
           != wrk4->getWorkerId() || fixedWorkerParentSrcCrdIt2 == fixedWorkerParentSrcCrdIt) {
        fixedWorkerParentSrcCrdIt2++;
    }
    ASSERT_NE(fixedWorkerParentSrcCrdIt2, fixedWorkerParentSrcCrdIt);
    auto fixedCoordinatorSource2 = *fixedWorkerParentSrcCrdIt2;

    Network::NesPartition mobileSourceCrdPartition(sharedQueryId,
                                                   mobileCoordinatorSource->getOperatorId(),
                                                   PartitionId(0),
                                                   SubpartitionId(0));
    auto oldWorker = wrk2;
    while (actualReconnects < numberOfReconnectsToPerform) {
        ASSERT_EQ(wrk1->getNodeEngine()->getPartitionManager()->getProducerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
        subPlanIdWrk3++;
        subPlanIdCrd++;

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
            oss << std::to_string(i + addToSrcTwo) << std::endl;
            oss << std::to_string(i + addToSrcThree) << std::endl;
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
        ASSERT_TRUE(waitForNodes(5, nodeCount + actualReconnects, topology));

        reconnectParents.push_back(wrk3);

        std::string compareStringAfter;
        std::ostringstream ossAfter;
        ossAfter << "seq$value:INTEGER(64 bits)" << std::endl;
        for (uint64_t i = 0;
             i < (numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect + numBuffersToProduceWhileBuffering)
                 * tuplesPerBuffer * (actualReconnects + 1);
             ++i) {
            ossAfter << std::to_string(i) << std::endl;
            ossAfter << std::to_string(i + addToSrcTwo) << std::endl;
            ossAfter << std::to_string(i + addToSrcThree) << std::endl;
        }
        compareStringAfter = ossAfter.str();

        std::shared_ptr<Network::NetworkSinkDescriptor> newNetworkSinkDescriptor = nullptr;
        auto networkSourceWrk3Partition =
            NES::Network::NesPartition(sharedQueryId, OperatorId(networkSrcWrk3Id), PartitionId(0), SubpartitionId(0));
        if (performReconnects) {
            int noOfMigratingPlans = 0;
            int noOfCompletedMigrations = 0;
            int noOfRunningPlans = 0;

            auto worker3Location = NES::Network::NodeLocation(wrk3->getWorkerId(), "localhost", *wrk3DataPort);

            auto oldDecomposedId =
                crd->getNesWorker()
                    ->getNodeEngine()
                    ->getDecomposedQueryIdsWithStatus(sharedQueryId, Runtime::Execution::ExecutableQueryPlanStatus::Running)
                    .front();
            auto oldPlanCrd = crd->getNesWorker()->getNodeEngine()->getExecutableQueryPlan(oldDecomposedId);

            //this is the partition for the fixed node
            mobileSourceCrdPartition =
                Network::NesPartition(sharedQueryId, OperatorId(networkSourceCrdIncId++), PartitionId(0), SubpartitionId(0));

            DecomposedQueryId newCrdDecomposedId(subPlanIdCrd);
            auto newPlanCrd = DecomposedQueryPlan::create(newCrdDecomposedId, sharedQueryId, crd->getNesWorker()->getWorkerId());
            //add unchanged source for fixed
            auto fixedNetworkSource = std::static_pointer_cast<Network::NetworkSource>(fixedCoordinatorSource);
            auto fixedNetworkSource2 = std::static_pointer_cast<Network::NetworkSource>(fixedCoordinatorSource2);
            auto fixedNetworkSrcDescriptor = Network::NetworkSourceDescriptor::create(fixedCoordinatorSource->getSchema(),
                                                                                      fixedNetworkSource->getPartition(),
                                                                                      fixedNetworkSource->getSenderLocation(),
                                                                                      std::chrono::milliseconds(1000),
                                                                                      5,
                                                                                      0,
                                                                                      fixedNetworkSource->getUniqueId());
            auto fixedSourceOpCrdNew = std::make_shared<SourceLogicalOperator>(
                fixedNetworkSrcDescriptor,
                OperatorId(fixedCoordinatorSource->getOperatorId()));//todo: check if we need a new operator id
            newPlanCrd->addRootOperator(fixedSourceOpCrdNew);

            auto fixedNetworkSrcDescriptor2 = Network::NetworkSourceDescriptor::create(fixedCoordinatorSource2->getSchema(),
                                                                                      fixedNetworkSource2->getPartition(),
                                                                                      fixedNetworkSource2->getSenderLocation(),
                                                                                      std::chrono::milliseconds(1000),
                                                                                      5,
                                                                                      0,
                                                                                      fixedNetworkSource2->getUniqueId());
            fixedSourceOpCrdNew = std::make_shared<SourceLogicalOperator>(
                fixedNetworkSrcDescriptor2,
                OperatorId(fixedCoordinatorSource2->getOperatorId()));//todo: check if we need a new operator id
            newPlanCrd->addRootOperator(fixedSourceOpCrdNew);
            auto mobileNetworkSource = std::static_pointer_cast<Network::NetworkSource>(mobileCoordinatorSource);
            auto mobileNetworkSourceDescriptor =
                Network::NetworkSourceDescriptor::create(mobileCoordinatorSource->getSchema(),
                                                         mobileSourceCrdPartition,
                                                         networkSourceCrdLocation,
                                                         std::chrono::milliseconds(1000),
                                                         5,
                                                         0,
                                                         mobileSourceCrdPartition.getOperatorId());
            auto mobileSourceOpCrdNew = std::make_shared<SourceLogicalOperator>(
                mobileNetworkSourceDescriptor,
                OperatorId(mobileSourceCrdPartition.getOperatorId())//todo: check if we need to adjust htis
            );
            newPlanCrd->addRootOperator(mobileSourceOpCrdNew);
            auto fileSink = oldPlanCrd->getSinks().front();
            auto fileSinkDescriptor = FileSinkDescriptor::create(testFile, fileSink->getSinkFormat(), "APPEND");
            auto newFileSink = std::make_shared<SinkLogicalOperator>(fileSinkDescriptor, OperatorId(fileSinkId++));
            newPlanCrd->appendOperatorAsNewRoot(newFileSink);

            newPlanCrd->getSinkOperators().front()->inferSchema();

            //start new plan at crd
            auto success_register_crd = crd->getNodeEngine()->registerDecomposableQueryPlan(newPlanCrd);
            ASSERT_TRUE(success_register_crd);
            auto success_start_crd =
                crd->getNodeEngine()->startDecomposedQueryPlan(sharedQueryId, newPlanCrd->getDecomposedQueryId());
            ASSERT_TRUE(success_register_crd);

            auto subQueryIds = wrk1->getNodeEngine()->getDecomposedQueryIds(sharedQueryId);
            EXPECT_EQ(subQueryIds.size(), 1);
            //retrieve data about running network sink at wrk1
            auto networkSink = std::dynamic_pointer_cast<Network::NetworkSink>(
                wrk1->getNodeEngine()->getExecutableQueryPlan(subQueryIds.front())->getSinks().front());
            auto uniqueNetworkSinkDescriptorId = networkSink->getUniqueNetworkSinkDescriptorId();
            //trigger sink reconnection to new source on wrk2
            //start query on wrk3
            DecomposedQueryId decomposedId(subPlanIdWrk3);
            auto queryPlan3 = DecomposedQueryPlan::create(decomposedId, sharedQueryId, wrk3->getWorkerId());
            //create network source getting data from sink at wrk1
            auto networkSourceDescriptorWrk3 = Network::NetworkSourceDescriptor::create(schema,
                                                                                        networkSourceWrk3Partition,
                                                                                        worker3Location,
                                                                                        std::chrono::milliseconds(1000),
                                                                                        5,
                                                                                        0,
                                                                                        OperatorId(networkSrcWrk3Id));
            auto sourceOperatorNodeWrk3 =
                std::make_shared<SourceLogicalOperator>(networkSourceDescriptorWrk3, OperatorId(networkSrcWrk3Id));
            queryPlan3->addRootOperator(sourceOperatorNodeWrk3);

            auto crdDecomposedId = crd->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();
            auto crdDecomposedPlan = crd->getNodeEngine()->getExecutableQueryPlan(crdDecomposedId);
            auto networkSourceCrd = crdDecomposedPlan->getSources().front();

            auto networkSinkDescriptorWrk3 = Network::NetworkSinkDescriptor::create(networkSourceCrdLocation,
                                                                                    mobileSourceCrdPartition,
                                                                                    waitTime,
                                                                                    retryTimes,
                                                                                    0,
                                                                                    0,
                                                                                    OperatorId(networkSinkWrk3Id));
            auto networkSinkOperatorNodeWrk3 =
                std::make_shared<SinkLogicalOperator>(networkSinkDescriptorWrk3, OperatorId(networkSinkWrk3Id));
            queryPlan3->appendOperatorAsNewRoot(networkSinkOperatorNodeWrk3);
            queryPlan3->getSinkOperators().front()->inferSchema();
            //register and start query on worker 3
            auto success_register_wrk3 = wrk3->getNodeEngine()->registerDecomposableQueryPlan(queryPlan3);
            ASSERT_TRUE(success_register_wrk3);
            auto success_start_wrk3 =
                wrk3->getNodeEngine()->startDecomposedQueryPlan(sharedQueryId, queryPlan3->getDecomposedQueryId());
            ASSERT_TRUE(success_start_wrk3);

            auto networkSourceWrk3Descriptor =
                std::dynamic_pointer_cast<Network::NetworkSourceDescriptor>(sourceOperatorNodeWrk3->getSourceDescriptor());
            auto newSinkDescriptor = Network::NetworkSinkDescriptor::create(networkSourceWrk3Descriptor->getNodeLocation(),
                                                                            networkSourceWrk3Descriptor->getNesPartition(),
                                                                            waitTime,
                                                                            retryTimes,
                                                                            0,
                                                                            0,
                                                                            OperatorId(0));
            newNetworkSinkDescriptor = std::dynamic_pointer_cast<Network::NetworkSinkDescriptor>(newSinkDescriptor);

            //create marker with events to undeploy obsolete plans
            auto reconfigMarker = ReconfigurationMarker::create();
            auto metadata = std::make_shared<DrainQueryMetadata>(1);
            auto event = ReconfigurationMarkerEvent::create(QueryState::RUNNING, metadata);
            reconfigMarker->addReconfigurationEvent(oldSubplanId, event);
            metadata = std::make_shared<DrainQueryMetadata>(2);
            event = ReconfigurationMarkerEvent::create(QueryState::RUNNING, metadata);
            reconfigMarker->addReconfigurationEvent(oldPlanCrd->getDecomposedQueryId(), event);

            //insert reconfiguration at sink
            auto sinkToReconfigure = wrk1->getNodeEngine()->getExecutableQueryPlan(subQueryIds.front())->getSinks().front();
            auto networkSinkToReconfigure = std::dynamic_pointer_cast<Network::NetworkSink>(sinkToReconfigure);
            networkSinkToReconfigure->configureNewSinkDescriptor(*newNetworkSinkDescriptor, reconfigMarker);

        }

        //notify lambda source that reconfig happened and make it release more tuples into the buffer
        waitForFinalCount = false;
        waitForReconfig = true;
        //wait for tuples in order to make sure that the buffer is actually tested
        while (!waitForReconnect) {
        }

        if (performReconnects) {
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

            ASSERT_EQ(wrk1->getNodeEngine()->getPartitionManager()->getProducerRegistrationStatus(
                          newNetworkSinkDescriptor->getNesPartition()),
                      Network::PartitionRegistrationStatus::Registered);
            EXPECT_NE(
                oldWorker->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(currentWrk1TargetPartition),
                Network::PartitionRegistrationStatus::Registered);
            currentWrk1TargetPartition = networkSourceWrk3Partition;

            //verify that query has been undeployed from old parent
            while (oldWorker->getNodeEngine()->getQueryStatus(sharedQueryId)
                   == Runtime::Execution::ExecutableQueryPlanStatus::Running) {
                NES_DEBUG("Query has not yet stopped on worker {}", oldWorker->getWorkerId());
                std::this_thread::sleep_for(std::chrono::milliseconds(2000));
                if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                    FAIL();
                }
            }
            ASSERT_EQ(oldWorker->getNodeEngine()->getQueryStatus(sharedQueryId),
                      Runtime::Execution::ExecutableQueryPlanStatus::Finished);

            //verify that query has been undeployed from coordinator
            auto runningPlans =
                crd->getNodeEngine()
                    ->getDecomposedQueryIdsWithStatus(sharedQueryId, Runtime::Execution::ExecutableQueryPlanStatus::Running)
                    .size();
            while (runningPlans != 1) {
                NES_DEBUG("Old decomposed query plan has not yet stopped on coordinator ({} plans deployed, {} running)",
                          crd->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).size(),
                          runningPlans)
                std::this_thread::sleep_for(std::chrono::milliseconds(2000));
                if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                    FAIL();
                }
                if (crd->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).empty()) {
                    NES_DEBUG("Could not find executable query plan for shared query on coordinator");
                    FAIL();
                }
                runningPlans =
                    crd->getNodeEngine()
                        ->getDecomposedQueryIdsWithStatus(sharedQueryId, Runtime::Execution::ExecutableQueryPlanStatus::Running)
                        .size();
            }
            ASSERT_EQ(crd->getNodeEngine()
                          ->getDecomposedQueryIdsWithStatus(sharedQueryId, Runtime::Execution::ExecutableQueryPlanStatus::Running)
                          .size(),
                      1);

            oldWorker = wrk3;
            EXPECT_EQ(
                oldWorker->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(currentWrk1TargetPartition),
                Network::PartitionRegistrationStatus::Registered);
            oldSubplanId = oldWorker->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();

        }

        //check that all tuples arrived
        ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringAfter, testFile));

        waitForReconfig = false;
        waitForReconnect = false;
        actualReconnects++;
        waitForFinalCount = true;

        networkSinkWrk3Id += 10;
        networkSrcWrk3Id += 10;
    }

    //use marker to shut down to not cause coordinator side errors
    //create marker
    auto reconfigMarker = ReconfigurationMarker::create();
    auto metadata = std::make_shared<DrainQueryMetadata>(1);
    auto event = ReconfigurationMarkerEvent::create(QueryState::RUNNING, metadata);
    reconfigMarker->addReconfigurationEvent(wrk1->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);
    for (auto plan : wrk4->getNodeEngine()->getDecomposedQueryIds(sharedQueryId)) {
        reconfigMarker->addReconfigurationEvent(plan, event);
    }
    reconfigMarker->addReconfigurationEvent(oldWorker->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);
    reconfigMarker->addReconfigurationEvent(crd->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);

    //insert reconfig marker at the source
    auto dataSource = wrk1->getNodeEngine()
                          ->getExecutableQueryPlan(wrk1->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front())
                          ->getSources()
                          .front();
    dataSource->handleReconfigurationMarker(reconfigMarker);
    for (auto p : wrk4->getNodeEngine()->getDecomposedQueryIds(sharedQueryId)) {
        wrk4->getNodeEngine()
    ->getExecutableQueryPlan(p)->getSources().front()->handleReconfigurationMarker(reconfigMarker);
    }

    //send the last tuples, after which the lambda source shuts down
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk1));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk4));

    auto lastReconnectParent = reconnectParents.back();
    if (performReconnects) {
        ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, lastReconnectParent));
    }
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk2));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, crd->getNesWorker()));
    cout << "stopping worker" << endl;
    bool retStopLastParent = lastReconnectParent->stop(false);
    ASSERT_TRUE(retStopLastParent);
    reconnectParents.pop_back();
    for (auto parent : reconnectParents) {
        NES_DEBUG("stopping parent node")
        if (performReconnects) {
            ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, parent));
        }
        cout << "stopping worker" << endl;
        bool stopParent = parent->stop(false);
        ASSERT_TRUE(stopParent);
    }

    int response = remove(testFile.c_str());
    ASSERT_TRUE(response == 0);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    cout << "stopping worker" << endl;
    bool retStopWrk2 = wrk2->stop(false);
    ASSERT_TRUE(retStopWrk2);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/**
 * @brief This tests multiple iterations of inserting VersionDrain events to trigger the reconfiguration of a network sink to point to a new source.
 */
TEST_P(QueryRedeploymentIntegrationTest, testMultiplePlannedReconnectsFromCrd) {
    const uint32_t retryTimes = 10;
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
        if (currentCount > totalBuffersToProduce) {
            return;
        }//avoid eos
        auto valCount = (currentCount - 1) * (numberOfTuplesToProduce);
        auto* records = buffer.getBuffer<Record>();
        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            records[u].value = valCount + u;
        }
    };
    auto lambdaSourceType = LambdaSourceType::create("seq",
                                                     "test_stream",
                                                     std::move(lambdaSourceFunction),
                                                     // totalBuffersToProduce,
                                                     totalBuffersToProduce + 1000,//avoid eos
                                                     gatheringValue,
                                                     GatheringMode::INTERVAL_MODE);

    NES_INFO("rest port = {}", *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    auto crdWorkerDataPort = getAvailablePort();
    coordinatorConfig->worker.dataPort = *crdWorkerDataPort;
    coordinatorConfig->worker.connectSourceEventChannelsAsync.setValue(true);
    coordinatorConfig->optimizer.enableIncrementalPlacement.setValue(true);

    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    auto schema = Schema::create()->addField(createField("value", BasicType::UINT64));
    crd->getSourceCatalog()->addLogicalSource("seq", schema);

    NES_INFO("start worker 0");
    WorkerConfigurationPtr wrkConf0 = WorkerConfiguration::create();
    wrkConf0->coordinatorPort.setValue(*rpcCoordinatorPort);
    wrkConf0->numWorkerThreads.setValue(GetParam());
    wrkConf0->connectSinksAsync.setValue(true);
    wrkConf0->connectSourceEventChannelsAsync.setValue(true);
    wrkConf0->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);

    auto wrk0DataPort = getAvailablePort();
    wrkConf0->dataPort = *wrk0DataPort;
    NesWorkerPtr wrk0 = std::make_shared<NesWorker>(std::move(wrkConf0));
    bool retStart0 = wrk0->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart0);
    ASSERT_TRUE(waitForNodes(5, 2, topology));

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    wrkConf1->numWorkerThreads.setValue(GetParam());
    wrkConf1->connectSinksAsync.setValue(true);
    wrkConf1->connectSourceEventChannelsAsync.setValue(true);
    wrkConf1->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
    wrkConf1->numberOfSlots.setValue(1);

    wrkConf1->physicalSourceTypes.add(lambdaSourceType);

    auto wrk1DataPort = getAvailablePort();
    wrkConf1->dataPort = *wrk1DataPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(waitForNodes(5, 3, topology));

    NES_INFO("start worker 2");
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort.setValue(*rpcCoordinatorPort);
    auto wrk2DataPort = getAvailablePort();
    wrkConf2->dataPort = *wrk2DataPort;
    wrkConf2->numWorkerThreads.setValue(GetParam());
    wrkConf2->connectSinksAsync.setValue(true);
    wrkConf2->connectSourceEventChannelsAsync.setValue(true);
    wrkConf2->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
    wrkConf2->numberOfSlots.setValue(1);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart2);
    ASSERT_TRUE(waitForNodes(5, 4, topology));

    wrk2->replaceParent(crd->getNesWorker()->getWorkerId(), wrk0->getWorkerId());
    wrk1->replaceParent(crd->getNesWorker()->getWorkerId(), wrk2->getWorkerId());

    //start query
    QueryId queryId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(
        R"(Query::from("seq").map(Attribute("value") = Attribute("value")).map(Attribute("value") = Attribute("value")).sink(FileSinkDescriptor::create(")"
            + testFile + R"(", "CSV_FORMAT", "APPEND"));)",
        Optimizer::PlacementStrategy::BottomUp);
    auto networkSinkWrk3Id = 31;
    auto networkSrcWrk3Id = 32;

    auto sinkLocationWrk1 = NES::Network::NodeLocation(wrk1->getWorkerId(), "localhost", *wrk1DataPort);
    auto networkSourceCrdLocation =
        NES::Network::NodeLocation(crd->getNesWorker()->getWorkerId(), "localhost", *crdWorkerDataPort);

    std::vector<NesWorkerPtr> reconnectParents;

    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, crd->getQueryCatalog()));
    SharedQueryId sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    ASSERT_NE(sharedQueryId, INVALID_SHARED_QUERY_ID);
    //reconfiguration
    auto subPlanIdWrk3 = 300;
    ASSERT_EQ(wrk2->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).size(), 1);
    auto oldSubplanId = wrk2->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();
    auto wrk2Source = wrk2->getNodeEngine()->getExecutableQueryPlan(oldSubplanId)->getSources().front();
    Network::NesPartition currentWrk1TargetPartition(sharedQueryId,
                                                     wrk2Source->getOperatorId(),
                                                     PartitionId(0),
                                                     SubpartitionId(0));

    ASSERT_EQ(crd->getNesWorker()->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).size(), 1);

    auto oldWorker = wrk2;
    while (actualReconnects < numberOfReconnectsToPerform) {
        ASSERT_EQ(wrk1->getNodeEngine()->getPartitionManager()->getProducerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
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
        wrkConf3->numberOfSlots.setValue(1);
        NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
        bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
        ASSERT_TRUE(retStart3);
        ASSERT_TRUE(waitForNodes(5, 5 + actualReconnects, topology));
        wrk3->replaceParent(crd->getNesWorker()->getWorkerId(), wrk0->getWorkerId());
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

        auto addLinkEvent = std::make_shared<RequestProcessor::ISQPAddLinkEvent>(wrk3->getWorkerId(), wrk1->getWorkerId());
        auto removeLinkEvent =
            std::make_shared<RequestProcessor::ISQPRemoveLinkEvent>(oldWorker->getWorkerId(), wrk1->getWorkerId());
        crd->getRequestHandlerService()->queueISQPRequest({addLinkEvent, removeLinkEvent});

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

        //coordinator side checks
        auto lockedExecutionNode = crd->getGlobalExecutionPlan()->getLockedExecutionNode(wrk3->getWorkerId());
        auto updatedPartition =
            std::dynamic_pointer_cast<Network::NetworkSourceDescriptor>(lockedExecutionNode->operator*()
                                                                            ->getAllDecomposedQueryPlans(sharedQueryId)
                                                                            .front()
                                                                            ->getSourceOperators()
                                                                            .front()
                                                                            ->getSourceDescriptor())
                ->getNesPartition();

        ASSERT_EQ(wrk1->getNodeEngine()->getPartitionManager()->getProducerRegistrationStatus(updatedPartition),
                  Network::PartitionRegistrationStatus::Registered);
        //todo: reactivate
        EXPECT_NE(oldWorker->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
        currentWrk1TargetPartition = updatedPartition;

        //verify that query has been undeployed from old parent
        while (oldWorker->getNodeEngine()->getQueryStatus(sharedQueryId)
               == Runtime::Execution::ExecutableQueryPlanStatus::Running) {
            NES_DEBUG("Query has not yet stopped on worker {}", oldWorker->getWorkerId());
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_EQ(oldWorker->getNodeEngine()->getQueryStatus(sharedQueryId),
                  Runtime::Execution::ExecutableQueryPlanStatus::Finished);

        oldWorker = wrk3;
        EXPECT_EQ(oldWorker->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
        oldSubplanId = oldWorker->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();

        //check that query has left migrating state and is running again
        //todo #5133: reactivate this check once the logic in the query catalog has been updated
        // ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, crd->getQueryCatalog()));
        // auto entries = crd->getQueryCatalog()->getQueryEntriesWithStatus("MIGRATING");
        // ASSERT_TRUE(entries.empty());

        //check that all tuples arrived
        ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringAfter, testFile));

        waitForReconfig = false;
        waitForReconnect = false;
        actualReconnects++;
        waitForFinalCount = true;

        networkSinkWrk3Id += 10;
        networkSrcWrk3Id += 10;
    }

    //use marker to shut down to not cause coordinator side errors
    //create marker
    auto reconfigMarker = ReconfigurationMarker::create();
    auto metadata = std::make_shared<DrainQueryMetadata>(1);
    auto event = ReconfigurationMarkerEvent::create(QueryState::RUNNING, metadata);
    reconfigMarker->addReconfigurationEvent(wrk1->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);
    reconfigMarker->addReconfigurationEvent(oldWorker->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);
    reconfigMarker->addReconfigurationEvent(wrk0->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).back(), event);
    reconfigMarker->addReconfigurationEvent(crd->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);

    //insert reconfig marker at the source
    auto dataSource = wrk1->getNodeEngine()
                          ->getExecutableQueryPlan(wrk1->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front())
                          ->getSources()
                          .front();
    dataSource->handleReconfigurationMarker(reconfigMarker);

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

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    cout << "stopping worker" << endl;
    bool retStopWrk2 = wrk2->stop(false);
    ASSERT_TRUE(retStopWrk2);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/**
 * @brief This tests multiple iterations of inserting VersionDrain events to trigger the reconfiguration of a network sink to point to a new source.
 */
TEST_P(QueryRedeploymentIntegrationTest, testsourceReuseFromCrd) {
    const uint32_t retryTimes = 10;
    const uint64_t addToSrcTwo = 10000;
    const uint64_t addToSrcThree = 20000;
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
        if (currentCount > totalBuffersToProduce) {
            return;
        }//avoid eos
        auto valCount = (currentCount - 1) * (numberOfTuplesToProduce);
        auto* records = buffer.getBuffer<Record>();
        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            records[u].value = valCount + u;
        }
    };
    std::atomic<uint64_t> bufferCount2 = 0;
    auto lambdaSourceFunction2 =
        [&bufferCount2, &waitForReconfig, &waitForFinalCount, &actualReconnects](NES::Runtime::TupleBuffer& buffer,
                                                                                 uint64_t numberOfTuplesToProduce) {
            struct Record {
                uint64_t value;
            };
            auto currentCount = ++bufferCount2;
            if (currentCount > numBuffersToProduceBeforeReconnect + (actualReconnects * buffersToProducePerReconnectCycle)) {
                //after sending the specified amount of tuples, wait until the reconfiguration has been triggered, subsequent tuples will be buffered
                while (!waitForReconfig)
                    ;
            }
            if (currentCount > numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect
                    + numBuffersToProduceWhileBuffering + (actualReconnects * buffersToProducePerReconnectCycle)) {
                while (!waitForFinalCount)
                    ;
            }
            if (currentCount > totalBuffersToProduce) {
                return;
            }//avoid eos
            auto valCount = (currentCount - 1) * (numberOfTuplesToProduce);
            auto* records = buffer.getBuffer<Record>();
            for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
                records[u].value = valCount + u + addToSrcTwo;
            }
        };
    std::atomic<uint64_t> bufferCount3 = 0;
    auto lambdaSourceFunction3 =
        [&bufferCount3, &waitForReconfig, &waitForFinalCount, &actualReconnects](NES::Runtime::TupleBuffer& buffer,
                                                                                 uint64_t numberOfTuplesToProduce) {
            struct Record {
                uint64_t value;
            };
            auto currentCount = ++bufferCount3;
            if (currentCount > numBuffersToProduceBeforeReconnect + (actualReconnects * buffersToProducePerReconnectCycle)) {
                //after sending the specified amount of tuples, wait until the reconfiguration has been triggered, subsequent tuples will be buffered
                while (!waitForReconfig)
                    ;
            }
            if (currentCount > numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect
                    + numBuffersToProduceWhileBuffering + (actualReconnects * buffersToProducePerReconnectCycle)) {
                while (!waitForFinalCount)
                    ;
            }
            if (currentCount > totalBuffersToProduce) {
                return;
            }//avoid eos
            auto valCount = (currentCount - 1) * (numberOfTuplesToProduce);
            auto* records = buffer.getBuffer<Record>();
            for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
                records[u].value = valCount + u + addToSrcThree;
            }
        };
    auto lambdaSourceType = LambdaSourceType::create("seq",
                                                     "test_stream",
                                                     std::move(lambdaSourceFunction),
                                                     // totalBuffersToProduce,
                                                     totalBuffersToProduce + 1000,//avoid eos
                                                     gatheringValue,
                                                     GatheringMode::INTERVAL_MODE);
    auto lambdaSourceType2 = LambdaSourceType::create("seq",
                                                      "test_stream2",
                                                      std::move(lambdaSourceFunction2),
                                                      totalBuffersToProduce + 1000,//avoid eos
                                                      gatheringValue,
                                                      GatheringMode::INTERVAL_MODE);
    auto lambdaSourceType3 = LambdaSourceType::create("seq",
                                                      "test_stream3",
                                                      std::move(lambdaSourceFunction3),
                                                      totalBuffersToProduce + 1000,//avoid eos
                                                      gatheringValue,
                                                      GatheringMode::INTERVAL_MODE);

    NES_INFO("rest port = {}", *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    auto crdWorkerDataPort = getAvailablePort();
    coordinatorConfig->worker.dataPort = *crdWorkerDataPort;
    coordinatorConfig->worker.connectSourceEventChannelsAsync.setValue(true);
    coordinatorConfig->optimizer.enableIncrementalPlacement.setValue(true);

    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    TopologyPtr topology = crd->getTopology();
    auto nodeCount = 1;
    ASSERT_TRUE(waitForNodes(5, nodeCount, topology));
    nodeCount++;

    auto schema = Schema::create()->addField(createField("value", BasicType::UINT64));
    crd->getSourceCatalog()->addLogicalSource("seq", schema);

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    wrkConf1->numWorkerThreads.setValue(GetParam());
    wrkConf1->connectSinksAsync.setValue(true);
    wrkConf1->connectSourceEventChannelsAsync.setValue(true);
    wrkConf1->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
    wrkConf1->numberOfSlots.setValue(1);

    wrkConf1->physicalSourceTypes.add(lambdaSourceType);

    auto wrk1DataPort = getAvailablePort();
    wrkConf1->dataPort = *wrk1DataPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(waitForNodes(5, nodeCount, topology));
    nodeCount++;

    NES_INFO("start worker 4");
    WorkerConfigurationPtr wrkConf4 = WorkerConfiguration::create();
    wrkConf4->coordinatorPort.setValue(*rpcCoordinatorPort);
    wrkConf4->numWorkerThreads.setValue(GetParam());
    wrkConf4->connectSinksAsync.setValue(true);
    wrkConf4->connectSourceEventChannelsAsync.setValue(true);
    wrkConf4->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
    wrkConf4->numberOfSlots.setValue(4);

    wrkConf4->physicalSourceTypes.add(lambdaSourceType2);

    auto wrk4DataPort = getAvailablePort();
    wrkConf4->dataPort = *wrk4DataPort;
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(wrkConf4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart4);

    ASSERT_TRUE(waitForNodes(5, nodeCount, topology));
    nodeCount++;

    NES_INFO("start worker 2");
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort.setValue(*rpcCoordinatorPort);
    auto wrk2DataPort = getAvailablePort();
    wrkConf2->dataPort = *wrk2DataPort;
    wrkConf2->numWorkerThreads.setValue(GetParam());
    wrkConf2->connectSinksAsync.setValue(true);
    wrkConf2->connectSourceEventChannelsAsync.setValue(true);
    wrkConf2->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
    wrkConf2->numberOfSlots.setValue(1);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart2);
    ASSERT_TRUE(waitForNodes(5, nodeCount, topology));
    nodeCount++;

    wrk1->replaceParent(crd->getNesWorker()->getWorkerId(), wrk2->getWorkerId());

    //start query
    QueryId queryId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(
        R"(Query::from("seq").map(Attribute("value") = Attribute("value")).map(Attribute("value") = Attribute("value")).sink(FileSinkDescriptor::create(")"
            + testFile + R"(", "CSV_FORMAT", "APPEND"));)",
        Optimizer::PlacementStrategy::BottomUp);
    auto networkSinkWrk3Id = 31;
    auto networkSrcWrk3Id = 32;

    auto sinkLocationWrk1 = NES::Network::NodeLocation(wrk1->getWorkerId(), "localhost", *wrk1DataPort);
    auto networkSourceCrdLocation =
        NES::Network::NodeLocation(crd->getNesWorker()->getWorkerId(), "localhost", *crdWorkerDataPort);

    std::vector<NesWorkerPtr> reconnectParents;

    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, crd->getQueryCatalog()));
    SharedQueryId sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    ASSERT_NE(sharedQueryId, INVALID_SHARED_QUERY_ID);
    //reconfiguration
    auto subPlanIdWrk3 = 300;
    ASSERT_EQ(wrk2->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).size(), 1);
    auto oldSubplanId = wrk2->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();
    auto wrk2Source = wrk2->getNodeEngine()->getExecutableQueryPlan(oldSubplanId)->getSources().front();
    Network::NesPartition currentWrk1TargetPartition(sharedQueryId,
                                                     wrk2Source->getOperatorId(),
                                                     PartitionId(0),
                                                     SubpartitionId(0));

    ASSERT_EQ(crd->getNesWorker()->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).size(), 1);

    auto oldWorker = wrk2;
    while (actualReconnects < numberOfReconnectsToPerform) {
        ASSERT_EQ(wrk1->getNodeEngine()->getPartitionManager()->getProducerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
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
            oss << std::to_string(i + addToSrcTwo) << std::endl;
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
        wrkConf3->numberOfSlots.setValue(1);
        NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
        bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
        ASSERT_TRUE(retStart3);
        ASSERT_TRUE(waitForNodes(5, nodeCount, topology));
        nodeCount++;
        reconnectParents.push_back(wrk3);

        std::string compareStringAfter;
        std::ostringstream ossAfter;
        ossAfter << "seq$value:INTEGER(64 bits)" << std::endl;
        for (uint64_t i = 0;
             i < (numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect + numBuffersToProduceWhileBuffering)
                 * tuplesPerBuffer * (actualReconnects + 1);
             ++i) {
            ossAfter << std::to_string(i) << std::endl;
            ossAfter << std::to_string(i + addToSrcTwo) << std::endl;
        }
        compareStringAfter = ossAfter.str();

        int noOfMigratingPlans = 0;
        int noOfCompletedMigrations = 0;
        int noOfRunningPlans = 0;

        auto addLinkEvent = std::make_shared<RequestProcessor::ISQPAddLinkEvent>(wrk3->getWorkerId(), wrk1->getWorkerId());
        auto removeLinkEvent =
            std::make_shared<RequestProcessor::ISQPRemoveLinkEvent>(oldWorker->getWorkerId(), wrk1->getWorkerId());
        crd->getRequestHandlerService()->queueISQPRequest({addLinkEvent, removeLinkEvent});

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

        //coordinator side checks
        auto lockedExecutionNode = crd->getGlobalExecutionPlan()->getLockedExecutionNode(wrk3->getWorkerId());
        auto updatedPartition =
            std::dynamic_pointer_cast<Network::NetworkSourceDescriptor>(lockedExecutionNode->operator*()
                                                                            ->getAllDecomposedQueryPlans(sharedQueryId)
                                                                            .front()
                                                                            ->getSourceOperators()
                                                                            .front()
                                                                            ->getSourceDescriptor())
                ->getNesPartition();

        ASSERT_EQ(wrk1->getNodeEngine()->getPartitionManager()->getProducerRegistrationStatus(updatedPartition),
                  Network::PartitionRegistrationStatus::Registered);
        EXPECT_NE(oldWorker->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
        currentWrk1TargetPartition = updatedPartition;

        //verify that query has been undeployed from old parent
        while (oldWorker->getNodeEngine()->getQueryStatus(sharedQueryId)
               == Runtime::Execution::ExecutableQueryPlanStatus::Running) {
            NES_DEBUG("Query has not yet stopped on worker {}", oldWorker->getWorkerId());
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_EQ(oldWorker->getNodeEngine()->getQueryStatus(sharedQueryId),
                  Runtime::Execution::ExecutableQueryPlanStatus::Finished);

        auto runningPlans =
            crd->getNodeEngine()
                ->getDecomposedQueryIdsWithStatus(sharedQueryId, Runtime::Execution::ExecutableQueryPlanStatus::Running)
                .size();
        while (runningPlans != 1) {
            NES_DEBUG("Old decomposed query plan has not yet stopped on coordinator ({} plans deployed, {} running)",
                      crd->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).size(),
                      runningPlans)
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
            if (crd->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).empty()) {
                NES_DEBUG("Could not find executable query plan for shared query on coordinator");
                FAIL();
            }
            runningPlans =
                crd->getNodeEngine()
                    ->getDecomposedQueryIdsWithStatus(sharedQueryId, Runtime::Execution::ExecutableQueryPlanStatus::Running)
                    .size();
        }
        ASSERT_EQ(crd->getNodeEngine()
                      ->getDecomposedQueryIdsWithStatus(sharedQueryId, Runtime::Execution::ExecutableQueryPlanStatus::Running)
                      .size(),
                  1);

        oldWorker = wrk3;
        EXPECT_EQ(oldWorker->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
        oldSubplanId = oldWorker->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();

        //check that query has left migrating state and is running again
        //todo #5133: reactivate this check once the logic in the query catalog has been updated
        // ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, crd->getQueryCatalog()));
        // auto entries = crd->getQueryCatalog()->getQueryEntriesWithStatus("MIGRATING");
        // ASSERT_TRUE(entries.empty());

        //check that all tuples arrived
        ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringAfter, testFile));

        waitForReconfig = false;
        waitForReconnect = false;
        actualReconnects++;
        waitForFinalCount = true;

        networkSinkWrk3Id += 10;
        networkSrcWrk3Id += 10;
    }

    //use marker to shut down to not cause coordinator side errors
    //create marker
    auto reconfigMarker = ReconfigurationMarker::create();
    auto metadata = std::make_shared<DrainQueryMetadata>(1);
    auto event = ReconfigurationMarkerEvent::create(QueryState::RUNNING, metadata);
    reconfigMarker->addReconfigurationEvent(wrk1->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);
    for (auto plan : wrk4->getNodeEngine()->getDecomposedQueryIds(sharedQueryId)) {
        reconfigMarker->addReconfigurationEvent(plan, event);
    }
    reconfigMarker->addReconfigurationEvent(oldWorker->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);
    reconfigMarker->addReconfigurationEvent(crd->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);

    //insert reconfig marker at the source
    auto dataSource = wrk1->getNodeEngine()
                          ->getExecutableQueryPlan(wrk1->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front())
                          ->getSources()
                          .front();
    dataSource->handleReconfigurationMarker(reconfigMarker);
    for (auto p : wrk4->getNodeEngine()->getDecomposedQueryIds(sharedQueryId)) {
        wrk4->getNodeEngine()
            ->getExecutableQueryPlan(p)->getSources().front()->handleReconfigurationMarker(reconfigMarker);
    }

    //send the last tuples, after which the lambda source shuts down
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk1));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk4));

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

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    cout << "stopping worker" << endl;
    bool retStopWrk2 = wrk2->stop(false);
    ASSERT_TRUE(retStopWrk2);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

INSTANTIATE_TEST_CASE_P(QueryRedeploymentIntegrationTestParam, QueryRedeploymentIntegrationTest, ::testing::Values(1, 4));
}// namespace NES

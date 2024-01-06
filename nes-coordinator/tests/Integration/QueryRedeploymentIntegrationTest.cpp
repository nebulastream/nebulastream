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
#include <Network/NetworkSink.hpp>
#include <Network/PartitionManager.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LambdaSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Services/QueryService.hpp>
#include <Util/TestUtils.hpp>
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

    QueryId queryId = crd->getQueryService()->validateAndQueueAddQueryRequest(
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
 * @brief This tests inserting VersionDrain events to trigger the reconfiguration of a network sink to point to a new source.
 */
TEST_P(QueryRedeploymentIntegrationTest, testSinkReconnect) {
    const uint64_t numBuffersToProduceBeforeReconnect = 40;
    const uint64_t numBuffersToProduceWhileBuffering = 20;
    const uint64_t numBuffersToProduceAfterReconnect = 40;
    //these buffers are sent afte the final count, they make sure that no eos comes before the reconnect succeeded
    const uint64_t numBuffersToProduceAfterFinalCount = 20;
    const uint64_t totalBuffersToProduce = numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect
        + numBuffersToProduceWhileBuffering + numBuffersToProduceAfterFinalCount;
    const uint64_t gatheringValue = 10;
    const std::chrono::seconds waitTime(10);
    uint64_t tuplesPerBuffer = 10;
    uint8_t bytesPerTuple = sizeof(uint64_t);
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";
    remove(testFile.c_str());

    std::string compareStringBefore;
    std::ostringstream oss;
    oss << "value:INTEGER(64 bits)" << std::endl;
    for (uint64_t i = 0; i < numBuffersToProduceBeforeReconnect * tuplesPerBuffer; ++i) {
        oss << std::to_string(i) << std::endl;
    }
    compareStringBefore = oss.str();

    std::string compareStringAfter;
    std::ostringstream ossAfter;
    ossAfter << "value:INTEGER(64 bits)" << std::endl;
    for (uint64_t i = 0;
         i < (numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect + numBuffersToProduceWhileBuffering)
             * tuplesPerBuffer;
         ++i) {
        ossAfter << std::to_string(i) << std::endl;
    }
    compareStringAfter = ossAfter.str();

    std::atomic<uint64_t> bufferCount = 0;
    std::atomic<bool> waitForReconfig = false;
    std::atomic<bool> waitForReconnect = false;
    std::atomic<bool> waitForFinalCount = false;
    auto lambdaSourceFunction =
        [&bufferCount, &waitForReconfig, &waitForReconnect, &waitForFinalCount](NES::Runtime::TupleBuffer& buffer,
                                                                                uint64_t numberOfTuplesToProduce) {
            struct Record {
                uint64_t value;
            };
            auto currentCount = ++bufferCount;
            if (currentCount > numBuffersToProduceBeforeReconnect) {
                //after sending the specified amount of tuples, wait until the reconfiguration has been triggered, subsequent tuples will be buffered
                while (!waitForReconfig)
                    ;
            }
            if (currentCount > numBuffersToProduceBeforeReconnect + numBuffersToProduceWhileBuffering) {
                //after writing some tuples into the buffer, give signal to start the new operators to finish the reconnect, tuples will be unbuffered to new destination
                waitForReconnect = true;
            }
            if (currentCount
                > numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect + numBuffersToProduceWhileBuffering) {
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
    auto crdWorkerDataPort = getAvailablePort();
    coordinatorConfig->worker.dataPort = *crdWorkerDataPort;

    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    auto schema = Schema::create()->addField(createField("value", BasicType::UINT64));
    crd->getSourceCatalog()->addLogicalSource("seq", schema);
    QueryId sharedQueryId = 800;
    QueryId queryId = 900;
    auto crdQueryPlan = QueryPlan::create();
    crdQueryPlan->setQueryId(queryId);
    crd->getQueryCatalogService()->createNewEntry("test_query", crdQueryPlan, Optimizer::PlacementStrategy::BottomUp);
    crd->getQueryCatalogService()->mapSharedQueryPlanId(sharedQueryId, queryId);

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    wrkConf1->numWorkerThreads.setValue(GetParam());
    wrkConf1->connectSinksAsync.setValue(true);
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
    wrkConf2->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart2);
    ASSERT_TRUE(waitForNodes(5, 3, topology));

    NES_INFO("start worker 3");
    WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
    wrkConf3->coordinatorPort.setValue(*rpcCoordinatorPort);
    auto wrk3DataPort = getAvailablePort();
    wrkConf3->dataPort = *wrk3DataPort;
    wrkConf3->numWorkerThreads.setValue(GetParam());
    wrkConf3->connectSinksAsync.setValue(true);
    wrkConf3->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart3);
    ASSERT_TRUE(waitForNodes(5, 4, topology));

    NES_INFO("Shared Query ID: {}", sharedQueryId);
    ASSERT_NE(sharedQueryId, INVALID_QUERY_ID);

    //start query

    //set operator ids
    auto fileSinkCrdId = 91;
    auto networkSrcCrdId = 92;
    auto networkSinkWrk1Id = 11;
    auto lambdaSourceId = 12;
    auto networkSinkWrk2Id = 21;
    auto networkSrcWrk2Id = 22;
    auto networkSinkWrk3Id = 31;
    auto networkSrcWrk3Id = 32;

    //start query on wrk1
    auto subPlanIdWrk1 = 10;
    auto queryPlan1 = QueryPlan::create(sharedQueryId, subPlanIdWrk1);
    //create lambda source
    constexpr auto sourceAffinity = 0;
    constexpr auto taskQueueId = 0;
    auto lambdaSourceDescriptor = LambdaSourceDescriptor::create(schema,
                                                                 lambdaSourceFunction,
                                                                 totalBuffersToProduce,
                                                                 gatheringValue,
                                                                 GatheringMode::INTERVAL_MODE,
                                                                 sourceAffinity,
                                                                 taskQueueId,
                                                                 "seq",
                                                                 "test_stream");
    auto lambdaSourceOperatorNode = std::make_shared<SourceLogicalOperatorNode>(lambdaSourceDescriptor, lambdaSourceId);
    queryPlan1->addRootOperator(lambdaSourceOperatorNode);
    //create network sink
    auto networkSourceWrk2Location = NES::Network::NodeLocation(wrk2->getWorkerId(), "localhost", *wrk2DataPort);
    auto networkSourceWrk2Partition = NES::Network::NesPartition(sharedQueryId, networkSrcWrk2Id, 0, 0);
    Version firstVersion = 0;
    auto networkSinkDescriptor1 = Network::NetworkSinkDescriptor::create(networkSourceWrk2Location,
                                                                         networkSourceWrk2Partition,
                                                                         waitTime,
                                                                         DATA_CHANNEL_RETRY_TIMES,
                                                                         firstVersion,
                                                                         DEFAULT_NUMBER_OF_ORIGINS,
                                                                         networkSinkWrk1Id);
    auto networkSinkOperatorNode1 = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor1, networkSinkWrk1Id);
    queryPlan1->appendOperatorAsNewRoot(networkSinkOperatorNode1);
    queryPlan1->getSinkOperators().front()->inferSchema();
    //register and start query on worker 1
    auto success_register_wrk1 = wrk1->getNodeEngine()->registerQueryInNodeEngine(queryPlan1);
    ASSERT_TRUE(success_register_wrk1);
    auto success_start_wrk1 = wrk1->getNodeEngine()->startQuery(sharedQueryId);
    ASSERT_TRUE(success_start_wrk1);

    //start query on wrk2
    auto subPlanIdWrk2 = 20;
    auto queryPlan2 = QueryPlan::create(sharedQueryId, subPlanIdWrk2);
    //create network source getting data from sink at wrk1
    auto sinkLocationWrk1 = NES::Network::NodeLocation(wrk1->getWorkerId(), "localhost", *wrk1DataPort);
    auto networkSourceDescriptorWrk2 = Network::NetworkSourceDescriptor::create(schema,
                                                                                networkSourceWrk2Partition,
                                                                                sinkLocationWrk1,
                                                                                WAIT_TIME,
                                                                                EVENT_CHANNEL_RETRY_TIMES,
                                                                                firstVersion);
    auto sourceOperatorNodeWrk2 = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptorWrk2, networkSrcWrk2Id);
    queryPlan2->addRootOperator(sourceOperatorNodeWrk2);
    //create network sink connected to coordinator
    auto networkSourceCrdLocation =
        NES::Network::NodeLocation(crd->getNesWorker()->getWorkerId(), "localhost", *crdWorkerDataPort);
    auto networkSourceCrdPartition = NES::Network::NesPartition(sharedQueryId, networkSrcCrdId, 0, 0);
    auto networkSinkDescriptorWrk2 = Network::NetworkSinkDescriptor::create(networkSourceCrdLocation,
                                                                            networkSourceCrdPartition,
                                                                            waitTime,
                                                                            DATA_CHANNEL_RETRY_TIMES,
                                                                            firstVersion,
                                                                            DEFAULT_NUMBER_OF_ORIGINS,
                                                                            networkSinkWrk2Id);
    auto networkSinkOperatorNodeWrk2 = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptorWrk2, networkSinkWrk2Id);
    queryPlan2->appendOperatorAsNewRoot(networkSinkOperatorNodeWrk2);
    queryPlan2->getSinkOperators().front()->inferSchema();
    //register and start query on worker 2
    auto success_register_wrk2 = wrk2->getNodeEngine()->registerQueryInNodeEngine(queryPlan2);
    ASSERT_TRUE(success_register_wrk2);
    auto success_start_wrk2 = wrk2->getNodeEngine()->startQuery(sharedQueryId);
    ASSERT_TRUE(success_start_wrk2);

    //deploy to coordinator
    auto subPlanId = 90;
    auto queryPlan = QueryPlan::create(sharedQueryId, subPlanId);
    //create network source operator at coordinator
    auto sinkLocation = NES::Network::NodeLocation(wrk2->getWorkerId(), "localhost", *wrk2DataPort);
    auto networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                            networkSourceCrdPartition,
                                                                            sinkLocation,
                                                                            WAIT_TIME,
                                                                            EVENT_CHANNEL_RETRY_TIMES,
                                                                            firstVersion);
    auto sourceOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSrcCrdId);
    queryPlan->addRootOperator(sourceOperatorNode);
    //create file sink at coordinator
    auto fileSinkDescriptor = FileSinkDescriptor::create(testFile, "CSV_FORMAT", "APPEND");
    auto sinkOperatorNode = std::make_shared<SinkLogicalOperatorNode>(fileSinkDescriptor, fileSinkCrdId);
    queryPlan->appendOperatorAsNewRoot(sinkOperatorNode);
    queryPlan->getSinkOperators().front()->inferSchema();
    //deploy and start query at coordinator
    auto success_register = crd->getNesWorker()->getNodeEngine()->registerQueryInNodeEngine(queryPlan);
    ASSERT_TRUE(success_register);
    auto success_start = crd->getNesWorker()->getNodeEngine()->startQuery(sharedQueryId);
    ASSERT_TRUE(success_start);

    //wait for data to be written
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringBefore, testFile));

    //reconfiguration
    crd->getQueryCatalogService()->checkAndMarkForMigration(sharedQueryId, subPlanIdWrk2, QueryState::MIGRATING);

    Version nextVersion = 1;
    //reconfigure network sink on wrk1 to point to wrk3 instead of to wrk2
    auto subQueryIds = wrk1->getNodeEngine()->getSubQueryIds(sharedQueryId);
    EXPECT_EQ(subQueryIds.size(), 1);
    //retrieve data about running network sink at wrk1
    auto networkSink = std::dynamic_pointer_cast<Network::NetworkSink>(
        wrk1->getNodeEngine()->getExecutableQueryPlan(subQueryIds.front())->getSinks().front());
    Network::NodeLocation newNodeLocation(crd->getNesWorker()->getWorkerId(), "localhost", *wrk3DataPort);
    auto networkSourceWrk3Partition = NES::Network::NesPartition(sharedQueryId, networkSrcWrk3Id, 0, 0);

    //add a pending version to the subplan containing the file sink
    auto reconfiguredSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                                 networkSourceCrdPartition,
                                                                                 newNodeLocation,
                                                                                 WAIT_TIME,
                                                                                 EVENT_CHANNEL_RETRY_TIMES,
                                                                                 nextVersion);

    crd->getNesWorker()->getNodeEngine()->getPartitionManager()->addNextVersion(
        *reconfiguredSourceDescriptor->as<Network::NetworkSourceDescriptor>());
    auto uniqueId = 0;// will be ignored
    auto reconfiguredSinkDescriptor = Network::NetworkSinkDescriptor::create(newNodeLocation,
                                                                             networkSourceWrk3Partition,
                                                                             waitTime,
                                                                             EVENT_CHANNEL_RETRY_TIMES,
                                                                             nextVersion,
                                                                             DEFAULT_NUMBER_OF_ORIGINS,
                                                                             uniqueId);
    networkSink->configureNewSinkDescriptor(*reconfiguredSinkDescriptor->as<Network::NetworkSinkDescriptor>());

    //reconfig performed but new network source not started yet. tuples are buffered at wrk1

    //notify lambda source that reconfig happened and make it release more tuples into the buffer
    waitForReconfig = true;
    //wait for tuples in order to make sure that the buffer is actually tested
    while (!waitForReconnect)
        ;

    //start operator at new destination, buffered tuples will be unbuffered to node 3 once the operators there become active
    //start query on wrk3
    auto subPlanIdWrk3 = 30;
    auto queryPlan3 = QueryPlan::create(sharedQueryId, subPlanIdWrk3);
    //create network source getting data from sink at wrk1
    auto networkSourceDescriptorWrk3 = Network::NetworkSourceDescriptor::create(schema,
                                                                                networkSourceWrk3Partition,
                                                                                sinkLocationWrk1,
                                                                                WAIT_TIME,
                                                                                EVENT_CHANNEL_RETRY_TIMES,
                                                                                nextVersion);
    auto sourceOperatorNodeWrk3 = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptorWrk3, networkSrcWrk3Id);
    queryPlan3->addRootOperator(sourceOperatorNodeWrk3);
    //create network sink connected to coordinator
    auto networkSinkDescriptorWrk3 = Network::NetworkSinkDescriptor::create(networkSourceCrdLocation,
                                                                            networkSourceCrdPartition,
                                                                            waitTime,
                                                                            DATA_CHANNEL_RETRY_TIMES,
                                                                            nextVersion,
                                                                            DEFAULT_NUMBER_OF_ORIGINS,
                                                                            networkSinkWrk3Id);
    auto networkSinkOperatorNodeWrk3 = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptorWrk3, networkSinkWrk3Id);
    queryPlan3->appendOperatorAsNewRoot(networkSinkOperatorNodeWrk3);
    queryPlan3->getSinkOperators().front()->inferSchema();
    //register and start query on worker 3
    auto success_register_wrk3 = wrk3->getNodeEngine()->registerQueryInNodeEngine(queryPlan3);
    ASSERT_TRUE(success_register_wrk3);
    auto success_start_wrk3 = wrk3->getNodeEngine()->startQuery(sharedQueryId);
    ASSERT_TRUE(success_start_wrk3);

    //check that all tuples arrived
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringAfter, testFile));
    waitForFinalCount = true;

    //send the last tuples, after which the lambda source shuts down
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk1));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk3));
    //worker 2 did not receive any soft stop, because the node reconnected
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk2));
    //worker at coordinator expects double number of threads to send soft stop because source reconfig is not implemented yet
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, crd->getNesWorker()));

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

    auto stopSuccess3 = wrk3->getNodeEngine()->stopQuery(sharedQueryId);
    cout << "stopping worker" << endl;
    bool retStopWrk3 = wrk3->stop(false);
    ASSERT_TRUE(retStopWrk3);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/**
 * @brief This tests multiple iterations of inserting VersionDrain events to trigger the reconfiguration of a network sink to point to a new source.
 */
// TODO enabled/refactored in #3083
TEST_P(QueryRedeploymentIntegrationTest, DISABLED_testMultiplePlannedReconnects) {
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
    auto crdWorkerDataPort = getAvailablePort();
    coordinatorConfig->worker.dataPort = *crdWorkerDataPort;

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
    wrkConf2->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart2);
    ASSERT_TRUE(waitForNodes(5, 3, topology));

    wrk1->replaceParent(crd->getNesWorker()->getWorkerId(), wrk2->getWorkerId());

    //start query
    QueryId queryId = crd->getQueryService()->validateAndQueueAddQueryRequest(
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
    ASSERT_EQ(wrk2->getNodeEngine()->getSubQueryIds(queryId).size(), 1);
    auto oldSubplanId = wrk2->getNodeEngine()->getSubQueryIds(queryId).front();
    auto wrk2Source = wrk2->getNodeEngine()->getExecutableQueryPlan(oldSubplanId)->getSources().front();
    Network::NesPartition currentWrk1TargetPartition(queryId, wrk2Source->getOperatorId(), 0, 0);

    ASSERT_EQ(crd->getNesWorker()->getNodeEngine()->getSubQueryIds(queryId).size(), 1);
    auto coordinatorSubplanId = crd->getNesWorker()->getNodeEngine()->getSubQueryIds(queryId).front();
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

        //set the old plan to migrating
        crd->getQueryCatalogService()->checkAndMarkForMigration(sharedQueryId, oldSubplanId, QueryState::MIGRATING);
        //todo: make sure the state on the query plan is set in the right phase
        crd->getGlobalExecutionPlan()
            ->getExecutionNodeById(oldWorker->getWorkerId())
            ->getQuerySubPlans(sharedQueryId)
            .front()
            ->setQueryState(QueryState::MARKED_FOR_MIGRATION);
        //check that the data for the migrating plan has been set correctly
        auto migratingEntries = crd->getQueryCatalogService()->getAllEntriesInStatus("MIGRATING");
        ASSERT_EQ(migratingEntries.size(), 1);
        auto subplans = crd->getQueryCatalogService()->getEntryForQuery(queryId)->getAllSubQueryPlanMetaData();
        ASSERT_FALSE(subplans.empty());
        int noOfMigratingPlans = 0;
        int noOfCompletedMigrations = 0;
        int noOfRunningPlans = 0;
        for (auto& plan : subplans) {
            switch (plan->getSubQueryStatus()) {
                case QueryState::MARKED_FOR_MIGRATION: {
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
        ASSERT_EQ(noOfMigratingPlans, 1);
        ASSERT_EQ(noOfCompletedMigrations, actualReconnects);
        ASSERT_EQ(noOfRunningPlans, 2);
        ASSERT_EQ(crd->getGlobalExecutionPlan()->getExecutionNodesByQueryId(sharedQueryId).size(), 3);

        //reconfigure network sink on wrk1 to point to wrk3 instead of to wrk2
        auto subQueryIds = wrk1->getNodeEngine()->getSubQueryIds(sharedQueryId);
        EXPECT_EQ(subQueryIds.size(), 1);

        //retrieve data about running network sink at wrk1
        Network::NodeLocation newNodeLocation(wrk3->getWorkerId(), "localhost", *wrk3DataPort);
        networkSrcWrk3Id += 10;
        networkSinkWrk3Id += 10;
        auto networkSourceWrk3Partition = NES::Network::NesPartition(sharedQueryId, networkSrcWrk3Id, 0, 0);
        Version nextVersion = actualReconnects + 1;

        //start operator at new destination, buffered tuples will be unbuffered to node 3 once the operators there become active
        //create query for worker 3
        auto queryPlan3 = QueryPlan::create(sharedQueryId, subPlanIdWrk3);
        //create network source getting data from sink at wrk1
        auto networkSourceDescriptorWrk3 = Network::NetworkSourceDescriptor::create(schema,
                                                                                    networkSourceWrk3Partition,
                                                                                    sinkLocationWrk1,
                                                                                    WAIT_TIME,
                                                                                    EVENT_CHANNEL_RETRY_TIMES,
                                                                                    nextVersion);
        auto sourceOperatorNodeWrk3 = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptorWrk3, networkSrcWrk3Id);
        queryPlan3->addRootOperator(sourceOperatorNodeWrk3);
        //create network sink connected to coordinator
        auto networkSinkDescriptorWrk3 = Network::NetworkSinkDescriptor::create(networkSourceCrdLocation,
                                                                                networkSourceCrdPartition,
                                                                                waitTime,
                                                                                DATA_CHANNEL_RETRY_TIMES,
                                                                                nextVersion,
                                                                                DEFAULT_NUMBER_OF_ORIGINS,
                                                                                networkSinkWrk3Id);
        auto networkSinkOperatorNodeWrk3 =
            std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptorWrk3, networkSinkWrk3Id);
        queryPlan3->appendOperatorAsNewRoot(networkSinkOperatorNodeWrk3);
        queryPlan3->getSinkOperators().front()->inferSchema();
        queryPlan3->setQueryState(QueryState::MARKED_FOR_DEPLOYMENT);

        //deploy new query
        auto topologyNode3 = topology->getCopyOfTopologyNodeWithId(wrk3->getWorkerId());
        auto executionNode3 = ExecutionNode::createExecutionNode(topologyNode3);
        executionNode3->addNewQuerySubPlan(sharedQueryId, queryPlan3);
        executionNode3->getTopologyNode()->occupySlots(1);
        crd->getGlobalExecutionPlan()->addExecutionNode(executionNode3);
        ASSERT_EQ(crd->getGlobalExecutionPlan()->getExecutionNodesByQueryId(sharedQueryId).size(), 4);
        auto queryDeploymentPhase =
            QueryDeploymentPhase::create(crd->getGlobalExecutionPlan(), crd->getQueryCatalogService(), coordinatorConfig);
        auto sqp = crd->getGlobalQueryPlan()->getSharedQueryPlan(sharedQueryId);
        sqp->setStatus(SharedQueryPlanStatus::MIGRATING);

        auto globalExecutionPlan = crd->getGlobalExecutionPlan();

        //source reconfig
        auto reconfiguredSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                                     networkSourceCrdPartition,
                                                                                     newNodeLocation,
                                                                                     waitTime,
                                                                                     EVENT_CHANNEL_RETRY_TIMES,
                                                                                     nextVersion);
        auto crdExecutionNode = globalExecutionPlan->getExecutionNodeById(crd->getNesWorker()->getWorkerId());
        auto subplanAtCrd = crdExecutionNode->getQuerySubPlans(sharedQueryId).front();
        subplanAtCrd->getSourceOperators().front()->setSourceDescriptor(reconfiguredSourceDescriptor);
        subplanAtCrd->setQueryState(QueryState::MARKED_FOR_REDEPLOYMENT);
        crd->getQueryCatalogService()
            ->getEntryForQuery(queryId)
            ->getQuerySubPlanMetaData(crd->getNesWorker()->getNodeEngine()->getSubQueryIds(sharedQueryId).front())
            ->updateStatus(QueryState::MARKED_FOR_REDEPLOYMENT);

        //sink reconfig
        auto wrk1ExecutionNode = globalExecutionPlan->getExecutionNodeById(wrk1->getWorkerId());
        auto subplanAtWrk1 = wrk1ExecutionNode->getQuerySubPlans(sharedQueryId).front();
        subplanAtWrk1->setQueryState(QueryState::MARKED_FOR_REDEPLOYMENT);
        auto existingSink = subplanAtWrk1->getSinkOperators().front();
        auto reconfiguredSinkDescriptor = Network::NetworkSinkDescriptor::create(
            newNodeLocation,
            networkSourceWrk3Partition,
            waitTime,
            DATA_CHANNEL_RETRY_TIMES,
            nextVersion,
            DEFAULT_NUMBER_OF_ORIGINS,
            existingSink->getId());//on the coordinator side the operator id equals the unique identifier
        existingSink->setSinkDescriptor(reconfiguredSinkDescriptor);
        crd->getQueryCatalogService()
            ->getEntryForQuery(queryId)
            ->getQuerySubPlanMetaData(wrk1->getNodeEngine()->getSubQueryIds(sharedQueryId).front())
            ->updateStatus(QueryState::MARKED_FOR_REDEPLOYMENT);

        queryDeploymentPhase->execute(sqp);

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
            NES_DEBUG("Partition {} has not yet been unregistered", currentWrk1TargetPartition);
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_NE(wrk1->getNodeEngine()->getPartitionManager()->getProducerRegistrationStatus(currentWrk1TargetPartition),
                  Network::PartitionRegistrationStatus::Registered);
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
        oldSubplanId = oldWorker->getNodeEngine()->getSubQueryIds(queryId).front();

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
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk2));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, crd->getNesWorker()));

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

/**
 * @brief This test the reconfiguration of a network sink that is already buffering
 */
TEST_P(QueryRedeploymentIntegrationTest, testEndOfStreamWhileBuffering) {
    const uint64_t numBuffersToProduceBeforeReconnect = 40;
    const uint64_t numBuffersToProduceWhileBuffering = 20;
    const uint64_t totalBuffersToProduce = numBuffersToProduceBeforeReconnect + numBuffersToProduceWhileBuffering;
    const uint64_t gatheringValue = 10;
    const std::chrono::milliseconds waitTime(1000);
    uint64_t tuplesPerBuffer = 10;
    uint8_t bytesPerTuple = sizeof(uint64_t);
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";
    remove(testFile.c_str());

    std::string compareStringBefore;
    std::ostringstream oss;
    oss << "value:INTEGER(64 bits)" << std::endl;
    for (uint64_t i = 0; i < numBuffersToProduceBeforeReconnect * tuplesPerBuffer; ++i) {
        oss << std::to_string(i) << std::endl;
    }
    compareStringBefore = oss.str();

    std::string compareStringAfter;
    std::ostringstream ossAfter;
    ossAfter << "value:INTEGER(64 bits)" << std::endl;
    for (uint64_t i = 0; i < (numBuffersToProduceBeforeReconnect + numBuffersToProduceWhileBuffering) * tuplesPerBuffer; ++i) {
        ossAfter << std::to_string(i) << std::endl;
    }
    compareStringAfter = ossAfter.str();

    std::atomic<uint64_t> bufferCount = 0;
    std::atomic<bool> waitForReconfig = false;
    std::atomic<bool> waitForReconnect = false;
    auto lambdaSourceFunction = [&bufferCount, &waitForReconfig, &waitForReconnect](NES::Runtime::TupleBuffer& buffer,
                                                                                    uint64_t numberOfTuplesToProduce) {
        struct Record {
            uint64_t value;
        };
        auto currentCount = ++bufferCount;
        if (currentCount > numBuffersToProduceBeforeReconnect) {
            //after sending the specified amount of tuples, wait until the reconfiguration has been triggered, subsequent tuples will be buffered
            while (!waitForReconfig)
                ;
        }
        if (currentCount >= numBuffersToProduceBeforeReconnect + numBuffersToProduceWhileBuffering) {
            //after writing all remaining tuples into the buffer, give signal to start the new operators to finish the reconnect, tuples will be unbuffered to new destination
            waitForReconnect = true;
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
                                                     numBuffersToProduceBeforeReconnect,
                                                     gatheringValue,
                                                     GatheringMode::INTERVAL_MODE);

    NES_INFO("rest port = {}", *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    auto crdWorkerDataPort = getAvailablePort();
    coordinatorConfig->worker.dataPort = *crdWorkerDataPort;

    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    auto schema = Schema::create()->addField(createField("value", BasicType::UINT64));
    crd->getSourceCatalog()->addLogicalSource("seq", schema);
    QueryId sharedQueryId = 800;
    QueryId queryId = 900;
    auto crdQueryPlan = QueryPlan::create();
    crdQueryPlan->setQueryId(queryId);
    crd->getQueryCatalogService()->createNewEntry("test_query", crdQueryPlan, Optimizer::PlacementStrategy::BottomUp);
    crd->getQueryCatalogService()->mapSharedQueryPlanId(sharedQueryId, queryId);

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    wrkConf1->numWorkerThreads.setValue(GetParam());
    wrkConf1->connectSinksAsync.setValue(true);
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
    wrkConf2->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart2);
    ASSERT_TRUE(waitForNodes(5, 3, topology));

    NES_INFO("start worker 3");
    WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
    wrkConf3->coordinatorPort.setValue(*rpcCoordinatorPort);
    auto wrk3DataPort = getAvailablePort();
    wrkConf3->dataPort = *wrk3DataPort;
    wrkConf3->numWorkerThreads.setValue(GetParam());
    wrkConf3->connectSinksAsync.setValue(true);
    wrkConf3->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart3);
    ASSERT_TRUE(waitForNodes(5, 4, topology));

    NES_INFO("Shared Query ID: {}", sharedQueryId);
    ASSERT_NE(sharedQueryId, INVALID_QUERY_ID);

    //start query

    //set operator ids
    auto fileSinkCrdId = 91;
    auto networkSrcCrdId = 92;
    auto networkSinkWrk1Id = 11;
    auto lambdaSourceId = 12;
    auto networkSinkWrk2Id = 21;
    auto networkSrcWrk2Id = 22;
    auto networkSinkWrk3Id = 31;
    auto networkSrcWrk3Id = 32;

    //start query on wrk1
    auto subPlanIdWrk1 = 10;
    auto queryPlan1 = QueryPlan::create(sharedQueryId, subPlanIdWrk1);
    //create lambda source
    constexpr auto sourceAffinity = 0;
    constexpr auto taskQueueId = 0;
    auto lambdaSourceDescriptor = LambdaSourceDescriptor::create(schema,
                                                                 lambdaSourceFunction,
                                                                 totalBuffersToProduce,
                                                                 gatheringValue,
                                                                 GatheringMode::INTERVAL_MODE,
                                                                 sourceAffinity,
                                                                 taskQueueId,
                                                                 "seq",
                                                                 "test_stream");
    auto lambdaSourceOperatorNode = std::make_shared<SourceLogicalOperatorNode>(lambdaSourceDescriptor, lambdaSourceId);
    queryPlan1->addRootOperator(lambdaSourceOperatorNode);
    //create network sink
    auto networkSourceWrk2Location = NES::Network::NodeLocation(wrk2->getWorkerId(), "localhost", *wrk2DataPort);
    auto networkSourceWrk2Partition = NES::Network::NesPartition(sharedQueryId, networkSrcWrk2Id, 0, 0);
    Version firstVersion = 0;
    auto networkSinkDescriptor1 = Network::NetworkSinkDescriptor::create(networkSourceWrk2Location,
                                                                         networkSourceWrk2Partition,
                                                                         waitTime,
                                                                         DATA_CHANNEL_RETRY_TIMES,
                                                                         firstVersion,
                                                                         DEFAULT_NUMBER_OF_ORIGINS,
                                                                         networkSinkWrk1Id);
    auto networkSinkOperatorNode1 = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor1, networkSinkWrk1Id);
    queryPlan1->appendOperatorAsNewRoot(networkSinkOperatorNode1);
    queryPlan1->getSinkOperators().front()->inferSchema();
    //register and start query on worker 1
    auto success_register_wrk1 = wrk1->getNodeEngine()->registerQueryInNodeEngine(queryPlan1);
    ASSERT_TRUE(success_register_wrk1);
    auto success_start_wrk1 = wrk1->getNodeEngine()->startQuery(sharedQueryId);
    ASSERT_TRUE(success_start_wrk1);

    //start query on wrk2
    auto subPlanIdWrk2 = 20;
    auto queryPlan2 = QueryPlan::create(sharedQueryId, subPlanIdWrk2);
    //create network source getting data from sink at wrk1
    auto sinkLocationWrk1 = NES::Network::NodeLocation(wrk1->getWorkerId(), "localhost", *wrk1DataPort);
    auto networkSourceDescriptorWrk2 = Network::NetworkSourceDescriptor::create(schema,
                                                                                networkSourceWrk2Partition,
                                                                                sinkLocationWrk1,
                                                                                WAIT_TIME,
                                                                                EVENT_CHANNEL_RETRY_TIMES,
                                                                                firstVersion);
    auto sourceOperatorNodeWrk2 = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptorWrk2, networkSrcWrk2Id);
    queryPlan2->addRootOperator(sourceOperatorNodeWrk2);
    //create network sink connected to coordinator
    auto networkSourceCrdLocation =
        NES::Network::NodeLocation(crd->getNesWorker()->getWorkerId(), "localhost", *crdWorkerDataPort);
    auto networkSourceCrdPartition = NES::Network::NesPartition(sharedQueryId, networkSrcCrdId, 0, 0);
    auto networkSinkDescriptorWrk2 = Network::NetworkSinkDescriptor::create(networkSourceCrdLocation,
                                                                            networkSourceCrdPartition,
                                                                            waitTime,
                                                                            DATA_CHANNEL_RETRY_TIMES,
                                                                            firstVersion,
                                                                            DEFAULT_NUMBER_OF_ORIGINS,
                                                                            networkSrcWrk2Id);
    auto networkSinkOperatorNodeWrk2 = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptorWrk2, networkSinkWrk2Id);
    queryPlan2->appendOperatorAsNewRoot(networkSinkOperatorNodeWrk2);
    queryPlan2->getSinkOperators().front()->inferSchema();
    //register and start query on worker 2
    auto success_register_wrk2 = wrk2->getNodeEngine()->registerQueryInNodeEngine(queryPlan2);
    ASSERT_TRUE(success_register_wrk2);
    auto success_start_wrk2 = wrk2->getNodeEngine()->startQuery(sharedQueryId);
    ASSERT_TRUE(success_start_wrk2);

    //deploy to coordinator
    auto subPlanId = 90;
    auto queryPlan = QueryPlan::create(sharedQueryId, subPlanId);
    //create network source operator at coordinator
    auto sinkLocation = NES::Network::NodeLocation(wrk2->getWorkerId(), "localhost", *wrk2DataPort);
    auto networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                            networkSourceCrdPartition,
                                                                            sinkLocation,
                                                                            WAIT_TIME,
                                                                            EVENT_CHANNEL_RETRY_TIMES,
                                                                            firstVersion);
    auto sourceOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSrcCrdId);
    queryPlan->addRootOperator(sourceOperatorNode);
    //create file sink at coordinator
    auto fileSinkDescriptor = FileSinkDescriptor::create(testFile, "CSV_FORMAT", "APPEND");
    auto sinkOperatorNode = std::make_shared<SinkLogicalOperatorNode>(fileSinkDescriptor, fileSinkCrdId);
    queryPlan->appendOperatorAsNewRoot(sinkOperatorNode);
    queryPlan->getSinkOperators().front()->inferSchema();
    //deploy and start query at coordinator
    auto success_register = crd->getNesWorker()->getNodeEngine()->registerQueryInNodeEngine(queryPlan);
    ASSERT_TRUE(success_register);
    auto success_start = crd->getNesWorker()->getNodeEngine()->startQuery(sharedQueryId);
    ASSERT_TRUE(success_start);

    //wait for data to be written
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringBefore, testFile));

    //reconfiguration
    crd->getQueryCatalogService()->checkAndMarkForMigration(sharedQueryId, 0, QueryState::MIGRATING);

    Version nextVersion = 1;
    //reconfigure network sink on wrk1 to point to wrk3 instead of to wrk2
    auto subQueryIds = wrk1->getNodeEngine()->getSubQueryIds(sharedQueryId);
    EXPECT_EQ(subQueryIds.size(), 1);
    //retrieve data about running network sink at wrk1
    auto networkSink = std::dynamic_pointer_cast<Network::NetworkSink>(
        wrk1->getNodeEngine()->getExecutableQueryPlan(subQueryIds.front())->getSinks().front());
    auto uniqueNetworkSinkDescriptorId = networkSink->getUniqueNetworkSinkDescriptorId();
    //trigger sink reconnection to new source on wrk2
    auto networkSourceWrk3Partition = NES::Network::NesPartition(sharedQueryId, networkSrcWrk3Id, 0, 0);
    Network::NodeLocation newNodeLocation(wrk3->getWorkerId(), "localhost", *wrk3DataPort);
    auto reconfigureNetworkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                                       networkSourceCrdPartition,
                                                                                       newNodeLocation,
                                                                                       WAIT_TIME,
                                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                                       nextVersion);
    crd->getNesWorker()->getNodeEngine()->getPartitionManager()->addNextVersion(
        *reconfigureNetworkSourceDescriptor->as<Network::NetworkSourceDescriptor>());
    wrk1->getNodeEngine()->experimentalReconfigureNetworkSink(crd->getNesWorker()->getWorkerId(),
                                                              "localhost",
                                                              *wrk3DataPort,
                                                              subQueryIds.front(),
                                                              uniqueNetworkSinkDescriptorId,
                                                              networkSourceWrk3Partition,
                                                              nextVersion);

    //reconfig performed but new network source not started yet. tuples are buffered at wrk1

    //notify lambda source that reconfig happened and make it release more tuples into the buffer
    waitForReconfig = true;
    //wait for tuples in order to make sure that the buffer is actually tested
    while (!waitForReconnect)
        ;
    //wait some more to make sure the eos reaches the sink
    std::this_thread::sleep_for(std::chrono::seconds(1));

    //start operator at new destination, buffered tuples will be unbuffered to node 3 once the operators there become active

    //start query on wrk3
    auto subPlanIdWrk3 = 30;
    auto queryPlan3 = QueryPlan::create(sharedQueryId, subPlanIdWrk3);
    //create network source getting data from sink at wrk1
    auto networkSourceDescriptorWrk3 = Network::NetworkSourceDescriptor::create(schema,
                                                                                networkSourceWrk3Partition,
                                                                                sinkLocationWrk1,
                                                                                WAIT_TIME,
                                                                                EVENT_CHANNEL_RETRY_TIMES,
                                                                                nextVersion);
    auto sourceOperatorNodeWrk3 = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptorWrk3, networkSrcWrk3Id);
    queryPlan3->addRootOperator(sourceOperatorNodeWrk3);
    //create network sink connected to coordinator
    auto networkSinkDescriptorWrk3 = Network::NetworkSinkDescriptor::create(networkSourceCrdLocation,
                                                                            networkSourceCrdPartition,
                                                                            waitTime,
                                                                            DATA_CHANNEL_RETRY_TIMES,
                                                                            nextVersion,
                                                                            DEFAULT_NUMBER_OF_ORIGINS,
                                                                            networkSinkWrk3Id);
    auto networkSinkOperatorNodeWrk3 = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptorWrk3, networkSinkWrk3Id);
    queryPlan3->appendOperatorAsNewRoot(networkSinkOperatorNodeWrk3);
    queryPlan3->getSinkOperators().front()->inferSchema();
    //register and start query on worker 3
    auto success_register_wrk3 = wrk3->getNodeEngine()->registerQueryInNodeEngine(queryPlan3);
    ASSERT_TRUE(success_register_wrk3);
    auto success_start_wrk3 = wrk3->getNodeEngine()->startQuery(sharedQueryId);
    ASSERT_TRUE(success_start_wrk3);

    //check that all tuples arrived and that query terminated successfully
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringAfter, testFile));

    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk1));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk3));
    //worker 2 did not receive any soft stop, because the node reconnected
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk2));
    //worker at coordinator expects double number of threads to send soft stop because source reconfig is not implemented yet
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, crd->getNesWorker()));

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

    auto stopSuccess3 = wrk3->getNodeEngine()->stopQuery(sharedQueryId);
    cout << "stopping worker" << endl;
    bool retStopWrk3 = wrk3->stop(false);
    ASSERT_TRUE(retStopWrk3);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/**
 * @brief This test the reconfiguration of a network sink that is already buffering
 */
TEST_P(QueryRedeploymentIntegrationTest, testReconfigureWhileAlreadyBuffering) {
    const uint64_t numBuffersToProduceBeforeReconnect = 40;
    const uint64_t numBuffersToProduceWhileBuffering = 20;
    const uint64_t totalBuffersToProduce = numBuffersToProduceBeforeReconnect + numBuffersToProduceWhileBuffering;
    const uint64_t gatheringValue = 10;
    const std::chrono::seconds waitTime(10);
    uint64_t tuplesPerBuffer = 10;
    uint8_t bytesPerTuple = sizeof(uint64_t);
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";
    remove(testFile.c_str());

    std::string compareStringBefore;
    std::ostringstream oss;
    oss << "value:INTEGER(64 bits)" << std::endl;
    for (uint64_t i = 0; i < numBuffersToProduceBeforeReconnect * tuplesPerBuffer; ++i) {
        oss << std::to_string(i) << std::endl;
    }
    compareStringBefore = oss.str();

    std::string compareStringAfter;
    std::ostringstream ossAfter;
    ossAfter << "value:INTEGER(64 bits)" << std::endl;
    for (uint64_t i = 0; i < (numBuffersToProduceBeforeReconnect + numBuffersToProduceWhileBuffering) * tuplesPerBuffer; ++i) {
        ossAfter << std::to_string(i) << std::endl;
    }
    compareStringAfter = ossAfter.str();

    std::atomic<uint64_t> bufferCount = 0;
    std::atomic<bool> waitForReconfig = false;
    std::atomic<bool> waitForReconnect = false;
    auto lambdaSourceFunction = [&bufferCount, &waitForReconfig, &waitForReconnect](NES::Runtime::TupleBuffer& buffer,
                                                                                    uint64_t numberOfTuplesToProduce) {
        struct Record {
            uint64_t value;
        };
        auto currentCount = ++bufferCount;
        if (currentCount > numBuffersToProduceBeforeReconnect) {
            //after sending the specified amount of tuples, wait until the reconfiguration has been triggered, subsequent tuples will be buffered
            while (!waitForReconfig)
                ;
        }
        if (currentCount >= numBuffersToProduceBeforeReconnect + numBuffersToProduceWhileBuffering) {
            //after writing all remaining tuples into the buffer, give signal to start the new operators to finish the reconnect, tuples will be unbuffered to new destination
            waitForReconnect = true;
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
                                                     numBuffersToProduceBeforeReconnect,
                                                     gatheringValue,
                                                     GatheringMode::INTERVAL_MODE);

    NES_INFO("rest port = {}", *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    auto crdWorkerDataPort = getAvailablePort();
    coordinatorConfig->worker.dataPort = *crdWorkerDataPort;

    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    auto schema = Schema::create()->addField(createField("value", BasicType::UINT64));
    crd->getSourceCatalog()->addLogicalSource("seq", schema);
    QueryId sharedQueryId = 800;
    QueryId queryId = 900;
    auto crdQueryPlan = QueryPlan::create();
    crdQueryPlan->setQueryId(queryId);
    crd->getQueryCatalogService()->createNewEntry("test_query", crdQueryPlan, Optimizer::PlacementStrategy::BottomUp);
    crd->getQueryCatalogService()->mapSharedQueryPlanId(sharedQueryId, queryId);

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    wrkConf1->numWorkerThreads.setValue(GetParam());
    wrkConf1->connectSinksAsync.setValue(true);
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
    wrkConf2->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart2);
    ASSERT_TRUE(waitForNodes(5, 3, topology));

    NES_INFO("start worker 3");
    WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
    wrkConf3->coordinatorPort.setValue(*rpcCoordinatorPort);
    auto wrk3DataPort = getAvailablePort();
    wrkConf3->dataPort = *wrk3DataPort;
    wrkConf3->numWorkerThreads.setValue(GetParam());
    wrkConf3->connectSinksAsync.setValue(true);
    wrkConf3->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart3);
    ASSERT_TRUE(waitForNodes(5, 4, topology));

    NES_INFO("Shared Query ID: {}", sharedQueryId);
    ASSERT_NE(sharedQueryId, INVALID_QUERY_ID);

    //start query

    //set operator ids
    auto fileSinkCrdId = 91;
    auto networkSrcCrdId = 92;
    auto networkSinkWrk1Id = 11;
    auto lambdaSourceId = 12;
    auto networkSinkWrk2Id = 21;
    auto networkSrcWrk2Id = 22;
    auto networkSinkWrk3Id = 31;
    auto networkSrcWrk3Id = 32;

    //start query on wrk1
    auto subPlanIdWrk1 = 10;
    auto queryPlan1 = QueryPlan::create(sharedQueryId, subPlanIdWrk1);
    //create lambda source
    constexpr auto sourceAffinity = 0;
    constexpr auto taskQueueId = 0;
    auto lambdaSourceDescriptor = LambdaSourceDescriptor::create(schema,
                                                                 lambdaSourceFunction,
                                                                 totalBuffersToProduce,
                                                                 gatheringValue,
                                                                 GatheringMode::INTERVAL_MODE,
                                                                 sourceAffinity,
                                                                 taskQueueId,
                                                                 "seq",
                                                                 "test_stream");
    auto lambdaSourceOperatorNode = std::make_shared<SourceLogicalOperatorNode>(lambdaSourceDescriptor, lambdaSourceId);
    queryPlan1->addRootOperator(lambdaSourceOperatorNode);
    //create network sink
    auto networkSourceWrk2Location = NES::Network::NodeLocation(wrk2->getWorkerId(), "localhost", *wrk2DataPort);
    auto networkSourceWrk2Partition = NES::Network::NesPartition(sharedQueryId, networkSrcWrk2Id, 0, 0);
    Version firstVersion = 0;
    auto networkSinkDescriptor1 = Network::NetworkSinkDescriptor::create(networkSourceWrk2Location,
                                                                         networkSourceWrk2Partition,
                                                                         waitTime,
                                                                         DATA_CHANNEL_RETRY_TIMES,
                                                                         firstVersion,
                                                                         DEFAULT_NUMBER_OF_ORIGINS,
                                                                         networkSinkWrk1Id);
    auto networkSinkOperatorNode1 = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor1, networkSinkWrk1Id);
    queryPlan1->appendOperatorAsNewRoot(networkSinkOperatorNode1);
    queryPlan1->getSinkOperators().front()->inferSchema();
    //register and start query on worker 1
    auto success_register_wrk1 = wrk1->getNodeEngine()->registerQueryInNodeEngine(queryPlan1);
    ASSERT_TRUE(success_register_wrk1);
    auto success_start_wrk1 = wrk1->getNodeEngine()->startQuery(sharedQueryId);
    ASSERT_TRUE(success_start_wrk1);

    //start query on wrk2
    auto subPlanIdWrk2 = 20;
    auto queryPlan2 = QueryPlan::create(sharedQueryId, subPlanIdWrk2);
    //create network source getting data from sink at wrk1
    auto sinkLocationWrk1 = NES::Network::NodeLocation(wrk1->getWorkerId(), "localhost", *wrk1DataPort);
    auto networkSourceDescriptorWrk2 = Network::NetworkSourceDescriptor::create(schema,
                                                                                networkSourceWrk2Partition,
                                                                                sinkLocationWrk1,
                                                                                WAIT_TIME,
                                                                                EVENT_CHANNEL_RETRY_TIMES,
                                                                                firstVersion);
    auto sourceOperatorNodeWrk2 = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptorWrk2, networkSrcWrk2Id);
    queryPlan2->addRootOperator(sourceOperatorNodeWrk2);
    //create network sink connected to coordinator
    auto networkSourceCrdLocation =
        NES::Network::NodeLocation(crd->getNesWorker()->getWorkerId(), "localhost", *crdWorkerDataPort);
    auto networkSourceCrdPartition = NES::Network::NesPartition(sharedQueryId, networkSrcCrdId, 0, 0);
    auto networkSinkDescriptorWrk2 = Network::NetworkSinkDescriptor::create(networkSourceCrdLocation,
                                                                            networkSourceCrdPartition,
                                                                            waitTime,
                                                                            DATA_CHANNEL_RETRY_TIMES,
                                                                            firstVersion,
                                                                            DEFAULT_NUMBER_OF_ORIGINS,
                                                                            networkSinkWrk2Id);
    auto networkSinkOperatorNodeWrk2 = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptorWrk2, networkSinkWrk2Id);
    queryPlan2->appendOperatorAsNewRoot(networkSinkOperatorNodeWrk2);
    queryPlan2->getSinkOperators().front()->inferSchema();
    //register and start query on worker 2
    auto success_register_wrk2 = wrk2->getNodeEngine()->registerQueryInNodeEngine(queryPlan2);
    ASSERT_TRUE(success_register_wrk2);
    auto success_start_wrk2 = wrk2->getNodeEngine()->startQuery(sharedQueryId);
    ASSERT_TRUE(success_start_wrk2);

    //deploy to coordinator
    auto subPlanId = 90;
    auto queryPlan = QueryPlan::create(sharedQueryId, subPlanId);
    //create network source operator at coordinator
    auto sinkLocation = NES::Network::NodeLocation(wrk2->getWorkerId(), "localhost", *wrk2DataPort);
    auto networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                            networkSourceCrdPartition,
                                                                            sinkLocation,
                                                                            WAIT_TIME,
                                                                            EVENT_CHANNEL_RETRY_TIMES,
                                                                            firstVersion);
    auto sourceOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSrcCrdId);
    queryPlan->addRootOperator(sourceOperatorNode);
    //create file sink at coordinator
    auto fileSinkDescriptor = FileSinkDescriptor::create(testFile, "CSV_FORMAT", "APPEND");
    auto sinkOperatorNode = std::make_shared<SinkLogicalOperatorNode>(fileSinkDescriptor, fileSinkCrdId);
    queryPlan->appendOperatorAsNewRoot(sinkOperatorNode);
    queryPlan->getSinkOperators().front()->inferSchema();
    //deploy and start query at coordinator
    auto success_register = crd->getNesWorker()->getNodeEngine()->registerQueryInNodeEngine(queryPlan);
    ASSERT_TRUE(success_register);
    auto success_start = crd->getNesWorker()->getNodeEngine()->startQuery(sharedQueryId);
    ASSERT_TRUE(success_start);

    //wait for data to be written
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringBefore, testFile));

    //reconfiguration
    Version nextVersion = 1;
    Network::NodeLocation newNodeLocation(crd->getNesWorker()->getWorkerId(), "localhost", *wrk3DataPort);
    auto reconfiguredNetworkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                                        networkSourceCrdPartition,
                                                                                        newNodeLocation,
                                                                                        WAIT_TIME,
                                                                                        EVENT_CHANNEL_RETRY_TIMES,
                                                                                        nextVersion);
    crd->getNesWorker()->getNodeEngine()->getPartitionManager()->addNextVersion(
        *reconfiguredNetworkSourceDescriptor->as<Network::NetworkSourceDescriptor>());
    crd->getQueryCatalogService()->checkAndMarkForMigration(sharedQueryId, subPlanIdWrk2, QueryState::MIGRATING);
    //retrieve data about running network sink at wrk1
    auto subQueryIds = wrk1->getNodeEngine()->getSubQueryIds(sharedQueryId);
    EXPECT_EQ(subQueryIds.size(), 1);
    auto networkSink = std::dynamic_pointer_cast<Network::NetworkSink>(
        wrk1->getNodeEngine()->getExecutableQueryPlan(subQueryIds.front())->getSinks().front());
    auto uniqueNetworkSinkDescriptorId = networkSink->getUniqueNetworkSinkDescriptorId();

    //reconfigure with unreachable target to get sink into buffering state
    auto inexistentPartition = NES::Network::NesPartition(queryId, 8, 0, 0);
    wrk1->getNodeEngine()->experimentalReconfigureNetworkSink(9999,
                                                              "localhost",
                                                              *wrk2DataPort,
                                                              subQueryIds.front(),
                                                              uniqueNetworkSinkDescriptorId,
                                                              inexistentPartition,
                                                              nextVersion);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    //reconfigure network sink on wrk1 to point to wrk3 instead of to the previous invalid partition
    //trigger sink reconnection to new source on wrk2
    auto networkSourceWrk3Partition = NES::Network::NesPartition(sharedQueryId, networkSrcWrk3Id, 0, 0);
    wrk1->getNodeEngine()->experimentalReconfigureNetworkSink(crd->getNesWorker()->getWorkerId(),
                                                              "localhost",
                                                              *wrk3DataPort,
                                                              subQueryIds.front(),
                                                              uniqueNetworkSinkDescriptorId,
                                                              networkSourceWrk3Partition,
                                                              nextVersion);

    //reconfig performed but new network source not started yet. tuples are buffered at wrk1

    //notify lambda source that reconfig happened and make it release more tuples into the buffer
    waitForReconfig = true;
    //wait for tuples in order to make sure that the buffer is actually tested
    while (!waitForReconnect)
        ;
    //wait some more to make sure the eos reaches the sink
    std::this_thread::sleep_for(std::chrono::seconds(1));

    //start operator at new destination, buffered tuples will be unbuffered to node 3 once the operators there become active

    //start query on wrk3
    auto subPlanIdWrk3 = 30;
    auto queryPlan3 = QueryPlan::create(sharedQueryId, subPlanIdWrk3);
    //create network source getting data from sink at wrk1
    auto networkSourceDescriptorWrk3 = Network::NetworkSourceDescriptor::create(schema,
                                                                                networkSourceWrk3Partition,
                                                                                sinkLocationWrk1,
                                                                                WAIT_TIME,
                                                                                EVENT_CHANNEL_RETRY_TIMES,
                                                                                nextVersion);
    auto sourceOperatorNodeWrk3 = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptorWrk3, networkSrcWrk3Id);
    queryPlan3->addRootOperator(sourceOperatorNodeWrk3);
    //create network sink connected to coordinator
    auto networkSinkDescriptorWrk3 = Network::NetworkSinkDescriptor::create(networkSourceCrdLocation,
                                                                            networkSourceCrdPartition,
                                                                            waitTime,
                                                                            DATA_CHANNEL_RETRY_TIMES,
                                                                            nextVersion,
                                                                            DEFAULT_NUMBER_OF_ORIGINS,
                                                                            networkSrcWrk3Id);
    auto networkSinkOperatorNodeWrk3 = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptorWrk3, networkSinkWrk3Id);
    queryPlan3->appendOperatorAsNewRoot(networkSinkOperatorNodeWrk3);
    queryPlan3->getSinkOperators().front()->inferSchema();
    //register and start query on worker 3
    auto success_register_wrk3 = wrk3->getNodeEngine()->registerQueryInNodeEngine(queryPlan3);
    ASSERT_TRUE(success_register_wrk3);
    auto success_start_wrk3 = wrk3->getNodeEngine()->startQuery(sharedQueryId);
    ASSERT_TRUE(success_start_wrk3);

    //check that all tuples arrived and that query terminated successfully
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringAfter, testFile));

    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk1));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk3));
    //worker 2 did not receive any soft stop, because the node reconnected
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk2));
    //worker at coordinator expects double number of threads to send soft stop because source reconfig is not implemented yet
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, crd->getNesWorker()));

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

    auto stopSuccess3 = wrk3->getNodeEngine()->stopQuery(sharedQueryId);
    cout << "stopping worker" << endl;
    bool retStopWrk3 = wrk3->stop(false);
    ASSERT_TRUE(retStopWrk3);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}
INSTANTIATE_TEST_CASE_P(QueryRedeploymentIntegrationTestParam, QueryRedeploymentIntegrationTest, ::testing::Values(1, 4));
}// namespace NES

#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Network/NetworkSink.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LambdaSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhaseContext.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Topology/Topology.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
//TODO: to remove later
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Runtime/ReconfigurationMessage.hpp>
#include <Services/QueryService.hpp>
#include <Util/TimeMeasurement.hpp>

namespace NES {

class QueryRedeploymentIntegrationTest : public Testing::BaseIntegrationTest, public testing::WithParamInterface<uint32_t> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryRedeploymentIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Set up QueryRedeploymentIntegrationTest test class");
        //todo: remove this when lambda sources are implemented
        auto inputSequence = std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv";
        std::ofstream inputSequenceStream(inputSequence);
        for (int i = 1; i < 100000; ++i) {
            inputSequenceStream << std::to_string(i) << std::endl;
        }
        inputSequenceStream.close();
        ASSERT_FALSE(inputSequenceStream.fail());
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

    std::chrono::duration<int64_t, std::milli> defaultTimeoutInSec = std::chrono::seconds(TestUtils::defaultTimeout);
};

//todo: use lambda source here
/**
     * @brief This tests the asynchronous connection establishment, where the sink buffers incoming tuples while waiting for the
     * network channel to become available
     */
TEST_P(QueryRedeploymentIntegrationTest, testAsyncConnectingSink) {
    uint64_t numBuffersToProduce = 400;
    uint64_t tuplesPerBuffer = 10;
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";
    remove(testFile.c_str());

    std::string compareString;
    std::ostringstream oss;
    oss << "seq$value:INTEGER(64 bits)" << std::endl;
    for (uint64_t i = 1; i <= numBuffersToProduce * tuplesPerBuffer; ++i) {
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

    auto stype = CSVSourceType::create();
    stype->setFilePath(std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv");
    stype->setNumberOfBuffersToProduce(numBuffersToProduce);
    stype->setNumberOfTuplesToProducePerBuffer(tuplesPerBuffer);
    stype->setGatheringInterval(1);
    auto sequenceSource = PhysicalSource::create("seq", "test_stream", stype);
    wrkConf1->physicalSources.add(sequenceSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(waitForNodes(5, 2, topology));

    QueryId queryId = crd->getQueryService()->validateAndQueueAddQueryRequest(
        R"(Query::from("seq").sink(FileSinkDescriptor::create(")" + testFile + R"(", "CSV_FORMAT", "APPEND"));)",
        Optimizer::PlacementStrategy::BottomUp,
        FaultToleranceType::NONE,
        LineageType::NONE);

    NES_INFO("Query ID: {}", queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);

    auto startTimestamp = std::chrono::system_clock::now();
    size_t recv_tuples = 0;

    while (recv_tuples < numBuffersToProduce * tuplesPerBuffer
           && std::chrono::system_clock::now() < startTimestamp + defaultTimeoutInSec) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile), std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv after buffering: {}", recv_tuples)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

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

//todo: remove because redundant?
TEST_P(QueryRedeploymentIntegrationTest, testPlannedReconnect) {
    const uint64_t numBuffersToProduceBeforeReconnect = 40;
    const uint64_t numBuffersToProduceWhileBuffering = 20;
    const uint64_t numBuffersToProduceAfterReconnect = 40;
    const uint64_t totalBuffersToProduce =
        numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect + numBuffersToProduceWhileBuffering;
    const uint64_t gatheringValue = 10;
    const std::chrono::seconds waitTime(10);
    const uint32_t retryTimes = 10;
    //todo: how do these get set?
    const uint64_t tuplesPerBuffer = 512;
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";
    remove(testFile.c_str());

    std::string compareStringBefore;
    std::ostringstream oss;
    oss << "value:INTEGER(64 bits)" << std::endl;
    for (uint64_t i = 0; i < numBuffersToProduceBeforeReconnect * tuplesPerBuffer; ++i) {
        oss << std::to_string(i) << std::endl;
        compareStringBefore = oss.str();
    }

    std::string compareStringAfter;
    std::ostringstream ossAfter;
    ossAfter << "value:INTEGER(64 bits)" << std::endl;
    for (uint64_t i = 0;
         i < (numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect + numBuffersToProduceWhileBuffering)
             * tuplesPerBuffer;
         ++i) {
        ossAfter << std::to_string(i) << std::endl;
        compareStringAfter = ossAfter.str();
    }

    std::atomic<uint64_t> bufferCount = 0;
    std::atomic<bool> waitForReconfig = false;
    std::atomic<bool> waitForReconnect = false;
    std::atomic<bool> waitForFinalCount = false;
    auto lambdaSourceFunction = [&bufferCount, &waitForReconfig, &waitForReconnect, &waitForFinalCount](NES::Runtime::TupleBuffer& buffer,
                                                                                                        uint64_t numberOfTuplesToProduce) {
      struct Record {
          uint64_t value;
      };
      auto currentCount = ++bufferCount;
      if (currentCount > numBuffersToProduceBeforeReconnect) {
          //after sending the specified amount of tuples, wait until the reconfiguration has been triggered, subsequent tuples will be buffered
          waitForReconfig.wait(false);
      }
      if (currentCount > numBuffersToProduceBeforeReconnect + numBuffersToProduceWhileBuffering) {
          //after writing some tuples into the buffer, give signal to start the new operators to finish the reconnect, tuples will be unbuffered to new destination
          waitForReconnect = true;
          waitForReconnect.notify_all();
      }
      if (currentCount
          > numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect + numBuffersToProduceWhileBuffering) {
          waitForFinalCount.wait(false);
      }
      auto valCount = (currentCount - 1) * (numberOfTuplesToProduce);
      auto* records = buffer.getBuffer<Record>();
      for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
          records[u].value = valCount + u;
      }
    };
    auto lambdaSourceType = LambdaSourceType::create(std::move(lambdaSourceFunction),
                                                     numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect,
                                                     10,
                                                     GatheringMode::INTERVAL_MODE);
    auto physicalSource = PhysicalSource::create("seq", "test_stream", lambdaSourceType);

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

    wrkConf1->physicalSources.add(physicalSource);

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
    auto lambdaSourceDescriptor = LambdaSourceDescriptor::create(schema,
                                                                 lambdaSourceFunction,
                                                                 totalBuffersToProduce,
                                                                 gatheringValue,
                                                                 GatheringMode::INTERVAL_MODE,
                                                                 0,
                                                                 0,
                                                                 "seq",
                                                                 "test_stream");
    auto lambdaSourceOperatorNode = std::make_shared<SourceLogicalOperatorNode>(lambdaSourceDescriptor, lambdaSourceId);
    queryPlan1->addRootOperator(lambdaSourceOperatorNode);
    //create network sink
    auto networkSourceWrk2Location = NES::Network::NodeLocation(wrk2->getWorkerId(), "localhost", *wrk2DataPort);
    auto networkSourceWrk2Partition = NES::Network::NesPartition(sharedQueryId, networkSrcWrk2Id, 0, 0);
    auto networkSinkDescriptor1 =
        Network::NetworkSinkDescriptor::create(networkSourceWrk2Location, networkSourceWrk2Partition, waitTime, retryTimes);
    auto networkSinkOperatorNode1 = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor1, networkSinkWrk1Id);
    NES::Optimizer::TypeInferencePhaseContext context1({}, {});
    queryPlan1->appendOperatorAsNewRoot(networkSinkOperatorNode1);
    queryPlan1->getSinkOperators().front()->inferSchema(context1);
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
                                                                                std::chrono::milliseconds(1000),
                                                                                5);
    auto sourceOperatorNodeWrk2 = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptorWrk2, networkSrcWrk2Id);
    queryPlan2->addRootOperator(sourceOperatorNodeWrk2);
    //create network sink connected to coordinator
    auto networkSourceCrdLocation =
        NES::Network::NodeLocation(crd->getNesWorker()->getWorkerId(), "localhost", *crdWorkerDataPort);
    auto networkSourceCrdPartition = NES::Network::NesPartition(sharedQueryId, networkSrcCrdId, 0, 0);
    auto networkSinkDescriptorWrk2 =
        Network::NetworkSinkDescriptor::create(networkSourceCrdLocation, networkSourceCrdPartition, waitTime, retryTimes);
    auto networkSinkOperatorNodeWrk2 = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptorWrk2, networkSinkWrk2Id);
    NES::Optimizer::TypeInferencePhaseContext context2({}, {});
    queryPlan2->appendOperatorAsNewRoot(networkSinkOperatorNodeWrk2);
    queryPlan2->getSinkOperators().front()->inferSchema(context2);
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
                                                                            std::chrono::milliseconds(1000),
                                                                            5);
    auto sourceOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSrcCrdId);
    queryPlan->addRootOperator(sourceOperatorNode);
    //create file sink at coordinator
    auto fileSinkDescriptor = FileSinkDescriptor::create(testFile, "CSV_FORMAT", "APPEND");
    auto sinkOperatorNode = std::make_shared<SinkLogicalOperatorNode>(fileSinkDescriptor, fileSinkCrdId);
    NES::Optimizer::TypeInferencePhaseContext context({}, {});
    queryPlan->appendOperatorAsNewRoot(sinkOperatorNode);
    queryPlan->getSinkOperators().front()->inferSchema(context);
    //deploy and start query at coordinator
    auto success_register = crd->getNesWorker()->getNodeEngine()->registerQueryInNodeEngine(queryPlan);
    ASSERT_TRUE(success_register);
    auto success_start = crd->getNesWorker()->getNodeEngine()->startQuery(sharedQueryId);
    ASSERT_TRUE(success_start);

    //wait for data to be written
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringBefore, testFile));

    //reconfiguration

    //reconfigure network sink on wrk1 to point to wrk3 instead of to wrk2
    auto subQueryIds = wrk1->getNodeEngine()->getSubQueryIds(sharedQueryId);
    EXPECT_EQ(subQueryIds.size(), 1);
    //retrieve data about running network sink at wrk1
    auto networkSink = std::dynamic_pointer_cast<Network::NetworkSink>(
        wrk1->getNodeEngine()->getExecutableQueryPlan(subQueryIds.front())->getSinks().front());
    auto uniqueNetworkSinkDescriptorId = networkSink->getUniqueNetworkSinkDescriptorId();
    //trigger sink reconnection to new source on wrk2
    auto networkSourceWrk3Partition = NES::Network::NesPartition(sharedQueryId, networkSrcWrk3Id, 0, 0);
    wrk1->getNodeEngine()->reconfigureNetworkSink(crd->getNesWorker()->getWorkerId(),
                                                  "localhost",
                                                  *wrk3DataPort,
                                                  subQueryIds.front(),
                                                  uniqueNetworkSinkDescriptorId,
                                                  networkSourceWrk3Partition);

    //reconfig performed but new network source not started yet. tuples are buffered at wrk1

    //notify lambda source that reconfig happened and make it release more tuples into the buffer
    waitForReconfig = true;
    waitForReconfig.notify_all();
    //wait for tuples in order to make sure that the buffer is actually tested
    waitForReconnect.wait(false);

    //start operator at new destination, buffered tuples will be unbuffered to node 3 once the operators there become active

    //start query on wrk3
    auto subPlanIdWrk3 = 30;
    auto queryPlan3 = QueryPlan::create(sharedQueryId, subPlanIdWrk3);
    //create network source getting data from sink at wrk1
    auto networkSourceDescriptorWrk3 = Network::NetworkSourceDescriptor::create(schema,
                                                                                networkSourceWrk3Partition,
                                                                                sinkLocationWrk1,
                                                                                std::chrono::milliseconds(1000),
                                                                                5);
    auto sourceOperatorNodeWrk3 = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptorWrk3, networkSrcWrk3Id);
    queryPlan3->addRootOperator(sourceOperatorNodeWrk3);
    //create network sink connected to coordinator
    auto networkSinkDescriptorWrk3 =
        Network::NetworkSinkDescriptor::create(networkSourceCrdLocation, networkSourceCrdPartition, waitTime, retryTimes);
    auto networkSinkOperatorNodeWrk3 = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptorWrk3, networkSinkWrk3Id);
    NES::Optimizer::TypeInferencePhaseContext context3({}, {});
    queryPlan3->appendOperatorAsNewRoot(networkSinkOperatorNodeWrk3);
    queryPlan3->getSinkOperators().front()->inferSchema(context3);
    //register and start query on worker 3
    auto success_register_wrk3 = wrk3->getNodeEngine()->registerQueryInNodeEngine(queryPlan3);
    ASSERT_TRUE(success_register_wrk3);
    auto success_start_wrk3 = wrk3->getNodeEngine()->startQuery(sharedQueryId);
    ASSERT_TRUE(success_start_wrk3);

    //check that all tuples arrived
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringAfter, testFile));
    waitForFinalCount = true;
    waitForFinalCount.notify_all();
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk1));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk3));
    //worker 2 did not receive any soft stop, because the node reconnected
    ASSERT_TRUE(wrk2->getNodeEngine()->stopQuery(sharedQueryId));
    //worker at coordinator expects double number of threads to send soft stop because source reconfig is not implemented yet
    ASSERT_TRUE(crd->getNesWorker()->getNodeEngine()->stopQuery(sharedQueryId));

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

TEST_P(QueryRedeploymentIntegrationTest, testPlannedReconnectWithVersionDrainEvents) {
    const uint64_t numBuffersToProduceBeforeReconnect = 40;
    const uint64_t numBuffersToProduceWhileBuffering = 20;
    const uint64_t numBuffersToProduceAfterReconnect = 40;
    //these buffers are sent afte the final count, they make sure that no eos comes before the reconnect succeeded
    const uint64_t numBuffersToProduceAfterFinalCount = 20;
    const uint64_t totalBuffersToProduce =
        numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect + numBuffersToProduceWhileBuffering + numBuffersToProduceAfterFinalCount;
    const uint64_t gatheringValue = 10;
    const std::chrono::seconds waitTime(10);
    const uint32_t retryTimes = 10;
    //todo: how do these get set?
    const uint64_t tuplesPerBuffer = 512;
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";
    remove(testFile.c_str());

    std::string compareStringBefore;
    std::ostringstream oss;
    oss << "value:INTEGER(64 bits)" << std::endl;
    for (uint64_t i = 0; i < numBuffersToProduceBeforeReconnect * tuplesPerBuffer; ++i) {
        oss << std::to_string(i) << std::endl;
        compareStringBefore = oss.str();
    }

    std::string compareStringAfter;
    std::ostringstream ossAfter;
    ossAfter << "value:INTEGER(64 bits)" << std::endl;
    for (uint64_t i = 0;
         i < (numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect + numBuffersToProduceWhileBuffering)
             * tuplesPerBuffer;
         ++i) {
        ossAfter << std::to_string(i) << std::endl;
        compareStringAfter = ossAfter.str();
    }

    std::atomic<uint64_t> bufferCount = 0;
    std::atomic<bool> waitForReconfig = false;
    std::atomic<bool> waitForReconnect = false;
    std::atomic<bool> waitForFinalCount = false;
    auto lambdaSourceFunction = [&bufferCount, &waitForReconfig, &waitForReconnect, &waitForFinalCount](NES::Runtime::TupleBuffer& buffer,
                                                                                      uint64_t numberOfTuplesToProduce) {
        struct Record {
            uint64_t value;
        };
        auto currentCount = ++bufferCount;
        if (currentCount > numBuffersToProduceBeforeReconnect) {
            //after sending the specified amount of tuples, wait until the reconfiguration has been triggered, subsequent tuples will be buffered
            waitForReconfig.wait(false);
        }
        if (currentCount > numBuffersToProduceBeforeReconnect + numBuffersToProduceWhileBuffering) {
            //after writing some tuples into the buffer, give signal to start the new operators to finish the reconnect, tuples will be unbuffered to new destination
            waitForReconnect = true;
            waitForReconnect.notify_all();
        }
        if (currentCount
            > numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect + numBuffersToProduceWhileBuffering) {
            waitForFinalCount.wait(false);
        }
        auto valCount = (currentCount - 1) * (numberOfTuplesToProduce);
        auto* records = buffer.getBuffer<Record>();
        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            records[u].value = valCount + u;
        }
    };
    auto lambdaSourceType = LambdaSourceType::create(std::move(lambdaSourceFunction),
                                                     //numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect,
                                                     totalBuffersToProduce,
                                                     10,
                                                     GatheringMode::INTERVAL_MODE);
    auto physicalSource = PhysicalSource::create("seq", "test_stream", lambdaSourceType);

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
    wrkConf1->queryCompiler.queryCompilerType.setValue(QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER);

    wrkConf1->physicalSources.add(physicalSource);

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
    wrkConf2->queryCompiler.queryCompilerType.setValue(QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER);
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
    wrkConf3->queryCompiler.queryCompilerType.setValue(QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER);
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
    auto lambdaSourceDescriptor = LambdaSourceDescriptor::create(schema,
                                                                 lambdaSourceFunction,
                                                                 totalBuffersToProduce,
                                                                 gatheringValue,
                                                                 GatheringMode::INTERVAL_MODE,
                                                                 0,
                                                                 0,
                                                                 "seq",
                                                                 "test_stream");
    auto lambdaSourceOperatorNode = std::make_shared<SourceLogicalOperatorNode>(lambdaSourceDescriptor, lambdaSourceId);
    queryPlan1->addRootOperator(lambdaSourceOperatorNode);
    //create network sink
    auto networkSourceWrk2Location = NES::Network::NodeLocation(wrk2->getWorkerId(), "localhost", *wrk2DataPort);
    auto networkSourceWrk2Partition = NES::Network::NesPartition(sharedQueryId, networkSrcWrk2Id, 0, 0);
    auto networkSinkDescriptor1 =
        Network::NetworkSinkDescriptor::create(networkSourceWrk2Location, networkSourceWrk2Partition, waitTime, retryTimes);
    auto networkSinkOperatorNode1 = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor1, networkSinkWrk1Id);
    NES::Optimizer::TypeInferencePhaseContext context1({}, {});
    queryPlan1->appendOperatorAsNewRoot(networkSinkOperatorNode1);
    queryPlan1->getSinkOperators().front()->inferSchema(context1);
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
                                                                                std::chrono::milliseconds(1000),
                                                                                5);
    auto sourceOperatorNodeWrk2 = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptorWrk2, networkSrcWrk2Id);
    queryPlan2->addRootOperator(sourceOperatorNodeWrk2);
    //create network sink connected to coordinator
    auto networkSourceCrdLocation =
        NES::Network::NodeLocation(crd->getNesWorker()->getWorkerId(), "localhost", *crdWorkerDataPort);
    auto networkSourceCrdPartition = NES::Network::NesPartition(sharedQueryId, networkSrcCrdId, 0, 0);
    auto networkSinkDescriptorWrk2 =
        Network::NetworkSinkDescriptor::create(networkSourceCrdLocation, networkSourceCrdPartition, waitTime, retryTimes);
    auto networkSinkOperatorNodeWrk2 = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptorWrk2, networkSinkWrk2Id);
    NES::Optimizer::TypeInferencePhaseContext context2({}, {});
    queryPlan2->appendOperatorAsNewRoot(networkSinkOperatorNodeWrk2);
    queryPlan2->getSinkOperators().front()->inferSchema(context2);
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
                                                                            std::chrono::milliseconds(1000),
                                                                            5);
    auto sourceOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSrcCrdId);
    queryPlan->addRootOperator(sourceOperatorNode);
    //create file sink at coordinator
    auto fileSinkDescriptor = FileSinkDescriptor::create(testFile, "CSV_FORMAT", "APPEND");
    auto sinkOperatorNode = std::make_shared<SinkLogicalOperatorNode>(fileSinkDescriptor, fileSinkCrdId);
    NES::Optimizer::TypeInferencePhaseContext context({}, {});
    queryPlan->appendOperatorAsNewRoot(sinkOperatorNode);
    queryPlan->getSinkOperators().front()->inferSchema(context);
    //deploy and start query at coordinator
    auto success_register = crd->getNesWorker()->getNodeEngine()->registerQueryInNodeEngine(queryPlan);
    ASSERT_TRUE(success_register);
    auto success_start = crd->getNesWorker()->getNodeEngine()->startQuery(sharedQueryId);
    ASSERT_TRUE(success_start);

    //wait for data to be written
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringBefore, testFile));

    //reconfiguration

    //reconfigure network sink on wrk1 to point to wrk3 instead of to wrk2
    auto subQueryIds = wrk1->getNodeEngine()->getSubQueryIds(sharedQueryId);
    EXPECT_EQ(subQueryIds.size(), 1);
    //retrieve data about running network sink at wrk1
    auto networkSink = std::dynamic_pointer_cast<Network::NetworkSink>(
        wrk1->getNodeEngine()->getExecutableQueryPlan(subQueryIds.front())->getSinks().front());
    //todo: add new descriptor here
    Network::NodeLocation newNodeLocation(crd->getNesWorker()->getWorkerId(),"localhost", *wrk3DataPort);
    auto networkSourceWrk3Partition = NES::Network::NesPartition(sharedQueryId, networkSrcWrk3Id, 0, 0);
    networkSink->addPendingReconfiguration(newNodeLocation, networkSourceWrk3Partition);
    //todo: insert vde here instead of reconfig
    auto message = Runtime::ReconfigurationMessage(sharedQueryId,
                                                            subPlanIdWrk1,
                                                            Runtime::ReconfigurationType::DrainVersion,
                                                            networkSink);
    //todo: get query manager and add it that way instead
    wrk1->getNodeEngine()->getQueryManager()->addReconfigurationMessage(sharedQueryId, subPlanIdWrk1, message, true);

    //reconfig performed but new network source not started yet. tuples are buffered at wrk1

    //notify lambda source that reconfig happened and make it release more tuples into the buffer
    waitForReconfig = true;
    waitForReconfig.notify_all();
    //wait for tuples in order to make sure that the buffer is actually tested
    waitForReconnect.wait(false);

    //start operator at new destination, buffered tuples will be unbuffered to node 3 once the operators there become active

    //start query on wrk3
    auto subPlanIdWrk3 = 30;
    auto queryPlan3 = QueryPlan::create(sharedQueryId, subPlanIdWrk3);
    //create network source getting data from sink at wrk1
    auto networkSourceDescriptorWrk3 = Network::NetworkSourceDescriptor::create(schema,
                                                                                networkSourceWrk3Partition,
                                                                                sinkLocationWrk1,
                                                                                std::chrono::milliseconds(1000),
                                                                                5);
    auto sourceOperatorNodeWrk3 = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptorWrk3, networkSrcWrk3Id);
    queryPlan3->addRootOperator(sourceOperatorNodeWrk3);
    //create network sink connected to coordinator
    auto networkSinkDescriptorWrk3 =
        Network::NetworkSinkDescriptor::create(networkSourceCrdLocation, networkSourceCrdPartition, waitTime, retryTimes);
    auto networkSinkOperatorNodeWrk3 = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptorWrk3, networkSinkWrk3Id);
    NES::Optimizer::TypeInferencePhaseContext context3({}, {});
    queryPlan3->appendOperatorAsNewRoot(networkSinkOperatorNodeWrk3);
    queryPlan3->getSinkOperators().front()->inferSchema(context3);
    //register and start query on worker 3
    auto success_register_wrk3 = wrk3->getNodeEngine()->registerQueryInNodeEngine(queryPlan3);
    ASSERT_TRUE(success_register_wrk3);
    auto success_start_wrk3 = wrk3->getNodeEngine()->startQuery(sharedQueryId);
    ASSERT_TRUE(success_start_wrk3);

    //check that all tuples arrived
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringAfter, testFile));
    waitForFinalCount = true;
    waitForFinalCount.notify_all();

    //send the last tuples, after which the lambda source shuts down

    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk1));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk3));
    //worker 2 did not receive any soft stop, because the node reconnected
    ASSERT_TRUE(wrk2->getNodeEngine()->stopQuery(sharedQueryId));
    //worker at coordinator expects double number of threads to send soft stop because source reconfig is not implemented yet
    ASSERT_TRUE(crd->getNesWorker()->getNodeEngine()->stopQuery(sharedQueryId));

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

TEST_P(QueryRedeploymentIntegrationTest, DISABLED_testEndOfStreamWhileBuffering) {
    const uint64_t numBuffersToProduceBeforeReconnect = 40;
    const uint64_t numBuffersToProduceWhileBuffering = 20;
    //const uint64_t numBuffersToProduceAfterReconnect = 40;
    const uint64_t totalBuffersToProduce =
        numBuffersToProduceBeforeReconnect + numBuffersToProduceWhileBuffering;
    const uint64_t gatheringValue = 10;
    const std::chrono::seconds waitTime(10);
    const uint32_t retryTimes = 50;
    //todo: how do these get set?
    const uint64_t tuplesPerBuffer = 512;
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";
    remove(testFile.c_str());

    std::string compareStringBefore;
    std::ostringstream oss;
    oss << "value:INTEGER(64 bits)" << std::endl;
    for (uint64_t i = 0; i < numBuffersToProduceBeforeReconnect * tuplesPerBuffer; ++i) {
        oss << std::to_string(i) << std::endl;
        compareStringBefore = oss.str();
    }

    std::string compareStringAfter;
    std::ostringstream ossAfter;
    ossAfter << "value:INTEGER(64 bits)" << std::endl;
    for (uint64_t i = 0;
         i < (numBuffersToProduceBeforeReconnect + numBuffersToProduceWhileBuffering)
             * tuplesPerBuffer;
         ++i) {
        ossAfter << std::to_string(i) << std::endl;
        compareStringAfter = ossAfter.str();
    }

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
          waitForReconfig.wait(false);
      }
      if (currentCount >= numBuffersToProduceBeforeReconnect + numBuffersToProduceWhileBuffering) {
          //after writing all remaining tuples into the buffer, give signal to start the new operators to finish the reconnect, tuples will be unbuffered to new destination
          waitForReconnect = true;
          waitForReconnect.notify_all();
      }
      auto valCount = (currentCount - 1) * (numberOfTuplesToProduce);
      auto* records = buffer.getBuffer<Record>();
      for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
          records[u].value = valCount + u;
      }
    };
    auto lambdaSourceType = LambdaSourceType::create(std::move(lambdaSourceFunction),
                                                     numBuffersToProduceBeforeReconnect,
                                                     10,
                                                     GatheringMode::INTERVAL_MODE);
    auto physicalSource = PhysicalSource::create("seq", "test_stream", lambdaSourceType);

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

    wrkConf1->physicalSources.add(physicalSource);

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
    auto lambdaSourceDescriptor = LambdaSourceDescriptor::create(schema,
                                                                 lambdaSourceFunction,
                                                                 totalBuffersToProduce,
                                                                 gatheringValue,
                                                                 GatheringMode::INTERVAL_MODE,
                                                                 0,
                                                                 0,
                                                                 "seq",
                                                                 "test_stream");
    auto lambdaSourceOperatorNode = std::make_shared<SourceLogicalOperatorNode>(lambdaSourceDescriptor, lambdaSourceId);
    queryPlan1->addRootOperator(lambdaSourceOperatorNode);
    //create network sink
    auto networkSourceWrk2Location = NES::Network::NodeLocation(wrk2->getWorkerId(), "localhost", *wrk2DataPort);
    auto networkSourceWrk2Partition = NES::Network::NesPartition(sharedQueryId, networkSrcWrk2Id, 0, 0);
    auto networkSinkDescriptor1 =
        Network::NetworkSinkDescriptor::create(networkSourceWrk2Location, networkSourceWrk2Partition, waitTime, retryTimes);
    auto networkSinkOperatorNode1 = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor1, networkSinkWrk1Id);
    NES::Optimizer::TypeInferencePhaseContext context1({}, {});
    queryPlan1->appendOperatorAsNewRoot(networkSinkOperatorNode1);
    queryPlan1->getSinkOperators().front()->inferSchema(context1);
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
                                                                                std::chrono::milliseconds(1000),
                                                                                5);
    auto sourceOperatorNodeWrk2 = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptorWrk2, networkSrcWrk2Id);
    queryPlan2->addRootOperator(sourceOperatorNodeWrk2);
    //create network sink connected to coordinator
    auto networkSourceCrdLocation =
        NES::Network::NodeLocation(crd->getNesWorker()->getWorkerId(), "localhost", *crdWorkerDataPort);
    auto networkSourceCrdPartition = NES::Network::NesPartition(sharedQueryId, networkSrcCrdId, 0, 0);
    auto networkSinkDescriptorWrk2 =
        Network::NetworkSinkDescriptor::create(networkSourceCrdLocation, networkSourceCrdPartition, waitTime, retryTimes);
    auto networkSinkOperatorNodeWrk2 = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptorWrk2, networkSinkWrk2Id);
    NES::Optimizer::TypeInferencePhaseContext context2({}, {});
    queryPlan2->appendOperatorAsNewRoot(networkSinkOperatorNodeWrk2);
    queryPlan2->getSinkOperators().front()->inferSchema(context2);
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
                                                                            std::chrono::milliseconds(1000),
                                                                            5);
    auto sourceOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSrcCrdId);
    queryPlan->addRootOperator(sourceOperatorNode);
    //create file sink at coordinator
    auto fileSinkDescriptor = FileSinkDescriptor::create(testFile, "CSV_FORMAT", "APPEND");
    auto sinkOperatorNode = std::make_shared<SinkLogicalOperatorNode>(fileSinkDescriptor, fileSinkCrdId);
    NES::Optimizer::TypeInferencePhaseContext context({}, {});
    queryPlan->appendOperatorAsNewRoot(sinkOperatorNode);
    queryPlan->getSinkOperators().front()->inferSchema(context);
    //deploy and start query at coordinator
    auto success_register = crd->getNesWorker()->getNodeEngine()->registerQueryInNodeEngine(queryPlan);
    ASSERT_TRUE(success_register);
    auto success_start = crd->getNesWorker()->getNodeEngine()->startQuery(sharedQueryId);
    ASSERT_TRUE(success_start);

    //wait for data to be written
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringBefore, testFile));

    //reconfiguration

    //reconfigure network sink on wrk1 to point to wrk3 instead of to wrk2
    auto subQueryIds = wrk1->getNodeEngine()->getSubQueryIds(sharedQueryId);
    EXPECT_EQ(subQueryIds.size(), 1);
    //retrieve data about running network sink at wrk1
    auto networkSink = std::dynamic_pointer_cast<Network::NetworkSink>(
        wrk1->getNodeEngine()->getExecutableQueryPlan(subQueryIds.front())->getSinks().front());
    auto uniqueNetworkSinkDescriptorId = networkSink->getUniqueNetworkSinkDescriptorId();
    //trigger sink reconnection to new source on wrk2
    auto networkSourceWrk3Partition = NES::Network::NesPartition(sharedQueryId, networkSrcWrk3Id, 0, 0);
    wrk1->getNodeEngine()->reconfigureNetworkSink(crd->getNesWorker()->getWorkerId(),
                                                  "localhost",
                                                  *wrk3DataPort,
                                                  subQueryIds.front(),//todo: reenable actual id
                                                  uniqueNetworkSinkDescriptorId,
                                                  networkSourceWrk3Partition);

    //reconfig performed but new network source not started yet. tuples are buffered at wrk1

    //notify lambda source that reconfig happened and make it release more tuples into the buffer
    waitForReconfig = true;
    waitForReconfig.notify_all();
    //wait for tuples in order to make sure that the buffer is actually tested
    waitForReconnect.wait(false);
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
                                                                                std::chrono::milliseconds(1000),
                                                                                5);
    auto sourceOperatorNodeWrk3 = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptorWrk3, networkSrcWrk3Id);
    queryPlan3->addRootOperator(sourceOperatorNodeWrk3);
    //create network sink connected to coordinator
    auto networkSinkDescriptorWrk3 =
        Network::NetworkSinkDescriptor::create(networkSourceCrdLocation, networkSourceCrdPartition, waitTime, retryTimes);
    auto networkSinkOperatorNodeWrk3 = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptorWrk3, networkSinkWrk3Id);
    NES::Optimizer::TypeInferencePhaseContext context3({}, {});
    queryPlan3->appendOperatorAsNewRoot(networkSinkOperatorNodeWrk3);
    queryPlan3->getSinkOperators().front()->inferSchema(context3);
    //register and start query on worker 3
    auto success_register_wrk3 = wrk3->getNodeEngine()->registerQueryInNodeEngine(queryPlan3);
    ASSERT_TRUE(success_register_wrk3);
    auto success_start_wrk3 = wrk3->getNodeEngine()->startQuery(sharedQueryId);
    ASSERT_TRUE(success_start_wrk3);

    //check that all tuples arrived and that query terminated successfully

    //todo: reactivate the following check once tuple loss is fixed
    //ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringAfter, testFile));

    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk1));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk3));
    //worker 2 did not receive any soft stop, because the node reconnected
    ASSERT_TRUE(wrk2->getNodeEngine()->stopQuery(sharedQueryId));
    //worker at coordinator expects double number of threads to send soft stop because source reconfig is not implemented yet
    ASSERT_TRUE(crd->getNesWorker()->getNodeEngine()->stopQuery(sharedQueryId));

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

//todo rewrite this test like the other ones with lambda source and without deployment by coordinator and then reenable
TEST_P(QueryRedeploymentIntegrationTest, DISABLED_testReconfigureWhileAlreadyBuffering) {
    uint64_t numBuffersToProduce = 400;
    uint64_t tuplesPerBuffer = 10;
    uint64_t numThreads = GetParam();
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";
    remove(testFile.c_str());

    std::string compareString;
    std::ostringstream oss;
    oss << "seq$value:INTEGER(64 bits)" << std::endl;
    for (uint64_t i = 1; i <= numBuffersToProduce * tuplesPerBuffer; ++i) {
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

    auto schema = Schema::create()->addField(createField("value", BasicType::UINT64));
    crd->getSourceCatalog()->addLogicalSource("seq", schema);

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    wrkConf1->numWorkerThreads.setValue(numThreads);
    wrkConf1->connectSinksAsync.setValue(true);

    auto stype = CSVSourceType::create();
    stype->setFilePath(std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv");
    stype->setNumberOfBuffersToProduce(numBuffersToProduce);
    stype->setNumberOfTuplesToProducePerBuffer(tuplesPerBuffer);
    stype->setGatheringInterval(1);
    auto sequenceSource = PhysicalSource::create("seq", "test_stream", stype);
    wrkConf1->physicalSources.add(sequenceSource);
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
    wrkConf2->numWorkerThreads.setValue(numThreads);
    wrkConf2->connectSinksAsync.setValue(true);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart2);
    ASSERT_TRUE(waitForNodes(5, 3, topology));

    QueryId queryId = crd->getQueryService()->validateAndQueueAddQueryRequest(
        R"(Query::from("seq").sink(FileSinkDescriptor::create(")" + testFile + R"(", "CSV_FORMAT", "APPEND"));)",
        Optimizer::PlacementStrategy::BottomUp,
        FaultToleranceType::NONE,
        LineageType::NONE);

    NES_INFO("Query ID: {}", queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);

    auto startTimestamp = std::chrono::system_clock::now();
    uint64_t recv_tuples = 0;
    uint64_t recv_tuples_after_reconnect = 0;

    //wait for the query to be running
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, crd->getQueryCatalogService(), std::chrono::seconds(10)));
    auto subQueryIds = wrk1->getNodeEngine()->getSubQueryIds(queryId);
    EXPECT_EQ(subQueryIds.size(), 1);

    //retrieve data about running network sink
    auto networkSink = std::dynamic_pointer_cast<Network::NetworkSink>(
        wrk1->getNodeEngine()->getExecutableQueryPlan(subQueryIds.front())->getSinks().front());
    auto uniqueNetworkSinkDescriptorId = networkSink->getUniqueNetworkSinkDescriptorId();

    //create new plan to deploy new source which the sink can reconnect to
    auto subPlanId = 4;
    auto queryPlan = QueryPlan::create(queryId, subPlanId);

    //create source operator
    auto sinkLocation = NES::Network::NodeLocation(wrk1->getWorkerId(), "localhost", *wrk1DataPort);
    auto partition = NES::Network::NesPartition(queryId, 7, 0, 0);
    auto networkSourceDescriptor =
        Network::NetworkSourceDescriptor::create(schema, partition, sinkLocation, std::chrono::milliseconds(1000), 5);
    auto sourceOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, 1);
    queryPlan->addRootOperator(sourceOperatorNode);

    //create new file sink for wrk2
    std::string testFileAfterReconnect = getTestResourceFolder() / "sequence_with_buffering_out_after_reconnect.csv";
    remove(testFileAfterReconnect.c_str());
    auto fileSinkDescriptor = FileSinkDescriptor::create(testFileAfterReconnect, "CSV_FORMAT", "APPEND");
    auto sinkOperatorNode = std::make_shared<SinkLogicalOperatorNode>(fileSinkDescriptor, 2);
    NES::Optimizer::TypeInferencePhaseContext context({}, {});
    queryPlan->appendOperatorAsNewRoot(sinkOperatorNode);
    queryPlan->getSinkOperators().front()->inferSchema(context);

    //deploy and start new query
    auto success_register = wrk2->getNodeEngine()->registerQueryInNodeEngine(queryPlan);
    ASSERT_TRUE(success_register);
    auto success_start = wrk2->getNodeEngine()->startQuery(queryId);
    ASSERT_TRUE(success_start);

    //reconfigure with unreachable target to get sink into buffering state
    auto inexistentPartition = NES::Network::NesPartition(queryId, 8, 0, 0);
    wrk1->getNodeEngine()->reconfigureNetworkSink(9999,
                                                  "localhost",
                                                  *wrk2DataPort,
                                                  subQueryIds.front(),
                                                  uniqueNetworkSinkDescriptorId,
                                                  inexistentPartition);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    //trigger sink reconnection to new source on wrk2 while sink is already buffering
    wrk1->getNodeEngine()->reconfigureNetworkSink(wrk2->getWorkerId(),
                                                  "localhost",
                                                  *wrk2DataPort,
                                                  subQueryIds.front(),
                                                  uniqueNetworkSinkDescriptorId,
                                                  partition);

    while (recv_tuples + recv_tuples_after_reconnect < numBuffersToProduce * tuplesPerBuffer
           && std::chrono::system_clock::now() < startTimestamp + defaultTimeoutInSec) {
        std::ifstream inFile(testFile);
        std::ifstream inFileAfterReconnect(testFileAfterReconnect);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile), std::istreambuf_iterator<char>(), '\n');
        recv_tuples_after_reconnect =
            std::count(std::istreambuf_iterator<char>(inFileAfterReconnect), std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv after buffering: {} + {} = {}",
                  recv_tuples,
                  recv_tuples_after_reconnect,
                  recv_tuples + recv_tuples_after_reconnect)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    //the tuples written to the initial file sink added to the ones written to the file sink of wrk2 should together equal the
    //total amount of tuples produced
    //subtract 2 from the values because each output file includes a header line which is also counted
    EXPECT_EQ(recv_tuples + recv_tuples_after_reconnect - 2, numBuffersToProduce * tuplesPerBuffer);

    int response = remove(testFile.c_str());
    ASSERT_TRUE(response == 0);
    response = remove(testFileAfterReconnect.c_str());
    ASSERT_TRUE(response == 0);

    auto stopSuccess = wrk1->getNodeEngine()->stopQuery(queryId);
    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    auto stopSuccess2 = wrk2->getNodeEngine()->stopQuery(queryId);
    cout << "stopping worker" << endl;
    bool retStopWrk2 = wrk2->stop(false);
    ASSERT_TRUE(retStopWrk2);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

INSTANTIATE_TEST_CASE_P(QueryRedeploymentIntegrationTestParam, QueryRedeploymentIntegrationTest, ::testing::Values(1, 4));
}// namespace NES

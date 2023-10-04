#include <BaseIntegrationTest.hpp>
#include <gtest/gtest.h>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Topology/Topology.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Util/TestUtils.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Network/NetworkSink.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhaseContext.hpp>
//TODO: to remove later
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
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

    TEST_P(QueryRedeploymentIntegrationTest, testPlannedReconnect) {
        const uint64_t numBuffersToProduceBeforeReconnect = 40;
        //todo: add amount that should definitely be buffered
        const uint64_t numBuffersToProduceAfterReconnect = 40;
        //todo: how do these get set?
        const uint64_t tuplesPerBuffer = 512;
        NES_INFO(" start coordinator");
        std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";
        remove(testFile.c_str());

        std::string compareStringBefore;
        std::ostringstream oss;
        oss << "seq$value:INTEGER(64 bits)" << std::endl;
        for (uint64_t i = 0; i < numBuffersToProduceBeforeReconnect * tuplesPerBuffer; ++i) {
            oss << std::to_string(i) << std::endl;
            compareStringBefore = oss.str();
        }

        std::string compareStringAfter;
        std::ostringstream ossAfter;
        //there is no logical source info in the second sink
        ossAfter << "value:INTEGER(64 bits)" << std::endl;
        for (uint64_t i = numBuffersToProduceBeforeReconnect * tuplesPerBuffer; i < (numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect) * tuplesPerBuffer; ++i) {
            ossAfter << std::to_string(i) << std::endl;
            compareStringAfter = ossAfter.str();
        }

        std::atomic<uint64_t> bufferCount = 0;
        std::atomic<bool> waitForReconnect = false;
        std::atomic<bool> waitForFinalCount = false; //todo: do we need this one?
        auto lambdaSourceFunction = [&bufferCount, &waitForReconnect, &waitForFinalCount](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
          struct Record {
              uint64_t value;
          };
          auto currentCount = ++bufferCount;
          if (currentCount > numBuffersToProduceBeforeReconnect) {
              waitForReconnect.wait(false);
          }
          if (currentCount > numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect) {
              waitForFinalCount.wait(false);
          }
          auto valCount = (currentCount - 1) * (numberOfTuplesToProduce);
          auto* records = buffer.getBuffer<Record>();
          for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
              records[u].value = valCount + u;
          }
        };
        auto lambdaSourceType = LambdaSourceType::create(std::move(lambdaSourceFunction), numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect, 10, GatheringMode::INTERVAL_MODE);
        auto physicalSource = PhysicalSource::create("seq", "test_stream", lambdaSourceType);

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
        //crd->getSourceCatalog()->addLogicalSource("seq", schema);

        NES_INFO("start worker 1");
        WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
        wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
        wrkConf1->numWorkerThreads.setValue(GetParam());
        wrkConf1->connectSinksAsync.setValue(true);
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
        //add lambda source
        wrkConf3->physicalSources.add(physicalSource);
        NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
        bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
        ASSERT_TRUE(retStart3);
        ASSERT_TRUE(waitForNodes(5, 4, topology));


        /*
        QueryId queryId = crd->getQueryService()->validateAndQueueAddQueryRequest(
            R"(Query::from("seq").sink(FileSinkDescriptor::create(")" + testFile + R"(", "CSV_FORMAT", "APPEND"));)",
            Optimizer::PlacementStrategy::BottomUp,
            FaultToleranceType::NONE,
            LineageType::NONE);
            */

        //NES_INFO("Query ID: {}", queryId);
        //ASSERT_NE(queryId, INVALID_QUERY_ID);
        QueryId queryId = 1;

//        auto startTimestamp = std::chrono::system_clock::now();
//        uint64_t recv_tuples = 0;
//        uint64_t recv_tuples_after_reconnect = 0;

        //wait for the query to be running
        //ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, crd->getQueryCatalogService(), std::chrono::seconds(10)));

        //todo: deploy initial query


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

        //todo: reactiveate
        ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringBefore, testFile));

        //deploy and start new query
        auto success_register = wrk2->getNodeEngine()->registerQueryInNodeEngine(queryPlan);
        ASSERT_TRUE(success_register);
        auto success_start = wrk2->getNodeEngine()->startQuery(queryId);
        ASSERT_TRUE(success_start);

        //trigger sink reconnection to new source on wrk2
        wrk1->getNodeEngine()->reconfigureNetworkSink(wrk2->getWorkerId(),
                                                      "localhost",
                                                      *wrk2DataPort,
                                                      subQueryIds.front(),
                                                      uniqueNetworkSinkDescriptorId,
                                                      partition);

        waitForReconnect = true;
        waitForReconnect.notify_all();

        ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringAfter, testFileAfterReconnect));
        waitForFinalCount = true;
        waitForFinalCount.notify_all();

//        while (recv_tuples + recv_tuples_after_reconnect < numBuffersToProduce * tuplesPerBuffer
//               && std::chrono::system_clock::now() < startTimestamp + defaultTimeoutInSec) {
//            std::ifstream inFile(testFile);
//            std::ifstream inFileAfterReconnect(testFileAfterReconnect);
//            recv_tuples = std::count(std::istreambuf_iterator<char>(inFile), std::istreambuf_iterator<char>(), '\n');
//            recv_tuples_after_reconnect =
//                std::count(std::istreambuf_iterator<char>(inFileAfterReconnect), std::istreambuf_iterator<char>(), '\n');
//            NES_DEBUG("recv after buffering: {} + {} = {}",
//                      recv_tuples,
//                      recv_tuples_after_reconnect,
//                      recv_tuples + recv_tuples_after_reconnect)
//            std::this_thread::sleep_for(std::chrono::milliseconds(100));
//        }
//
//        //the tuples written to the initial file sink added to the ones written to the file sink of wrk2 should together equal the
//        //total amount of tuples produced
//        //subtract 2 from the values because each output file includes a header line which is also counted
//        EXPECT_EQ(recv_tuples + recv_tuples_after_reconnect - 2, numBuffersToProduce * tuplesPerBuffer);
//        wait1 = false;

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

    //todo #4229: reenable this test
    TEST_P(QueryRedeploymentIntegrationTest, testReconfigureWhileAlreadyBuffering) {
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
}

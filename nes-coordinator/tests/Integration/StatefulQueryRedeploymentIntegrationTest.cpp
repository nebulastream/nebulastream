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
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
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
#include <Plans/Query/QueryPlan.hpp>
#include <API/QueryAPI.hpp>
#include <atomic>
#include <gtest/gtest.h>

namespace NES {

class StatefulQueryRedeploymentIntegrationTest : public Testing::BaseIntegrationTest, public testing::WithParamInterface<uint32_t> {
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

    bool waitForQueryStatus(QueryId queryId, const Catalogs::Query::QueryCatalogPtr& queryCatalog, QueryState state, std::chrono::seconds timeoutInSec) {
        NES_TRACE("TestUtils: wait till the query {} gets into Running status.", queryId);
        auto start_timestamp = std::chrono::system_clock::now();

        NES_TRACE("TestUtils: Keep checking the status of query {} until a fixed time out", queryId);
        while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
            QueryState queryState = queryCatalog->getQueryState(queryId);
            NES_DEBUG("TestUtils: Query {} is now in status {}", queryId, magic_enum::enum_name(queryState));
            switch (queryState) {
                case QueryState::MARKED_FOR_HARD_STOP:
                case QueryState::MARKED_FOR_SOFT_STOP:
                case QueryState::SOFT_STOP_TRIGGERED: {
                    continue;
                }
                case QueryState::STOPPED: {
                    if (state == NES::QueryState::STOPPED) {
                        return true;
                    }
                }
                case QueryState::SOFT_STOP_COMPLETED: {
                    if (state == NES::QueryState::SOFT_STOP_COMPLETED) {
                        return true;
                    }
                }
                case QueryState::RUNNING: {
                    NES_TRACE("TestUtils: Query {} is now in status {}", queryId, magic_enum::enum_name(queryState));
                    if (state == NES::QueryState::RUNNING) {
                        return true;
                    }
                    continue;
                }
                case QueryState::FAILED: {
                    NES_ERROR("Query failed to start. Expected: Running or Optimizing but found {}",
                              magic_enum::enum_name(queryState));
                    return false;
                }
                default: {
                    NES_WARNING("Expected: Running or Scheduling but found {}", magic_enum::enum_name(queryState));
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }
        NES_TRACE("checkCompleteOrTimeout: waitForStart expected results are not reached after timeout");
        return false;
    }

    NesWorkerPtr startSourceWorker(std::string logicalSourceName,
                                   uint64_t& totalBuffersToProduce,
                                   uint64_t& numBuffersToProduceBeforeReconnect,
                                   std::atomic<uint64_t>& bufferCount,
                                   std::atomic<bool>& sourceShouldWait,
                                   bool replay) {
        const uint64_t gatheringValue = 10;

        NES_DEBUG("Start source worker");
        WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
        wrkConf->coordinatorPort.setValue(*rpcCoordinatorPort);
        auto wrkDataPort = getAvailablePort();
        wrkConf->dataPort = *wrkDataPort;
        wrkConf->numWorkerThreads.setValue(1);
        wrkConf->connectSinksAsync.setValue(true);
        wrkConf->connectSourceEventChannelsAsync.setValue(true);
        wrkConf->bufferSizeInBytes.setValue(1024);
        std::string configPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "emptyWorker.yaml";
        wrkConf->configPath = configPath;
        // wrkConf->numberOfBuffersToProduce = numberOfBuffersToProduce;
        wrkConf->numberOfBuffersInGlobalBufferManager = 200000;
        wrkConf->numberOfBuffersInSourceLocalBufferPool = 20000;
        wrkConf->numberOfBuffersPerWorker = 20000;

        if (replay) {
            std::map<std::string, std::string> sourceConfig {
                {Configurations::FILE_PATH_CONFIG, "/Users/danilaferentz/Desktop/experiments/nebulastream/cmake-build/start_source1.csv"},
                {Configurations::SOURCE_GATHERING_INTERVAL_CONFIG, "2"},
                {Configurations::NUMBER_OF_BUFFER_TO_PRODUCE, std::to_string(0)}
            };
            auto csvSourceType = CSVSourceType::create(logicalSourceName,
                                                       "phy_" + logicalSourceName,
                                                       sourceConfig);
            wrkConf->physicalSourceTypes.add(csvSourceType);
        } else {
            auto lambdaSourceFunction = [
                                            &bufferCount, &sourceShouldWait,
                                            &numBuffersToProduceBeforeReconnect, &totalBuffersToProduce](
                                            NES::Runtime::TupleBuffer& buffer,
                                            uint64_t numberOfTuplesToProduce) {
                struct Record {
                    uint64_t id;
                    uint64_t value;
                    uint64_t secretValue;
                    uint64_t timestamp;
                };
                auto currentCount = ++bufferCount;
                if (currentCount > numBuffersToProduceBeforeReconnect) {
                    while (!sourceShouldWait) {
                        std::this_thread::sleep_for(std::chrono::microseconds(500));
                    }
                }
                if (currentCount > totalBuffersToProduce) {
                    sleep(100);
                }//avoid eos
                static std::uint64_t counter = 0;
                static std::uint64_t timestamp = 0;
                auto* records = buffer.getBuffer<Record>();
                for (auto u = 0u; u < 3; ++u) {
                    records[u].id = 1;
                    records[u].value = counter;
                    records[u].secretValue = counter;
                    records[u].timestamp = timestamp;
                }
                for (auto u = 3u; u < numberOfTuplesToProduce; ++u) {
                    records[u].id = 1;
                    records[u].value = 500;
                    records[u].secretValue = 500;
                    records[u].timestamp = timestamp;
                }
                counter++;
                timestamp += 200;
            };
            auto lambdaSourceType = LambdaSourceType::create(logicalSourceName,
                                                             "phy_" + logicalSourceName,
                                                             std::move(lambdaSourceFunction),
                                                             // totalBuffersToProduce,
                                                             totalBuffersToProduce + 1000,//avoid eos
                                                             gatheringValue,
                                                             GatheringMode::INTERVAL_MODE);

            wrkConf->physicalSourceTypes.add(lambdaSourceType);
//            wrkConf->queryCompiler.nautilusBackend = QueryCompilation::NautilusBackend::INTERPRETER;
        }

        NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
        bool resStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
        return wrk;
    }

    NesWorkerPtr startChangingSourceWorker(std::string logicalSourceName,
                                           uint64_t& buffersToProducePerReconnectCycle,
                                           uint64_t& totalBuffersToProduce,
                                           uint64_t& numBuffersToProduceBeforeReconnect,
                                           uint64_t& numBuffersToProduceWhileBuffering,
                                           uint64_t& numBuffersToProduceAfterReconnect,
                                           std::atomic<uint64_t>& bufferCount,
                                           std::atomic<uint64_t>& actualReconnects,
                                           std::atomic<bool>& waitForReconfig,
                                           std::atomic<bool>& waitForReconnect,
                                           std::atomic<bool>& waitForFinalCount,
                                           bool replay) {
        const uint64_t gatheringValue = 10;

        NES_DEBUG("Start source worker");
        WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
        wrkConf->coordinatorPort.setValue(*rpcCoordinatorPort);
        auto wrkDataPort = getAvailablePort();
        wrkConf->dataPort = *wrkDataPort;
        wrkConf->numWorkerThreads.setValue(1);
         wrkConf->connectSinksAsync.setValue(true);
         wrkConf->connectSourceEventChannelsAsync.setValue(true);
        wrkConf->bufferSizeInBytes.setValue(1024);
        std::string configPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "emptyWorker.yaml";
        wrkConf->configPath = configPath;
        // wrkConf->numberOfBuffersToProduce = numberOfBuffersToProduce;
        wrkConf->numberOfBuffersInGlobalBufferManager = 200000;
        wrkConf->numberOfBuffersInSourceLocalBufferPool = 20000;
        wrkConf->numberOfBuffersPerWorker = 20000;
//        wrkConf->queryCompiler.nautilusBackend = QueryCompilation::NautilusBackend::INTERPRETER;

        if (replay) {
            std::map<std::string, std::string> sourceConfig {
                {Configurations::FILE_PATH_CONFIG, "/Users/danilaferentz/Desktop/experiments/nebulastream/cmake-build/start_source2.csv"},
                {Configurations::SOURCE_GATHERING_INTERVAL_CONFIG, "2"},
                {Configurations::NUMBER_OF_BUFFER_TO_PRODUCE, std::to_string(0)}
            };
            auto csvSourceType = CSVSourceType::create(logicalSourceName,
                                                       "phy_" + logicalSourceName,
                                                       sourceConfig);
            wrkConf->physicalSourceTypes.add(csvSourceType);
        } else {
            auto lambdaSourceFunction = [
                                            &bufferCount, &waitForReconfig, &waitForReconnect, &waitForFinalCount, &actualReconnects,
                                            &numBuffersToProduceBeforeReconnect, &numBuffersToProduceWhileBuffering, &numBuffersToProduceAfterReconnect,
                                            &buffersToProducePerReconnectCycle, &totalBuffersToProduce](
                                            NES::Runtime::TupleBuffer& buffer,
                                            uint64_t numberOfTuplesToProduce) {
                struct Record {
                    uint64_t id;
                    uint64_t value;
                    uint64_t secretValue;
                    uint64_t timestamp;
                };
                auto currentCount = ++bufferCount;
                if (currentCount > numBuffersToProduceBeforeReconnect + (actualReconnects * buffersToProducePerReconnectCycle)) {
                    //after sending the specified amount of tuples, wait until the reconfiguration has been triggered, subsequent tuples will be buffered
                    while (!waitForReconfig) {
                        std::this_thread::sleep_for(std::chrono::microseconds(500));
                    }
                }
                if (currentCount > numBuffersToProduceBeforeReconnect + numBuffersToProduceWhileBuffering
                        + (actualReconnects * buffersToProducePerReconnectCycle)) {
                    //after writing some tuples into the buffer, give signal to start the new operators to finish the reconnect, tuples will be unbuffered to new destination
                    waitForReconnect = true;
                }
                if (currentCount > numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect
                        + numBuffersToProduceWhileBuffering + (actualReconnects * buffersToProducePerReconnectCycle)) {
                    while (!waitForFinalCount) {
                        std::this_thread::sleep_for(std::chrono::microseconds(500));
                    }
                }
                if (currentCount > totalBuffersToProduce) {
                    sleep(100);
                }//avoid eos
                static std::uint64_t timestamp = 0;
                static std::uint64_t counter = 0;
                auto* records = buffer.getBuffer<Record>();
                for (auto u = 0u; u < 3; ++u) {
                    records[u].id = 1;
                    records[u].value = counter;
                    records[u].secretValue = counter;
                    records[u].timestamp = timestamp;
                }
                for (auto u = 3u; u < numberOfTuplesToProduce; ++u) {
                    records[u].id = 1;
                    records[u].value = 501;
                    records[u].secretValue = 501;
                    records[u].timestamp = timestamp;
                }
                timestamp += 200;
                counter++;
            };
            auto lambdaSourceType = LambdaSourceType::create(logicalSourceName,
                                                             "phy_" + logicalSourceName,
                                                             std::move(lambdaSourceFunction),
                                                             // totalBuffersToProduce,
                                                             totalBuffersToProduce + 1000,//avoid eos
                                                             gatheringValue,
                                                             GatheringMode::INTERVAL_MODE);

            wrkConf->physicalSourceTypes.add(lambdaSourceType);
        }

        NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
        bool resStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
        return wrk;
    }

    NesWorkerPtr startIntermediateWorker() {
        NES_DEBUG("Start intermediate worker");
        WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
        wrkConf->coordinatorPort.setValue(*rpcCoordinatorPort);
        auto wrkDataPort = getAvailablePort();
        wrkConf->dataPort = *wrkDataPort;
        wrkConf->numWorkerThreads.setValue(1);
         wrkConf->connectSinksAsync.setValue(true);
         wrkConf->connectSourceEventChannelsAsync.setValue(true);
        wrkConf->bufferSizeInBytes.setValue(1024);
        std::string configPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "emptyWorker.yaml";
        wrkConf->configPath = configPath;
        wrkConf->numberOfBuffersInGlobalBufferManager = 200000;
        wrkConf->numberOfBuffersInSourceLocalBufferPool = 20000;
        wrkConf->numberOfBuffersPerWorker = 20000;
        // wrkConf->queryCompiler.nautilusBackend = QueryCompilation::NautilusBackend::INTERPRETER;

        NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
        bool resStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
        return wrk;
    }

    NesCoordinatorPtr startCoordinator(bool enableIncrementalPlacement) {
        NES_DEBUG("Starting coordinator");
        auto coordinatorConfiguration = NES::Configurations::CoordinatorConfiguration::createDefault();
        restPort = getAvailablePort();
        coordinatorConfiguration->rpcPort = *rpcCoordinatorPort;
        coordinatorConfiguration->restPort = *restPort;
        coordinatorConfiguration->logLevel = NES::LogLevel::LOG_NONE;
        std::string configPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "emptyWorker.yaml";
        coordinatorConfiguration->worker.configPath = configPath;
        coordinatorConfiguration->worker.numberOfBuffersInGlobalBufferManager = 200000;
        coordinatorConfiguration->worker.numberOfBuffersInSourceLocalBufferPool = 20000;
        coordinatorConfiguration->worker.numberOfBuffersPerWorker = 20000;
        coordinatorConfiguration->worker.connectSinksAsync.setValue(true);
        coordinatorConfiguration->worker.connectSourceEventChannelsAsync.setValue(true);
        // coordinatorConfiguration->worker.numberOfBuffersToProduce = numberOfBuffersToProduce;
        coordinatorConfiguration->worker.numWorkerThreads.setValue(1);
        coordinatorConfiguration->worker.bufferSizeInBytes = 1024;
        // coordinatorConfiguration->worker.queryCompiler.nautilusBackend = QueryCompilation::NautilusBackend::INTERPRETER;

        NES::Configurations::OptimizerConfiguration optimizerConfiguration;
        optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule;
        optimizerConfiguration.enableIncrementalPlacement = enableIncrementalPlacement;
        optimizerConfiguration.placementAmendmentThreadCount = 4;
        coordinatorConfiguration->optimizer = optimizerConfiguration;

        auto crd = std::make_shared<NesCoordinator>(coordinatorConfiguration);

        crd->startCoordinator(false);
        NES_DEBUG("Coordinator started");
        return crd;
    }
};

constexpr uint32_t DEFAULT_NUMBER_OF_ORIGINS = 0;
constexpr DecomposedQueryPlanVersion DEFAULT_VERSION = 1;

TEST_P(StatefulQueryRedeploymentIntegrationTest, testReplay) {
    uint32_t retryTimes = 10;
    uint64_t numberOfReconnectsToPerform = 1;
    uint64_t numBuffersToProduceBeforeReconnect = 10;
    uint64_t numBuffersToProduceWhileBuffering = 10;
    uint64_t numBuffersToProduceAfterReconnect = 10;
    uint64_t buffersToProducePerReconnectCycle =
        (numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect + numBuffersToProduceWhileBuffering);
    uint64_t totalBuffersToProduce = numberOfReconnectsToPerform * buffersToProducePerReconnectCycle;
    uint64_t gatheringValue = 10;
    std::chrono::seconds waitTime(10);
    uint64_t tuplesPerBuffer = 10;
    uint8_t bytesPerTuple = sizeof(uint64_t);
    NES_INFO("start coordinator");
    std::string testFile = "/Users/danilaferentz/Desktop/experiments/nebulastream/cmake-build/sequence_with_buffering_out.csv";
    remove(testFile.c_str());

    std::atomic<uint64_t> fixedSourceBuffersCount = 0;
    std::atomic<uint64_t> movingSourceBuffersCount = 0;
    std::atomic<uint64_t> actualReconnects = 0;
    std::atomic<bool> waitForReconfig = false;
    std::atomic<bool> waitForReconnect = false;
    std::atomic<bool> waitForFinalCount = false;
    std::atomic<bool> sourceShouldWait = false;

    auto crd = startCoordinator(false);
    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    std::string logicalSource1 = "source_1";
    std::string logicalSource2 = "source_2";
    NES::SchemaPtr schema1 = NES::Schema::create()
                                 ->addField("id1", BasicType::UINT64)
                                 ->addField("value1", BasicType::UINT64)
                                 ->addField("secretValue1", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);

    NES::SchemaPtr schema2 = NES::Schema::create()
                                 ->addField("id2", BasicType::UINT64)
                                 ->addField("value2", BasicType::UINT64)
                                 ->addField("secretValue2", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);
    crd->getSourceCatalog()->addLogicalSource(logicalSource1, schema1);
    crd->getSourceCatalog()->addLogicalSource(logicalSource2, schema2);
    crd->getSourceCatalog()->addLogicalSource("fake_migration_source", schema1);

    NES_DEBUG("start source worker 1");
    auto src1 = startSourceWorker(logicalSource1,
                                  totalBuffersToProduce,
                                  numBuffersToProduceBeforeReconnect,
                                  fixedSourceBuffersCount,
                                  sourceShouldWait,
                                  true);

    ASSERT_TRUE(waitForNodes(5, 2, topology));

    NES_DEBUG("start source worker 2");
    auto changingSrc2 = startChangingSourceWorker(logicalSource2,
                                                  buffersToProducePerReconnectCycle,
                                                  totalBuffersToProduce,
                                                  numBuffersToProduceBeforeReconnect,
                                                  numBuffersToProduceWhileBuffering,
                                                  numBuffersToProduceAfterReconnect,
                                                  movingSourceBuffersCount,
                                                  actualReconnects,
                                                  waitForReconfig,
                                                  waitForReconnect,
                                                  waitForFinalCount,
                                                  true);
    ASSERT_TRUE(waitForNodes(5, 3, topology));

    NES_DEBUG("start inter 1");
    auto inter1 = startIntermediateWorker();
    ASSERT_TRUE(waitForNodes(5, 4, topology));
    NES_DEBUG("start inter 2");
    auto inter2 = startIntermediateWorker();
    ASSERT_TRUE(waitForNodes(5, 5, topology));

    src1->replaceParent(crd->getNesWorker()->getWorkerId(), inter1->getWorkerId());
    changingSrc2->replaceParent(crd->getNesWorker()->getWorkerId(), inter1->getWorkerId());
    // src1->addParent(inter2->getWorkerId());

    auto requestHandlerService = crd->getRequestHandlerService();
    std::cout << crd->getTopology()->toString();
    crd->getTopology()->print();
    auto fileSinkDescriptor = FileSinkDescriptor::create(testFile, "CSV_FORMAT", "APPEND");
    auto query = Query::from(logicalSource1)
                     .joinWith(Query::from(logicalSource2))
                     .where(Attribute("value1") == Attribute("value2"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                     .sink(fileSinkDescriptor);
    //start query
//    auto addQueryRequest =
//    QueryId queryId = crd->getRequestHandlerService()->queueISQPRequest();
    QueryId queryId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);

    std::vector<NesWorkerPtr> reconnectParents;

    // ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, crd->getQueryCatalog()));
    SharedQueryId sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    // ASSERT_NE(sharedQueryId, INVALID_SHARED_QUERY_ID);

    //reconfiguration
    // ASSERT_EQ(inter1->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).size(), 1);
//    auto oldSubplanIdWithVersion = inter1->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();
//    auto joinBeforeMigrationSource = inter1->getNodeEngine()->getExecutableQueryPlan(oldSubplanIdWithVersion.id, oldSubplanIdWithVersion.version)->getSources().front();
//    Network::NesPartition beforeMigrationTargetPartition(sharedQueryId,
//                                                         joinBeforeMigrationSource->getOperatorId(),
//                                                         PartitionId(0),
//                                                         SubpartitionId(0));

    // ASSERT_EQ(crd->getNesWorker()->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).size(), 1);

    while (actualReconnects < numberOfReconnectsToPerform) {
        // ASSERT_EQ(inter1->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(beforeMigrationTargetPartition),
//                  Network::PartitionRegistrationStatus::Registered);

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
        //        ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringBefore, testFile));
        waitForFinalCount = false;
        sleep(5);
//        crd->getRequestHandlerService()->validateAndQueueStopQueryRequest(queryId);
        auto addLinkEvent1 =
            std::make_shared<RequestProcessor::ISQPAddLinkEvent>(inter2->getWorkerId(), changingSrc2->getWorkerId());
        auto removeLinkEvent1 =
            std::make_shared<RequestProcessor::ISQPRemoveLinkEvent>(inter1->getWorkerId(), changingSrc2->getWorkerId());
        auto addLinkEvent2 = std::make_shared<RequestProcessor::ISQPAddLinkEvent>(inter2->getWorkerId(), src1->getWorkerId());
        auto removeLinkEvent2 =
            std::make_shared<RequestProcessor::ISQPRemoveLinkEvent>(inter1->getWorkerId(), src1->getWorkerId());
        crd->getRequestHandlerService()->queueISQPRequest({addLinkEvent1, removeLinkEvent1, addLinkEvent2, removeLinkEvent2});

        // sleep(5);
        sleep(100);
        //notify lambda source that reconfig happened and make it release more tuples into the buffer
        waitForFinalCount = false;
        waitForReconfig = true;
        //wait for tuples in order to make sure that the buffer is actually tested
        while (!waitForReconnect) {
            std::this_thread::sleep_for(std::chrono::microseconds(500));
        }

        //verify that the old partition gets unregistered
        auto timeoutInSec = std::chrono::seconds(TestUtils::defaultTimeout);
        auto start_timestamp = std::chrono::system_clock::now();
//        while (inter1->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(beforeMigrationTargetPartition)
//               == Network::PartitionRegistrationStatus::Registered) {
//            std::this_thread::sleep_for(std::chrono::seconds(1));
//            NES_DEBUG("Partition {} has not yet been unregistered", beforeMigrationTargetPartition);
//            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
//                FAIL();
//            }
//        }
//        ASSERT_NE(inter1->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(beforeMigrationTargetPartition),
//                  Network::PartitionRegistrationStatus::Registered);

        //coordinator side checks
        auto lockedExecutionNode = crd->getGlobalExecutionPlan()->getLockedExecutionNode(inter2->getWorkerId());
        auto updatedPartition =
            std::dynamic_pointer_cast<Network::NetworkSourceDescriptor>(lockedExecutionNode->operator*()
                                                                            ->getAllDecomposedQueryPlans(sharedQueryId)
                                                                            .front()
                                                                            ->getSourceOperators()
                                                                            .front()
                                                                            ->getSourceDescriptor())
                ->getNesPartition();

        //        ASSERT_EQ(inter2->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(updatedPartition),
        //                  Network::PartitionRegistrationStatus::Registered);

        //verify that query has been undeployed from old parent
        while (inter1->getNodeEngine()->getQueryStatus(sharedQueryId) == Runtime::Execution::ExecutableQueryPlanStatus::Running) {
            NES_DEBUG("Query has not yet stopped on worker {}", inter1->getWorkerId());
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_EQ(inter1->getNodeEngine()->getQueryStatus(sharedQueryId),
                  Runtime::Execution::ExecutableQueryPlanStatus::Finished);

        //check that all tuples arrived
        //        ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringAfter, testFile));

        waitForReconfig = false;
        waitForReconnect = false;
        actualReconnects++;
        waitForFinalCount = true;
        sourceShouldWait = true;
    }

    sleep(100);
    int response = remove(testFile.c_str());
    ASSERT_TRUE(response == 0);

    cout << "stopping worker" << endl;
    bool retStopWrk = src1->stop(false);
    ASSERT_TRUE(retStopWrk);

    cout << "stopping worker" << endl;
    bool retStopWrk2 = changingSrc2->stop(false);
    ASSERT_TRUE(retStopWrk2);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/**
 * @brief This tests multiple iterations of inserting VersionDrain events to trigger the reconfiguration of a network sink to point to a new source.
 */
TEST_P(StatefulQueryRedeploymentIntegrationTest, testMultiplePlannedReconnectsFromCrd) {
    uint32_t retryTimes = 10;
    uint64_t numberOfReconnectsToPerform = 1;
    uint64_t numBuffersToProduceBeforeReconnect = 10;
    uint64_t numBuffersToProduceWhileBuffering = 10;
    uint64_t numBuffersToProduceAfterReconnect = 10;
    uint64_t buffersToProducePerReconnectCycle =
        (numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect + numBuffersToProduceWhileBuffering);
    uint64_t totalBuffersToProduce = numberOfReconnectsToPerform * buffersToProducePerReconnectCycle;
    uint64_t gatheringValue = 10;
    std::chrono::seconds waitTime(10);
    uint64_t tuplesPerBuffer = 10;
    uint8_t bytesPerTuple = sizeof(uint64_t);
    NES_INFO(" start coordinator");
    std::string testFile = "/Users/danilaferentz/Desktop/experiments/nebulastream/cmake-build/sequence_with_buffering_out.csv";
    remove(testFile.c_str());

    std::atomic<uint64_t> fixedSourceBuffersCount = 0;
    std::atomic<uint64_t> movingSourceBuffersCount = 0;
    std::atomic<uint64_t> actualReconnects = 0;
    std::atomic<bool> waitForReconfig = false;
    std::atomic<bool> waitForReconnect = false;
    std::atomic<bool> waitForFinalCount = false;
    std::atomic<bool> sourceShouldWait = false;

    auto crd = startCoordinator(true);
    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    std::string logicalSource1 = "source_1";
    std::string logicalSource2 = "source_2";
    NES::SchemaPtr schema1 = NES::Schema::create()
                                 ->addField("id1", BasicType::UINT64)
                                 ->addField("value1", BasicType::UINT64)
                                 ->addField("secretValue1", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);

    NES::SchemaPtr schema2 = NES::Schema::create()
                                 ->addField("id2", BasicType::UINT64)
                                 ->addField("value2", BasicType::UINT64)
                                 ->addField("secretValue2", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);
    crd->getSourceCatalog()->addLogicalSource(logicalSource1, schema1);
    crd->getSourceCatalog()->addLogicalSource(logicalSource2, schema2);
    crd->getSourceCatalog()->addLogicalSource("fake_migration_source", schema1);

    NES_DEBUG("start source worker 1");
    auto src1 = startSourceWorker(logicalSource1,
                                  totalBuffersToProduce,
                                  numBuffersToProduceBeforeReconnect,
                                  fixedSourceBuffersCount,
                                  sourceShouldWait,
                                  false);

    ASSERT_TRUE(waitForNodes(5, 2, topology));

    NES_DEBUG("start source worker 2");
    auto changingSrc2 = startChangingSourceWorker(logicalSource2,
                                                  buffersToProducePerReconnectCycle,
                                                  totalBuffersToProduce,
                                                  numBuffersToProduceBeforeReconnect,
                                                  numBuffersToProduceWhileBuffering,
                                                  numBuffersToProduceAfterReconnect,
                                                  movingSourceBuffersCount,
                                                  actualReconnects,
                                                  waitForReconfig,
                                                  waitForReconnect,
                                                  waitForFinalCount,
                                                  false);
    ASSERT_TRUE(waitForNodes(5, 3, topology));

    NES_DEBUG("start inter 1");
    auto inter1 = startIntermediateWorker();
    ASSERT_TRUE(waitForNodes(5, 4, topology));
    NES_DEBUG("start inter 2");
    auto inter2 = startIntermediateWorker();
    ASSERT_TRUE(waitForNodes(5, 5, topology));

    src1->replaceParent(crd->getNesWorker()->getWorkerId(), inter1->getWorkerId());
    changingSrc2->replaceParent(crd->getNesWorker()->getWorkerId(), inter1->getWorkerId());
    // src1->addParent(inter2->getWorkerId());

    auto requestHandlerService = crd->getRequestHandlerService();
    std::cout << crd->getTopology()->toString();
    crd->getTopology()->print();
    auto fileSinkDescriptor = FileSinkDescriptor::create(testFile, "CSV_FORMAT", "APPEND");
    auto query = Query::from(logicalSource1)
                     .joinWith(Query::from(logicalSource2))
                     .where(Attribute("value1") == Attribute("value2"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                     .sink(fileSinkDescriptor);
    //start query
    QueryId queryId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);

    std::vector<NesWorkerPtr> reconnectParents;

    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, crd->getQueryCatalog()));
    SharedQueryId sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    ASSERT_NE(sharedQueryId, INVALID_SHARED_QUERY_ID);

    //reconfiguration
    ASSERT_EQ(inter1->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).size(), 1);
    auto oldSubplanIdWithVersion = inter1->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();
    auto joinBeforeMigrationSource = inter1->getNodeEngine()->getExecutableQueryPlan(oldSubplanIdWithVersion.id, oldSubplanIdWithVersion.version)->getSources().front();
    Network::NesPartition beforeMigrationTargetPartition(sharedQueryId,
                                                         joinBeforeMigrationSource->getOperatorId(),
                                                         PartitionId(0),
                                                         SubpartitionId(0));

    ASSERT_EQ(crd->getNesWorker()->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).size(), 1);

    while (actualReconnects < numberOfReconnectsToPerform) {
        ASSERT_EQ(inter1->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(beforeMigrationTargetPartition),
                  Network::PartitionRegistrationStatus::Registered);

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
        //        ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringBefore, testFile));
        waitForFinalCount = false;
        //        NES_INFO("start reconnect parent {}", actualReconnects);
        //        WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
        //        wrkConf3->coordinatorPort.setValue(*rpcCoordinatorPort);
        //        auto wrk3DataPort = getAvailablePort();
        //        wrkConf3->dataPort = *wrk3DataPort;
        //        wrkConf3->numWorkerThreads.setValue(GetParam());
        //        wrkConf3->connectSinksAsync.setValue(true);
        //        wrkConf3->connectSourceEventChannelsAsync.setValue(true);
        //        wrkConf3->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
        //        wrkConf3->numberOfSlots.setValue(1);
        //        NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
        //        bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
        //        ASSERT_TRUE(retStart3);
        //        ASSERT_TRUE(waitForNodes(5, 5 + actualReconnects, topology));
        //        wrk3->replaceParent(crd->getNesWorker()->getWorkerId(), wrk0->getWorkerId());
        //        reconnectParents.push_back(wrk3);

        //        std::string compareStringAfter;
        //        std::ostringstream ossAfter;
        //        ossAfter << "seq$value:INTEGER(64 bits)" << std::endl;
        //        for (uint64_t i = 0;
        //             i < (numBuffersToProduceBeforeReconnect + numBuffersToProduceAfterReconnect + numBuffersToProduceWhileBuffering)
        //                 * tuplesPerBuffer * (actualReconnects + 1);
        //             ++i) {
        //            ossAfter << std::to_string(i) << std::endl;
        //        }
        //        compareStringAfter = ossAfter.str();
        auto addLinkEvent1 = std::make_shared<RequestProcessor::ISQPAddLinkEvent>(inter2->getWorkerId(), changingSrc2->getWorkerId());
        auto removeLinkEvent1 =
            std::make_shared<RequestProcessor::ISQPRemoveLinkEvent>(inter1->getWorkerId(), changingSrc2->getWorkerId());
        auto addLinkEvent2 = std::make_shared<RequestProcessor::ISQPAddLinkEvent>(inter2->getWorkerId(), src1->getWorkerId());
        auto removeLinkEvent2 =
            std::make_shared<RequestProcessor::ISQPRemoveLinkEvent>(inter1->getWorkerId(), src1->getWorkerId());
        crd->getRequestHandlerService()->queueISQPRequest({addLinkEvent1, removeLinkEvent1, addLinkEvent2, removeLinkEvent2});

        // sleep(5);
        //notify lambda source that reconfig happened and make it release more tuples into the buffer
        waitForFinalCount = false;
        waitForReconfig = true;
        //wait for tuples in order to make sure that the buffer is actually tested
        while (!waitForReconnect) {
            std::this_thread::sleep_for(std::chrono::microseconds(500));
        }

        //verify that the old partition gets unregistered
        auto timeoutInSec = std::chrono::seconds(TestUtils::defaultTimeout);
        auto start_timestamp= std::chrono::system_clock::now();
        while (inter1->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(beforeMigrationTargetPartition)
               == Network::PartitionRegistrationStatus::Registered) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            NES_DEBUG("Partition {} has not yet been unregistered", beforeMigrationTargetPartition);
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_NE(inter1->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(beforeMigrationTargetPartition),
                  Network::PartitionRegistrationStatus::Registered);

        //coordinator side checks
        auto lockedExecutionNode = crd->getGlobalExecutionPlan()->getLockedExecutionNode(inter2->getWorkerId());
        auto updatedPartition =
            std::dynamic_pointer_cast<Network::NetworkSourceDescriptor>(lockedExecutionNode->operator*()
                                                                            ->getAllDecomposedQueryPlans(sharedQueryId)
                                                                            .front()
                                                                            ->getSourceOperators()
                                                                            .front()
                                                                            ->getSourceDescriptor())
                ->getNesPartition();

        //        ASSERT_EQ(inter2->getNodeEngine()->getPartitionManager()->getConsumerRegistrationStatus(updatedPartition),
        //                  Network::PartitionRegistrationStatus::Registered);

        //verify that query has been undeployed from old parent
        while (inter1->getNodeEngine()->getQueryStatus(sharedQueryId)
               == Runtime::Execution::ExecutableQueryPlanStatus::Running) {
            NES_DEBUG("Query has not yet stopped on worker {}", inter1->getWorkerId());
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_EQ(inter1->getNodeEngine()->getQueryStatus(sharedQueryId),
                  Runtime::Execution::ExecutableQueryPlanStatus::Finished);

        //check that all tuples arrived
        //        ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareStringAfter, testFile));

        waitForReconfig = false;
        waitForReconnect = false;
        actualReconnects++;
        waitForFinalCount = true;
        sourceShouldWait = true;
    }

    //use marker to shut down to not cause coordinator side errors
    //create marker
    //    auto reconfigMarker = ReconfigurationMarker::create();
    //    auto metadata = std::make_shared<DrainQueryMetadata>(1);
    //    auto event = ReconfigurationMarkerEvent::create(QueryState::RUNNING, metadata);
    //    reconfigMarker->addReconfigurationEvent(wrk1->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);
    //    reconfigMarker->addReconfigurationEvent(oldWorker->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);
    //    reconfigMarker->addReconfigurationEvent(wrk0->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).back(), event);
    //    reconfigMarker->addReconfigurationEvent(crd->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);
    //
    //    //insert reconfig marker at the source
    //    auto decomposedQueryIdWithVersion = wrk1->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();
    //    auto dataSource = wrk1->getNodeEngine()
    //                          ->getExecutableQueryPlan(decomposedQueryIdWithVersion.id, decomposedQueryIdWithVersion.version)
    //                          ->getSources()
    //                          .front();
    //    dataSource->handleReconfigurationMarker(reconfigMarker);
    //
    //    //send the last tuples, after which the lambda source shuts down
    //    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk1));
    //
    //    auto lastReconnectParent = reconnectParents.back();
    //    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, lastReconnectParent));
    //    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk2));
    //    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, crd->getNesWorker()));
    //    cout << "stopping worker" << endl;
    //    bool retStopLastParent = lastReconnectParent->stop(false);
    //    ASSERT_TRUE(retStopLastParent);
    //    reconnectParents.pop_back();
    //    for (auto parent : reconnectParents) {
    //        NES_DEBUG("stopping parent node")
    //        ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, parent));
    //        cout << "stopping worker" << endl;
    //        bool stopParent = parent->stop(false);
    //        ASSERT_TRUE(stopParent);
    //    }

    sleep(100);
    int response = remove(testFile.c_str());
    ASSERT_TRUE(response == 0);

    cout << "stopping worker" << endl;
    bool retStopWrk = src1->stop(false);
    ASSERT_TRUE(retStopWrk);

    cout << "stopping worker" << endl;
    bool retStopWrk2 = changingSrc2->stop(false);
    ASSERT_TRUE(retStopWrk2);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

INSTANTIATE_TEST_CASE_P(QueryRedeploymentIntegrationTestParam, StatefulQueryRedeploymentIntegrationTest, ::testing::Values(1, 4));
}// namespace NES

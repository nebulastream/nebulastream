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
#include <API/QueryAPI.hpp>
#include <API/TestSchemas.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Exceptions/InvalidQueryException.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Coordinator/LogicalSourceType.hpp>
#include <Configurations/Coordinator/SchemaType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Network/NetworkSink.hpp>
#include <Network/NetworkSource.hpp>
#include <Reconfiguration/Metadata/DrainQueryMetadata.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sources/DataSource.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <absl/types/compare.h>

using namespace std;

namespace NES {

using namespace Configurations;

class QueryReconfigurationTest : public Testing::BaseIntegrationTest, public testing::WithParamInterface<uint32_t> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryReconfigurationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryReconfiguraionTest test class.");
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

    //start the coordinator and return a pointer to it
    void startCoordinator() {
        NES_INFO("rest port = {}", *restPort);

        CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
        coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
        coordinatorConfig->restPort.setValue(*restPort);
        auto crdWorkerDataPort = getAvailablePort();
        coordinatorConfig->worker.dataPort = *crdWorkerDataPort;
        //todo: fix these
        coordinatorConfig->worker.connectSinksAsync.setValue(true);
        coordinatorConfig->worker.connectSourceEventChannelsAsync.setValue(true);
        coordinatorConfig->worker.bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);
        coordinatorConfig->worker.numWorkerThreads.setValue(4);//todo: check with 4 also
        NES_INFO("start coordinator")
        crd = std::make_shared<NesCoordinator>(coordinatorConfig);
        uint64_t port = crd->startCoordinator(/**blocking**/ false);
        NES_INFO("coordinator started successfully");

        topology = crd->getTopology();
        if (!waitForNodes(5, 1, topology)) {
            FAIL();
        }
        workers[crd->getNesWorker()->getWorkerId()] = crd->getNesWorker();
    }

    //start a worker and return the pointer
    NesWorkerPtr startWorker(std::vector<uint64_t> sourceIndices) {
        NES_INFO("start worker");
        WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
        wrkConf->coordinatorPort.setValue(*rpcCoordinatorPort);
        auto wrkDataPort = getAvailablePort();
        wrkConf->dataPort = *wrkDataPort;
        wrkConf->numWorkerThreads.setValue(4);//todo: check with 4 also
        //todo: fix these
        wrkConf->connectSinksAsync.setValue(true);
        wrkConf->connectSourceEventChannelsAsync.setValue(true);
        wrkConf->bufferSizeInBytes.setValue(tuplesPerBuffer * bytesPerTuple);

        for (auto& i : sourceIndices) {
            auto sourceType = sourceTypes.at(i);
            wrkConf->physicalSourceTypes.add(sourceType);
        }

        NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
        bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
        auto workerId = wrk->getWorkerId();
        workers[wrk->getWorkerId()] = wrk;

        for (auto& i : sourceIndices) {
            auto sourceType = sourceTypes.at(i);
            auto physicalSourceName = sourceType->getPhysicalSourceName();
            systemSourceIdentifierToTestLocalIdentifier[std::make_tuple(workerId, physicalSourceName)] = i;
        }

        auto nodeCount = workers.size();
        if (!retStart || !waitForNodes(5, nodeCount, topology)) {
            return nullptr;
        }
        return wrk;
    }

    NesWorkerPtr startWorkerAsChildOf(std::vector<uint64_t> sourceIndices, NesWorkerPtr parent) {
        auto wrk = startWorker(sourceIndices);
        if (wrk == nullptr) {
            return nullptr;
        }
        wrk->replaceParent(crd->getNesWorker()->getWorkerId(), parent->getWorkerId());
        return wrk;
    }

    NesWorkerPtr startWorkerWithLambdaSource(std::string logicalSourceName, std::string physicalSourceName, NesWorkerPtr parent) {
        auto localSourceId = addLogicalSourceAndCreatePhysicalSourceType(logicalSourceName, physicalSourceName);
        return startWorkerAsChildOf({localSourceId}, parent);
    }

    uint64_t addLogicalSourceAndCreatePhysicalSourceType(std::string logicalSourceName, std::string physicalSourceName) {
        //init vars
        auto newId = bufferCounts.size();
        bufferCounts[newId] = 0;
        finalBufferCounts[newId] = 0;
        stopTriggers[newId] = false;

        //get vars
        auto& bufferCount = bufferCounts.at(newId);
        auto& finalBufferCount = finalBufferCounts.at(newId);
        auto& stopTrigger = stopTriggers[newId];

        //check if logical source exists in catalog and if not, create it
        auto logicalSource = crd->getSourceCatalog()->getLogicalSource(logicalSourceName);
        if (logicalSource == nullptr) {
            crd->getSourceCatalog()->addLogicalSource(logicalSourceName, schema);
        }

        auto lambdaSourceFunction = [&bufferCount, &stopTrigger, &finalBufferCount](NES::Runtime::TupleBuffer& buffer,
                                                                                    uint64_t numberOfTuplesToProduce) {
            if (stopTrigger) {
                finalBufferCount = bufferCount.load();
                return;
            }
            struct Record {
                uint64_t value;
            };
            bufferCount += 1;
            auto valCount = (bufferCount.load() - 1) * (numberOfTuplesToProduce);
            auto* records = buffer.getBuffer<Record>();
            for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
                records[u].value = valCount + u;
            }
        };

        auto totalBuffersToProduce = 0;//indicates to produce indefinitely until lambda does not output anymore
        auto lambdaSourceType = LambdaSourceType::create(logicalSourceName,
                                                         physicalSourceName,
                                                         std::move(lambdaSourceFunction),
                                                         totalBuffersToProduce,
                                                         gatheringValue,
                                                         GatheringMode::INTERVAL_MODE);
        sourceTypes[newId] = lambdaSourceType;
        return newId;
    }

    void stop() {

        for (auto& [workerId, wrk] : workers) {
            cout << "stopping worker" << endl;
            bool retStopWrk = wrk->stop(false);
            ASSERT_TRUE(retStopWrk);
        }

        cout << "stopping coordinator" << endl;
        bool retStopCord = crd->stopCoordinator(false);
        ASSERT_TRUE(retStopCord);
    }

    uint64_t tuplesPerBuffer = 10;
    uint8_t bytesPerTuple = sizeof(uint64_t);
    TopologyPtr topology;
    NesCoordinatorPtr crd;
    std::map<WorkerId, NesWorkerPtr> workers;
    std::map<std::tuple<WorkerId, std::string>, uint64_t> systemSourceIdentifierToTestLocalIdentifier;
    std::map<uint64_t, std::atomic<uint64_t>> bufferCounts;
    std::map<uint64_t, std::atomic<uint64_t>> finalBufferCounts;
    std::map<uint64_t, LambdaSourceTypePtr> sourceTypes;
    std::map<uint64_t, std::atomic<bool>> stopTriggers;
    uint64_t gatheringValue = 10;
    SchemaPtr schema = Schema::create()->addField(createField("value", BasicType::UINT64));
};

TEST_F(QueryReconfigurationTest, testMarkerPropagation) {
    auto logicalSourceName = "seq";
    auto physicalSourceName = "test_stream";
    startCoordinator();
    ASSERT_NE(crd, nullptr);
    auto wrk = startWorker({});
    ASSERT_NE(wrk, nullptr);
    auto wrk2 = startWorkerAsChildOf({}, wrk);
    ASSERT_NE(wrk2, nullptr);
    auto wrk3 = startWorkerWithLambdaSource(logicalSourceName, physicalSourceName, wrk2);
    ASSERT_NE(wrk3, nullptr);

    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";
    remove(testFile.c_str());

    //start query
    auto query1 = Query::from("seq").sink(FileSinkDescriptor::create(testFile, "CSV_FORMAT", "APPEND"));

    QueryId queryId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(query1.getQueryPlan(),
                                                                                       Optimizer::PlacementStrategy::BottomUp);
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, crd->getQueryCatalog()));

    //get the executable plan
    SharedQueryId sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    auto decomposedPlanId = wrk3->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();
    auto decompPlan = wrk3->getNodeEngine()->getExecutableQueryPlan(decomposedPlanId);

    //get the source to reconfigure
    auto wrk3Source = decompPlan->getSources().front();

    //create marker
    auto reconfigMarker = ReconfigurationMarker::create();
    auto metadata = std::make_shared<DrainQueryMetadata>(1);
    auto event = ReconfigurationMarkerEvent::create(QueryState::RUNNING, metadata);
    reconfigMarker->addReconfigurationEvent(decomposedPlanId, event);
    reconfigMarker->addReconfigurationEvent(wrk2->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);
    reconfigMarker->addReconfigurationEvent(wrk->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);
    reconfigMarker->addReconfigurationEvent(crd->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);

    //insert reconfig marker at the source
    wrk3Source->handleReconfigurationMarker(reconfigMarker);

    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk3));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk2));

    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, crd->getNesWorker()));

    stop();
}

TEST_F(QueryReconfigurationTest, testPlanIdMatching) {
    auto logicalSourceName = "seq";
    auto physicalSourceName = "test_stream";
    startCoordinator();
    ASSERT_NE(crd, nullptr);
    auto wrk = startWorker({});
    ASSERT_NE(wrk, nullptr);
    auto wrk2 = startWorkerAsChildOf({}, wrk);
    ASSERT_NE(wrk2, nullptr);
    auto wrk3 = startWorkerWithLambdaSource(logicalSourceName, physicalSourceName, wrk2);
    ASSERT_NE(wrk3, nullptr);

    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";
    remove(testFile.c_str());

    //start query
    QueryId queryId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(
        R"(Query::from("seq").map(Attribute("value") = Attribute("value")).sink(FileSinkDescriptor::create(")" + testFile
            + R"(", "CSV_FORMAT", "APPEND"));)",
        Optimizer::PlacementStrategy::BottomUp);
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, crd->getQueryCatalog()));

    //get the executable plan
    SharedQueryId sharedQueryId = crd->getGlobalQueryPlan()->getSharedQueryId(queryId);
    auto decomposedPlanId = wrk3->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();
    auto decompPlan = wrk3->getNodeEngine()->getExecutableQueryPlan(decomposedPlanId);

    //get the source to reconfigure
    auto wrk3Source = decompPlan->getSources().front();

    //create marker
    auto reconfigMarker = ReconfigurationMarker::create();
    auto metadata = std::make_shared<DrainQueryMetadata>(1);
    auto event = ReconfigurationMarkerEvent::create(QueryState::RUNNING, metadata);
    reconfigMarker->addReconfigurationEvent(decomposedPlanId, event);
    reconfigMarker->addReconfigurationEvent(wrk2->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);

    //insert reconfig marker at the source
    wrk3Source->handleReconfigurationMarker(reconfigMarker);

    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk3));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk2));

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    ASSERT_EQ(wrk->getNodeEngine()->getQueryStatus(sharedQueryId), Runtime::Execution::ExecutableQueryPlanStatus::Running);
    ASSERT_EQ(crd->getNodeEngine()->getQueryStatus(sharedQueryId), Runtime::Execution::ExecutableQueryPlanStatus::Running);

    //create marker
    reconfigMarker = ReconfigurationMarker::create();
    metadata = std::make_shared<DrainQueryMetadata>(1);
    event = ReconfigurationMarkerEvent::create(QueryState::RUNNING, metadata);
    reconfigMarker->addReconfigurationEvent(wrk->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);
    reconfigMarker->addReconfigurationEvent(crd->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front(), event);

    decomposedPlanId = wrk->getNodeEngine()->getDecomposedQueryIds(sharedQueryId).front();
    decompPlan = wrk->getNodeEngine()->getExecutableQueryPlan(decomposedPlanId);

    //get the source to reconfigure
    auto wrkSource = decompPlan->getSources().front();
    wrkSource->handleReconfigurationMarker(reconfigMarker);

    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, wrk));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeoutAtWorker(sharedQueryId, crd->getNesWorker()));
    stop();
}
}// namespace NES

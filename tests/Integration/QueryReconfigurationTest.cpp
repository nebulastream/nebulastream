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

#include <Catalogs/LambdaSourceStreamConfig.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <algorithm>
#include <iostream>

using namespace std;

namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class QueryReconfigurationTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("QueryReconfigurationTest.log", NES::LOG_INFO);
        NES_INFO("Setup QueryReconfigurationTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() { NES_INFO("Tear down QueryReconfigurationTest class."); }
};

NesWorkerPtr
startWorker(WorkerConfigPtr wrkConf, uint16_t coordinatorPort, uint16_t workerId, NesNodeType nesNodeType, bool connect) {
    NES_INFO("QueryReconfigurationTest: Start worker: " << workerId);
    wrkConf->setCoordinatorPort(coordinatorPort);
    wrkConf->setRpcPort(coordinatorPort + 10 * workerId);
    wrkConf->setDataPort(coordinatorPort + (10 * workerId) + 1);
    wrkConf->setNumWorkerThreads(2);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(wrkConf, nesNodeType);
    EXPECT_TRUE(wrk->start(/**blocking**/ false, /**withConnect**/ connect));
    NES_INFO("QueryReconfigurationTest: Worker: " << workerId << " started successfully");
    return wrk;
}

void stopWorker(NesWorkerPtr wrk, uint16_t workerId) {
    NES_INFO("QueryReconfigurationTest: Stop worker " << workerId);
    EXPECT_TRUE(wrk->stop(true));
    NES_INFO("QueryReconfigurationTest: Stopped worker " << workerId);
}

std::function<void(NES::NodeEngine::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)> generatorLambda(uint64_t origin,
                                                                                                            uint64_t sleepTime) {
    return [origin, sleepTime](NES::NodeEngine::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
        struct Record {
            uint64_t id;
            uint64_t value;
            uint64_t timestamp;
            uint64_t originId;
        };
        std::this_thread::sleep_for(std::chrono::seconds{sleepTime});
        auto records = buffer.getBuffer<Record>();
        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            records[u].id = u;
            records[u].value = u % 10;
            records[u].timestamp = time(nullptr);
            records[u].originId = origin;
        }
        return;
    };
}

std::pair<SchemaPtr, Optimizer::TypeInferencePhasePtr> getDummyNetworkSrcSchema(SchemaPtr schema,
                                                                                Network::NesPartition nesPartition) {
    auto streamCatalog = std::make_shared<StreamCatalog>();
    const char* logicalStreamName = "default";
    streamCatalog->addLogicalStream(logicalStreamName, schema);
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto src = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create(logicalStreamName),
                                                            UtilityFunctions::getNextOperatorId());
    Network::NodeLocation location{1, "localhost", static_cast<uint32_t>(11001)};
    SinkDescriptorPtr baseSinkDescPtr =
        Network::NetworkSinkDescriptor::create(location, nesPartition, std::chrono::seconds(60), 2);
    OperatorNodePtr sink = LogicalOperatorFactory::createSinkOperator(baseSinkDescPtr, UtilityFunctions::getNextOperatorId());
    auto queryPlan = QueryPlan::create(src);
    queryPlan->appendOperatorAsNewRoot(sink);
    auto tQueryPlan = typeInferencePhase->execute(queryPlan);
    return std::pair<SchemaPtr, Optimizer::TypeInferencePhasePtr>(sink->getOutputSchema(), typeInferencePhase);
}

TEST_F(QueryReconfigurationTest, testPartitionPinningQepStarts) {
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    auto port = 11000;

    auto schema = Schema::create()->addField("id", UINT64);

    Network::NesPartition nesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};

    NesWorkerPtr wrk1 = startWorker(wrkConf, port, 1, NesNodeType::Worker, false);
    auto nodeEngine = wrk1->getNodeEngine();
    auto queryManager = nodeEngine->getQueryManager();
    auto partitionManager = nodeEngine->getPartitionManager();
    auto defaultOutputSchema = getDummyNetworkSrcSchema(schema, nesPartition);

    auto networkSrc = LogicalOperatorFactory::createSourceOperator(
        Network::NetworkSourceDescriptor::create(defaultOutputSchema.first, nesPartition),
        nesPartition.getOperatorId());

    auto dataSink = LogicalOperatorFactory::createSinkOperator(FileSinkDescriptor::create("testPartitionPinning_1.out"),
                                                               UtilityFunctions::getNextOperatorId());

    auto qsp1 = QueryPlan::create(networkSrc);
    qsp1->appendOperatorAsNewRoot(dataSink);
    qsp1->setQueryId(1);
    qsp1->setQuerySubPlanId(1);
    auto tqsp1 = defaultOutputSchema.second->execute(qsp1);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp1->getQuerySubPlanId() << ".\n" << tqsp1->toString());

    nodeEngine->registerQueryForReconfigurationInNodeEngine(tqsp1);

    ASSERT_EQ(queryManager->getQepStatus(1) == NodeEngine::Execution::ExecutableQueryPlanStatus::Invalid, true);

    // Mimic announcement message from producers
    auto numberOfProducers = 100;
    auto reconfigurationPlan = QueryReconfigurationPlan::create(std::vector<QuerySubPlanId>{1, 222},
                                                                std::vector<QuerySubPlanId>{333},
                                                                std::unordered_map<QuerySubPlanId, QuerySubPlanId>{{444, 555}});
    auto announcementThreads = std::vector<std::thread>();
    for (uint32_t i = 0; i < numberOfProducers; i++) {
        std::thread producerThread([&, i] {
            const Network::ChannelId& id = Network::ChannelId{0, nesPartition, i};
            Network::Messages::ClientAnnounceMessage msg{id};
            usleep((rand() % numberOfProducers) * 1000);
            nodeEngine->onClientAnnouncement(msg);
            nodeEngine->onQueryReconfiguration(id, reconfigurationPlan);
        });
        announcementThreads.emplace_back(std::move(producerThread));
    }

    for (std::thread& t : announcementThreads) {
        if (t.joinable()) {
            t.join();
        }
    }

    ASSERT_EQ(partitionManager->getSubpartitionCounter(nesPartition), numberOfProducers);

    nodeEngine->onEndOfStream(Network::Messages::EndOfStreamMessage{Network::ChannelId{0, nesPartition, 1}, true});
    stopWorker(wrk1, 1);
}

TEST_F(QueryReconfigurationTest, testPartitionPinningQepReplacements) {
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    auto port = 11000;

    auto schema = Schema::create()->addField("id", UINT64);

    Network::NesPartition nesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};

    NesWorkerPtr wrk1 = startWorker(wrkConf, port, 1, NesNodeType::Worker, false);
    auto nodeEngine = wrk1->getNodeEngine();
    auto queryManager = nodeEngine->getQueryManager();
    auto partitionManager = nodeEngine->getPartitionManager();
    auto schemaHelperQsp1 = getDummyNetworkSrcSchema(schema, nesPartition);
    auto numberOfProducers = 100;

    auto networkSrc = LogicalOperatorFactory::createSourceOperator(
        Network::NetworkSourceDescriptor::create(schemaHelperQsp1.first, nesPartition),
        nesPartition.getOperatorId());

    auto dataSink1 =
        LogicalOperatorFactory::createSinkOperator(FileSinkDescriptor::create("testPartitionPinningQepReplacements_1.out"),
                                                   UtilityFunctions::getNextOperatorId());

    auto qsp1 = QueryPlan::create(networkSrc);
    qsp1->appendOperatorAsNewRoot(dataSink1);
    qsp1->setQueryId(1);
    qsp1->setQuerySubPlanId(1);
    auto tqsp1 = schemaHelperQsp1.second->execute(qsp1);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp1->getQuerySubPlanId() << ".\n" << tqsp1->toString());

    nodeEngine->registerQueryInNodeEngine(tqsp1);
    nodeEngine->startQuery(1);
    ASSERT_EQ(queryManager->getQepStatus(1) == NodeEngine::Execution::ExecutableQueryPlanStatus::Running, true);

    auto announcementThreads = std::vector<std::thread>();
    for (uint32_t i = 0; i < numberOfProducers; i++) {
        std::thread producerThread([&, i] {
            const Network::ChannelId& id = Network::ChannelId{0, nesPartition, i};
            Network::Messages::ClientAnnounceMessage msg{id};
            nodeEngine->onClientAnnouncement(msg);
        });
        announcementThreads.emplace_back(std::move(producerThread));
    }

    for (std::thread& t : announcementThreads) {
        if (t.joinable()) {
            t.join();
        }
    }

    ASSERT_EQ(partitionManager->getSubpartitionCounter(nesPartition), numberOfProducers);

    auto dataSink2 =
        LogicalOperatorFactory::createSinkOperator(FileSinkDescriptor::create("testPartitionPinningQepReplacements_2.out"),
                                                   UtilityFunctions::getNextOperatorId());
    auto qsp2 = QueryPlan::create(networkSrc);
    qsp2->appendOperatorAsNewRoot(dataSink2);
    qsp2->setQueryId(1);
    qsp2->setQuerySubPlanId(2);
    auto tqsp2 = schemaHelperQsp1.second->execute(qsp2);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp2->getQuerySubPlanId() << ".\n" << tqsp2->toString());

    nodeEngine->registerQueryForReconfigurationInNodeEngine(tqsp2);

    ASSERT_EQ(queryManager->getQepStatus(2) == NodeEngine::Execution::ExecutableQueryPlanStatus::Invalid, true);

    auto reconfigurationPlan = QueryReconfigurationPlan::create(std::vector<QuerySubPlanId>{222},
                                                                std::vector<QuerySubPlanId>{333},
                                                                std::unordered_map<QuerySubPlanId, QuerySubPlanId>{{1, 2}});

    auto reconfigurationThreads = std::vector<std::thread>();
    for (uint32_t i = 0; i < numberOfProducers; i++) {
        std::thread reconfigurationThread([&, i] {
            const Network::ChannelId& id = Network::ChannelId{0, nesPartition, i};
            nodeEngine->onQueryReconfiguration(id, reconfigurationPlan);
        });
        reconfigurationThreads.emplace_back(std::move(reconfigurationThread));
    }

    for (std::thread& t : reconfigurationThreads) {
        if (t.joinable()) {
            t.join();
        }
    }

    ASSERT_EQ(partitionManager->getSubpartitionCounter(nesPartition), numberOfProducers);

    nodeEngine->onEndOfStream(Network::Messages::EndOfStreamMessage{Network::ChannelId{0, nesPartition, 1}, true});
    stopWorker(wrk1, 1);
}

std::function<bool(std::vector<std::vector<std::string>>)>
distinctColumnValuesValidator(uint8_t colId, std::unordered_set<std::string> requiredValues) {
    return [colId, requiredValues](std::vector<std::vector<std::string>> rows) {
        auto colPos = colId - 1;
        std::unordered_set<std::string> actualValues;
        for (auto row : rows) {
            actualValues.insert(row[colPos]);
            if (requiredValues == actualValues) {
                return true;
            }
        }
        return false;
    };
}

/**
 * Test adding a new branch for a QEP in Level 3
 *
 * Running Query:
 *
 * Lambda Source 1 (1) ----> Network Sink 1 (1) --|
 *                                                |----> Network Source 3 (3) --|
 *                                                                              |
 *                                                                              -----> Map 1 (3) ----> Network Sink 3 (3) ----> Network Source 5 (4) ----> File Sink 1 (4)
 *                                                                              |
 *                                                |----> Network Source 4 (3)--|
 * Lambda Source 2 (2) ----> Network Sink 2 (2) --|
 *
 *
 * Running Query: Add new branch to QEP running on Node 3
 *
 * Lambda Source 1 (1) ----> Network Sink 1 (1) --|
 *                                                |----> Network Source 3 (3)---|
 *                                                                              |
 *                                                                              |                  |--> Map 2 (3)--> Network Sink 4 (3) ----> Network Source 6 (4) ----> File Sink 2 (4)
 *                                                                              |                  |
 *                                                                              -----> Map 1 (3) --|
 *                                                                              |                  |
 *                                                                              |                  |--> Network Sink 3 (3) ----> Network Source 5 (4) ----> File Sink 1 (4)
 *                                                                              |
 *                                                |----> Network Source 4 (3)---|
 * Lambda Source 2 (2) ----> Network Sink 2 (2) --|
 *
 *
 */
TEST_F(QueryReconfigurationTest, testReconfigurationNewBranchOnLevel3) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);
    NES_INFO("QueryReconfigurationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);

    auto phase = Optimizer::TypeInferencePhase::create(crd->getStreamCatalog());

    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryReconfigurationTest: Coordinator started successfully");

    NesWorkerPtr wrk1 = startWorker(wrkConf, port, 1, NesNodeType::Sensor, true);
    NesWorkerPtr wrk2 = startWorker(wrkConf, port, 2, NesNodeType::Sensor, true);
    NesWorkerPtr wrk3 = startWorker(wrkConf, port, 3, NesNodeType::Worker, true);
    NesWorkerPtr wrk4 = startWorker(wrkConf, port, 4, NesNodeType::Worker, true);

    std::string testSchema =
        "Schema::create()->addField(\"id\", BasicType::UINT64)->addField(\"value\", "
        "BasicType::UINT64)->addField(\"timestamp\", BasicType::UINT64)->addField(\"originId\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();

    wrk1->registerLogicalStream("car", testSchemaFileName);
    wrk2->registerLogicalStream("car", testSchemaFileName);

    //register physical stream - wrk1
    NES::AbstractPhysicalStreamConfigPtr confCar1 =
        NES::LambdaSourceStreamConfig::create("LambdaSource", "car1", "car", generatorLambda(1, 2), 0, 5, "frequency");
    wrk1->registerPhysicalStream(confCar1);

    //register physical stream - wrk2
    NES::AbstractPhysicalStreamConfigPtr confCar2 =
        NES::LambdaSourceStreamConfig::create("LambdaSource", "car2", "car", generatorLambda(2, 4), 0, 5, "frequency");
    wrk2->registerPhysicalStream(confCar2);

    auto wrk1Src = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"),
                                                                UtilityFunctions::getNextOperatorId());
    auto wrk2Src = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"),
                                                                UtilityFunctions::getNextOperatorId());

    Network::NesPartition wrk13NSrcNesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};
    Network::NesPartition wrk23NSrcNesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};

    Network::NodeLocation wrk3NSrcLocation{3, "localhost", static_cast<uint32_t>(port + 31)};

    const chrono::duration<int64_t>& sinkConnnectionWaitTime = std::chrono::seconds(60);
    int connectionRetries = 2;
    SinkDescriptorPtr wrk13NSinkDescPtr = Network::NetworkSinkDescriptor::create(wrk3NSrcLocation,
                                                                                 wrk13NSrcNesPartition,
                                                                                 sinkConnnectionWaitTime,
                                                                                 connectionRetries);
    SinkDescriptorPtr wrk23NSinkDescPtr = Network::NetworkSinkDescriptor::create(wrk3NSrcLocation,
                                                                                 wrk23NSrcNesPartition,
                                                                                 sinkConnnectionWaitTime,
                                                                                 connectionRetries);

    OperatorNodePtr wrk1NSink =
        LogicalOperatorFactory::createSinkOperator(wrk13NSinkDescPtr, UtilityFunctions::getNextOperatorId());
    OperatorNodePtr wrk2NSink =
        LogicalOperatorFactory::createSinkOperator(wrk23NSinkDescPtr, UtilityFunctions::getNextOperatorId());

    auto qsp1 = QueryPlan::create(wrk1Src);
    qsp1->appendOperatorAsNewRoot(wrk1NSink);
    qsp1->setQueryId(1);
    qsp1->setQuerySubPlanId(1);
    auto tqsp1 = phase->execute(qsp1);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp1->getQuerySubPlanId() << ".\n" << tqsp1->toString());

    auto qsp2 = QueryPlan::create(wrk2Src);
    qsp2->appendOperatorAsNewRoot(wrk2NSink);
    qsp2->setQueryId(1);
    qsp2->setQuerySubPlanId(2);
    auto tqsp2 = phase->execute(qsp2);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp2->getQuerySubPlanId() << ".\n" << tqsp2->toString());

    wrk1->getNodeEngine()->registerQueryInNodeEngine(tqsp1);
    wrk1->getNodeEngine()->startQuery(tqsp1->getQueryId());

    wrk2->getNodeEngine()->registerQueryInNodeEngine(tqsp2);
    wrk2->getNodeEngine()->startQuery(tqsp2->getQueryId());

    auto wrk13Schema = tqsp1->getRootOperators()[0]->getOutputSchema();
    auto wrk23Schema = tqsp2->getRootOperators()[0]->getOutputSchema();

    auto wrk13Src =
        LogicalOperatorFactory::createSourceOperator(Network::NetworkSourceDescriptor::create(wrk13Schema, wrk13NSrcNesPartition),
                                                     wrk13NSrcNesPartition.getOperatorId());
    auto wrk23Src =
        LogicalOperatorFactory::createSourceOperator(Network::NetworkSourceDescriptor::create(wrk23Schema, wrk23NSrcNesPartition),
                                                     wrk23NSrcNesPartition.getOperatorId());

    Network::NodeLocation wrk4NSrcLocation{4, "localhost", static_cast<uint32_t>(port + 41)};
    Network::NesPartition qsp34NSrcNesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};

    SinkDescriptorPtr wrk34NSinkDescPtr = Network::NetworkSinkDescriptor::create(wrk4NSrcLocation,
                                                                                 qsp34NSrcNesPartition,
                                                                                 sinkConnnectionWaitTime,
                                                                                 connectionRetries);

    OperatorNodePtr qsp34NSink =
        LogicalOperatorFactory::createSinkOperator(wrk34NSinkDescPtr, UtilityFunctions::getNextOperatorId());

    const LogicalUnaryOperatorNodePtr wrk3Map =
        LogicalOperatorFactory::createMapOperator(Attribute("newId") = Attribute("id") + Attribute("value"),
                                                  UtilityFunctions::getNextOperatorId());

    auto qsp3 = QueryPlan::create(wrk13Src);
    qsp3->addRootOperator(wrk23Src);
    qsp3->appendOperatorAsNewRoot(wrk3Map);
    qsp3->appendOperatorAsNewRoot(qsp34NSink);
    qsp3->setQueryId(1);
    qsp3->setQuerySubPlanId(3);
    auto tqsp3 = phase->execute(qsp3);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp3->getQuerySubPlanId() << ".\n" << tqsp3->toString());

    wrk3->getNodeEngine()->registerQueryInNodeEngine(tqsp3);
    wrk3->getNodeEngine()->startQuery(tqsp3->getQueryId());

    auto wrk34Schema = qsp34NSink->getOutputSchema();
    auto wrk34Src =
        LogicalOperatorFactory::createSourceOperator(Network::NetworkSourceDescriptor::create(wrk34Schema, qsp34NSrcNesPartition),
                                                     qsp34NSrcNesPartition.getOperatorId());
    auto querySink1 = LogicalOperatorFactory::createSinkOperator(
        FileSinkDescriptor::create("testReconfigurationNewBranchOnLevel3_1.csv", "CSV_FORMAT", "OVERWRITE"),
        UtilityFunctions::getNextOperatorId());

    auto qsp4 = QueryPlan::create(wrk34Src);
    qsp4->appendOperatorAsNewRoot(querySink1);
    qsp4->setQueryId(1);
    qsp4->setQuerySubPlanId(4);
    auto tqsp4 = phase->execute(qsp4);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp4->getQuerySubPlanId() << ".\n" << tqsp4->toString());

    wrk4->getNodeEngine()->registerQueryInNodeEngine(tqsp4);
    wrk4->getNodeEngine()->startQuery(tqsp4->getQueryId());

    EXPECT_TRUE(TestUtils::checkIfCSVHasContent(distinctColumnValuesValidator(4, std::unordered_set<std::string>{"1", "2"}),
                                                "testReconfigurationNewBranchOnLevel3_1.csv",
                                                true,
                                                ",",
                                                120));

    auto wrk3Qsp5Map = wrk3Map->duplicate();
    const LogicalUnaryOperatorNodePtr wrk3NewBranchMap =
        LogicalOperatorFactory::createMapOperator(Attribute("newBranch") = Attribute("id") + 2000,
                                                  UtilityFunctions::getNextOperatorId());

    Network::NesPartition qsp56NSrcNesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};

    SinkDescriptorPtr wrk56NSinkDescPtr = Network::NetworkSinkDescriptor::create(wrk4NSrcLocation,
                                                                                 qsp56NSrcNesPartition,
                                                                                 sinkConnnectionWaitTime,
                                                                                 connectionRetries);

    OperatorNodePtr qsp56NSink =
        LogicalOperatorFactory::createSinkOperator(wrk56NSinkDescPtr, UtilityFunctions::getNextOperatorId());

    auto qsp5 = QueryPlan::create(wrk13Src);
    qsp5->addRootOperator(wrk23Src);
    qsp5->appendOperatorAsNewRoot(wrk3Qsp5Map);
    qsp5->appendOperatorAsNewRoot(qsp34NSink);
    wrk3Qsp5Map->addParent(wrk3NewBranchMap);
    wrk3NewBranchMap->addParent(qsp56NSink);
    qsp5->addRootOperator(qsp56NSink);
    qsp5->setQueryId(1);
    qsp5->setQuerySubPlanId(5);
    auto tqsp5 = phase->execute(qsp5);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp5->getQuerySubPlanId() << ".\n" << tqsp5->toString());
    wrk3->getNodeEngine()->registerQueryForReconfigurationInNodeEngine(tqsp5);

    auto wrk56Schema = qsp56NSink->getOutputSchema();
    auto wrk56Src =
        LogicalOperatorFactory::createSourceOperator(Network::NetworkSourceDescriptor::create(wrk56Schema, qsp56NSrcNesPartition),
                                                     qsp56NSrcNesPartition.getOperatorId());
    auto querySink2 = LogicalOperatorFactory::createSinkOperator(
        FileSinkDescriptor::create("testReconfigurationNewBranchOnLevel3_2.csv", "CSV_FORMAT", "OVERWRITE"),
        UtilityFunctions::getNextOperatorId());

    auto qsp6 = QueryPlan::create(wrk56Src);
    qsp6->appendOperatorAsNewRoot(querySink2);
    qsp6->setQueryId(1);
    qsp6->setQuerySubPlanId(6);
    auto tqsp6 = phase->execute(qsp6);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp6->getQuerySubPlanId() << ".\n" << tqsp6->toString());
    wrk4->getNodeEngine()->registerQueryForReconfigurationInNodeEngine(tqsp6);

    std::vector<QuerySubPlanId> qepStarts{6};
    std::unordered_map<QuerySubPlanId, QuerySubPlanId> qepReplace{{3, 5}};
    std::vector<QuerySubPlanId> qepStops{};
    auto queryReconfigurationPlan = QueryReconfigurationPlan::create(qepStarts, qepStops, qepReplace);

    wrk1->getNodeEngine()->startQueryReconfiguration(1, queryReconfigurationPlan);

    while (wrk3->getNodeEngine()->getQueryManager()->getQepStatus(5)
           != NodeEngine::Execution::ExecutableQueryPlanStatus::Running) {
        NES_DEBUG("QueryReconfigurationTest: Waiting for QEP 5 to be in running mode.");
        sleep(10);
    }
    NES_DEBUG("QueryReconfigurationTest: QEP 5 is running.");

    while (wrk4->getNodeEngine()->getQueryManager()->getQepStatus(6)
           != NodeEngine::Execution::ExecutableQueryPlanStatus::Running) {
        NES_DEBUG("QueryReconfigurationTest: Waiting for QEP 6 to be in running mode.");
        sleep(10);
    }
    NES_DEBUG("QueryReconfigurationTest: QEP 6 is running.");

    // FileSink will only receive data from source 1, wait for `timeout` seconds to give confidence that no values received from `car2`
    EXPECT_FALSE(TestUtils::checkIfCSVHasContent(distinctColumnValuesValidator(4, std::unordered_set<std::string>{"2"}),
                                                 "testReconfigurationNewBranchOnLevel3_2.csv",
                                                 true,
                                                 ","));
    EXPECT_TRUE(TestUtils::checkIfCSVHasContent(distinctColumnValuesValidator(4, std::unordered_set<std::string>{"1"}),
                                                "testReconfigurationNewBranchOnLevel3_2.csv",
                                                true,
                                                ",",
                                                120));

    wrk2->getNodeEngine()->startQueryReconfiguration(1, queryReconfigurationPlan);

    // Since source 2 sends reconfiguration only now, we will data from both 1 and 2 must write data to
    EXPECT_TRUE(TestUtils::checkIfCSVHasContent(distinctColumnValuesValidator(4, std::unordered_set<std::string>{"1", "2"}),
                                                "testReconfigurationNewBranchOnLevel3_2.csv",
                                                true,
                                                ",",
                                                120));

    stopWorker(wrk1, 1);
    stopWorker(wrk2, 2);
    stopWorker(wrk3, 3);
    stopWorker(wrk4, 4);

    NES_INFO("QueryReconfigurationTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryReconfigurationTest: Test finished");
}

TEST_F(QueryReconfigurationTest, DISABLED_testReconfigurationReplacementInLevel3) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);
    NES_INFO("QueryReconfigurationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);

    auto phase = Optimizer::TypeInferencePhase::create(crd->getStreamCatalog());

    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryReconfigurationTest: Coordinator started successfully");

    NesWorkerPtr wrk1 = startWorker(wrkConf, port, 1, NesNodeType::Sensor, true);
    NesWorkerPtr wrk2 = startWorker(wrkConf, port, 2, NesNodeType::Sensor, true);
    NesWorkerPtr wrk3 = startWorker(wrkConf, port, 3, NesNodeType::Worker, true);
    NesWorkerPtr wrk4 = startWorker(wrkConf, port, 4, NesNodeType::Worker, true);

    std::string testSchema =
        "Schema::create()->addField(\"id\", BasicType::UINT64)->addField(\"value\", "
        "BasicType::UINT64)->addField(\"timestamp\", BasicType::UINT64)->addField(\"originId\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();

    wrk1->registerLogicalStream("car", testSchemaFileName);
    wrk2->registerLogicalStream("car", testSchemaFileName);

    //register physical stream - wrk1
    NES::AbstractPhysicalStreamConfigPtr confCar1 =
        NES::LambdaSourceStreamConfig::create("LambdaSource", "car1", "car", generatorLambda(1, 2), 0, 5, "frequency");
    wrk1->registerPhysicalStream(confCar1);

    //register physical stream - wrk2
    NES::AbstractPhysicalStreamConfigPtr confCar2 =
        NES::LambdaSourceStreamConfig::create("LambdaSource", "car2", "car", generatorLambda(2, 4), 0, 5, "frequency");
    wrk2->registerPhysicalStream(confCar2);

    auto wrk1Src = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"),
                                                                UtilityFunctions::getNextOperatorId());
    auto wrk2Src = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"),
                                                                UtilityFunctions::getNextOperatorId());

    Network::NesPartition wrk13NSrcNesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};
    Network::NesPartition wrk23NSrcNesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};

    Network::NodeLocation wrk3NSrcLocation{3, "localhost", static_cast<uint32_t>(port + 31)};

    const chrono::duration<int64_t>& sinkConnnectionWaitTime = std::chrono::seconds(60);
    int connectionRetries = 2;
    SinkDescriptorPtr wrk13NSinkDescPtr = Network::NetworkSinkDescriptor::create(wrk3NSrcLocation,
                                                                                 wrk13NSrcNesPartition,
                                                                                 sinkConnnectionWaitTime,
                                                                                 connectionRetries);
    SinkDescriptorPtr wrk23NSinkDescPtr = Network::NetworkSinkDescriptor::create(wrk3NSrcLocation,
                                                                                 wrk23NSrcNesPartition,
                                                                                 sinkConnnectionWaitTime,
                                                                                 connectionRetries);

    OperatorNodePtr wrk1NSink =
        LogicalOperatorFactory::createSinkOperator(wrk13NSinkDescPtr, UtilityFunctions::getNextOperatorId());
    OperatorNodePtr wrk2NSink =
        LogicalOperatorFactory::createSinkOperator(wrk23NSinkDescPtr, UtilityFunctions::getNextOperatorId());

    auto qsp1 = QueryPlan::create(wrk1Src);
    qsp1->appendOperatorAsNewRoot(wrk1NSink);
    qsp1->setQueryId(1);
    qsp1->setQuerySubPlanId(1);
    auto tqsp1 = phase->execute(qsp1);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp1->getQuerySubPlanId() << ".\n" << tqsp1->toString());

    auto qsp2 = QueryPlan::create(wrk2Src);
    qsp2->appendOperatorAsNewRoot(wrk2NSink);
    qsp2->setQueryId(1);
    qsp2->setQuerySubPlanId(2);
    auto tqsp2 = phase->execute(qsp2);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp2->getQuerySubPlanId() << ".\n" << tqsp2->toString());

    wrk1->getNodeEngine()->registerQueryInNodeEngine(tqsp1);

    wrk2->getNodeEngine()->registerQueryInNodeEngine(tqsp2);

    auto wrk13Schema = tqsp1->getRootOperators()[0]->getOutputSchema();
    auto wrk23Schema = tqsp2->getRootOperators()[0]->getOutputSchema();

    auto wrk13Src =
        LogicalOperatorFactory::createSourceOperator(Network::NetworkSourceDescriptor::create(wrk13Schema, wrk13NSrcNesPartition),
                                                     wrk13NSrcNesPartition.getOperatorId());
    auto wrk23Src =
        LogicalOperatorFactory::createSourceOperator(Network::NetworkSourceDescriptor::create(wrk23Schema, wrk23NSrcNesPartition),
                                                     wrk23NSrcNesPartition.getOperatorId());

    Network::NodeLocation wrk4NSrcLocation{4, "localhost", static_cast<uint32_t>(port + 41)};
    Network::NesPartition qsp34NSrcNesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};

    SinkDescriptorPtr wrk34NSinkDescPtr = Network::NetworkSinkDescriptor::create(wrk4NSrcLocation,
                                                                                 qsp34NSrcNesPartition,
                                                                                 sinkConnnectionWaitTime,
                                                                                 connectionRetries);

    OperatorNodePtr qsp34NSink =
        LogicalOperatorFactory::createSinkOperator(wrk34NSinkDescPtr, UtilityFunctions::getNextOperatorId());

    auto qsp3 = QueryPlan::create(wrk13Src);
    qsp3->addRootOperator(wrk23Src);
    qsp3->appendOperatorAsNewRoot(qsp34NSink);
    qsp3->setQueryId(1);
    qsp3->setQuerySubPlanId(3);
    auto tqsp3 = phase->execute(qsp3);
    auto tqsp5 = tqsp3->copy();
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp3->getQuerySubPlanId() << ".\n" << tqsp3->toString());

    wrk3->getNodeEngine()->registerQueryInNodeEngine(tqsp3);

    auto wrk34Schema = qsp34NSink->getOutputSchema();
    auto wrk34Src =
        LogicalOperatorFactory::createSourceOperator(Network::NetworkSourceDescriptor::create(wrk34Schema, qsp34NSrcNesPartition),
                                                     qsp34NSrcNesPartition.getOperatorId());
    const char* fileSink1Name = "testReconfigurationReplacementInLevel3_1.csv";
    auto querySink1 =
        LogicalOperatorFactory::createSinkOperator(FileSinkDescriptor::create(fileSink1Name, "CSV_FORMAT", "OVERWRITE"),
                                                   UtilityFunctions::getNextOperatorId());

    auto qsp4 = QueryPlan::create(wrk34Src);
    qsp4->appendOperatorAsNewRoot(querySink1);
    qsp4->setQueryId(1);
    qsp4->setQuerySubPlanId(4);
    auto tqsp4 = phase->execute(qsp4);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp4->getQuerySubPlanId() << ".\n" << tqsp4->toString());

    wrk4->getNodeEngine()->registerQueryInNodeEngine(tqsp4);

    wrk4->getNodeEngine()->startQuery(tqsp4->getQueryId());
    wrk3->getNodeEngine()->startQuery(tqsp4->getQueryId());
    wrk2->getNodeEngine()->startQuery(tqsp4->getQueryId());
    wrk1->getNodeEngine()->startQuery(tqsp4->getQueryId());

    EXPECT_TRUE(TestUtils::checkIfCSVHasContent(distinctColumnValuesValidator(4, std::unordered_set<std::string>{"1", "2"}),
                                                fileSink1Name,
                                                true,
                                                ","));

    const char* fileSink2Name = "testReconfigurationReplacementInLevel3_2.csv";

    Network::NesPartition qsp56NSrcNesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};

    SinkDescriptorPtr wrk56NSinkDescPtr = Network::NetworkSinkDescriptor::create(wrk4NSrcLocation,
                                                                                 qsp56NSrcNesPartition,
                                                                                 sinkConnnectionWaitTime,
                                                                                 connectionRetries);

    OperatorNodePtr qsp56NSink =
        LogicalOperatorFactory::createSinkOperator(wrk56NSinkDescPtr, UtilityFunctions::getNextOperatorId());

    tqsp5->replaceRootOperator(tqsp5->getRootOperators()[0], qsp56NSink);
    tqsp5->setQuerySubPlanId(5);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp5->getQuerySubPlanId() << ".\n" << tqsp5->toString());
    wrk3->getNodeEngine()->registerQueryForReconfigurationInNodeEngine(tqsp5);

    auto querySink2 =
        LogicalOperatorFactory::createSinkOperator(FileSinkDescriptor::create(fileSink2Name, "CSV_FORMAT", "OVERWRITE"),
                                                   UtilityFunctions::getNextOperatorId());

    auto wrk56Src =
        LogicalOperatorFactory::createSourceOperator(Network::NetworkSourceDescriptor::create(wrk34Schema, qsp34NSrcNesPartition),
                                                     qsp56NSrcNesPartition.getOperatorId());
    auto querySink1Copy =
        LogicalOperatorFactory::createSinkOperator(FileSinkDescriptor::create(fileSink1Name, "CSV_FORMAT", "OVERWRITE"),
                                                   querySink1->getId());

    auto qsp6 = QueryPlan::create(wrk56Src);
    qsp6->appendOperatorAsNewRoot(querySink2);
    wrk56Src->addParent(querySink1Copy);
    qsp6->addRootOperator(querySink1Copy);
    qsp6->setQueryId(1);
    qsp6->setQuerySubPlanId(6);
    auto tqsp6 = phase->execute(qsp6);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp6->getQuerySubPlanId() << ".\n" << tqsp6->toString());
    wrk4->getNodeEngine()->registerQueryForReconfigurationInNodeEngine(tqsp6);

    std::vector<QuerySubPlanId> qepStarts{};
    std::unordered_map<QuerySubPlanId, QuerySubPlanId> qepReplace{{4, 6}, {3, 5}};
    std::vector<QuerySubPlanId> qepStops{};
    auto queryReconfigurationPlan = QueryReconfigurationPlan::create(qepStarts, qepStops, qepReplace);

    wrk1->getNodeEngine()->startQueryReconfiguration(1, queryReconfigurationPlan);

    while (wrk3->getNodeEngine()->getQueryManager()->getQepStatus(5)
           != NodeEngine::Execution::ExecutableQueryPlanStatus::Running) {
        NES_DEBUG("QueryReconfigurationTest: Waiting for QEP 5 to be in running mode.");
        sleep(5);
    }
    while (wrk4->getNodeEngine()->getQueryManager()->getQepStatus(6)
           != NodeEngine::Execution::ExecutableQueryPlanStatus::Running) {
        NES_DEBUG("QueryReconfigurationTest: Waiting for QEP 5 to be in running mode.");
        sleep(5);
    }
    // FileSink will only receive data from source 1, wait for `timeout` seconds to give confidence that no values received from `car2`
    EXPECT_FALSE(TestUtils::checkIfCSVHasContent(distinctColumnValuesValidator(4, std::unordered_set<std::string>{"2"}),
                                                 fileSink2Name,
                                                 true,
                                                 ","));
    EXPECT_TRUE(TestUtils::checkIfCSVHasContent(distinctColumnValuesValidator(4, std::unordered_set<std::string>{"1"}),
                                                fileSink2Name,
                                                true,
                                                ",",
                                                120));

    wrk2->getNodeEngine()->startQueryReconfiguration(1, queryReconfigurationPlan);

    // Since source 2 sends reconfiguration only now, we will data from both 1 and 2 must write data to
    EXPECT_TRUE(TestUtils::checkIfCSVHasContent(distinctColumnValuesValidator(4, std::unordered_set<std::string>{"1", "2"}),
                                                fileSink2Name,
                                                true,
                                                ",",
                                                120));

    stopWorker(wrk1, 1);
    stopWorker(wrk2, 2);
    stopWorker(wrk3, 3);
    stopWorker(wrk4, 4);

    NES_INFO("QueryReconfigurationTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryReconfigurationTest: Test finished");
}
}// namespace NES

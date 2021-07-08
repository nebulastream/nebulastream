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
#include <Plans/Query/QueryId.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

using namespace std;

namespace NES {

static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class QueryReconfigurationTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("QueryReconfigurationTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryReconfigurationTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() { NES_INFO("Tear down QueryReconfigurationTest class."); }
};

NesWorkerPtr startWorker(const WorkerConfigPtr& wrkConf,
                         uint16_t coordinatorPort,
                         uint16_t workerId,
                         NesNodeType nesNodeType,
                         bool connect,
                         uint16_t slots = 65353UL) {
    NES_INFO("QueryReconfigurationTest: Start worker: " << workerId);
    wrkConf->setCoordinatorPort(coordinatorPort);
    wrkConf->setRpcPort(coordinatorPort + 10 * workerId);
    wrkConf->setDataPort(coordinatorPort + (10 * workerId) + 1);
    wrkConf->setNumWorkerThreads(2);
    wrkConf->setNumberOfSlots(slots);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(wrkConf, nesNodeType);
    EXPECT_TRUE(wrk->start(/**blocking**/ false, /**withConnect**/ connect));
    NES_INFO("QueryReconfigurationTest: Worker: " << workerId << " started successfully");
    return wrk;
}

void stopWorker(const NesWorkerPtr& wrk, uint16_t workerId) {
    NES_INFO("QueryReconfigurationTest: Stop worker " << workerId);
    EXPECT_TRUE(wrk->stop(true));
    NES_INFO("QueryReconfigurationTest: Stopped worker " << workerId);
}

std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)> generatorLambda(uint64_t origin,
                                                                                                         uint64_t sleepTime) {
    uint64_t counter = 0;
    return [origin, sleepTime, counter](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) mutable {
        struct Record {
            uint64_t id;
            uint64_t value;
            uint64_t timestamp;
            uint64_t originId;
        };
        std::this_thread::sleep_for(std::chrono::seconds{sleepTime});
        NES_DEBUG("generatorLambda: invoked on threadId: " << std::this_thread::get_id() << " originId: " << origin
                                                           << " customCounter: " << counter);
        auto* records = buffer.getBuffer<Record>();
        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            records[u].id = counter++;
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
    streamCatalog->addLogicalStream(logicalStreamName, std::move(schema));
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

std::function<bool(std::vector<std::vector<std::string>>)>
distinctColumnValuesValidator(uint8_t colId, const std::unordered_set<std::string>& requiredValues) {
    return [colId, requiredValues](const std::vector<std::vector<std::string>>& rows) {
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

std::function<bool(std::vector<std::vector<std::string>>)> noMissingValuesForIntRange(uint8_t colId,
                                                                                      uint64_t minimumNumberOfRows) {
    return [=](const std::vector<std::vector<std::string>>& rows) {
        auto colPos = colId - 1;
        if (rows.size() < minimumNumberOfRows) {
            return false;
        }
        std::vector<uint64_t> intValues;
        intValues.reserve(rows.size());
        for (auto row : rows) {
            intValues.emplace_back(std::stoull(row[colPos]));
        }
        std::sort(intValues.begin(), intValues.end());
        for (uint64_t i = 0; i < intValues.size() - 1; i++) {
            if (intValues[i + 1] - intValues[i] != 1) {
                return false;
            }
        }
        return true;
    };
}

/**
 * Test adding a new branch for a QEP in the source node
 *
 * Running Query:
 * Lambda Source 1 (1) ----> Map 1 (1) ----> Network Sink 1 (1) ----> Network Source 1 (2) ----> File Sink 1 (2)
 *
 * Running Query: Add new branch to QEP running on Node 1
 *
 *
 *
 *                                       |----> Map 2 (1) ----> Network Sink 2 (1) ----> Network Source 2 (2) ----> File Sink 2 (2)
 * Lambda Source 1 (1) ----> Map 1 (1) --|
 *                                       |----> Network Sink 1 (1) ----> Network Source 1 (2) ----> File Sink 1 (2)
 */
TEST_F(QueryReconfigurationTest, testReconfigurationAtSourceNode) {
    WorkerConfigPtr wrkConf = WorkerConfig::create();

    auto streamCatalog = std::make_shared<StreamCatalog>();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);

    uint64_t port = 2000;
    wrkConf->setRpcPort(port);

    NesWorkerPtr wrk1 = startWorker(wrkConf, port, 1, NesNodeType::Sensor, false);
    NesWorkerPtr wrk2 = startWorker(wrkConf, port, 2, NesNodeType::Worker, false);

    auto schema = Schema::create()
                      ->addField("id", UINT64)
                      ->addField("value", UINT64)
                      ->addField("timestamp", UINT64)
                      ->addField("originId", UINT64);
    streamCatalog->addLogicalStream("car", schema);

    //register physical stream - wrk1
    NES::AbstractPhysicalStreamConfigPtr confCar1 =
        NES::LambdaSourceStreamConfig::create("LambdaSource", "car1", "car", generatorLambda(1, 2), 0, 5, "frequency");
    wrk1->getNodeEngine()->setConfig(confCar1);

    auto wrk1Src = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"),
                                                                UtilityFunctions::getNextOperatorId());

    const LogicalUnaryOperatorNodePtr oldBranchMap =
        LogicalOperatorFactory::createMapOperator(Attribute("initialBranch") = 1, UtilityFunctions::getNextOperatorId());

    Network::NesPartition wrk12NSrcNesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};

    Network::NodeLocation wrk2NSrcLocation{2, "localhost", static_cast<uint32_t>(port + 21)};

    const auto sinkConnectionWaitTime = std::chrono::seconds(60);
    int connectionRetries = 2;
    SinkDescriptorPtr wrk12NSinkDescPtr = Network::NetworkSinkDescriptor::create(wrk2NSrcLocation,
                                                                                 wrk12NSrcNesPartition,
                                                                                 sinkConnectionWaitTime,
                                                                                 connectionRetries);
    OperatorNodePtr wrk12NSink =
        LogicalOperatorFactory::createSinkOperator(wrk12NSinkDescPtr, UtilityFunctions::getNextOperatorId());

    auto qsp1Template = QueryPlan::create(wrk1Src);
    qsp1Template->appendOperatorAsNewRoot(oldBranchMap);
    qsp1Template->appendOperatorAsNewRoot(wrk12NSink);
    qsp1Template->setQueryId(1);
    auto qsp1 = qsp1Template->copy();
    qsp1->setQuerySubPlanId(1);
    auto tqsp1 = typeInferencePhase->execute(qsp1);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp1->getQuerySubPlanId() << ".\n" << tqsp1->toString());
    wrk1->getNodeEngine()->registerQueryInNodeEngine(tqsp1);

    auto wrk12Src = LogicalOperatorFactory::createSourceOperator(
        Network::NetworkSourceDescriptor::create(wrk12NSink->getOutputSchema(), wrk12NSrcNesPartition),
        wrk12NSrcNesPartition.getOperatorId());

    auto querySink1 = LogicalOperatorFactory::createSinkOperator(
        FileSinkDescriptor::create("testReconfigurationAtSourceNode_1.csv", "CSV_FORMAT", "OVERWRITE"),
        UtilityFunctions::getNextOperatorId());

    auto qsp2 = QueryPlan::create(wrk12Src);
    qsp2->appendOperatorAsNewRoot(querySink1);
    qsp2->setQueryId(1);
    qsp2->setQuerySubPlanId(2);
    auto tqsp2 = typeInferencePhase->execute(qsp2);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp2->getQuerySubPlanId() << ".\n" << tqsp2->toString());
    wrk2->getNodeEngine()->registerQueryInNodeEngine(tqsp2);

    wrk2->getNodeEngine()->startQuery(tqsp2->getQueryId());
    wrk1->getNodeEngine()->startQuery(tqsp1->getQueryId());

    EXPECT_TRUE(TestUtils::checkIfOutputFileIsNotEmtpy(100, "testReconfigurationAtSourceNode_1.csv"));

    // Reconfiguration Related changes

    // Prestart qsp4 in wrk2, it will be waiting for data from new sink in qsp3

    Network::NesPartition wrk34NSrcNesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};

    SinkDescriptorPtr wrk34NSinkDescPtr = Network::NetworkSinkDescriptor::create(wrk2NSrcLocation,
                                                                                 wrk34NSrcNesPartition,
                                                                                 sinkConnectionWaitTime,
                                                                                 connectionRetries);
    OperatorNodePtr wrk34NSink =
        LogicalOperatorFactory::createSinkOperator(wrk34NSinkDescPtr, UtilityFunctions::getNextOperatorId());

    const LogicalUnaryOperatorNodePtr newBranchMap =
        LogicalOperatorFactory::createMapOperator(Attribute("newBranch") = 1, UtilityFunctions::getNextOperatorId());

    auto qsp3 = QueryPlan::create(wrk1Src->copy());
    const OperatorNodePtr& oldBranchCopy = oldBranchMap->copy();
    qsp3->appendOperatorAsNewRoot(oldBranchCopy);
    qsp3->appendOperatorAsNewRoot(newBranchMap);
    qsp3->appendOperatorAsNewRoot(wrk34NSink);
    const OperatorNodePtr& wrk12NSinkCopy = wrk12NSink->copy();
    qsp3->addRootOperator(wrk12NSinkCopy);
    oldBranchCopy->addParent(wrk12NSinkCopy);
    qsp3->setQueryId(1);
    qsp3->setQuerySubPlanId(3);
    auto tqsp3 = typeInferencePhase->execute(qsp3);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp3->getQuerySubPlanId() << ".\n" << tqsp3->toString());
    wrk1->getNodeEngine()->registerQueryForReconfigurationInNodeEngine(tqsp3);

    auto wrk34Src = LogicalOperatorFactory::createSourceOperator(
        Network::NetworkSourceDescriptor::create(wrk34NSink->getOutputSchema(), wrk34NSrcNesPartition),
        wrk34NSrcNesPartition.getOperatorId());

    auto querySink2 = LogicalOperatorFactory::createSinkOperator(
        FileSinkDescriptor::create("testReconfigurationAtSourceNode_2.csv", "CSV_FORMAT", "OVERWRITE"),
        UtilityFunctions::getNextOperatorId());

    auto qsp4 = QueryPlan::create(wrk34Src);
    qsp4->appendOperatorAsNewRoot(querySink2);
    qsp4->setQueryId(1);
    qsp4->setQuerySubPlanId(4);
    auto tqsp4 = typeInferencePhase->execute(qsp4);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp4->getQuerySubPlanId() << ".\n" << tqsp4->toString());
    wrk2->getNodeEngine()->registerQueryInNodeEngine(qsp4);
    wrk2->getNodeEngine()->startQuery(1);

    auto queryReconfigurationPlan = QueryReconfigurationPlan::create(1, DATA_SOURCE, 1, 3);
    wrk1->getNodeEngine()->startQueryReconfiguration(queryReconfigurationPlan);

    while (wrk1->getNodeEngine()->getQueryManager()->getQepStatus(tqsp3->getQuerySubPlanId())
           != Runtime::Execution::ExecutableQueryPlanStatus::Running) {
        NES_DEBUG("QueryReconfigurationTest: Waiting for QEP 3 to be in running mode.");
        sleep(1);
    }
    NES_DEBUG("QueryReconfigurationTest: QEP 3 is running.");

    // wait for more buffers to be written to file before comparison
    sleep(5);

    EXPECT_TRUE(TestUtils::validateCSVContent(noMissingValuesForIntRange(1, 5000),
                                              "no missing integers ID column",
                                              "testReconfigurationAtSourceNode_1.csv",
                                              true,
                                              ",",
                                              120));
    EXPECT_TRUE(TestUtils::validateCSVContent(noMissingValuesForIntRange(1, 5000),
                                              "no missing integers ID column",
                                              "testReconfigurationAtSourceNode_2.csv",
                                              true));

    stopWorker(wrk1, 1);
    stopWorker(wrk2, 2);
}

TEST_F(QueryReconfigurationTest, testReconfigurationAtSinkNode) {
    WorkerConfigPtr wrkConf = WorkerConfig::create();

    auto streamCatalog = std::make_shared<StreamCatalog>();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);

    uint64_t port = 2000;
    wrkConf->setRpcPort(port);

    NesWorkerPtr wrk1 = startWorker(wrkConf, port, 1, NesNodeType::Sensor, false);
    NesWorkerPtr wrk2 = startWorker(wrkConf, port, 2, NesNodeType::Worker, false);

    auto schema = Schema::create()
                      ->addField("id", UINT64)
                      ->addField("value", UINT64)
                      ->addField("timestamp", UINT64)
                      ->addField("originId", UINT64);
    streamCatalog->addLogicalStream("car", schema);

    //register physical stream - wrk1
    NES::AbstractPhysicalStreamConfigPtr confCar1 =
        NES::LambdaSourceStreamConfig::create("LambdaSource", "car1", "car", generatorLambda(1, 2), 0, 5, "frequency");
    wrk1->getNodeEngine()->setConfig(confCar1);

    auto wrk1Src = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"),
                                                                UtilityFunctions::getNextOperatorId());

    Network::NesPartition wrk12NSrcNesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};

    Network::NodeLocation wrk2NSrcLocation{2, "localhost", static_cast<uint32_t>(port + 21)};

    const auto sinkConnectionWaitTime = std::chrono::seconds(60);
    int connectionRetries = 2;
    SinkDescriptorPtr wrk12NSinkDescPtr = Network::NetworkSinkDescriptor::create(wrk2NSrcLocation,
                                                                                 wrk12NSrcNesPartition,
                                                                                 sinkConnectionWaitTime,
                                                                                 connectionRetries);
    OperatorNodePtr wrk12NSink =
        LogicalOperatorFactory::createSinkOperator(wrk12NSinkDescPtr, UtilityFunctions::getNextOperatorId());

    auto qsp1Template = QueryPlan::create(wrk1Src);
    qsp1Template->appendOperatorAsNewRoot(wrk12NSink);
    qsp1Template->setQueryId(1);
    auto qsp1 = qsp1Template->copy();
    qsp1->setQuerySubPlanId(1);
    auto tqsp1 = typeInferencePhase->execute(qsp1);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp1->getQuerySubPlanId() << ".\n" << tqsp1->toString());
    wrk1->getNodeEngine()->registerQueryInNodeEngine(tqsp1);

    auto wrk12Src = LogicalOperatorFactory::createSourceOperator(
        Network::NetworkSourceDescriptor::create(wrk12NSink->getOutputSchema(), wrk12NSrcNesPartition),
        wrk12NSrcNesPartition.getOperatorId());

    auto querySink1 = LogicalOperatorFactory::createSinkOperator(
        FileSinkDescriptor::create("testReconfigurationAtSinkNode_1.csv", "CSV_FORMAT", "OVERWRITE"),
        UtilityFunctions::getNextOperatorId());

    auto qsp2 = QueryPlan::create(wrk12Src);
    qsp2->appendOperatorAsNewRoot(querySink1);
    qsp2->setQueryId(1);
    qsp2->setQuerySubPlanId(2);
    auto tqsp2 = typeInferencePhase->execute(qsp2);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp2->getQuerySubPlanId() << ".\n" << tqsp2->toString());
    wrk2->getNodeEngine()->registerQueryInNodeEngine(tqsp2);

    wrk2->getNodeEngine()->startQuery(tqsp2->getQueryId());
    wrk1->getNodeEngine()->startQuery(tqsp1->getQueryId());

    EXPECT_TRUE(TestUtils::checkIfOutputFileIsNotEmtpy(100, "testReconfigurationAtSinkNode_1.csv"));

    Network::NesPartition wrk34NSrcNesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};

    SinkDescriptorPtr wrk34NSinkDescPtr = Network::NetworkSinkDescriptor::create(wrk2NSrcLocation,
                                                                                 wrk34NSrcNesPartition,
                                                                                 sinkConnectionWaitTime,
                                                                                 connectionRetries);
    OperatorNodePtr wrk34NSink =
        LogicalOperatorFactory::createSinkOperator(wrk34NSinkDescPtr, UtilityFunctions::getNextOperatorId());

    auto qsp3 = QueryPlan::create(wrk1Src->copy());
    qsp3->appendOperatorAsNewRoot(wrk34NSink);
    qsp3->setQueryId(1);
    qsp3->setQuerySubPlanId(3);
    auto tqsp3 = typeInferencePhase->execute(qsp3);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp3->getQuerySubPlanId() << ".\n" << tqsp3->toString());
    wrk1->getNodeEngine()->registerQueryForReconfigurationInNodeEngine(tqsp3);

    auto wrk34Src = LogicalOperatorFactory::createSourceOperator(
        Network::NetworkSourceDescriptor::create(wrk34NSink->getOutputSchema(), wrk34NSrcNesPartition),
        wrk34NSrcNesPartition.getOperatorId());

    auto querySink2 = LogicalOperatorFactory::createSinkOperator(
        FileSinkDescriptor::create("testReconfigurationAtSinkNode_2.csv", "CSV_FORMAT", "OVERWRITE"),
        UtilityFunctions::getNextOperatorId());

    auto qsp4 = QueryPlan::create(wrk34Src);
    qsp4->appendOperatorAsNewRoot(querySink2);
    auto querySink1Copy = querySink1->copy();
    qsp4->addRootOperator(querySink1Copy);
    wrk34Src->addParent(querySink1Copy);
    qsp4->setQueryId(1);
    qsp4->setQuerySubPlanId(4);
    auto tqsp4 = typeInferencePhase->execute(qsp4);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp4->getQuerySubPlanId() << ".\n" << tqsp4->toString());
    wrk2->getNodeEngine()->registerQueryForReconfigurationInNodeEngine(tqsp4);

    auto queryReconfigurationPlanSink = QueryReconfigurationPlan::create(1, DATA_SINK, 2, 4);
    wrk2->getNodeEngine()->startQueryReconfiguration(queryReconfigurationPlanSink);

    auto queryReconfigurationPlanSource = QueryReconfigurationPlan::create(1, DATA_SOURCE, 1, 3);
    wrk1->getNodeEngine()->startQueryReconfiguration(queryReconfigurationPlanSource);

    while (wrk1->getNodeEngine()->getQueryManager()->getQepStatus(tqsp3->getQuerySubPlanId())
           != Runtime::Execution::ExecutableQueryPlanStatus::Running) {
        NES_DEBUG("QueryReconfigurationTest: Waiting for QEP 3 to be in running mode.");
        sleep(1);
    }
    NES_DEBUG("QueryReconfigurationTest: QEP 3 is running.");

    // wait for more buffers to be written to file before comparison
    sleep(5);

    EXPECT_TRUE(TestUtils::validateCSVContent(noMissingValuesForIntRange(1, 5000),
                                              "no missing integers ID column",
                                              "testReconfigurationAtSinkNode_1.csv",
                                              true,
                                              ",",
                                              120));
    EXPECT_TRUE(TestUtils::validateCSVContent(noMissingValuesForIntRange(1, 5000),
                                              "no missing integers ID column",
                                              "testReconfigurationAtSinkNode_2.csv",
                                              true));

    stopWorker(wrk1, 1);
    stopWorker(wrk2, 2);
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
    EXPECT_NE(port, 0UL);
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

    auto wrk1SrcTemplate = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"),
                                                                        UtilityFunctions::getNextOperatorId());
    auto wrk2SrcTemplate = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"),
                                                                        UtilityFunctions::getNextOperatorId());

    Network::NesPartition qep13NSrcNesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};
    Network::NesPartition qep23NSrcNesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};

    Network::NodeLocation wrk3NSrcLocation{3, "localhost", static_cast<uint32_t>(port + 31)};

    const chrono::duration<int64_t>& sinkConnnectionWaitTime = std::chrono::seconds(60);
    int connectionRetries = 2;
    SinkDescriptorPtr qep13NSinkDescPtr = Network::NetworkSinkDescriptor::create(wrk3NSrcLocation,
                                                                                 qep13NSrcNesPartition,
                                                                                 sinkConnnectionWaitTime,
                                                                                 connectionRetries);
    SinkDescriptorPtr qep23NSinkDescPtr = Network::NetworkSinkDescriptor::create(wrk3NSrcLocation,
                                                                                 qep23NSrcNesPartition,
                                                                                 sinkConnnectionWaitTime,
                                                                                 connectionRetries);

    OperatorNodePtr qep13NSink =
        LogicalOperatorFactory::createSinkOperator(qep13NSinkDescPtr, UtilityFunctions::getNextOperatorId());
    OperatorNodePtr qep23Sink =
        LogicalOperatorFactory::createSinkOperator(qep23NSinkDescPtr, UtilityFunctions::getNextOperatorId());

    auto qep1Src = wrk1SrcTemplate->copy();
    auto qsp1 = QueryPlan::create(qep1Src);
    qsp1->appendOperatorAsNewRoot(qep13NSink);
    qsp1->setQueryId(1);
    qsp1->setQuerySubPlanId(1);
    auto tqsp1 = phase->execute(qsp1);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp1->getQuerySubPlanId() << ".\n" << tqsp1->toString());

    auto qep2Src = wrk2SrcTemplate->copy();
    auto qsp2 = QueryPlan::create(qep2Src);
    qsp2->appendOperatorAsNewRoot(qep23Sink);
    qsp2->setQueryId(1);
    qsp2->setQuerySubPlanId(2);
    auto tqsp2 = phase->execute(qsp2);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp2->getQuerySubPlanId() << ".\n" << tqsp2->toString());

    wrk1->getNodeEngine()->registerQueryInNodeEngine(tqsp1);
    wrk2->getNodeEngine()->registerQueryInNodeEngine(tqsp2);

    auto wrk13Schema = tqsp1->getRootOperators()[0]->getOutputSchema();
    auto wrk23Schema = tqsp2->getRootOperators()[0]->getOutputSchema();

    auto qep13Src =
        LogicalOperatorFactory::createSourceOperator(Network::NetworkSourceDescriptor::create(wrk13Schema, qep13NSrcNesPartition),
                                                     qep13NSrcNesPartition.getOperatorId());
    auto qep23Src =
        LogicalOperatorFactory::createSourceOperator(Network::NetworkSourceDescriptor::create(wrk23Schema, qep23NSrcNesPartition),
                                                     qep23NSrcNesPartition.getOperatorId());

    Network::NodeLocation wrk4NSrcLocation{4, "localhost", static_cast<uint32_t>(port + 41)};
    Network::NesPartition qep34NSrcNesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};

    SinkDescriptorPtr wrk34NSinkDescPtr = Network::NetworkSinkDescriptor::create(wrk4NSrcLocation,
                                                                                 qep34NSrcNesPartition,
                                                                                 sinkConnnectionWaitTime,
                                                                                 connectionRetries);

    OperatorNodePtr qep34NSink =
        LogicalOperatorFactory::createSinkOperator(wrk34NSinkDescPtr, UtilityFunctions::getNextOperatorId());

    const LogicalUnaryOperatorNodePtr firstMapTemplate =
        LogicalOperatorFactory::createMapOperator(Attribute("newId") = Attribute("id") + Attribute("value"),
                                                  UtilityFunctions::getNextOperatorId());

    auto qep3FirstMap = firstMapTemplate->copy();
    auto qsp3 = QueryPlan::create(qep13Src);
    qsp3->addRootOperator(qep23Src);
    qsp3->appendOperatorAsNewRoot(qep3FirstMap);
    qsp3->appendOperatorAsNewRoot(qep34NSink);
    qsp3->setQueryId(1);
    qsp3->setQuerySubPlanId(3);
    auto tqsp3 = phase->execute(qsp3);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp3->getQuerySubPlanId() << ".\n" << tqsp3->toString());

    wrk3->getNodeEngine()->registerQueryInNodeEngine(tqsp3);

    auto qep34Schema = qep34NSink->getOutputSchema();
    auto qep34Src =
        LogicalOperatorFactory::createSourceOperator(Network::NetworkSourceDescriptor::create(qep34Schema, qep34NSrcNesPartition),
                                                     qep34NSrcNesPartition.getOperatorId());
    auto querySink1 = LogicalOperatorFactory::createSinkOperator(
        FileSinkDescriptor::create("testReconfigurationNewBranchOnLevel3_1.csv", "CSV_FORMAT", "OVERWRITE"),
        UtilityFunctions::getNextOperatorId());

    auto qsp4 = QueryPlan::create(qep34Src);
    qsp4->appendOperatorAsNewRoot(querySink1);
    qsp4->setQueryId(1);
    qsp4->setQuerySubPlanId(4);
    auto tqsp4 = phase->execute(qsp4);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp4->getQuerySubPlanId() << ".\n" << tqsp4->toString());

    wrk4->getNodeEngine()->registerQueryInNodeEngine(tqsp4);

    wrk4->getNodeEngine()->startQuery(tqsp4->getQueryId());
    wrk3->getNodeEngine()->startQuery(tqsp3->getQueryId());
    wrk2->getNodeEngine()->startQuery(tqsp2->getQueryId());
    wrk1->getNodeEngine()->startQuery(tqsp1->getQueryId());

    EXPECT_TRUE(TestUtils::validateCSVContent(distinctColumnValuesValidator(4, std::unordered_set<std::string>{"1", "2"}),
                                              "column contains required values",
                                              "testReconfigurationNewBranchOnLevel3_1.csv",
                                              true,
                                              ",",
                                              120));

    // QEP 5 is copy of QEP1 with sink point to QEP 7, QEP 5 will replace QEP 1

    Network::NesPartition qep57NSrcNesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};
    Network::NesPartition qep67NSrcNesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};

    SinkDescriptorPtr qep57NSinkDescPtr = Network::NetworkSinkDescriptor::create(wrk3NSrcLocation,
                                                                                 qep57NSrcNesPartition,
                                                                                 sinkConnnectionWaitTime,
                                                                                 connectionRetries);
    SinkDescriptorPtr qep67NSinkDescPtr = Network::NetworkSinkDescriptor::create(wrk3NSrcLocation,
                                                                                 qep67NSrcNesPartition,
                                                                                 sinkConnnectionWaitTime,
                                                                                 connectionRetries);

    OperatorNodePtr qep57NSink =
        LogicalOperatorFactory::createSinkOperator(qep57NSinkDescPtr, UtilityFunctions::getNextOperatorId());
    OperatorNodePtr qep67NSink =
        LogicalOperatorFactory::createSinkOperator(qep67NSinkDescPtr, UtilityFunctions::getNextOperatorId());

    auto qsp5Src = wrk1SrcTemplate->copy();
    auto qsp5 = QueryPlan::create(qsp5Src);
    qsp5->appendOperatorAsNewRoot(qep57NSink);
    qsp5->setQueryId(1);
    qsp5->setQuerySubPlanId(5);
    auto tqsp5 = phase->execute(qsp5);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp5->getQuerySubPlanId() << ".\n" << tqsp5->toString());
    wrk1->getNodeEngine()->registerQueryForReconfigurationInNodeEngine(tqsp5);

    auto qsp6Src = wrk2SrcTemplate->copy();
    auto qsp6 = QueryPlan::create(qsp6Src);
    qsp6->appendOperatorAsNewRoot(qep67NSink);
    qsp6->setQueryId(1);
    qsp6->setQuerySubPlanId(6);
    auto tqsp6 = phase->execute(qsp6);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp6->getQuerySubPlanId() << ".\n" << tqsp6->toString());
    wrk2->getNodeEngine()->registerQueryForReconfigurationInNodeEngine(tqsp6);

    auto qep57Src = LogicalOperatorFactory::createSourceOperator(
        Network::NetworkSourceDescriptor::create(qep57NSink->getOutputSchema(), qep57NSrcNesPartition),
        qep57NSrcNesPartition.getOperatorId());
    auto qep67Src = LogicalOperatorFactory::createSourceOperator(
        Network::NetworkSourceDescriptor::create(qep67NSink->getOutputSchema(), qep67NSrcNesPartition),
        qep67NSrcNesPartition.getOperatorId());

    Network::NesPartition qep78NSrcNesPartition{1, UtilityFunctions::getNextOperatorId(), 1, 1};
    SinkDescriptorPtr qep78NSinkDescPtr = Network::NetworkSinkDescriptor::create(wrk4NSrcLocation,
                                                                                 qep78NSrcNesPartition,
                                                                                 sinkConnnectionWaitTime,
                                                                                 connectionRetries);
    auto qep78NSink = LogicalOperatorFactory::createSinkOperator(qep78NSinkDescPtr, UtilityFunctions::getNextOperatorId());
    auto qep74Nsink = LogicalOperatorFactory::createSinkOperator(wrk34NSinkDescPtr, UtilityFunctions::getNextOperatorId());

    auto qsp7FirstMap = firstMapTemplate->copy();
    const LogicalUnaryOperatorNodePtr qsp7NewBranchMap =
        LogicalOperatorFactory::createMapOperator(Attribute("newBranch") = Attribute("id") + 2000,
                                                  UtilityFunctions::getNextOperatorId());
    auto qsp7 = QueryPlan::create(qep57Src);
    qsp7->addRootOperator(qep67Src);
    qsp7->appendOperatorAsNewRoot(qsp7FirstMap);
    qsp7->appendOperatorAsNewRoot(qsp7NewBranchMap);
    qsp7->appendOperatorAsNewRoot(qep78NSink);
    qsp7->addRootOperator(qep74Nsink);
    qsp7FirstMap->addParent(qep74Nsink);
    qsp7->setQueryId(1);
    qsp7->setQuerySubPlanId(7);
    auto tqsp7 = phase->execute(qsp7);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp7->getQuerySubPlanId() << ".\n" << tqsp7->toString());
    wrk3->getNodeEngine()->registerQueryInNodeEngine(tqsp7);
    wrk3->getNodeEngine()->startQuery(1);

    auto qep78NSrc = LogicalOperatorFactory::createSourceOperator(
        Network::NetworkSourceDescriptor::create(qep78NSink->getOutputSchema(), qep78NSrcNesPartition),
        qep78NSrcNesPartition.getOperatorId());
    auto querySink2 = LogicalOperatorFactory::createSinkOperator(
        FileSinkDescriptor::create("testReconfigurationNewBranchOnLevel3_2.csv", "CSV_FORMAT", "OVERWRITE"),
        UtilityFunctions::getNextOperatorId());

    auto qsp8 = QueryPlan::create(qep78NSrc);
    qsp8->appendOperatorAsNewRoot(querySink2);
    qsp8->setQueryId(1);
    qsp8->setQuerySubPlanId(8);
    auto tqsp8 = phase->execute(qsp8);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp8->getQuerySubPlanId() << ".\n" << tqsp8->toString());
    wrk4->getNodeEngine()->registerQueryInNodeEngine(tqsp8);
    wrk4->getNodeEngine()->startQuery(1);

    auto reconfigurationPlanWrk1 = QueryReconfigurationPlan::create(1, DATA_SOURCE, 1, 5);

    wrk1->getNodeEngine()->startQueryReconfiguration(reconfigurationPlanWrk1);

    // New sink must not contain data from source 2, give 2 minutes to and then say correct
    EXPECT_FALSE(TestUtils::validateCSVContent(distinctColumnValuesValidator(4, std::unordered_set<std::string>{"2"}),
                                               "column does not contain {2}",
                                               "testReconfigurationNewBranchOnLevel3_2.csv",
                                               true,
                                               ",",
                                               120));

    EXPECT_TRUE(TestUtils::validateCSVContent(distinctColumnValuesValidator(4, std::unordered_set<std::string>{"1"}),
                                              "column contains value {1}",
                                              "testReconfigurationNewBranchOnLevel3_2.csv",
                                              true));

    auto reconfigurationPlanWrk2 = QueryReconfigurationPlan::create(1, DATA_SOURCE, 2, 6);

    wrk2->getNodeEngine()->startQueryReconfiguration(reconfigurationPlanWrk2);

    EXPECT_TRUE(TestUtils::validateCSVContent(distinctColumnValuesValidator(4, std::unordered_set<std::string>{"1", "2"}),
                                              "column contains value {1, 2}",
                                              "testReconfigurationNewBranchOnLevel3_2.csv",
                                              true,
                                              ",",
                                              120));
    for (auto wrk : std::vector<NesWorkerPtr>{wrk1, wrk2, wrk3, wrk4}) {
        wrk->getNodeEngine()->stopQuery(1, true);
        wrk->getNodeEngine()->undeployQuery(1);
    }

    stopWorker(wrk1, 1);
    stopWorker(wrk2, 2);
    stopWorker(wrk3, 3);
    stopWorker(wrk4, 4);

    NES_INFO("QueryReconfigurationTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryReconfigurationTest: Test finished");
}

class QueryReconfigurationPlacementParameterizedTest : public ::testing::TestWithParam<const char*> {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("QueryReconfigurationPlacementParameterizedTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryReconfigurationPlacementParameterizedTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() { NES_INFO("Tear down QueryReconfigurationPlacementParameterizedTest class."); }
};

INSTANTIATE_TEST_SUITE_P(Placement, QueryReconfigurationPlacementParameterizedTest, testing::Values("BottomUp", "TopDown"));

TEST_P(QueryReconfigurationPlacementParameterizedTest, reconfigurationTotalMerging) {

    std::string strategy = GetParam();

    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setQueryReconfiguration(true);
    crdConf->setRestPort(restPort);

    wrkConf->setCoordinatorPort(rpcPort);

    std::string outputFilePath1 = "reconfigurationTotalMerging" + strategy + "1.out";
    std::string outputFilePath2 = "reconfigurationTotalMerging" + strategy + "2.out";

    remove(outputFilePath1.c_str());
    remove(outputFilePath2.c_str());

    NES_INFO("QueryReconfigurationTest: Start coordinator");

    crdConf->setQueryMergerRule("Z3SignatureBasedCompleteQueryMergerRule");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("QueryReconfigurationTest: Coordinator started successfully");

    NES_INFO("QueryReconfigurationTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryReconfigurationTest: Worker1 started successfully");

    NES_INFO("QueryReconfigurationTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("QueryReconfigurationTest: Worker2 started successfully");

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
        NES::LambdaSourceStreamConfig::create("LambdaSource", "car1", "car", generatorLambda(1, 10), 0, 5, "frequency");
    wrk1->registerPhysicalStream(confCar1);

    //register physical stream - wrk2
    NES::AbstractPhysicalStreamConfigPtr confCar2 =
        NES::LambdaSourceStreamConfig::create("LambdaSource", "car2", "car", generatorLambda(2, 20), 0, 5, "frequency");
    wrk2->registerPhysicalStream(confCar2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryReconfigurationTest: Submit query");
    string query1 =
        R"(Query::from("car").sink(FileSinkDescriptor::create(")" + outputFilePath1 + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId1 = queryService->validateAndQueueAddRequest(query1, strategy);
    string query2 =
        R"(Query::from("car").sink(FileSinkDescriptor::create(")" + outputFilePath2 + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId2 = queryService->validateAndQueueAddRequest(query2, strategy);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalog));

    EXPECT_TRUE(TestUtils::checkIfOutputFileIsNotEmtpy(100, outputFilePath1));

    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalog));

    EXPECT_TRUE(TestUtils::checkIfOutputFileIsNotEmtpy(100, outputFilePath2));

    NES_INFO("QueryReconfigurationTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId1);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalog));

    NES_INFO("QueryReconfigurationTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId2);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalog));

    NES_INFO("QueryReconfigurationTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryReconfigurationTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("QueryReconfigurationTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryReconfigurationTest: Test finished");

    int response1 = remove(outputFilePath1.c_str());
    EXPECT_EQ(response1, 0);

    int response2 = remove(outputFilePath2.c_str());
    EXPECT_EQ(response2, 0);
}

TEST_F(QueryReconfigurationTest, reconfigurationPartialMergingTopDown) {

    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setQueryReconfiguration(true);
    crdConf->setRestPort(restPort);

    wrkConf->setCoordinatorPort(rpcPort);

    std::string outputFilePath1 = "reconfigurationPartialMergingTopDown1.out";
    std::string outputFilePath2 = "reconfigurationPartialMergingTopDown2.out";

    remove(outputFilePath1.c_str());
    remove(outputFilePath2.c_str());

    NES_INFO("QueryReconfigurationTest: Start coordinator");

    crdConf->setQueryMergerRule("Z3SignatureBasedPartialQueryMergerRule");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("QueryReconfigurationTest: Coordinator started successfully");

    auto wrk1 = startWorker(wrkConf, port, 1, NesNodeType::Sensor, true);
    auto wrk2 = startWorker(wrkConf, port, 2, NesNodeType::Sensor, true);
    auto wrk3 = startWorker(wrkConf, port, 3, NesNodeType::Worker, true);
    auto wrk4 = startWorker(wrkConf, port, 4, NesNodeType::Worker, true);

    std::string testSchema =
        "Schema::create()->addField(\"id\", BasicType::UINT64)->addField(\"value\", "
        "BasicType::UINT64)->addField(\"timestamp\", BasicType::UINT64)->addField(\"originId\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();

    wrk1->registerLogicalStream("car", testSchemaFileName);
    wrk2->registerLogicalStream("car", testSchemaFileName);

    wrk1->addParent(wrk3->getWorkerId());
    wrk2->addParent(wrk4->getWorkerId());

    wrk1->removeParent(crd->getNesWorker()->getWorkerId());
    wrk2->removeParent(crd->getNesWorker()->getWorkerId());

    //register physical stream - wrk1
    NES::AbstractPhysicalStreamConfigPtr confCar1 =
        NES::LambdaSourceStreamConfig::create("LambdaSource", "car1", "car", generatorLambda(1, 2), 0, 5, "frequency");
    wrk1->registerPhysicalStream(confCar1);

    //register physical stream - wrk2
    NES::AbstractPhysicalStreamConfigPtr confCar2 =
        NES::LambdaSourceStreamConfig::create("LambdaSource", "car2", "car", generatorLambda(2, 4), 0, 5, "frequency");
    wrk2->registerPhysicalStream(confCar2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryReconfigurationTest: Submit query");

    string query1 = R"(Query::from("car").filter(Attribute("value") / 5 > 0).sink(FileSinkDescriptor::create(")" + outputFilePath1
        + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId1 = queryService->validateAndQueueAddRequest(query1, "TopDown");
    string query2 =
        R"(Query::from("car").filter(Attribute("value") / 5 > 0).map(Attribute("newQueryId") = 2).sink(FileSinkDescriptor::create(")"
        + outputFilePath2 + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId2 = queryService->validateAndQueueAddRequest(query2, "TopDown");
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalog));
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalog));

    EXPECT_TRUE(TestUtils::checkIfOutputFileIsNotEmtpy(100, outputFilePath1));
    EXPECT_TRUE(TestUtils::checkIfOutputFileIsNotEmtpy(100, outputFilePath2));

    NES_INFO("QueryReconfigurationTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId1);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalog));

    NES_INFO("QueryReconfigurationTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId2);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalog));

    stopWorker(wrk1, 1);
    stopWorker(wrk2, 2);
    stopWorker(wrk3, 3);
    stopWorker(wrk4, 4);

    NES_INFO("QueryReconfigurationTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryReconfigurationTest: Test finished");

    int response1 = remove(outputFilePath1.c_str());
    EXPECT_EQ(response1, 0);

    int response2 = remove(outputFilePath2.c_str());
    EXPECT_EQ(response2, 0);
}

TEST_F(QueryReconfigurationTest, reconfigurationPartialMergingBottomUp) {

    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setQueryReconfiguration(true);
    crdConf->setRestPort(restPort);

    wrkConf->setCoordinatorPort(rpcPort);

    std::string outputFilePath1 = "reconfigurationPartialMergingBottomUp1.out";
    std::string outputFilePath2 = "reconfigurationPartialMergingBottomUp2.out";

    remove(outputFilePath1.c_str());
    remove(outputFilePath2.c_str());

    NES_INFO("QueryReconfigurationTest: Start coordinator");

    crdConf->setQueryMergerRule("Z3SignatureBasedPartialQueryMergerRule");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("QueryReconfigurationTest: Coordinator started successfully");

    crd->getTopology()->toString();

    auto wrk1 = startWorker(wrkConf, port, 1, NesNodeType::Sensor, true, 1);
    auto wrk2 = startWorker(wrkConf, port, 2, NesNodeType::Sensor, true, 1);
    auto wrk3 = startWorker(wrkConf, port, 3, NesNodeType::Worker, true);
    auto wrk4 = startWorker(wrkConf, port, 4, NesNodeType::Worker, true);

    std::string testSchema =
        "Schema::create()->addField(\"id\", BasicType::UINT64)->addField(\"value\", "
        "BasicType::UINT64)->addField(\"timestamp\", BasicType::UINT64)->addField(\"originId\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();

    wrk1->registerLogicalStream("car", testSchemaFileName);
    wrk2->registerLogicalStream("car", testSchemaFileName);

    wrk1->addParent(wrk3->getWorkerId());
    wrk2->addParent(wrk4->getWorkerId());

    wrk1->removeParent(crd->getNesWorker()->getWorkerId());
    wrk2->removeParent(crd->getNesWorker()->getWorkerId());

    NES_INFO("Topology: \n" << crd->getTopology()->toString());

    //register physical stream - wrk1
    NES::AbstractPhysicalStreamConfigPtr confCar1 =
        NES::LambdaSourceStreamConfig::create("LambdaSource", "car1", "car", generatorLambda(1, 2), 0, 5, "frequency");
    wrk1->registerPhysicalStream(confCar1);

    //register physical stream - wrk2
    NES::AbstractPhysicalStreamConfigPtr confCar2 =
        NES::LambdaSourceStreamConfig::create("LambdaSource", "car2", "car", generatorLambda(2, 4), 0, 5, "frequency");
    wrk2->registerPhysicalStream(confCar2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryReconfigurationTest: Submit query");

    string query1 = R"(Query::from("car").filter(Attribute("value") / 5 > 0).sink(FileSinkDescriptor::create(")" + outputFilePath1
        + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId1 = queryService->validateAndQueueAddRequest(query1, "BottomUp");
    string query2 =
        R"(Query::from("car").filter(Attribute("value") / 5 > 0).map(Attribute("newQueryId") = 2).sink(FileSinkDescriptor::create(")"
        + outputFilePath2 + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId2 = queryService->validateAndQueueAddRequest(query2, "BottomUp");
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalog));
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalog));

    EXPECT_TRUE(TestUtils::checkIfOutputFileIsNotEmtpy(100, outputFilePath1));
    EXPECT_TRUE(TestUtils::checkIfOutputFileIsNotEmtpy(100, outputFilePath2));

    NES_INFO("QueryReconfigurationTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId1);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalog));

    NES_INFO("QueryReconfigurationTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId2);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalog));

    stopWorker(wrk1, 1);
    stopWorker(wrk2, 2);
    stopWorker(wrk3, 3);
    stopWorker(wrk4, 4);

    NES_INFO("QueryReconfigurationTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryReconfigurationTest: Test finished");

    int response1 = remove(outputFilePath1.c_str());
    EXPECT_EQ(response1, 0);

    int response2 = remove(outputFilePath2.c_str());
    EXPECT_EQ(response2, 0);
}

TEST_F(QueryReconfigurationTest, reconfigurationPartialMoreThanTwoQueriesMergingBottomUp) {

    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setQueryReconfiguration(true);
    crdConf->setRestPort(restPort);

    wrkConf->setCoordinatorPort(rpcPort);

    std::string outputFilePath1 = "reconfigurationPartialMoreThanTwoQueriesMergingBottomUp1.out";
    std::string outputFilePath2 = "reconfigurationPartialMoreThanTwoQueriesMergingBottomUp2.out";
    std::string outputFilePath3 = "reconfigurationPartialMoreThanTwoQueriesMergingBottomUp3.out";

    remove(outputFilePath1.c_str());
    remove(outputFilePath2.c_str());
    remove(outputFilePath3.c_str());

    NES_INFO("QueryReconfigurationTest: Start coordinator");

    crdConf->setQueryMergerRule("Z3SignatureBasedPartialQueryMergerRule");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("QueryReconfigurationTest: Coordinator started successfully");

    crd->getTopology()->toString();

    auto wrk1 = startWorker(wrkConf, port, 1, NesNodeType::Sensor, true, 1);
    auto wrk2 = startWorker(wrkConf, port, 2, NesNodeType::Sensor, true, 1);
    auto wrk3 = startWorker(wrkConf, port, 3, NesNodeType::Worker, true);
    auto wrk4 = startWorker(wrkConf, port, 4, NesNodeType::Worker, true);

    std::string testSchema =
        "Schema::create()->addField(\"id\", BasicType::UINT64)->addField(\"value\", "
        "BasicType::UINT64)->addField(\"timestamp\", BasicType::UINT64)->addField(\"originId\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();

    wrk1->registerLogicalStream("car", testSchemaFileName);
    wrk2->registerLogicalStream("car", testSchemaFileName);

    wrk1->addParent(wrk3->getWorkerId());
    wrk2->addParent(wrk4->getWorkerId());

    wrk1->removeParent(crd->getNesWorker()->getWorkerId());
    wrk2->removeParent(crd->getNesWorker()->getWorkerId());

    NES_INFO("Topology: \n" << crd->getTopology()->toString());

    //register physical stream - wrk1
    NES::AbstractPhysicalStreamConfigPtr confCar1 =
        NES::LambdaSourceStreamConfig::create("LambdaSource", "car1", "car", generatorLambda(1, 2), 0, 5, "frequency");
    wrk1->registerPhysicalStream(confCar1);

    //register physical stream - wrk2
    NES::AbstractPhysicalStreamConfigPtr confCar2 =
        NES::LambdaSourceStreamConfig::create("LambdaSource", "car2", "car", generatorLambda(2, 4), 0, 5, "frequency");
    wrk2->registerPhysicalStream(confCar2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryReconfigurationTest: Submit query");

    string query1 = R"(Query::from("car").filter(Attribute("value") / 5 > 0).sink(FileSinkDescriptor::create(")" + outputFilePath1
        + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId1 = queryService->validateAndQueueAddRequest(query1, "BottomUp");
    string query2 =
        R"(Query::from("car").filter(Attribute("value") / 5 > 0).map(Attribute("newQueryId") = 2).sink(FileSinkDescriptor::create(")"
        + outputFilePath2 + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId2 = queryService->validateAndQueueAddRequest(query2, "BottomUp");
    string query3 =
        R"(Query::from("car").filter(Attribute("value") / 5 > 0).map(Attribute("newQueryId") = 3).sink(FileSinkDescriptor::create(")"
        + outputFilePath3 + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId3 = queryService->validateAndQueueAddRequest(query3, "BottomUp");

    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalog));
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalog));
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId3, queryCatalog));

    EXPECT_TRUE(TestUtils::checkIfOutputFileIsNotEmtpy(100, outputFilePath1));
    EXPECT_TRUE(TestUtils::checkIfOutputFileIsNotEmtpy(100, outputFilePath2));
    EXPECT_TRUE(TestUtils::checkIfOutputFileIsNotEmtpy(100, outputFilePath3));

    NES_INFO("QueryReconfigurationTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId1);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalog));

    NES_INFO("QueryReconfigurationTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId2);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalog));

    NES_INFO("QueryReconfigurationTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId2);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId3, queryCatalog));

    stopWorker(wrk1, 1);
    stopWorker(wrk2, 2);
    stopWorker(wrk3, 3);
    stopWorker(wrk4, 4);

    NES_INFO("QueryReconfigurationTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryReconfigurationTest: Test finished");

    int response1 = remove(outputFilePath1.c_str());
    EXPECT_EQ(response1, 0);

    int response2 = remove(outputFilePath2.c_str());
    EXPECT_EQ(response2, 0);
}

}// namespace NES

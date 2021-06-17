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
        NES::setupLogging("QueryReconfigurationTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryReconfigurationTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() { NES_INFO("Tear down QueryReconfigurationTest class."); }
};

NesWorkerPtr startWorker(WorkerConfigPtr wrkConf, uint16_t coordinatorPort, uint16_t workerId, NesNodeType nesNodeType) {
    NES_INFO("QueryReconfigurationTest: Start worker: " << workerId);
    wrkConf->setCoordinatorPort(coordinatorPort);
    wrkConf->setRpcPort(coordinatorPort + 10 * workerId);
    wrkConf->setDataPort(coordinatorPort + (10 * workerId) + 1);
    wrkConf->setNumWorkerThreads(4);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(wrkConf, nesNodeType);
    EXPECT_TRUE(wrk->start(/**blocking**/ false, /**withConnect**/ true));
    NES_INFO("QueryReconfigurationTest: Worker: " << workerId << " started successfully");
    return wrk;
}

void stopWorker(NesWorkerPtr wrk, uint16_t workerId) {
    NES_INFO("QueryReconfigurationTest: Stop worker " << workerId);
    EXPECT_TRUE(wrk->stop(true));
    NES_INFO("QueryReconfigurationTest: Stopped worker " << workerId);
}

/**
 * Test adding a new branch for a QEP in Level 3
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

    NesWorkerPtr wrk1 = startWorker(wrkConf, port, 1, NesNodeType::Sensor);
    NesWorkerPtr wrk2 = startWorker(wrkConf, port, 2, NesNodeType::Sensor);
    NesWorkerPtr wrk3 = startWorker(wrkConf, port, 3, NesNodeType::Worker);
    NesWorkerPtr wrk4 = startWorker(wrkConf, port, 4, NesNodeType::Worker);

    std::string outputFilePath = "testReconfigurationNewBranchOnLevel3.out";
    remove(outputFilePath.c_str());

    //register logical stream
    SchemaPtr schema = Schema::create()->addField("car$id", BasicType::UINT32)->addField("car$value", BasicType::UINT64);
    std::string testSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)->addField(\"value\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();

    wrk1->registerLogicalStream("car", testSchemaFileName);
    wrk2->registerLogicalStream("car", testSchemaFileName);

    srcConf->setSourceConfig("");
    srcConf->setSourceFrequency(3000);
    srcConf->setNumberOfTuplesToProducePerBuffer(10);
    srcConf->setNumberOfBuffersToProduce(3000);
    srcConf->setLogicalStreamName("car");

    //register physical stream - wrk1
    srcConf->setPhysicalStreamName("car1");
    PhysicalStreamConfigPtr confCar1 = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(confCar1);

    //register physical stream - wrk2
    srcConf->setPhysicalStreamName("car2");
    PhysicalStreamConfigPtr confCar2 = PhysicalStreamConfig::create(srcConf);
    wrk2->registerPhysicalStream(confCar2);

    auto wrk1Src = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"), 1001);
    auto wrk2Src = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"), 2001);

    Network::NesPartition wrk13NSrcNesPartition{1, 3001, 1, 1};
    Network::NesPartition wrk23NSrcNesPartition{1, 3002, 1, 1};

    Network::NodeLocation wrk3NSrcLocation{3, "localhost", static_cast<uint32_t>(port + 31)};

    SinkDescriptorPtr wrk13NSinkDescPtr =
        Network::NetworkSinkDescriptor::create(wrk3NSrcLocation, wrk13NSrcNesPartition, std::chrono::seconds(60), 5);
    SinkDescriptorPtr wrk23NSinkDescPtr =
        Network::NetworkSinkDescriptor::create(wrk3NSrcLocation, wrk23NSrcNesPartition, std::chrono::seconds(60), 5);

    OperatorNodePtr wrk1NSink = LogicalOperatorFactory::createSinkOperator(wrk13NSinkDescPtr, 1002);
    OperatorNodePtr wrk2NSink = LogicalOperatorFactory::createSinkOperator(wrk23NSinkDescPtr, 2002);

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
    Network::NesPartition qsp34NSrcNesPartition{1, 4001, 1, 1};

    SinkDescriptorPtr wrk34NSinkDescPtr =
        Network::NetworkSinkDescriptor::create(wrk4NSrcLocation, qsp34NSrcNesPartition, std::chrono::seconds(60), 5);

    OperatorNodePtr qsp34NSink = LogicalOperatorFactory::createSinkOperator(wrk34NSinkDescPtr, 3004);

    const LogicalUnaryOperatorNodePtr wrk3Map =
        LogicalOperatorFactory::createMapOperator(Attribute("newId") = Attribute("id") + Attribute("value"), 3003);

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
    auto querySink1 =
        LogicalOperatorFactory::createSinkOperator(FileSinkDescriptor::create("testReconfigurationNewBranchOnLevel3_1.out"),
                                                   4002);

    auto qsp4 = QueryPlan::create(wrk34Src);
    qsp4->appendOperatorAsNewRoot(querySink1);
    qsp4->setQueryId(1);
    qsp4->setQuerySubPlanId(4);
    auto tqsp4 = phase->execute(qsp4);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp4->getQuerySubPlanId() << ".\n" << tqsp4->toString());

    wrk4->getNodeEngine()->registerQueryInNodeEngine(tqsp4);
    wrk4->getNodeEngine()->startQuery(tqsp4->getQueryId());

    auto wrk3Qsp5Map = wrk3Map->duplicate();
    const LogicalUnaryOperatorNodePtr wrk3NewBranchMap =
        LogicalOperatorFactory::createMapOperator(Attribute("newBranch") = Attribute("id") + 2000, 3103);

    Network::NesPartition qsp56NSrcNesPartition{1, 4101, 1, 1};

    SinkDescriptorPtr wrk56NSinkDescPtr =
        Network::NetworkSinkDescriptor::create(wrk4NSrcLocation, qsp56NSrcNesPartition, std::chrono::seconds(60), 5);

    OperatorNodePtr qsp56NSink = LogicalOperatorFactory::createSinkOperator(wrk56NSinkDescPtr, 3104);

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
    auto querySink2 =
        LogicalOperatorFactory::createSinkOperator(FileSinkDescriptor::create("testReconfigurationNewBranchOnLevel3_2.out"),
                                                   4102);

    auto qsp6 = QueryPlan::create(wrk56Src);
    qsp6->appendOperatorAsNewRoot(querySink2);
    qsp6->setQueryId(1);
    qsp6->setQuerySubPlanId(6);
    auto tqsp6 = phase->execute(qsp6);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp6->getQuerySubPlanId() << ".\n" << tqsp6->toString());
    wrk4->getNodeEngine()->registerQueryForReconfigurationInNodeEngine(tqsp6);

    auto queryReconfigurationPlan = QueryReconfigurationPlan::create(std::vector<QuerySubPlanId>{6},
                                                                     std::map<QuerySubPlanId, QuerySubPlanId>{{3, 5}},
                                                                     std::vector<QuerySubPlanId>{});
    wrk1->getNodeEngine()->startQueryReconfiguration(1, queryReconfigurationPlan);

    sleep(1000);

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

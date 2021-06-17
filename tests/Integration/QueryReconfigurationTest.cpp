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
    Network::NesPartition wrk34NSrcNesPartition{1, 4001, 1, 1};

    SinkDescriptorPtr wrk34NSinkDescPtr =
        Network::NetworkSinkDescriptor::create(wrk4NSrcLocation, wrk34NSrcNesPartition, std::chrono::seconds(60), 5);

    OperatorNodePtr wrk3NSink = LogicalOperatorFactory::createSinkOperator(wrk34NSinkDescPtr, 3004);

    auto qsp3 = QueryPlan::create(wrk13Src);
    qsp3->addRootOperator(wrk23Src);
    qsp3->appendOperatorAsNewRoot(
        LogicalOperatorFactory::createMapOperator(Attribute("newId") = Attribute("id") + Attribute("value"), 3003));
    qsp3->appendOperatorAsNewRoot(wrk3NSink);
    qsp3->setQueryId(1);
    qsp3->setQuerySubPlanId(3);
    auto tqsp3 = phase->execute(qsp3);
    NES_INFO("QueryReconfigurationTest: Query Sub Plan: " << tqsp3->getQuerySubPlanId() << ".\n" << tqsp3->toString());

    wrk3->getNodeEngine()->registerQueryInNodeEngine(tqsp3);
    wrk3->getNodeEngine()->startQuery(tqsp3->getQueryId());

    auto wrk34Schema = wrk3NSink->getOutputSchema();
    auto wrk34Src =
        LogicalOperatorFactory::createSourceOperator(Network::NetworkSourceDescriptor::create(wrk34Schema, wrk34NSrcNesPartition),
                                                     wrk34NSrcNesPartition.getOperatorId());
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

    sleep(100);

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

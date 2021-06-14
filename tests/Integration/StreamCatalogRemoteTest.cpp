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

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <Util/Logger.hpp>
#include <fstream>
#include <gtest/gtest.h>

using namespace std;
namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 4000;
uint64_t restPort = 8081;

class StreamCatalogRemoteTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("StreamCatalogRemoteTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup StreamCatalogRemoteTest test class.");
    }

    void SetUp() override { rpcPort = rpcPort + 30; }

    void TearDown() override { std::cout << "Tear down StreamCatalogRemoteTest test class." << std::endl; }
};
TEST_F(StreamCatalogRemoteTest, testAddLogStreamRemote) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    //create test schema
    std::string testSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)->addField("
                             "\"value\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();

    bool registered = wrk->registerLogicalStream("testStream1", testSchemaFileName);
    EXPECT_TRUE(registered);

    SchemaPtr sPtr = crd->getStreamCatalog()->getSchemaForLogicalStream("testStream1");
    EXPECT_NE(sPtr, nullptr);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(StreamCatalogRemoteTest, testAddExistingLogStreamRemote) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    //create test schema
    std::string testSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)->addField("
                             "\"value\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();

    bool success = wrk->registerLogicalStream("default_logical", testSchemaFileName);
    EXPECT_TRUE(!success);

    SchemaPtr sPtr = crd->getStreamCatalog()->getSchemaForLogicalStream("default_logical");
    EXPECT_NE(sPtr, nullptr);

    //check if schma was not overwritten
    SchemaPtr sch = crd->getStreamCatalog()->getSchemaForLogicalStream("default_logical");
    EXPECT_NE(sch, nullptr);

    map<std::string, SchemaPtr> allLogicalStream = crd->getStreamCatalog()->getAllLogicalStream();
    string exp = "id:INTEGER value:INTEGER ";
    EXPECT_EQ(allLogicalStream.size(), 2U);

    SchemaPtr defaultSchema = allLogicalStream["default_logical"];
    EXPECT_EQ(exp, defaultSchema->toString());

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(StreamCatalogRemoteTest, testAddRemoveEmptyLogStreamRemote) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    //create test schema
    std::string testSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)->addField("
                             "\"value\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();

    bool success = wrk->registerLogicalStream("testStream", testSchemaFileName);
    EXPECT_TRUE(success);

    SchemaPtr sPtr = crd->getStreamCatalog()->getSchemaForLogicalStream("testStream");
    EXPECT_NE(sPtr, nullptr);

    bool success2 = wrk->unregisterLogicalStream("testStream");
    EXPECT_TRUE(success2);

    SchemaPtr sPtr2 = crd->getStreamCatalog()->getSchemaForLogicalStream("testStream");
    EXPECT_EQ(sPtr2, nullptr);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(StreamCatalogRemoteTest, testAddRemoveNotEmptyLogStreamRemote) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool success = wrk->unregisterLogicalStream("default_logical");
    EXPECT_TRUE(!success);

    SchemaPtr sPtr = crd->getStreamCatalog()->getSchemaForLogicalStream("default_logical");
    EXPECT_NE(sPtr, nullptr);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(StreamCatalogRemoteTest, addPhysicalToExistingLogicalStreamRemote) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    sourceConfig->setSourceConfig("");
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(2);
    sourceConfig->setPhysicalStreamName("physical_test");
    sourceConfig->setLogicalStreamName("default_logical");
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

    bool success = wrk->registerPhysicalStream(conf);
    EXPECT_TRUE(success);

    cout << crd->getStreamCatalog()->getPhysicalStreamAndSchemaAsString() << endl;
    std::vector<StreamCatalogEntryPtr> phys = crd->getStreamCatalog()->getPhysicalStreams("default_logical");

    EXPECT_EQ(phys.size(), 2U);
    EXPECT_EQ(phys[0]->getPhysicalName(), "default_physical");
    EXPECT_EQ(phys[1]->getPhysicalName(), "physical_test");

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(StreamCatalogRemoteTest, addPhysicalToNewLogicalStreamRemote) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    //create test schema
    std::string testSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)->addField("
                             "\"value\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();

    bool success = wrk->registerLogicalStream("testStream", testSchemaFileName);
    EXPECT_TRUE(success);

    sourceConfig->setSourceConfig("");
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(2);
    sourceConfig->setPhysicalStreamName("physical_test");
    sourceConfig->setLogicalStreamName("testStream");
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

    bool success2 = wrk->registerPhysicalStream(conf);
    EXPECT_TRUE(success2);

    cout << crd->getStreamCatalog()->getPhysicalStreamAndSchemaAsString() << endl;
    std::vector<StreamCatalogEntryPtr> phys = crd->getStreamCatalog()->getPhysicalStreams("testStream");

    EXPECT_EQ(phys.size(), 1U);
    EXPECT_EQ(phys[0]->getPhysicalName(), "physical_test");

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(StreamCatalogRemoteTest, removePhysicalFromNewLogicalStreamRemote) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool success = wrk->unregisterPhysicalStream("default_logical", "default_physical");
    EXPECT_TRUE(success);

    cout << crd->getStreamCatalog()->getPhysicalStreamAndSchemaAsString() << endl;
    std::vector<StreamCatalogEntryPtr> phys = crd->getStreamCatalog()->getPhysicalStreams("default_logical");

    EXPECT_EQ(phys.size(), 0U);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(StreamCatalogRemoteTest, removeNotExistingStreamRemote) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool success = wrk->unregisterPhysicalStream("default_logical2", "default_physical");
    EXPECT_TRUE(!success);

    SchemaPtr sPtr = crd->getStreamCatalog()->getSchemaForLogicalStream("default_logical");
    EXPECT_NE(sPtr, nullptr);

    cout << crd->getStreamCatalog()->getPhysicalStreamAndSchemaAsString() << endl;
    std::vector<StreamCatalogEntryPtr> phys = crd->getStreamCatalog()->getPhysicalStreams("default_logical");

    EXPECT_EQ(phys.size(), 1U);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}
}// namespace NES

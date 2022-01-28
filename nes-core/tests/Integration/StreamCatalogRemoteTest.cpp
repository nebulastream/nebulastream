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

#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Util/Logger.hpp>
#include <fstream>
#include <gtest/gtest.h>

using namespace std;
namespace NES {

using namespace Configurations;

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

TEST_F(StreamCatalogRemoteTest, addPhysicalToExistingLogicalStreamRemote) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    NES_INFO("StreamCatalogRemoteTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("StreamCatalogRemoteTest: Coordinator started successfully");

    NES_DEBUG("StreamCatalogRemoteTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setRpcPort(port + 10);
    workerConfig1->setDataPort(port + 11);
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath("");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    auto physicalSource1 = PhysicalSource::create("default_logical", "physical_test", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("StreamCatalogRemoteTest: Worker1 started successfully");

    cout << crd->getStreamCatalog()->getPhysicalStreamAndSchemaAsString() << endl;
    std::vector<SourceCatalogEntryPtr> phys = crd->getStreamCatalog()->getPhysicalSources("default_logical");

    EXPECT_EQ(phys.size(), 1U);
    EXPECT_EQ(phys[0]->getPhysicalSource()->getPhysicalSourceName(), "physical_test");

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(StreamCatalogRemoteTest, addPhysicalToNewLogicalStreamRemote) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    NES_INFO("StreamCatalogRemoteTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical stream qnv
    std::string window = "Schema::create()->addField(\"id\", BasicType::UINT32)->addField("
                         "\"value\", BasicType::UINT64);";
    crd->getStreamCatalogService()->registerLogicalSource("testStream", window);
    NES_DEBUG("StreamCatalogRemoteTest: Coordinator started successfully");

    NES_DEBUG("StreamCatalogRemoteTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setRpcPort(port + 10);
    workerConfig1->setDataPort(port + 11);
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath("");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    auto physicalSource1 = PhysicalSource::create("testStream", "physical_test", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("StreamCatalogRemoteTest: Worker1 started successfully");

    cout << crd->getStreamCatalog()->getPhysicalStreamAndSchemaAsString() << endl;
    std::vector<SourceCatalogEntryPtr> phys = crd->getStreamCatalog()->getPhysicalSources("testStream");

    EXPECT_EQ(phys.size(), 1U);
    EXPECT_EQ(phys[0]->getPhysicalSource()->getPhysicalSourceName(), "physical_test");

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(StreamCatalogRemoteTest, removePhysicalFromNewLogicalStreamRemote) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    NES_INFO("StreamCatalogRemoteTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical stream qnv
    std::string window = "Schema::create()->addField(\"id\", BasicType::UINT32)->addField("
                         "\"value\", BasicType::UINT64);";
    crd->getStreamCatalogService()->registerLogicalSource("testStream", window);
    NES_DEBUG("StreamCatalogRemoteTest: Coordinator started successfully");

    NES_DEBUG("StreamCatalogRemoteTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setRpcPort(port + 10);
    workerConfig1->setDataPort(port + 11);
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath("");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    auto physicalSource1 = PhysicalSource::create("default_logical", "physical_test", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("StreamCatalogRemoteTest: Worker1 started successfully");

    bool success = wrk1->unregisterPhysicalStream("default_logical", "physical_test");
    EXPECT_TRUE(success);

    cout << crd->getStreamCatalog()->getPhysicalStreamAndSchemaAsString() << endl;
    std::vector<SourceCatalogEntryPtr> phys = crd->getStreamCatalog()->getPhysicalSources("default_logical");

    EXPECT_EQ(phys.size(), 0U);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(StreamCatalogRemoteTest, removeNotExistingStreamRemote) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    NES_INFO("StreamCatalogRemoteTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical stream qnv
    std::string window = "Schema::create()->addField(\"id\", BasicType::UINT32)->addField("
                         "\"value\", BasicType::UINT64);";
    crd->getStreamCatalogService()->registerLogicalSource("testStream", window);
    NES_DEBUG("StreamCatalogRemoteTest: Coordinator started successfully");

    NES_DEBUG("StreamCatalogRemoteTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setRpcPort(port + 10);
    workerConfig1->setDataPort(port + 11);
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath("");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    auto physicalSource1 = PhysicalSource::create("default_logical", "physical_test", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("StreamCatalogRemoteTest: Worker1 started successfully");

    bool success = wrk1->unregisterPhysicalStream("default_logical2", "default_physical");
    EXPECT_TRUE(!success);

    SchemaPtr sPtr = crd->getStreamCatalog()->getSchemaForLogicalStream("default_logical");
    EXPECT_NE(sPtr, nullptr);

    cout << crd->getStreamCatalog()->getPhysicalStreamAndSchemaAsString() << endl;
    std::vector<SourceCatalogEntryPtr> phys = crd->getStreamCatalog()->getPhysicalSources("default_logical");

    EXPECT_EQ(phys.size(), 1U);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}
}// namespace NES

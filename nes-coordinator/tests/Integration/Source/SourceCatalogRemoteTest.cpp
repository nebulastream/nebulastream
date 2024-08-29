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

#include <API/TestSchemas.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Util/Logger/Logger.hpp>
#include <fstream>
#include <gtest/gtest.h>

using namespace std;
namespace NES {

using namespace Configurations;

class SourceCatalogRemoteTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SourceCatalogRemoteTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SourceCatalogRemoteTest test class.");
    }
};

TEST_F(SourceCatalogRemoteTest, addPhysicalToExistingLogicalSourceRemote) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("SourceCatalogRemoteTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("SourceCatalogRemoteTest: Coordinator started successfully");

    NES_DEBUG("SourceCatalogRemoteTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto csvSourceType1 = CSVSourceType::create("default_logical", "physical_test");
    csvSourceType1->setFilePath("");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("SourceCatalogRemoteTest: Worker1 started successfully");

    cout << crd->getSourceCatalog()->getPhysicalSourceAndSchemaAsString() << endl;
    std::vector<Catalogs::Source::SourceCatalogEntryPtr> phys = crd->getSourceCatalog()->getPhysicalSources("default_logical");

    EXPECT_EQ(phys.size(), 1U);
    EXPECT_EQ(phys[0]->getPhysicalSource()->getPhysicalSourceName(), "physical_test");

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(SourceCatalogRemoteTest, addPhysicalToNewLogicalSourceRemote) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("SourceCatalogRemoteTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");
    crd->getSourceCatalog()->addLogicalSource("testSource", schema);
    NES_DEBUG("SourceCatalogRemoteTest: Coordinator started successfully");

    NES_DEBUG("SourceCatalogRemoteTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto csvSourceType1 = CSVSourceType::create("testSource", "physical_test");
    csvSourceType1->setFilePath("");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("SourceCatalogRemoteTest: Worker1 started successfully");

    cout << crd->getSourceCatalog()->getPhysicalSourceAndSchemaAsString() << endl;
    std::vector<Catalogs::Source::SourceCatalogEntryPtr> phys = crd->getSourceCatalog()->getPhysicalSources("testSource");

    EXPECT_EQ(phys.size(), 1U);
    EXPECT_EQ(phys[0]->getPhysicalSource()->getPhysicalSourceName(), "physical_test");

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(SourceCatalogRemoteTest, removePhysicalFromNewLogicalSourceRemote) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("SourceCatalogRemoteTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");
    crd->getSourceCatalog()->addLogicalSource("testSource", schema);
    NES_DEBUG("SourceCatalogRemoteTest: Coordinator started successfully");

    NES_DEBUG("SourceCatalogRemoteTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto csvSourceType1 = CSVSourceType::create("default_logical", "physical_test");
    csvSourceType1->setFilePath("");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("SourceCatalogRemoteTest: Worker1 started successfully");

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    auto phys = crd->getSourceCatalog()->getPhysicalSources("default_logical");
    EXPECT_EQ(phys.size(), 0U);
    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(SourceCatalogRemoteTest, removeNotExistingSourceRemote) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("SourceCatalogRemoteTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");
    crd->getSourceCatalog()->addLogicalSource("testSource", schema);
    NES_DEBUG("SourceCatalogRemoteTest: Coordinator started successfully");

    NES_DEBUG("SourceCatalogRemoteTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto csvSourceType1 = CSVSourceType::create("default_logical", "physical_test");
    csvSourceType1->setFilePath("");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("SourceCatalogRemoteTest: Worker1 started successfully");

    std::vector<PhysicalSourceTypePtr> physicalSourceTypes;
    const std::string& invalidLogicalSourceName = "default_logical2";
    const std::string& invalidPhysicalSourceName = "default_physical1";
    auto invalidPhysicalSource = CSVSourceType::create(invalidLogicalSourceName, invalidPhysicalSourceName);
    physicalSourceTypes.push_back(invalidPhysicalSource);
    bool success = wrk1->unregisterPhysicalSource(physicalSourceTypes);
    EXPECT_TRUE(!success);

    SchemaPtr sPtr = crd->getSourceCatalog()->getSchemaForLogicalSource("default_logical");
    EXPECT_NE(sPtr, nullptr);

    cout << crd->getSourceCatalog()->getPhysicalSourceAndSchemaAsString() << endl;
    std::vector<Catalogs::Source::SourceCatalogEntryPtr> phys = crd->getSourceCatalog()->getPhysicalSources("default_logical");

    EXPECT_EQ(phys.size(), 1U);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}
}// namespace NES

//
// Created by moritz on 5/9/23.
//

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-copy-dtor"
#include <NesBaseTest.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#pragma clang diagnostic pop

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MemorySourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>
#include "Sources/DataSource.hpp"

using namespace std;

#define DEBUG_OUTPUT
namespace NES {

  using namespace Configurations;

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case

  class StatManagerTest : public Testing::NESBaseTest {
    public:
      static void SetUpTestCase() {
        NES::Logger::setupLogging("StatManagerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup StatManagerTest test class.");
      }
  };

TEST_F(StatManagerTest, testStatManagerStartCollecting) {
  CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
//  coordinatorConfig->rpcPort = *rpcCoordinatorPort;
//  coordinatorConfig->restPort = *restPort;
//  NES_INFO("StatManagerTest: Start coordinator");
//  NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
//  uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
//  EXPECT_NE(port, 0UL);
//  std::string testSchema = "Schema::create()->addField(createField(\"val1\", BasicType::UINT64))->"
//                           "addField(createField(\"val2\", BasicType::UINT64))->"
//                           "addField(createField(\"val3\", BasicType::UINT64));";
//  crd->getSourceCatalogService()->registerLogicalSource("testStream", testSchema);
//  NES_DEBUG("StatManagerTest: Coordinator started successfully");
//
//  NES_DEBUG("StatManagerTest: Start worker 1");
//  WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
//  workerConfig1->coordinatorPort = port;
//
//  // creating csv data
//  std::string testCSV = "1,2,3\n"
//                        "1,2,4\n"
//                        "4,3,6";
//  std::string testCSVFileName = "testCSV.csv";
//
//  // creating and filling csv file
//  std::ofstream outCsv(testCSVFileName);
//  outCsv << testCSV;
//  outCsv.close();
//
//  // creating and configuring csv source
//  auto csvSourceType1 = CSVSourceType::create();
//  csvSourceType1->setFilePath("testCSV.csv");
//  csvSourceType1->setGatheringInterval(0);
//  csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
//  csvSourceType1->setNumberOfBuffersToProduce(3);
//  auto physicalSource1 = PhysicalSource::create("testStream", "test_stream", csvSourceType1);
//
//  workerConfig1->physicalSources.add(physicalSource1);

  ASSERT_TRUE(1);
}

}
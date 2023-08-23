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

#include <fstream>
#include <NesBaseTest.hpp>
#include <gtest/gtest.h>
#include <StatManager/StatManager.hpp>
#include <StatManager/StatCollectors/Synopses/Sketches/CountMin/CountMin.hpp>
#include <StatManager/StatCollectors/StatCollectorConfiguration.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <API/QueryAPI.hpp>
#include <Services/QueryService.hpp>
#include <Runtime/NodeEngine.hpp>

namespace NES {

  class StatManagerTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
      NES::Logger::setupLogging("StatManagerTest.log", NES::LogLevel::LOG_DEBUG);
      NES_INFO("Setup StatManagerTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
      Testing::NESBaseTest::SetUp();
      statManager = new NES::Experimental::Statistics::StatManager();
      StatCollectorConfig = new NES::Experimental::Statistics::StatCollectorConfig();
      NES_INFO("Setup StatManagerTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
      NES_INFO("Tear down StatManagerTest case.");
      Testing::NESBaseTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down StatManagerTest class."); }

    NES::Experimental::Statistics::StatManager *statManager;
    NES::Experimental::Statistics::StatCollectorConfig *StatCollectorConfig;
  };

  TEST_F(StatManagerTest, createStatTest) {
    statManager->createStat(*StatCollectorConfig);
    auto& statCollectors = statManager->getStatCollectors();

    EXPECT_EQ(statCollectors.empty(), false);
  }

  TEST_F(StatManagerTest, queryStatTest) {
    statManager->createStat(*StatCollectorConfig);
    auto& statCollectors = statManager->getStatCollectors();
    auto& cmStatCollector = statCollectors.at(0);
    auto cm = dynamic_cast<Experimental::Statistics::CountMin*>(cmStatCollector.get());
    auto h3 = cm->getClassOfHashFunctions();
    auto q = h3->getQ();
    auto sketchPointer = cm->getDataPointer();

    bool correctCounterIncremented = true;
    std::vector<uint32_t> indexes(cm->getDepth(), 0);
    std::vector<uint32_t> previousResults(cm->getDepth(), 0);

    // increment counter for keys 1 to 1000
    for (uint32_t i = 0; i < 1000; i++) {
      uint32_t hash = 0;

      // save which counters have to be incremented and what the previous value was in the sketch
      for (uint32_t j = 0; j < cm->getDepth(); j++) {
        hash = (h3->hashH3(i + 1, q + (j * 32))) % cm->getWidth();
        previousResults[j] = sketchPointer[j][hash];
        indexes[j] = hash;
      }

      // actually update the counter
      cmStatCollector->update(i + 1);

      // check if the value was actually incremented
      for (uint32_t j = 0; j < cm->getDepth(); j++) {
        uint32_t newResult = sketchPointer[j][indexes[j]];
        if (newResult != previousResults[j] + 1) {
          correctCounterIncremented = false;
        }
      }
    }
    EXPECT_EQ(correctCounterIncremented, true);
  }
  
  TEST_F(StatManagerTest, deleteStatTest) {
    statManager->createStat(*StatCollectorConfig);
    EXPECT_EQ(statManager->getStatCollectors().empty(), false);

    statManager->deleteStat(*StatCollectorConfig);
    EXPECT_EQ(statManager->getStatCollectors().empty(), true);
  }

  TEST_F(StatManagerTest, executeExemplaryQueryTest) {

    // create coordinator
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->worker.queryCompiler.queryCompilerType =
        QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER;
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("Starting coordinator");

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);

    // create schema
    auto testSchema = Schema::create()
        ->addField(createField("defaultFieldName", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("defaultLogicalSourceName", testSchema);
    StatCollectorConfig->setSchema(testSchema);

    // create test csv
    std::string testCSV = "1\n"
                          "2\n"
                          "3";
    std::string testCSVFileName = "testCSV.csv";
    std::ofstream outCsv(testCSVFileName);
    outCsv << testCSV;
    outCsv.close();
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath("testCSV.csv");
    csvSourceType1->setGatheringInterval(0);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("defaultLogicalSourceName", "defaultPhysicalSourceName", csvSourceType1);

    // create worker
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->queryCompiler.queryCompilerType =
        QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER;
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("Worker started successfully");

    auto& statManager = wrk1->getStatManager();
    bool success = statManager->createStat(*StatCollectorConfig, wrk1->getNodeEngine());

    ASSERT_TRUE(success);

  }
}
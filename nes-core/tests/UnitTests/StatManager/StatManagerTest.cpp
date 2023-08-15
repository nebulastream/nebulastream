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
}
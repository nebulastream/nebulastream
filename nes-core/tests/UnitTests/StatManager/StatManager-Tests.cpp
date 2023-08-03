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
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "StatManager/StatManager.hpp"

#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Configurations/StatManagerConfiguration.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Services/QueryService.hpp>
#include <Util/TestUtils.hpp>

namespace NES {

  using namespace Configurations;

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
      statManager = new StatManager();
      statManagerConfig = new StatManagerConfig();
      NES_INFO("Setup StatManagerTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
      NES_INFO("Tear down StatManagerTest case.");
      Testing::NESBaseTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down StatManagerTest class."); }

    StatManager *statManager;
    StatManagerConfig *statManagerConfig;
  };

  TEST_F(StatManagerTest, createStatTest) {
    statManager->createStat(*statManagerConfig);
    auto statCollectors = statManager->getStatCollectors();

    EXPECT_EQ(statCollectors.empty(), false);
  }

  TEST_F(StatManagerTest, queryStatTest) {
    statManager->createStat(*statManagerConfig);
    auto statCollectors = statManager->getStatCollectors();
    auto cm = statCollectors.at(0);

    for (uint32_t i = 0; i < 1000000; i++) {
      cm->update(i);
    }

    bool nearlyEqual = 1;
    for (uint32_t i = 0; i < 32 * 1024; i++) {

      auto stat = statManager->queryStat(*statManagerConfig, i);
      if (stat < 29 && stat > 32) {
        nearlyEqual = 0;
      }
    }
    EXPECT_EQ(nearlyEqual, true);
  }


  TEST_F(StatManagerTest, deleteStatTest) {
    statManager->createStat(*statManagerConfig);
    EXPECT_EQ(statManager->getStatCollectors().empty(), false);

    statManager->deleteStat(*statManagerConfig);
    EXPECT_EQ(statManager->getStatCollectors().empty(), true);
  }
}
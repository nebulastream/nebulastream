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

#include "../util/NesBaseTest.hpp"
#include "Exceptions/InvalidQueryException.hpp"
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger//Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

class QueryFailureTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryFailureTest.log", NES::LogLevel::LOG_DEBUG);
    }
};

TEST_F(QueryFailureTest, DISABLED_failQueryUponSubmission) {
    struct Test {
        uint32_t id;
        uint32_t value;
    };

    struct Output {
        uint32_t id;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (id == rhs.id && value == rhs.value); }
    };

    auto defaultLogicalSchema =
        Schema::create()->addField("id", DataTypeFactory::createUInt32())->addField("value", DataTypeFactory::createUInt32());

    ASSERT_EQ(sizeof(Test), defaultLogicalSchema->getSchemaSizeInBytes());
    CSVSourceTypePtr cfg;
    cfg = CSVSourceType::create();
    cfg->setFilePath(std::string(TEST_DATA_DIRECTORY) + "/malformed_csv_test.csv");
    cfg->setGatheringInterval(1);
    cfg->setNumberOfTuplesToProducePerBuffer(2);
    cfg->setNumberOfBuffersToProduce(6);
    cfg->setSkipHeader(false);
    string query = R"(Query::from("test"))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("test", defaultLogicalSchema)
                                  .attachWorkerWithCSVSourceToCoordinator("test", cfg);

//    testHarness = testHarness.pushElement<Test>({1, 1}, 2);

    try {
        testHarness.validate().setupTopology();

        std::vector<Output> actualOutput = testHarness.getOutput<Output>(1, "BottomUp", "NONE", "IN_MEMORY");

        ASSERT_EQ(actualOutput.size(), 1);
    } catch (std::exception& ex) {
        NES_ERROR("Error: " << ex.what());
        FAIL();
    }

}

TEST_F(QueryFailureTest, DISABLED_failRunningQuery) {}

}// namespace NES
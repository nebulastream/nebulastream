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

#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>

namespace NES {

uint16_t timeout = 5;

class E2EChameleonTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("E2EChameleonTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup E2e test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down E2EChameleonTest test class."); }
};

TEST_F(E2EChameleonTest, testAdaptiveIngestion) {
    std::string pathQuery1 = "query1.out";
    remove(pathQuery1.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"sensors\",\"schema\" : \"Schema::create()->addField(createField(\\\"value\\\", "
              "BasicType::UINT64))->addField(createField(\\\"value\\\", "
              "BasicType::FLOAT64))->addField(createField(\\\"payload\\\", "
              "BasicType::FLOAT64))->addField(createField(\\\"timestamp\\\", BasicType::FLOAT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    auto workerConf = {TestUtils::rpcPort(0),
                       TestUtils::dataPort(0),
                       TestUtils::enableDebug(),
                       TestUtils::coordinatorPort(*rpcCoordinatorPort),
                       TestUtils::sourceType(SourceType::CSV_SOURCE),
                       TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "chameleon-test.csv"),
                       TestUtils::gatheringMode(GatheringMode::ADAPTIVE_MODE),
                       TestUtils::logicalSourceName("sensors"),
                       TestUtils::physicalSourceName("sensor1"),
                       TestUtils::numberOfBuffersToProduce(1),
                       TestUtils::sourceGatheringInterval(4000)};
    auto worker = TestUtils::startWorker(workerConf);
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"sensors\"))";
    ss << R"(.sink(FileSinkDescriptor::create(\")";
    ss << pathQuery1;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")))";
    ss << R"(;","placement" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit=" << ss.str());

    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));

    NES_INFO("try to acc return");
    QueryId queryId = json_return["queryId"].get<uint64_t>();
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));
    // TODO: dump stats
}
}
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

#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <chrono>//for timing execution
#include <gtest/gtest.h>
#include <iostream>
#include <regex>

namespace NES {

using namespace Configurations;

class FilterPushDownTest : public Testing::BaseIntegrationTest {
  public:
    CoordinatorConfigurationPtr coConf;
    SchemaPtr schema;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("AndOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup AndOperatorTest test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        coConf = CoordinatorConfiguration::createDefault();

        coConf->rpcPort = (*rpcCoordinatorPort);
        coConf->restPort = *restPort;

        schema = Schema::create()
                     ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                     ->addField(createField("timestamp", BasicType::UINT64))
                     ->addField(createField("velocity", BasicType::UINT64))
                     ->addField(createField("quantity", BasicType::UINT64));
    }

    struct Output {
        uint64_t QnV1$timestamp;
        uint64_t QnV1$velocity;
        uint64_t QnV1$quantity;

        bool operator==(Output const& rhs) const {
            return (QnV1$timestamp == rhs.QnV1$timestamp && QnV1$velocity == rhs.QnV1$velocity
                    && QnV1$quantity == rhs.QnV1$quantity);
        }
    };
};

/* 1.Test
 * This test checks if the filter push down below map keeps the correct order of operations when we apply a map with a substractions
 * followed by a map with a multiplication
 */
TEST_F(FilterPushDownTest, testCorrectResultsForFilterPushDownBelowTwoMaps) {

    NES_INFO("FilterPushDownTest: Start testCorrectResultsForFilterPushDownBelowTwoMaps");

    auto srcConf1 = CSVSourceType::create("QnV1", "PQnV1");
    srcConf1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(5);
    srcConf1->setNumberOfBuffersToProduce(20);

    std::string outputFilePath = getTestResourceFolder() / "filterPushDownTest.out";
    remove(outputFilePath.c_str());

    auto query = Query::from("QnV1")
                     .map(Attribute("velocity") = Attribute("velocity") - 5)
                     .map(Attribute("velocity") = 5 * Attribute("velocity"))
                     .filter(Attribute("velocity") < 100)
                     .project(Attribute("timestamp"), Attribute("velocity"), Attribute("quantity"));

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("QnV1", schema)
                                  .attachWorkerWithCSVSourceToCoordinator(srcConf1)
                                  .validate()
                                  .setupTopology();

    std::vector<Output> expectedOutput = {{1543624260000, 95, 2},
                                          {1543625520000, 95, 3},
                                          {1543625940000, 80, 1},
                                          {1543626120000, 75, 1},
                                          {1543626420000, 75, 2}};
    NES_INFO("FilterPushDownTest: Start testCorrectResultsForFilterPushDownBelowTwoMaps expected output size {},",
             expectedOutput.size());
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");
    NES_INFO("FilterPushDownTest: Start testCorrectResultsForFilterPushDownBelowTwoMaps expected output size {},",
             actualOutput.size());

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/* 2.Test
 * This test checks if the filter push down below map keeps the correct order of operations when we apply a map with a subtraction
 * in parentheses and a multiplication
 */
TEST_F(FilterPushDownTest, testSameResultsForPushDownBelowMapWithMul) {
    NES_INFO("FilterPushDownTest: Start testCorrectResultsForFilterPushDownBelowTwoMaps");

    auto srcConf1 = CSVSourceType::create("QnV1", "PQnV1");
    srcConf1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(5);
    srcConf1->setNumberOfBuffersToProduce(20);

    std::string outputFilePath = getTestResourceFolder() / "filterPushDownTest.out";
    remove(outputFilePath.c_str());

    auto query = Query::from("QnV1")
                     .map(Attribute("velocity") = 5 * (Attribute("velocity") - 5))
                     .filter(Attribute("velocity") < 100)
                     .project(Attribute("timestamp"), Attribute("velocity"), Attribute("quantity"));

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("QnV1", schema)
                                  .attachWorkerWithCSVSourceToCoordinator(srcConf1)
                                  .validate()
                                  .setupTopology();

    std::vector<Output> expectedOutput = {{1543624260000, 95, 2},
                                          {1543625520000, 95, 3},
                                          {1543625940000, 80, 1},
                                          {1543626120000, 75, 1},
                                          {1543626420000, 75, 2}};
    NES_INFO("FilterPushDownTest: Start testCorrectResultsForFilterPushDownBelowTwoMaps expected output size {},",
             expectedOutput.size());
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");
    NES_INFO("FilterPushDownTest: Start testCorrectResultsForFilterPushDownBelowTwoMaps expected output size {},",
             actualOutput.size());

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/* 2.Test
 * This test checks if the filter push down below a map with a new field name works correctly
 */
TEST_F(FilterPushDownTest, testSameResultsForPushDownBelowMapWithNewField) {
    NES_INFO("FilterPushDownTest: Start testSameResultsForPushDownBelowMapWithNewField");

    auto srcConf1 = CSVSourceType::create("QnV1", "PQnV1");
    srcConf1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(5);
    srcConf1->setNumberOfBuffersToProduce(20);

    std::string outputFilePath = getTestResourceFolder() / "filterPushDownTest.out";
    remove(outputFilePath.c_str());

    auto query = Query::from("QnV1")
                     .map(Attribute("NewVelocity") = 5 * (Attribute("velocity") - 5))
                     .filter(Attribute("NewVelocity") < 100)
                     .project(Attribute("timestamp"), Attribute("NewVelocity"), Attribute("quantity"));

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("QnV1", schema)
                                  .attachWorkerWithCSVSourceToCoordinator(srcConf1)
                                  .validate()
                                  .setupTopology();

    std::vector<Output> expectedOutput = {{1543624260000, 95, 2},
                                          {1543625520000, 95, 3},
                                          {1543625940000, 80, 1},
                                          {1543626120000, 75, 1},
                                          {1543626420000, 75, 2}};
    NES_INFO("FilterPushDownTest: Start testCorrectResultsForFilterPushDownBelowTwoMaps expected output size {},",
             expectedOutput.size());
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");
    NES_INFO("FilterPushDownTest: Start testCorrectResultsForFilterPushDownBelowTwoMaps expected output size {},",
             actualOutput.size());

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

}// namespace NES
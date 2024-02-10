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
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>

namespace NES {

using namespace Configurations;

class UnionDeploymentTest : public Testing::BaseIntegrationTest {
  public:
    CSVSourceTypePtr sourceCar;
    CSVSourceTypePtr sourceTruck;
    CSVSourceTypePtr sourceRuby;
    CSVSourceTypePtr sourceDiamond;
    SchemaPtr schemaCarTruck;
    SchemaPtr schemaRubyDiamond;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("UnionDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup UnionDeploymentTest test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();

        // Setup sources.
        sourceCar = TestUtils::createSourceTypeCSV({"car", "car_physical", "car.csv", 1, 40, 1});
        sourceTruck = TestUtils::createSourceTypeCSV({"truck", "truck_physical", "truck.csv", 1, 40, 1});
        sourceRuby = TestUtils::createSourceTypeCSV({"ruby", "ruby_physical", "window.csv", 1, 28, 1});
        sourceDiamond = TestUtils::createSourceTypeCSV({"diamond", "diamond_physical", "window.csv", 1, 28, 1});

        // Setup schemas.
        schemaCarTruck = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
        schemaRubyDiamond = Schema::create()
                                ->addField(createField("value", BasicType::UINT32))
                                ->addField(createField("id", BasicType::UINT32))
                                ->addField(createField("timestamp", BasicType::INT32));
    }

    std::string testName = "UnionDeploymentTest";
};

/**
 * Test deploying unionWith query with source on two different worker node using the bottom up strategy.
 */
TEST_F(UnionDeploymentTest, testDeployTwoWorkerMergeUsingBottomUp) {
    std::string outputFilePath = getTestResourceFolder() / "testDeployTwoWorkerMergeUsingBottomUp.out";
    const auto query = Query::from("car").unionWith(Query::from("truck"));
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("car", schemaCarTruck)
                                  .attachWorkerWithCSVSourceToCoordinator(sourceCar)
                                  .addLogicalSource("truck", schemaCarTruck)
                                  .attachWorkerWithCSVSourceToCoordinator(sourceTruck)
                                  .validate()
                                  .setupTopology();

    // Expected output the first 40 entries are car values (0,0 - 39,39), the last 40 entries are truck values (0,0 - 39,3900).
    std::stringstream expectedOutput;
    for (uint32_t i = 0; i < 80; i++) {
        bool isCarValue = i < 40;
        // Start counting from 0 again when the 40 car values were processed.
        uint32_t id = i % 40;
        uint32_t value = (isCarValue) ? id : id * 100;
        expectedOutput << id << ", " << value << "\n";
    }

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromStream(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createDynamicBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * Test deploying unionWith query with source on two different worker node using the topDown strategy.
 */
TEST_F(UnionDeploymentTest, testDeployTwoWorkerMergeUsingTopDown) {
    std::string outputFilePath = getTestResourceFolder() / "testDeployTwoWorkerMergeUsingTopDown.out";
    const auto query = Query::from("car").unionWith(Query::from("truck"));
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("car", schemaCarTruck)
                                  .attachWorkerWithCSVSourceToCoordinator(sourceCar)
                                  .addLogicalSource("truck", schemaCarTruck)
                                  .attachWorkerWithCSVSourceToCoordinator(sourceTruck)
                                  .validate()
                                  .setupTopology();

    // Expected output the first 40 entries are car values (0,0 - 39,39), the last 40 entries are truck values (0,0 - 39,3900).
    std::stringstream expectedOutput;
    for (uint32_t i = 0; i < 80; i++) {
        bool isCarValue = i < 40;
        // Start counting from 0 again when the 40 car values were processed.
        uint32_t id = i % 40;
        uint32_t value = (isCarValue) ? id : id * 100;
        expectedOutput << id << ", " << value << "\n";
    }

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromStream(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createDynamicBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * Test deploying a union query with filters and sources on two different worker nodes using bottomUp strategy.
 */
TEST_F(UnionDeploymentTest, testOneFilterPushDownWithMergeOfTwoDifferentSources) {
    std::string outputFilePath =
        getTestResourceFolder() / "testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentSources.out";
    const auto query = Query::from("ruby")
                           .filter(Attribute("id") > 4)
                           .unionWith(Query::from("diamond")
                                          .map(Attribute("timestamp") = 1)
                                          .filter(Attribute("id") > 3)
                                          .map(Attribute("timestamp") = 2)
                                          .filter(Attribute("id") > 4));
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("ruby", schemaRubyDiamond)
                                  .attachWorkerWithCSVSourceToCoordinator(sourceRuby)
                                  .addLogicalSource("diamond", schemaRubyDiamond)
                                  .attachWorkerWithCSVSourceToCoordinator(sourceDiamond)
                                  .validate()
                                  .setupTopology();

    // Expected output
    auto expectedOutput = "1, 12, 2\n"
                          "2, 11, 2\n"
                          "2, 16, 2\n"
                          "3, 11, 2\n"
                          "1, 12, 1001\n"
                          "2, 11, 2001\n"
                          "2, 16, 2002\n"
                          "3, 11, 3001\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createDynamicBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * Test deploying a union query with filters and sources on two different worker nodes using topDown strategy.
 */
TEST_F(UnionDeploymentTest, testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentSources) {
    std::string outputFilePath =
        getTestResourceFolder() / "testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentSources.out";
    const auto query = Query::from("ruby")
                           .filter(Attribute("value") > 3)
                           .unionWith(Query::from("diamond")
                                          .filter(Attribute("value") < 15)
                                          .map(Attribute("timestamp") = 1)
                                          .filter(Attribute("value") < 17)
                                          .map(Attribute("timestamp") = 2)
                                          .filter(Attribute("value") > 3));
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("ruby", schemaRubyDiamond)
                                  .attachWorkerWithCSVSourceToCoordinator(sourceRuby)
                                  .addLogicalSource("diamond", schemaRubyDiamond)
                                  .attachWorkerWithCSVSourceToCoordinator(sourceDiamond)
                                  .validate()
                                  .setupTopology();

    // Expected output the first 17 entries are raw entries from the window.csv file stream.
    // The next 12 entries are the result of the second query of the unionWith statement.
    std::stringstream expectedOutput;
    uint32_t numFirstQueryValues = 18;
    for (uint32_t i = 0; i < 29; i++) {
        uint32_t id = 1;
        uint32_t value = (i % numFirstQueryValues) + 4;
        uint32_t timestamp = (i < numFirstQueryValues) ? value * 1000 : 2;
        expectedOutput << value << ", " << id << ", " << timestamp << "\n";
    }

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromStream(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createDynamicBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * Test a union query with multiple filters for both sources and two different worker nodes using the bottomUp strategy.
 */
TEST_F(UnionDeploymentTest, testPushingTwoFiltersAlreadyBelowAndMergeOfTwoDifferentSources) {
    std::string outputFilePath =
        getTestResourceFolder() / "testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentSources.out";
    const auto query = Query::from("ruby")
                           .map(Attribute("timestamp") = 2)
                           .filter(Attribute("id") < 4)
                           .filter(Attribute("value") > 3)
                           .unionWith(Query::from("diamond")
                                          .map(Attribute("timestamp") = 1)
                                          .filter(Attribute("id") < 4)
                                          .filter(Attribute("value") > 3)
                                          .filter(Attribute("value") < 6));
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("ruby", schemaRubyDiamond)
                                  .attachWorkerWithCSVSourceToCoordinator(sourceRuby)
                                  .addLogicalSource("diamond", schemaRubyDiamond)
                                  .attachWorkerWithCSVSourceToCoordinator(sourceDiamond)
                                  .validate()
                                  .setupTopology();

    // Expected output the first 18 entries come from the first part of the unionWith statement.
    // The next 2 entries are the result of the second query of the unionWith statement.
    std::stringstream expectedOutput;
    uint32_t numFirstQueryValues = 18;
    for (uint32_t i = 0; i < 20; i++) {
        uint32_t id = 1;
        uint32_t value = (i % numFirstQueryValues) + 4;
        uint32_t timestamp = (i < numFirstQueryValues) ? 2 : 1;
        expectedOutput << value << ", " << id << ", " << timestamp << "\n";
    }

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromStream(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createDynamicBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}
}// namespace NES

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
        sourceCar = createCSVSource("car.csv", 40, 1);
        sourceTruck = createCSVSource("truck.csv", 40, 1);
        sourceRuby = createCSVSource("window.csv", 28, 1);
        sourceDiamond = createCSVSource("window.csv", 28, 1);

        // Setup schemas.
        schemaCarTruck = Schema::create()
                          ->addField("id", BasicType::UINT32)
                          ->addField("value", BasicType::UINT64);
        schemaRubyDiamond = Schema::create()
                        ->addField(createField("value", BasicType::UINT32))
                        ->addField(createField("id", BasicType::UINT32))
                        ->addField(createField("timestamp", BasicType::INT32));
    }

    /**
     * @brief Utility function that creates a csv source.
     * 
     * @param sourcePath: Path to the csv source file.
     * @param numTuplesPerBuffer: How many tuples are stored in one TupleBuffer.
     * @param numBuffersToProduce: The number of TupleBuffers that are produced.
     * @return CSVSourceTypePtr: The newly created CSV source.
     */
    CSVSourceTypePtr createCSVSource(const std::string& sourcePath, const uint64_t numTuplesPerBuffer, 
                                     const uint64_t numBuffersToProduce) {
        auto csvSource = CSVSourceType::create();
        csvSource->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / sourcePath);
        csvSource->setGatheringInterval(1);
        csvSource->setNumberOfTuplesToProducePerBuffer(numTuplesPerBuffer);
        csvSource->setNumberOfBuffersToProduce(numBuffersToProduce);
        csvSource->setSkipHeader(false);

        return csvSource;
    }

    std::string testName = "UnionDeploymentTest";

    // Without the packed attribute, the optimizer might use 64 bits for the uint32_t, causing an error.
    struct __attribute__((packed)) OutputCarTruck {
        uint32_t car$id;
        uint64_t car$value;

        bool operator==(OutputCarTruck const& rhs) const {
            return (car$id == rhs.car$id && car$value == rhs.car$value);
        }
    };

    struct __attribute__((packed)) OutputRubyDiamond {
        uint32_t ruby$id;
        uint32_t ruby$value;
        uint32_t ruby$timestamp;

        bool operator==(OutputRubyDiamond const& rhs) const {
            return (ruby$id == rhs.ruby$id && ruby$value == rhs.ruby$value && ruby$timestamp == rhs.ruby$timestamp);
        }
    };
};

/**
 * Test deploying unionWith query with source on two different worker node using bottom up strategy.
 */
TEST_F(UnionDeploymentTest, testDeployTwoWorkerMergeUsingBottomUp) {
    std::string outputFilePath = getTestResourceFolder() / "testDeployTwoWorkerMergeUsingBottomUp.out";
    const auto query = Query::from("car").unionWith(Query::from("truck"));
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("car", schemaCarTruck)
                                  .attachWorkerWithCSVSourceToCoordinator("car", sourceCar) 
                                  .addLogicalSource("truck", schemaCarTruck)
                                  .attachWorkerWithCSVSourceToCoordinator("truck", sourceTruck)
                                  .validate()
                                  .setupTopology();
    std::vector<OutputCarTruck> expectedOutput;
    // The first 40 entries are car values (0,0 - 39,39), the last 40 entries are truck values (0,0 - 39,3900).
    for(uint32_t i = 0; i < 80; i++) {
        bool isCarValue = i < 40;
        // Start counting from 0 again when the 40 car values were processed.
        uint32_t id = i % 40;
        uint32_t value = (isCarValue) ? id : id * 100;
        expectedOutput.emplace_back(OutputCarTruck{id, value});
    }

    std::string placementStrategy = "BottomUp";
    std::string faultTolerance = "NONE";
    std::string lineage = "IN_MEMORY";
    std::vector<OutputCarTruck> actualOutput = testHarness.getOutput<OutputCarTruck>(
        /* num output tuples expected */ expectedOutput.size(), 
        /* placement strategy*/ "BottomUp",
        /* fault tolerance*/ "NONE",
        /* lineage*/ "IN_MEMORY"
        );
    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * Test deploying unionWith query with source on two different worker node using top up strategy.
 */
TEST_F(UnionDeploymentTest, testDeployTwoWorkerMergeUsingTopDown) {
    std::string outputFilePath = getTestResourceFolder() / "testDeployTwoWorkerMergeUsingTopDown.out";
    const auto query = Query::from("car").unionWith(Query::from("truck"));
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("car", schemaCarTruck)
                                  .attachWorkerWithCSVSourceToCoordinator("car", sourceCar) 
                                  .addLogicalSource("truck", schemaCarTruck)
                                  .attachWorkerWithCSVSourceToCoordinator("truck", sourceTruck)
                                  .validate()
                                  .setupTopology();
    std::vector<OutputCarTruck> expectedOutput;
    // The first 40 entries are car values (0,0 - 39,39), the last 40 entries are truck values (0,0 - 39,3900).
    for(uint32_t i = 0; i < 80; i++) {
        bool isCarValue = i < 40;
        // Start counting from 0 again when the 40 car values were processed.
        uint32_t id = i % 40;
        uint32_t value = (isCarValue) ? id : id * 100;
        expectedOutput.emplace_back(OutputCarTruck{id, value});
    }

    std::vector<OutputCarTruck> actualOutput = testHarness.getOutput<OutputCarTruck>(
        /* num output tuples expected */ expectedOutput.size(), 
        /* placement strategy*/ "TopDown",
        /* fault tolerance*/ "NONE",
        /* lineage*/ "IN_MEMORY"
        );
    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * Test deploying filter-push-down on unionWith query with source on two different worker node using top down strategy.
 * Case: 2 filter operators are above a unionWith operator and will be pushed down towards both of the available sources.
 *       2 filter operators are already below unionWith operator and need to be pushed down normally towards its respective source.
 */
TEST_F(UnionDeploymentTest, testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentSources) {
    std::string outputFilePath = getTestResourceFolder() / "testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentSources.out";
    const auto query = 
        Query::from("ruby")
        .filter(Attribute("value") > 3)
        .unionWith(
            Query::from("diamond")
            .filter(Attribute("value") < 15)
            .map(Attribute("timestamp") = 1)
            .filter(Attribute("value") < 17)
            .map(Attribute("timestamp") = 2)
            .filter(Attribute("value") > 3)
        );
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("ruby", schemaRubyDiamond)
                                  .attachWorkerWithCSVSourceToCoordinator("ruby", sourceRuby) 
                                  .addLogicalSource("diamond", schemaRubyDiamond)
                                  .attachWorkerWithCSVSourceToCoordinator("diamond", sourceDiamond)
                                  .validate()
                                  .setupTopology();
                                  
    std::vector<OutputRubyDiamond> expectedOutput;
    // The first 17 entries are raw entries from the window.csv file stream.
    // The next 12 entries are the result of the second query of the unionWith statement.
    uint32_t numFirstQueryValues = 18;
    for(uint32_t i = 0; i < 29; i++) {
        uint32_t id = 1;
        uint32_t value = (i % numFirstQueryValues) + 4;
        uint32_t timestamp = (i < numFirstQueryValues) ? value * 1000 : 2;
        expectedOutput.emplace_back(OutputRubyDiamond{value, id, timestamp});
    }

    std::vector<OutputRubyDiamond> actualOutput = testHarness.getOutput<OutputRubyDiamond>(
        /* num output tuples expected */ expectedOutput.size(), 
        /* placement strategy*/ "BottomUp",
        /* fault tolerance*/ "NONE",
        /* lineage*/ "IN_MEMORY"
        );
    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * Test deploying filter-push-down on unionWith query with source on two different worker node using top down strategy.
 * Case: 1 filter operator is above a unionWith operator and will be pushed down towards both of the available sources.
 *       1 filter operator is already below unionWith operator and needs to be pushed down normally towards its own source.
 */
TEST_F(UnionDeploymentTest, testOneFilterPushDownWithMergeOfTwoDifferentSources) {
    std::string outputFilePath = getTestResourceFolder() / "testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentSources.out";
    const auto query = 
        Query::from("ruby")
        .filter(Attribute("id") > 4)
        .unionWith(
            Query::from("diamond")
            .map(Attribute("timestamp") = 1)
            .filter(Attribute("id") > 3)
            .map(Attribute("timestamp") = 2)
            .filter(Attribute("id") > 4)
        );
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("ruby", schemaRubyDiamond)
                                  .attachWorkerWithCSVSourceToCoordinator("ruby", sourceRuby) 
                                  .addLogicalSource("diamond", schemaRubyDiamond)
                                  .attachWorkerWithCSVSourceToCoordinator("diamond", sourceDiamond)
                                  .validate()
                                  .setupTopology();
                                  
    std::vector<OutputRubyDiamond> expectedOutput;
    expectedOutput.emplace_back(OutputRubyDiamond{1, 12, 2});
    expectedOutput.emplace_back(OutputRubyDiamond{2, 11, 2});
    expectedOutput.emplace_back(OutputRubyDiamond{2, 16, 2});
    expectedOutput.emplace_back(OutputRubyDiamond{3, 11, 2});
    expectedOutput.emplace_back(OutputRubyDiamond{1, 12, 1001});
    expectedOutput.emplace_back(OutputRubyDiamond{2, 11, 2001});
    expectedOutput.emplace_back(OutputRubyDiamond{2, 16, 2002});
    expectedOutput.emplace_back(OutputRubyDiamond{3, 11, 3001});

    std::vector<OutputRubyDiamond> actualOutput = testHarness.getOutput<OutputRubyDiamond>(
        /* num output tuples expected */ expectedOutput.size(), 
        /* placement strategy*/ "BottomUp",
        /* fault tolerance*/ "NONE",
        /* lineage*/ "IN_MEMORY"
        );
    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * Test deploying filter-push-down on unionWith query with source on two different worker node using top down strategy.
 * Case: 2 filter operators are already below unionWith operator and needs to be pushed down normally towards their respective source.
 *       Here the filters don't need to be pushed down over an existing unionWith operator.
 */
TEST_F(UnionDeploymentTest, testPushingTwoFiltersAlreadyBelowAndMergeOfTwoDifferentSources) {
    std::string outputFilePath = getTestResourceFolder() / "testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentSources.out";
    const auto query = 
        Query::from("ruby")
        .map(Attribute("timestamp") = 2)
        .filter(Attribute("id") < 4)
        .filter(Attribute("value") > 3)
        .unionWith(
            Query::from("diamond")
            .map(Attribute("timestamp") = 1)
            .filter(Attribute("id") < 4)
            .filter(Attribute("value") > 3)
            .filter(Attribute("value") < 6)
        );
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("ruby", schemaRubyDiamond)
                                  .attachWorkerWithCSVSourceToCoordinator("ruby", sourceRuby) 
                                  .addLogicalSource("diamond", schemaRubyDiamond)
                                  .attachWorkerWithCSVSourceToCoordinator("diamond", sourceDiamond)
                                  .validate()
                                  .setupTopology();

    std::vector<OutputRubyDiamond> expectedOutput;
    // The first 18 entries come from the first part of the unionWith statement.
    // The next 2 entries are the result of the second query of the unionWith statement.
    uint32_t numFirstQueryValues = 18;
    for(uint32_t i = 0; i < 20; i++) {
        uint32_t id = 1;
        uint32_t value = (i % numFirstQueryValues) + 4;
        uint32_t timestamp = (i < numFirstQueryValues) ? 2 : 1;
        expectedOutput.emplace_back(OutputRubyDiamond{value, id, timestamp});
    }

    std::vector<OutputRubyDiamond> actualOutput = testHarness.getOutput<OutputRubyDiamond>(
        /* num output tuples expected */ expectedOutput.size(), 
        /* placement strategy*/ "BottomUp",
        /* fault tolerance*/ "NONE",
        /* lineage*/ "IN_MEMORY"
        );
    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}
}// namespace NES

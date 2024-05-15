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
#include <Util/TestHarness/TestHarness.hpp>
#include <BaseIntegrationTest.hpp>
#include <gtest/gtest.h>

namespace NES {

class FileSinkIntegrationTest : public Testing::BaseIntegrationTest {
public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("FileSinkIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
    }
    TestHarness createTestHarness(const Query& query) const {
        struct Input { uint64_t id; };
        const auto schema = TestSchemas::getSchemaTemplate("id_u64")->updateSourceName(logicalSourceName);
        TestHarness testHarness {query, *restPort, *rpcCoordinatorPort, getTestResourceFolder()};
        testHarness.addLogicalSource(logicalSourceName, schema).attachWorkerWithMemorySourceToCoordinator(logicalSourceName);
        testHarness.pushElement<Input>({1}, 2);
        testHarness.validate().setupTopology();
        return testHarness;
    }
    void runQueryAndVerifyFailureState(const TestHarness& testHarness, const Query& query) {
        auto coordinator = testHarness.getCoordinator();
        QueryId queryId = coordinator->getRequestHandlerService()->validateAndQueueAddQueryRequest(query.getQueryPlan(), NES::Optimizer::PlacementStrategy::BottomUp);
        EXPECT_TRUE(TestUtils::checkFailedOrTimeout(queryId, coordinator->getQueryCatalog()));
    }
    const std::string logicalSourceName = "logicalSource";
};

TEST_F(FileSinkIntegrationTest, canWriteOutputFile) {
    // The test query copies the contents of the input source to the output CSV file of the test harness.
    const Query query = Query::from(logicalSourceName);

    // Setup a test harness with a memory source that contains a single input tuple.
    TestHarness testHarness = createTestHarness(query);

    // Run the query and compare the output to the expected value.
    const std::string expectedOutput = "1\n";
    auto actualBuffers = testHarness.runQuery(NES::Util::countLines(expectedOutput)).getOutput();
    auto tmpBuffer = TestUtils::createExpectedBufferFromCSVString(expectedOutput, testHarness.getOutputSchema(), testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffer, testHarness.getOutputSchema());
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

TEST_F(FileSinkIntegrationTest, cannotWriteOutputFile) {
    // The test query tries to copy the contents of the input source to an output CSV file inside a folder that does not exist.
    const std::string outputFilePath = getTestResourceFolder() / "bad_folder" / "output.csv";
    const Query query = Query::from(logicalSourceName)
        .sink(FileSinkDescriptor::create(outputFilePath, "CSV_FORMAT", "APPEND"));

    // Setup a test harness with a memory source that contains a single input tuple.
    TestHarness testHarness = createTestHarness(query);

    // Run the query and verify that its status is FAILED in the query catalog
    runQueryAndVerifyFailureState(testHarness, query);
}

TEST_F(FileSinkIntegrationTest, cannotRemoveOutputFile) {
    // Create an output file which cannot be removed.
    // We simulate this by creating a non-empty folder, which std::filesystem::remove will fail to remove.
    auto folder = getTestResourceFolder() / "folder";
    std::filesystem::create_directory(folder);
    std::ofstream dummyFile(folder / "dummy-file");
    dummyFile.close();

    // The test query outputs results to the folder, which cannot be removed.
    const Query query = Query::from(logicalSourceName)
        .sink(FileSinkDescriptor::create(folder, "CSV_FORMAT", "OVERWRITE"));

    // Setup a test harness with a memory source that contains a single input tuple.
    TestHarness testHarness = createTestHarness(query);

    // Run the query and verify that its status is FAILED in the query catalog
    runQueryAndVerifyFailureState(testHarness, query);
}



}

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
#include <BaseIntegrationTest.hpp>
#include <Sinks/Mediums/FileSink.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <gtest/gtest.h>

namespace NES {

// This test fixtures creates the following test harness during test setup:
// - A memory source is attached to a logical source. The memory source contains exactly one tuple with the value 1 (UINT64).
// - The query reads all tuples from the memory source.
// Additionally, there are two methods to run tests:
// - runQueryAndVerifyExpectedResults to check that expected output was written to a file sink.
// - runQueryAndVerifyFailureState to check that writing to the file sink caused the query to fail.
class FileSinkIntegrationTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() { NES::Logger::setupLogging("FileSinkIntegrationTest.log", NES::LogLevel::LOG_DEBUG); }
    void SetUp() override {
        BaseIntegrationTest::SetUp();
        createTestHarnessWithMemorySource();
    }
    void runQueryAndVerifyExpectedResults(const std::string& expectedOutput) const {
        testHarness->runQuery(NES::Util::countLines(expectedOutput));
        auto actualBuffers = testHarness->getOutput();
        auto tmpBuffer = TestUtils::createExpectedBufferFromCSVString(expectedOutput,
                                                                      testHarness->getOutputSchema(),
                                                                      testHarness->getBufferManager());
        auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffer, testHarness->getOutputSchema());
        EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
    }
    void runQueryAndVerifyFailureState() const {
        testHarness->addFileSink();
        testHarness->validateAndQueueAddQueryRequest();
        EXPECT_TRUE(testHarness->checkFailedOrTimeout());
        testHarness->stopCoordinatorAndWorkers();
    }

  private:
    void createTestHarnessWithMemorySource() {
        const std::string logicalSourceName = "logicalSource";
        Query query = Query::from(logicalSourceName);
        testHarness = std::make_unique<TestHarness>(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder());
        const auto schema = TestSchemas::getSchemaTemplate("id_u64")->updateSourceName(logicalSourceName);
        testHarness->addLogicalSource(logicalSourceName, schema).attachWorkerWithMemorySourceToCoordinator(logicalSourceName);
        struct Input {
            uint64_t id;
        };
        testHarness->pushElement<Input>({1}, 2);
        testHarness->validate().setupTopology();
    }

  protected:
    std::unique_ptr<TestHarness> testHarness;
};

// Test the APPEND mode of the file sink.
// We create a file that already contains a tuple with a value 3, and set it as the output file of the test harness.
// When the test harness executes the query in APPEND mode, the output file should contain both the tuple with value 3
// that we created in the beginning and the value 1 that was read from the memory source of the test harness.
TEST_F(FileSinkIntegrationTest, DISABLED_canWriteToOutputFileInAppendMode) {
    // Prepare output file
    const std::string outputFilePath = getTestResourceFolder() / "output.csv";
    std::ofstream outputFile{outputFilePath};
    outputFile << "3" << std::endl;
    outputFile.close();

    // Test logic
    testHarness->setOutputFilePath(outputFilePath);
    testHarness->setAppendMode("APPEND");
    runQueryAndVerifyExpectedResults("3\n1\n");
}

// Test the OVERWRITE mode of the file sink.
// We create a file that already contains a tuple with a value 3, and set it as the output file of the test harness.
// When the test harness executes the query in OVERWRITE mode, the output file should only contain the tuple with value 1
// that was read from the memory source of the test harness.
TEST_F(FileSinkIntegrationTest, canWriteToOutputFileInOverWriteMode) {
    // Prepare output file
    const std::string outputFilePath = getTestResourceFolder() / "output.csv";
    std::ofstream outputFile{outputFilePath};
    outputFile << "3" << std::endl;
    outputFile.close();

    // Test logic
    testHarness->setOutputFilePath(outputFilePath);
    testHarness->setAppendMode("OVERWRITE");
    runQueryAndVerifyExpectedResults("1\n");
}

// This test sets the output file to a filename that cannot be created because it is in a folder that does not exist.
// The query should fail.
TEST_F(FileSinkIntegrationTest, cannotOpenOutputFile) {
    const std::string outputFilePath = getTestResourceFolder() / "bad_folder" / "output.csv";
    testHarness->setOutputFilePath(outputFilePath);
    runQueryAndVerifyFailureState();
}

// This test creates an output file that cannot be removed. We simulate this by creating a non-empty folder and set it as output
// file (std::filesystem::remove will not remove this folder). The query should fail.
TEST_F(FileSinkIntegrationTest, cannotRemoveOutputFileInOverwriteMode) {
    // Prepare output file
    auto folder = getTestResourceFolder() / "folder";
    std::filesystem::create_directory(folder);
    std::ofstream dummyFile{folder / "dummy-file"};
    dummyFile.close();

    // Test logic
    testHarness->setOutputFilePath(folder);
    runQueryAndVerifyFailureState();
}

};// namespace NES

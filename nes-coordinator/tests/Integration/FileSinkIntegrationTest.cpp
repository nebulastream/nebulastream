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
#include <Sinks/Mediums/FileSink.hpp>
#include <gtest/gtest.h>

namespace NES {

// TODO Documentation: Fixture and each testx
class FileSinkIntegrationTest : public Testing::BaseIntegrationTest {
public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("FileSinkIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
    }
    void SetUp() override {
        BaseIntegrationTest::SetUp();
        createTestHarnessWithMemorySource();
    }
    void runQueryAndVerifyExpectedResults(const std::string& expectedOutput) const {
        testHarness->runQuery(NES::Util::countLines(expectedOutput));
        auto actualBuffers = testHarness->getOutput();
        auto tmpBuffer = TestUtils::createExpectedBufferFromCSVString(expectedOutput, testHarness->getOutputSchema(), testHarness->getBufferManager());
        auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffer, testHarness->getOutputSchema());
        EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
    }
    void runQueryAndVerifyFailureState() const {
        testHarness->addFileSink();
        testHarness->enqueueQuery();
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
        struct Input { uint64_t id; };
        testHarness->pushElement<Input>({1}, 2);
        testHarness->validate().setupTopology();
    }

protected:
    std::unique_ptr<TestHarness> testHarness;
};

TEST_F(FileSinkIntegrationTest, DISABLED_canWriteToOutputFileInAppendMode) {
    // Create the output CSV file with a single row.
    const std::string outputFilePath = getTestResourceFolder() / "output.csv";
    std::ofstream outputFile { outputFilePath };
    outputFile << "3" << std::endl;
    outputFile.close();

    // Test logic
    testHarness->setOutputFilePath(outputFilePath);
    testHarness->setAppendMode("APPEND");
    runQueryAndVerifyExpectedResults("3\n1\n");
}

TEST_F(FileSinkIntegrationTest, canWriteToOutputFileInOverWriteMode) {
    // Create the output CSV file with a single row.
    const std::string outputFilePath = getTestResourceFolder() / "output.csv";
    std::ofstream outputFile { outputFilePath };
    outputFile << "3" << std::endl;
    outputFile.close();

    // Test logic
    testHarness->setOutputFilePath(outputFilePath);
    testHarness->setAppendMode("OVERWRITE");
    runQueryAndVerifyExpectedResults("1\n");
}

TEST_F(FileSinkIntegrationTest, cannotOpenOutputFile) {
    // The test query tries to copy the contents of the input source to an output CSV file inside a folder that does not exist.
    const std::string outputFilePath = getTestResourceFolder() / "bad_folder" / "output.csv";
    testHarness->setOutputFilePath(outputFilePath);
    runQueryAndVerifyFailureState();
}

TEST_F(FileSinkIntegrationTest, cannotRemoveOutputFileInOverwriteMode) {
    // Create an output file which cannot be removed.
    // We simulate this by creating a non-empty folder, which std::filesystem::remove will fail to remove.
    auto folder = getTestResourceFolder() / "folder";
    std::filesystem::create_directory(folder);
    std::ofstream dummyFile { folder / "dummy-file" };
    dummyFile.close();

    // Test logic
    testHarness->setOutputFilePath(folder);
    runQueryAndVerifyFailureState();
}

};

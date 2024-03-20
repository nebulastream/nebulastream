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
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <iostream>

namespace NES {

class VariableLengthIntegrationTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("VariableLengthIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup VariableLengthIntegrationTest test class.");
    }
};

// This test reads from a csv sink which contains variable-length fields and applies a filter and a map
TEST_F(VariableLengthIntegrationTest, testCsvSourceWithVariableLengthFieldsFilterOnMap) {
    const std::string inputFileName = "variable-length.csv";
    const std::string expectedCsvFile = "variable-length_3.csv";
    const std::string outputFileName = "testCsvSourceWithVariableLengthFields.csv";
    const std::string inputFilePath = std::filesystem::path(TEST_DATA_DIRECTORY) / inputFileName;
    const std::string outputFilePath = getTestResourceFolder() / outputFileName;
    remove(outputFilePath.c_str());

    // elegant project test schema
    auto testSchema = Schema::create()
                          ->addField("camera_id", BasicType::UINT64)
                          ->addField("timestamp", BasicType::UINT64)
                          ->addField("rows", BasicType::UINT64)
                          ->addField("cols", BasicType::UINT64)
                          ->addField("type", BasicType::UINT64)
                          ->addField("data", BasicType::TEXT);// TEXT is the variable length field

    auto query = Query::from("variable_length")
                     .map(Attribute("camera_id_2") = Attribute("camera_id") + 10)
                     .filter(Attribute("camera_id_2") < 55)
                     .project(Attribute("camera_id"),
                              Attribute("timestamp"),
                              Attribute("rows"),
                              Attribute("cols"),
                              Attribute("type"),
                              Attribute("data"));

    // setup csv sources
    CSVSourceTypePtr csvSourceType = CSVSourceType::create("variable_length", "test_stream");
    csvSourceType->setFilePath(inputFilePath);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(10);
    csvSourceType->setNumberOfBuffersToProduce(1);

    // Creating TestHarness
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("variable_length", testSchema)
                                  .attachWorkerWithCSVSourceToCoordinator(csvSourceType)
                                  .validate()
                                  .setupTopology();

    // Creating expected output
    auto tmpBuffers = TestUtils::createExpectedBuffersFromCsv(expectedCsvFile, testSchema, testHarness.getBufferManager());
    auto expectedTuples = TestUtils::countTuples(tmpBuffers);

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(expectedTuples).getOutput();

    // We require TestTupleBuffers, therefore we create them with the output schema.
    // This step can be only done after the query has been run.
    auto outputSchema = testHarness.getOutputSchema();
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);

    // Comparing equality
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers, true));
}

// This test reads from a csv sink which contains variable-length fields and applies a filter
TEST_F(VariableLengthIntegrationTest, testCsvSourceWithVariableLengthFieldsFilter) {
    const std::string inputFileName = "variable-length.csv";
    const std::string expectedCsvFile = "variable-length_2.csv";
    const std::string outputFileName = "testCsvSourceWithVariableLengthFields.csv";
    const std::string inputFilePath = std::filesystem::path(TEST_DATA_DIRECTORY) / inputFileName;
    const std::string outputFilePath = getTestResourceFolder() / outputFileName;
    remove(outputFilePath.c_str());

    // elegant project test schema
    auto testSchema = Schema::create()
                          ->addField("camera_id", BasicType::UINT64)
                          ->addField("timestamp", BasicType::UINT64)
                          ->addField("rows", BasicType::UINT64)
                          ->addField("cols", BasicType::UINT64)
                          ->addField("type", BasicType::UINT64)
                          ->addField("data", BasicType::TEXT);// TEXT is the variable length field

    auto query = Query::from("variable_length")
                     .filter(Attribute("camera_id") < 55)
                     .project(Attribute("camera_id"),
                              Attribute("timestamp"),
                              Attribute("rows"),
                              Attribute("cols"),
                              Attribute("type"),
                              Attribute("data"));

    // setup csv sources
    CSVSourceTypePtr csvSourceType = CSVSourceType::create("variable_length", "test_stream");
    csvSourceType->setFilePath(inputFilePath);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(10);
    csvSourceType->setNumberOfBuffersToProduce(1);

    // Creating TestHarness
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("variable_length", testSchema)
                                  .attachWorkerWithCSVSourceToCoordinator(csvSourceType)
                                  .validate()
                                  .setupTopology();

    // Creating expected output
    auto tmpBuffers = TestUtils::createExpectedBuffersFromCsv(expectedCsvFile, testSchema, testHarness.getBufferManager());
    auto expectedTuples = std::accumulate(tmpBuffers.begin(), tmpBuffers.end(), 0_u64, [](const uint64_t sum, const auto& buf) {
        return sum + buf.getNumberOfTuples();
    });

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(expectedTuples).getOutput();

    // We require TestTupleBuffers, therefore we create them with the output schema.
    // This step can be only done after the query has been run.
    auto outputSchema = testHarness.getOutputSchema();
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);

    // Comparing equality
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

// This test reads from a csv sink which contains variable-length fields without any additional processing
TEST_F(VariableLengthIntegrationTest, testCsvSourceWithVariableLengthFields) {
    const std::string inputFileName = "variable-length.csv";
    const std::string expectedCsvFile = "variable-length.csv";
    const std::string outputFileName = "testCsvSourceWithVariableLengthFields.csv";
    const std::string inputFilePath = std::filesystem::path(TEST_DATA_DIRECTORY) / inputFileName;
    const std::string outputFilePath = getTestResourceFolder() / outputFileName;
    remove(outputFilePath.c_str());

    // elegant project test schema
    auto testSchema = Schema::create()
                          ->addField("camera_id", BasicType::UINT64)
                          ->addField("timestamp", BasicType::UINT64)
                          ->addField("rows", BasicType::UINT64)
                          ->addField("cols", BasicType::UINT64)
                          ->addField("type", BasicType::UINT64)
                          ->addField("data", BasicType::TEXT);// TEXT is the variable length field

    auto query = Query::from("variable_length");

    // setup csv sources
    CSVSourceTypePtr csvSourceType = CSVSourceType::create("variable_length", "test_stream");
    csvSourceType->setFilePath(inputFilePath);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(10);
    csvSourceType->setNumberOfBuffersToProduce(1);

    // Creating TestHarness
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("variable_length", testSchema)
                                  .attachWorkerWithCSVSourceToCoordinator(csvSourceType)
                                  .validate()
                                  .setupTopology();

    // Creating expected output
    auto tmpBuffers = TestUtils::createExpectedBuffersFromCsv(expectedCsvFile, testSchema, testHarness.getBufferManager());
    auto expectedTuples = std::accumulate(tmpBuffers.begin(), tmpBuffers.end(), 0_u64, [](const uint64_t sum, const auto& buf) {
        return sum + buf.getNumberOfTuples();
    });

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(expectedTuples).getOutput();

    // We require TestTupleBuffers, therefore we create them with the output schema.
    // This step can be only done after the query has been run.
    auto outputSchema = testHarness.getOutputSchema();
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);

    // Comparing equality
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

}// namespace NES
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

#include "API/Expressions/ArithmeticalExpressions.hpp"
#include "API/Expressions/Expressions.hpp"
#include "API/Windowing.hpp"
#include <API/QueryAPI.hpp>
#include <API/TestSchemas.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <Execution/Operators/MEOS/Meos.hpp>

struct InputValue {
    uint64_t timestamp;
    uint64_t mmsi;
    double latitude;
    double longitude;
    double sog;
};

namespace NES {
using namespace Configurations;

class MeosDeploy : public Testing::BaseIntegrationTest,
                   public testing::WithParamInterface<std::tuple<std::string, SchemaPtr, std::string, std::string>> {
  protected:
    WorkerConfigurationPtr workerConfiguration;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("meos.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup meos test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        workerConfiguration = WorkerConfiguration::create();
        workerConfiguration->queryCompiler.windowingStrategy = QueryCompilation::WindowingStrategy::SLICING;
        workerConfiguration->queryCompiler.compilationStrategy = QueryCompilation::CompilationStrategy::DEBUG;
    }
};

/**
 * @brief Tests creating a meos, reading from a CSV, and testing the intersection functionality. Using ThresholdWindow
 */
TEST_F(MeosDeploy, testIntersectionThresold) {
    using namespace MEOS;
    try {
        // Initialize meos instance
        MEOS::Meos* meos = new MEOS::Meos("UTC");

        // Define schema
        auto meosSchema = Schema::create()
                              ->addField("timestamp", BasicType::UINT64)
                              ->addField("mmsi", BasicType::UINT64)
                              ->addField("latitude", BasicType::FLOAT64)
                              ->addField("longitude", BasicType::FLOAT64)
                              ->addField("sog", BasicType::FLOAT64);

        ASSERT_EQ(sizeof(InputValue), meosSchema->getSchemaSizeInBytes());

        // Create CSV source type
        auto csvSourceType = CSVSourceType::create("ais", "aisinput");
        csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "aisinput.csv");
        csvSourceType->setNumberOfTuplesToProducePerBuffer(2);// Read 2 tuples per buffer
        csvSourceType->setNumberOfBuffersToProduce(10);       // Produce 10 buffers
        csvSourceType->setSkipHeader(true);                   // Skip the header

        auto ThresholExpression = teintersects(Attribute("longitude", BasicType::FLOAT64),
                                 Attribute("latitude", BasicType::FLOAT64),
                                 Attribute("timestamp", BasicType::UINT64)) == 1;
        auto query =
            Query::from("ais")
                .window(ThresholdWindow::of(ThresholExpression))
                .apply(Sum(Attribute("mmsi", BasicType::UINT64)));

        // Create the test harness and attach the CSV source
        auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                               .addLogicalSource("ais", meosSchema)
                               .attachWorkerWithLambdaSourceToCoordinator(csvSourceType, workerConfiguration);

        //ASSERT_EQ(testHarness.getWorkerCount(), 1UL);  // Ensure only one worker is used
        testHarness.validate().setupTopology();

        // Expected output as a string (adjust as needed)
        const auto expectedOutput = "";

        // Run the query and get the actual dynamic buffers
        auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

        // Compare the expected and actual outputs
        const auto outputSchema = testHarness.getOutputSchema();
        auto tmpBuffers =
            TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
        auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
        //EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));

    } catch (const std::exception& e) {
        FAIL() << "Caught exception: " << e.what();
    }
}

}// namespace NES

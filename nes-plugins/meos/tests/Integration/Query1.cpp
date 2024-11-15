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
#include <API/TestSchemas.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <chrono>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iostream>

#include <Execution/Operators/MEOS/Meos.hpp>

struct InputValue {
    double timestamp;
    uint64_t mmsi;
    float latitude;
    float longitude;
    float sog;
};

namespace NES {
using namespace Configurations;

class MeosDeploy : public Testing::BaseIntegrationTest,
                   public testing::WithParamInterface<std::tuple<std::string, SchemaPtr, std::string, std::string>> {
  protected:
    WorkerConfigurationPtr workerConfiguration;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MEOS.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MEOS test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        workerConfiguration = WorkerConfiguration::create();
        workerConfiguration->queryCompiler.windowingStrategy = QueryCompilation::WindowingStrategy::SLICING;
        workerConfiguration->queryCompiler.compilationStrategy = QueryCompilation::CompilationStrategy::DEBUG;
    }
};

PhysicalSourceTypePtr createSimpleInputStream(std::string logicalSourceName,
                                              std::string physicalSourceName,
                                              uint64_t numberOfBuffers,
                                              uint64_t numberOfKeys = 1) {
    return LambdaSourceType::create(
        logicalSourceName,
        physicalSourceName,
        [numberOfKeys](Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
            auto inputValue = (InputValue*) buffer.getBuffer();
            for (uint64_t i = 0; i < numberOfTuplesToProduce; i++) {
                inputValue[i].mmsi = i % numberOfKeys;
                inputValue[i].timestamp = 1610060400;
                inputValue[i].latitude = 57.059 + i % 10;
                inputValue[i].longitude = 12.272388 + i % 10;
                inputValue[i].sog = 0.0;
            }
            buffer.setNumberOfTuples(numberOfTuplesToProduce);
        },
        numberOfBuffers,
        0,
        GatheringMode::INTERVAL_MODE);
}

/**
 * @brief Tests creating a MEOS, reading from a CSV and summing speeds by ID.
 */
TEST_F(MeosDeploy, testSimpleWindowEventTime) {
    try {
        MEOS::Meos* meos;
        meos = new MEOS::Meos("UTC");

        auto meosSchema = Schema::create()
                              ->addField("timestamp", BasicType::UINT64)
                              ->addField("mmsi", BasicType::UINT64)
                              ->addField("latitude", BasicType::FLOAT64)
                              ->addField("longitude", BasicType::FLOAT32)
                              ->addField("sog", BasicType::FLOAT32);

        ASSERT_EQ(sizeof(InputValue), meosSchema->getSchemaSizeInBytes());

        auto csvSourceType = CSVSourceType::create("ais", "aisinput");
        csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "aisinput.csv");
        csvSourceType->setNumberOfTuplesToProducePerBuffer(2);
        csvSourceType->setNumberOfBuffersToProduce(10);
        csvSourceType->setSkipHeader(true);

        auto query = Query::from("ais")
                         .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Seconds(10)))
                         .byKey(Attribute("mmsi"))
                         .apply(Sum(Attribute("sog")));

        auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                               .addLogicalSource("ais", meosSchema)
                               .attachWorkerWithLambdaSourceToCoordinator(csvSourceType, workerConfiguration);

        ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
        testHarness.validate().setupTopology();

        const auto expectedOutput = "1610060000,1610070000,265513270,0.400000\n"
                                    "1610060000,1610070000,219027804,0.000000\n"
                                    "1610060000,1610070000,566948000,1.000000\n"
                                    "1610060000,1610070000,219001559,0.300000\n";

        // Run the query and get the actual dynamic buffers
        auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

        // Comparing equality
        const auto outputSchema = testHarness.getOutputSchema();
        auto tmpBuffers =
            TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
        auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
        EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));

    } catch (const std::exception& e) {
        FAIL() << "Caught exception: " << e.what();
    }
}

}// namespace NES

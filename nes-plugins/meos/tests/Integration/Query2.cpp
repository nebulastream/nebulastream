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
#include <future>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iostream>
#include <thread>

#include <Execution/Operators/MEOS/Meos.hpp>

using namespace std;

struct InputValue {
    double time;     
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

    void TearDown() override { Testing::BaseIntegrationTest::TearDown(); }

    bool intersects(ExpressionItem var) {
        NES::ExpressionNodePtr pointerex = var.getExpressionNode();
        return true;
    }
};

/**
 * @brief Real-Time Vessel intersection.
 * Tests creating a MEOS object, reading from a CSV, summing speeds by ID, and checking the intersection.
 * TODO: check how to pass a value from the window to a MEOS function 
 */
TEST_F(MeosDeploy, testThresholdWindow) {
    try {
        NES_INFO("Creating MEOS object");

        MEOS::Meos* meos;
        meos = new MEOS::Meos("UTC");
        NES_INFO("Creating schema for input data");

        auto meosSchema = Schema::create()
                              ->addField("time", BasicType::UINT64)
                              ->addField("mmsi", BasicType::UINT64)
                              ->addField("latitude", BasicType::FLOAT64)
                              ->addField("longitude", BasicType::FLOAT32)
                              ->addField("sog", BasicType::FLOAT32);

        ASSERT_EQ(sizeof(InputValue), meosSchema->getSchemaSizeInBytes());

        NES_INFO("Setting up CSV source type");
        auto csvSourceType = CSVSourceType::create("ais", "aisinput");
        csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "aisinputsmall.csv");
        csvSourceType->setNumberOfTuplesToProducePerBuffer(2);
        csvSourceType->setNumberOfBuffersToProduce(10);
        csvSourceType->setSkipHeader(true);

        NES_INFO("Creating query with SlidingWindow");

        auto query = Query::from("ais")
                         .filter(intersects(Attribute("latitude")) && Attribute("sog") > 0)
                         .window(SlidingWindow::of(EventTime(Attribute("time")), Seconds(10), Seconds(10)))
                         .byKey(Attribute("mmsi"))
                         .apply(Sum(Attribute("sog")));

        NES_INFO("Setup test harness");
        auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                               .addLogicalSource("ais", meosSchema)
                               .attachWorkerWithLambdaSourceToCoordinator(csvSourceType, workerConfiguration);

        ASSERT_EQ(testHarness.getWorkerCount(), 1);
        testHarness.validate().setupTopology();
        NES_INFO("Validating test harness");

        const auto expectedOutput = "";

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

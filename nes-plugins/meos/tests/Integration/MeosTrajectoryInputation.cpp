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
    float rot;
    float cog;
    float heading;
    uint64_t imo;
    float width;
    float length;
    float draught;
    float size_a;
    float size_b;
    float size_c;
    float size_d;
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

    void TearDown() override {
        // Add any necessary cleanup code here
        Testing::BaseIntegrationTest::TearDown();
    }

    bool intersects(ExpressionItem var) {
        NES::ExpressionNodePtr pointerex = var.getExpressionNode();
        return true;
    }
};

/**
 * @brief Real-Time Vessel trajectory inputation.
 * This test is to stress the integration capacity reading one full day of AIS. 
 * A real-time query that computes trajectory of vessels for a whole day 4.4GB of data points. 
 * This query utilizes meos's functions to create points to create the trajectory.
 * TODO: check how to pass a value from the window to a meos function 
 */
TEST_F(MeosDeploy, ReadBigFile) {
    using namespace MEOS;
    try {
        NES_INFO("Creating meos object");

        MEOS::Meos* meos = new MEOS::Meos("UTC");
        NES_INFO("Creating schema for input data");
        auto meosSchema = Schema::create()
                              ->addField("timestamp", BasicType::UINT64) // 1. Timestamp
                              ->addField("mmsi", BasicType::UINT64)      // 3. MMSI
                              ->addField("latitude", BasicType::FLOAT64) // 4. Latitude
                              ->addField("longitude", BasicType::FLOAT32)// 5. Longitude
                              ->addField("rot", BasicType::FLOAT32)      // 7. ROT (Rate of Turn)
                              ->addField("sog", BasicType::FLOAT32)      // 8. SOG (Speed Over Ground)
                              ->addField("cog", BasicType::FLOAT32)      // 9. COG (Course Over Ground)
                              ->addField("heading", BasicType::FLOAT32)  // 10. Heading
                              ->addField("imo", BasicType::UINT64)       // 11. IMO
                              ->addField("width", BasicType::FLOAT32)    // 16. Width
                              ->addField("length", BasicType::FLOAT32)   // 17. Length
                              ->addField("draught", BasicType::FLOAT32)  // 19. Draught
                              ->addField("size_a", BasicType::FLOAT32)   // 23. Size A (Length from GPS to bow)
                              ->addField("size_b", BasicType::FLOAT32)   // 24. Size B (Length from GPS to stern)
                              ->addField("size_c", BasicType::FLOAT32)   // 25. Size C (Length from GPS to starboard side)
                              ->addField("size_d", BasicType::FLOAT32);  // 26. Size D (Length from GPS to port side)

        ASSERT_EQ(sizeof(InputValue), meosSchema->getSchemaSizeInBytes());

        NES_INFO("Setting up CSV source type");
        auto csvSourceType = CSVSourceType::create("ais", "aisinput");
        csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "aisdk-2024-09.csv");
        csvSourceType->setNumberOfTuplesToProducePerBuffer(2);
        csvSourceType->setNumberOfBuffersToProduce(10);
        csvSourceType->setSkipHeader(true);

        NES_INFO("Creating query with SlidingWindow");

        auto query = Query::from("ais")
                         .filter(Attribute("sog") > 0)
                         .window(SlidingWindow::of(EventTime(Attribute("time")), Seconds(10), Seconds(10)))
                         .byKey(Attribute("mmsi"))
                         .apply(Max(Attribute("sog")));

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
        EXPECT_FALSE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));

    } catch (const std::exception& e) {
        FAIL() << "Caught exception: " << e.what();
    }
}

}// namespace NES

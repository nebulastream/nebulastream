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

#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Windowing.hpp>
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

#include <cstdint>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <Execution/Operators/MEOS/Meos.hpp>

struct InputValue {
    uint64_t timestamp;
    uint64_t id;
    double vbat;
    double PCFA_mbar;
    double PCFF_mbar;
    double PCF1_mbar;
    double PCF2_mbar;
    double T1_mbar;
    double T2_mbar;
    uint64_t Code1;
    uint64_t Code2;
    double speed;
    double latitude;
    double longitude;
};



namespace NES {
using namespace Configurations;

class ReadSNCB : public Testing::BaseIntegrationTest,
                   public testing::WithParamInterface<std::tuple<std::string, SchemaPtr, std::string, std::string>> {
  protected:
    WorkerConfigurationPtr workerConfiguration;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("meos.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SNCB test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        workerConfiguration = WorkerConfiguration::create();
        workerConfiguration->queryCompiler.windowingStrategy = QueryCompilation::WindowingStrategy::SLICING;
        workerConfiguration->queryCompiler.compilationStrategy = QueryCompilation::CompilationStrategy::DEBUG;


    }
};

/**
 * @brief Tests creating a meos instance, reading from a CSV, and verifying intersection functionality.
 */
TEST_F(ReadSNCB, testReadCSV) {
    using namespace MEOS;
    try {
        // Initialize MEOS instance
        MEOS::Meos* meos = new MEOS::Meos("UTC");
        auto workerConfiguration1 = WorkerConfiguration::create();
        auto workerConfiguration2 = WorkerConfiguration::create();

        // Define slack in meters and convert to degrees
        const double slackMeters = 5.0;
        const double slackDegrees = slackMeters / 111320.0; // 1 degree ≈ 111.32 km

        auto gpsSchema = Schema::create()
                ->addField("timestamp", BasicType::UINT64)
                ->addField("id", BasicType::UINT64)
                ->addField("Vbat", BasicType::FLOAT64)
                ->addField("PCFA_mbar", BasicType::FLOAT64)
                ->addField("PCFF_mbar", BasicType::FLOAT64)
                ->addField("PCF1_mbar", BasicType::FLOAT64)
                ->addField("PCF2_mbar", BasicType::FLOAT64)
                ->addField("T1_mbar", BasicType::FLOAT64)
                ->addField("T2_mbar", BasicType::FLOAT64)
                ->addField("Code1", BasicType::UINT64)
                ->addField("Code2", BasicType::UINT64)
                ->addField("speed", BasicType::FLOAT64)
                ->addField("latitude", BasicType::FLOAT64)
                ->addField("longitude", BasicType::FLOAT64);
                        

        ASSERT_EQ(sizeof(InputValue), gpsSchema->getSchemaSizeInBytes());

        auto csvSourceType = CSVSourceType::create("gps", "gpsSchema");
        csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "selected_columns_df.csv");
        csvSourceType->setNumberOfTuplesToProducePerBuffer(20);// Read 2 tuples per buffer
        csvSourceType->setNumberOfBuffersToProduce(40);       // Produce 10 buffers
        csvSourceType->setSkipHeader(true);                   // Skip the header


        // Define Workshops Schema
        auto workshopsSchema = Schema::create()
        ->addField("id", BasicType::UINT64)
        ->addField("latitude", BasicType::FLOAT64)
        ->addField("longitude", BasicType::FLOAT64)
         ->addField("timestamp", BasicType::UINT64);


        auto csvSourceType2 = CSVSourceType::create("workshops", "workshops_data");
        csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "SNCB_workshops.csv");
        csvSourceType2->setNumberOfTuplesToProducePerBuffer(10); // Read 10 tuples per buffer
        csvSourceType2->setNumberOfBuffersToProduce(1);         // Single buffer as workshops data is static
        csvSourceType2->setSkipHeader(true);                    // Skip the header


    auto  subquery = Query::from("workshops")
        .map(Attribute("workshops$timestamp") = Attribute("timestamp"))
        .project(
            Attribute("id"),
            Attribute("latitude"),
            Attribute("longitude"),
            Attribute("timestamp")
        );
    
    auto batteryMonitoringQuery = Query::from("gps")
        .map(Attribute("gps$latitude") = Attribute("latitude"))
        .map(Attribute("gps$longitude") = Attribute("longitude"))
        .seqWith(subquery)
        .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(60), Seconds(30)))
        .filter(
            // Distance check to nearest workshop using slack
            (ABS(Attribute("gps$latitude") - Attribute("workshops$latitude")) <= 0.5) &&  // 0.5 degree ≈ 55.66 km
            (ABS(Attribute("gps$longitude") - Attribute("workshops$longitude")) <= 0.5) &&
            // Battery and speed conditions
            Attribute("Vbat") < 30.0 &&
            Attribute("speed") > 0.0
        )
        .project(
            Attribute("timestamp"),
            Attribute("id"),
            Attribute("Vbat"),
            Attribute("speed"),
            Attribute("gps$latitude"),
            Attribute("gps$longitude"),
            Attribute("workshops$latitude"),
            Attribute("workshops$longitude")
        );

    
    // Create the Test Harness and Attach CSV Sources
    auto testHarness = TestHarness(batteryMonitoringQuery, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
        .addLogicalSource("gps", gpsSchema)
        .addLogicalSource("workshops", workshopsSchema) // Added workshops logical source
        .attachWorkerWithLambdaSourceToCoordinator(csvSourceType, workerConfiguration1)
        .attachWorkerWithLambdaSourceToCoordinator(csvSourceType2, workerConfiguration2);

        testHarness.validate().setupTopology();

        // Define expected output
        const auto expectedOutput = "1722520348, 3, 29.4, 1.4671, 50.6456, 4.3658, 50.4542, 4.8372\n"
                                    "1722520348, 3, 29.4, 1.4671, 50.6456, 4.3658, 50.8699, 4.3797\n"
                                    "1722520348, 5, 29.7, 94.9161, 51.3001, 4.4325, 50.8699, 4.3797\n"
                                    "1722520348, 3, 29.4, 1.4671, 50.6456, 4.3658, 50.4542, 4.8372\n"
                                    "1722520348, 3, 29.4, 1.4671, 50.6456, 4.3658, 50.8699, 4.3797\n"
                                    "1722520348, 5, 29.7, 94.9161, 51.3001, 4.4325, 50.8699, 4.3797\n";

          
        // Run the query and get the actual dynamic buffers
        auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput), "TopDown").getOutput();


            // Iterate Through Buffers and Log Results
        for (const auto& buffer : actualBuffers) {
            size_t numTuples = buffer.getNumberOfTuples();
            for (size_t i = 0; i < numTuples; ++i) {
                auto tuple = buffer[i];
                NES_INFO("Battery Monitoring Result - Timestamp: {}, ID: {}, Vbat: {}, Speed: {}, GPS Latitude: {}, GPS Longitude: {}, Workshop Latitude: {}, Workshop Longitude: {}",
                    tuple[0].read<uint64_t>(),    // timestamp
                    tuple[1].read<uint64_t>(),    // id
                    tuple[2].read<double>(),      // Vbat
                    tuple[3].read<double>(),      // speed
                    tuple[4].read<double>(),      // gps latitude
                    tuple[5].read<double>(),      // gps longitude
                    tuple[6].read<double>(),      // workshop latitude
                    tuple[7].read<double>());     // workshop longitude
            }
        }

        const auto outputSchema = testHarness.getOutputSchema();
        auto tmpBuffers =
            TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
        auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);


        EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));


    } catch (const std::exception& e) {
        FAIL() << "Caught exception: " << e.what();
    }
}

} // namespace NES
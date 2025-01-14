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

        auto gpsSchema = Schema::create()
            ->addField("timestamp", BasicType::UINT64)
            ->addField("device_id", BasicType::UINT64)
            ->addField("Vbat", BasicType::FLOAT64)
            ->addField("PCFA_mbar", BasicType::FLOAT64)
            ->addField("PCFF_mbar", BasicType::FLOAT64)
            ->addField("PCF1_mbar", BasicType::FLOAT64)
            ->addField("PCF2_mbar", BasicType::FLOAT64)
            ->addField("T1_mbar", BasicType::FLOAT64)
            ->addField("T2_mbar", BasicType::FLOAT64)
            ->addField("Code1", BasicType::UINT64)
            ->addField("Code2", BasicType::UINT64)
            ->addField("gps_speed", BasicType::FLOAT64)
            ->addField("gps_lat", BasicType::FLOAT64)
            ->addField("gps_lon", BasicType::FLOAT64);
                              

        ASSERT_EQ(sizeof(InputValue), gpsSchema->getSchemaSizeInBytes());

        auto csvSourceType = CSVSourceType::create("sncb", "sncbmerged");
        csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "selected_columns_df.csv");
        csvSourceType->setNumberOfTuplesToProducePerBuffer(20);// Read 2 tuples per buffer
        csvSourceType->setNumberOfBuffersToProduce(40);       // Produce 10 buffers
        csvSourceType->setSkipHeader(true);                   // Skip the header


    auto weatherSchema = Schema::create()
        ->addField("timestamp", BasicType::UINT64)           // timestamp
        ->addField("temperature", BasicType::FLOAT64)     // temperature in °C
        ->addField("precipitation", BasicType::FLOAT64)      // precipitation in mm
        ->addField("rain", BasicType::FLOAT64)              // rain in mm
        ->addField("snowfall", BasicType::FLOAT64)          // snowfall in cm
        ->addField("snowfall_height", BasicType::FLOAT64)    // snowfall height in m
        ->addField("freezing_level_height", BasicType::FLOAT64) // freezing level in m
        ->addField("weather_code", BasicType::UINT64)        // WMO code
        ->addField("wind_speed", BasicType::FLOAT64)     // wind speed at 10m in km/h
        ->addField("wind_speed_80m", BasicType::FLOAT64)     // wind speed at 80m in km/h
        ->addField("wind_direction_10m", BasicType::FLOAT64) // wind direction at 10m in %
        ->addField("wind_direction_80m", BasicType::FLOAT64) // wind direction at 80m in %
        ->addField("wind_gusts_10m", BasicType::FLOAT64)     // wind gusts at 10m in km/h
        ->addField("gps_lat", BasicType::FLOAT64)           // latitude coordinate
        ->addField("gps_lon", BasicType::FLOAT64);         // longitude coordinate



        auto csvSourceType2 = CSVSourceType::create("weather", "weather_time");
        csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "weatherwithsynccoordinates.csv");
        csvSourceType2->setNumberOfTuplesToProducePerBuffer(5); // Read 10 tuples per buffer
        csvSourceType2->setNumberOfBuffersToProduce(10);        // Produce 20 buffers
        csvSourceType2->setSkipHeader(true);                    // Skip the header


        /*
        Weather-Based Speed Zones
        The system uses integrated weather data to suggest speed limits for 
        conditions such as heavy rain or fog, maintaining safety operations.
        */
        
        auto weatherQuery = Query::from("weather")
            .filter(
                tedwithin(Attribute("gps_lon"), 
                        Attribute("gps_lat"),
                        Attribute("timestamp"))==1
            )
            .filter(
                Attribute("temperature") < 12.0 || 
                Attribute("temperature") > 16.0
            )
            .filter(
                Attribute("snowfall") >= 0.0 ||          
                Attribute("wind_speed") > 15.0 ||        // Higher than median wind speed
                Attribute("precipitation") > 0.1          // Match actual precipitation events
            )
            .map(Attribute("adjusted_speed_limit") = 15.0);

        auto query = Query::from("sncb")
        .joinWith(weatherQuery)
        .where(Attribute("timestamp") ==  Attribute("weather$timestamp"))  // Join on the mapped timestamp
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(10)))
        .filter(readT(Attribute("weather$timestamp"), Attribute("weather$gps_lat"), Attribute("weather$gps_long")) == 1)
        .project(Attribute("timestamp"),Attribute("device_id"), Attribute("gps_speed"), Attribute("adjusted_speed_limit"));



        auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                            .addLogicalSource("sncb", gpsSchema)
                            .addLogicalSource("weather", weatherSchema) // Added battery logical source
                            .attachWorkerWithLambdaSourceToCoordinator(csvSourceType, workerConfiguration1)
                            .attachWorkerWithLambdaSourceToCoordinator(csvSourceType2, workerConfiguration2);


        testHarness.validate().setupTopology();

        // Expected output as a string (adjust as needed)
        const auto expectedOutput = "1722523500, 5, 0.0056, 15\n";

                            
        // Run the query and get the actual dynamic buffers
        auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput), "TopDown").getOutput();


        for (const auto& buffer : actualBuffers) {
        size_t numTuples = buffer.getNumberOfTuples();
        for (size_t i = 0; i < numTuples; ++i) {
            auto tuple = buffer[i];
            NES_INFO("The result is : {}, {}, {}, {}",
                tuple[0].read<uint64_t>(),  // timestamp is UINT64
                tuple[1].read<uint64_t>(),  // device_id is UINT64
                tuple[2].read<double>(),    // gps_speed is FLOAT64
                tuple[3].read<double>());    // adjusted_speed_limit is FLOAT64
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
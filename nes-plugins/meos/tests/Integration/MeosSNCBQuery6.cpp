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

// struct InputValue {
//     uint64_t timestamp;
//     uint64_t id;
//     double speed;
//     double latitude;
//     double longitude;
// };

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

        auto csvSourceType = CSVSourceType::create("gps", "sncbmerged");
        csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "selected_columns_df.csv");
        csvSourceType->setNumberOfTuplesToProducePerBuffer(20); // Produce 20 tuples per buffer
        csvSourceType->setNumberOfBuffersToProduce(40);  
        csvSourceType->setSkipHeader(true);                 // Skip the header


        /*
        Heavy Load of Passengers
        Detect a heavy load of passengers by monitoring train sensors. 
        These sensors include weight detectors that provide real-time data for analysis. 
        The system can adjust temperature controls and lighting to maintain optimal conditions 
        and resource waste in response to heavy loads. 
        */

        auto subQueryA = Query::from("gps")
                    .map(
                        Attribute("passenger_count") = 
                            (Attribute("PCFA_mbar") + Attribute("PCFF_mbar") +
                            Attribute("PCF1_mbar") + Attribute("PCF2_mbar")) / 4.0
                    )
                    .filter(Attribute("passenger_count") > 3.15);

                        
        auto query = Query::from("gps")
                    .filter(tpointatstbox(Attribute("longitude", BasicType::FLOAT64),
                                              Attribute("latitude", BasicType::FLOAT64),
                                              Attribute("timestamp", BasicType::UINT64)) == 1 &&
                            Attribute("speed") > -1)
                    .andWith(subQueryA)
                    .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(20)))
                    .map( Attribute("adjusted_temp") = 20.0)
                    .map( Attribute("adjusted_light") = 80.0)
                    .project(
                        Attribute("timestamp"),
                        Attribute("id"),
                        Attribute("passenger_count"),
                        Attribute("adjusted_temp"),
                        Attribute("adjusted_light")
                    );

                    
    
        // Create the Test Harness and Attach CSV Sources
        auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
            .addLogicalSource("gps", gpsSchema)
            .attachWorkerWithLambdaSourceToCoordinator(csvSourceType, workerConfiguration1);

        testHarness.validate().setupTopology();

        // Define expected output
        const auto expectedOutput = "1722521041, 1, 3.3305, 20, 80\n"
                                                        "1722521051,1, 3.517, 20, 80\n"
                                                        "1722521041,1, 3.3305, 20, 80\n"
                                                        "1722521051,1, 3.517, 20, 80\n"
                                                        "1722521041,1, 3.3305, 20, 80\n"
                                                        "1722521051,1, 3.517, 20, 80\n"
                                                        "1722522227,5, 3.16375, 20, 80\n"
                                                        "1722522227,5, 3.16375, 20, 80\n"
                                                        "1722522227,5, 3.16375, 20, 80\n"
                                                        "1722522227,5, 3.16375, 20, 80\n";
                                 
          
        // Run the query and get the actual dynamic buffers
        auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput), "TopDown").getOutput();


            // Iterate Through Buffers and Log Results
        for (const auto& buffer : actualBuffers) {
            size_t numTuples = buffer.getNumberOfTuples();
            for (size_t i = 0; i < numTuples; ++i) {
                auto tuple = buffer[i];
                NES_INFO("Battery Monitoring Result -  Timestamp: {}, ID : {},passenger_count: {},adjusted_temp: {}, adjusted_light: {}",
                    tuple[0].read<uint64_t>(), 
                    tuple[1].read<uint64_t>(),
                    tuple[2].read<double>(),
                    tuple[3].read<double>(),
                    tuple[4].read<double>());
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
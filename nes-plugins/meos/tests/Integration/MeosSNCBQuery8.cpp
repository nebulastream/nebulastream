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

void exportToJson(const std::vector<NES::Runtime::MemoryLayouts::TestTupleBuffer>& actualBuffers,
                  const std::string& outputPath) {
    nlohmann::json jsonData = nlohmann::json::array();
    for (const auto& buffer : actualBuffers) {
        size_t numTuples = buffer.getNumberOfTuples();
        for (size_t i = 0; i < numTuples; ++i) {
            auto tuple = buffer[i];
            // Adjust fields as needed
            nlohmann::json row;
            row["timestamp"] = tuple[0].read<uint64_t>();
            row["id"]   = tuple[1].read<uint64_t>();
            row["speed"]    = tuple[2].read<double>();
            row["latitude"]    = tuple[3].read<double>();
            row["longitude"]    = tuple[4].read<double>();
            jsonData.push_back(row);
        }
    }
    std::ofstream outFile(outputPath);
    outFile << jsonData.dump(2);
    outFile.close();
}

void exportToCsv(const std::vector<NES::Runtime::MemoryLayouts::TestTupleBuffer>& actualBuffers,
                 const std::string& outputPath) {
    std::ofstream outFile(outputPath);
    
    // Write CSV header
    outFile << "timestamp, id, speed,latitude,longitude\n";
    
    for (const auto& buffer : actualBuffers) {
        size_t numTuples = buffer.getNumberOfTuples();
        for (size_t i = 0; i < numTuples; ++i) {
            auto tuple = buffer[i];
            outFile << tuple[0].read<uint64_t>() << ","   // timestamp
                   << tuple[1].read<uint64_t>() << ","   // id
                   << tuple[2].read<double>() << ","     // speed
                    << tuple[3].read<double>() << ","   // latitude
                    << tuple[4].read<double>() <<"\n";   // longitude
        }
    }
    outFile.close();
}
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

        /* Monitoring Emergency Brake Usage and Brake Pressures
        Trains rely on the correct brake pressure to stop safely. 
        If the pressure is out of range, there is a higher risk of mechanical damage and unsafe braking distances.
        In parallel, frequent emergency brake use can point to hazards or driver errors. 
        Collecting these two data points in real-time and mapping them to the train’s location, 
        the system can detect patterns such as repeated emergency brakes in specific track segments or persistent low-pressure readings on steep inclines. 
        When these anomalies appear, the system sends an alert to help staff take prompt action. 
        This approach ensures safer operations and targeted maintenance in unusual brake activity areas.*/
  


        // Main query combines patterns with location data
        // auto query = Query::from("gps")
        //                 .filter(
        //                     // Any brake activation
        //                     Attribute("Code1") > 0 &&
        //                     // More sensitive pressure ranges (20% deviation)
        //                     (
        //                         Attribute("PCFA_mbar") < 1.9 ||  // 20% below avg 2.48
        //                         Attribute("PCFF_mbar") < 1.4 ||  // 20% below avg 1.77
        //                         Attribute("PCF1_mbar") < 0.8 ||  // 20% below avg 1.08
        //                         Attribute("PCF2_mbar") < 1.0 ||  // 20% below avg 1.35
        //                         Attribute("PCFA_mbar") > 3.0 ||  // 20% above avg
        //                         Attribute("PCFF_mbar") > 2.1 ||  // 20% above avg
        //                         Attribute("PCF1_mbar") > 1.3 ||  // 20% above avg
        //                         Attribute("PCF2_mbar") > 1.6     // 20% above avg
        //                     ) &&
        //                     Attribute("latitude") > 0 &&
        //                     Attribute("longitude") > 0
        //                 )
        //                 .times()
        //                 .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(200)))
        //                 .limit(100);
                    

        auto pressureQuery = Query::from("gps")
            .filter(
                Attribute("Code1") > 0 &&
                (Attribute("PCFA_mbar") < 1.9 ||
                Attribute("PCFF_mbar") < 1.4 ||
                Attribute("PCF1_mbar") < 0.8 ||
                Attribute("PCF2_mbar") < 1.0 ||
                Attribute("PCFA_mbar") > 3.0 ||
                Attribute("PCFF_mbar") > 2.1 ||
                Attribute("PCF1_mbar") > 1.3 ||
                Attribute("PCF2_mbar") > 1.6)
            )
            .times()
            .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(200)));


        // Combine patterns using sequence
        auto query = Query::from("gps")
            .filter(tedwithin(Attribute("longitude"), 
                        Attribute("latitude"),
                        Attribute("timestamp"))==1)
            .joinWith(pressureQuery)
            .where(Attribute("timestamp") ==  Attribute("gps$timestamp")) 
            .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(200)))
            .project(Attribute("timestamp"), 
                    Attribute("id"), 
                    Attribute("speed"), 
                    Attribute("latitude"), 
                    Attribute("longitude"))
            .limit(20);

    
        // Create the Test Harness and Attach CSV Sources
        auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
            .addLogicalSource("gps", gpsSchema)
            .attachWorkerWithLambdaSourceToCoordinator(csvSourceType, workerConfiguration1);

        testHarness.validate().setupTopology();

        // Define expected output
        const auto expectedOutput = 
                                                "1722520697,3,60.0418,50.6803,4.3751\n"
                                                "1722520697,1,26.2793,50.8563,4.361\n"
                                                "1722520697,3,54.0015,50.6817,4.3753\n"
                                                "1722520697,5,27.8684,51.3175,4.4349\n"
                                                "1722520697,1,26.2793,50.8563,4.361\n"
                                                "1722520697,3,41.2051,50.6829,4.3755\n"
                                                "1722520697,5,36.1731,51.3182,4.4353\n"
                                                "1722520697,1,26.2793,50.8563,4.361\n"
                                                "1722520697,3,26.085,50.6837,4.3757\n"
                                                "1722520697,5,45.3861,51.3192,4.4358\n"
                                                "1722520697,1,22.2981,50.8568,4.3608\n"
                                                "1722520697,3,15.2644,50.6843,4.3758\n"
                                                "1722520697,5,53.6315,51.3204,4.4365\n"
                                                "1722520697,1,24.5384,50.8574,4.3605\n"
                                                "1722520697,3,4.0719,50.6845,4.3758\n"
                                                "1722520697,5,61.9343,51.3217,4.4372\n"
                                                "1722520697,1,25.543,50.858,4.3606\n"
                                                "1722520697,3,0.013,50.6845,4.3758\n"
                                                "1722520697,5,69.893,51.3233,4.4381\n"
                                                "1722520697,1,27.1488,50.8587,4.3608\n";
          




        // Run the query and get the actual dynamic buffers
        auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput), "TopDown").getOutput();


        //Iterate Through Buffers and Log Results
        // for (const auto& buffer : actualBuffers) {
        //         size_t numTuples = buffer.getNumberOfTuples();
        //         for (size_t i = 0; i < numTuples; ++i) {
        //             auto tuple = buffer[i];
        //             NES_INFO("Emergency Brake Monitoring Result - timestamp: {}, id: {}, speed: {}, latitude: {}, longitude: {}",
        //                 tuple[0].read<uint64_t>(),  // window start timestamp
        //                 tuple[1].read<uint64_t>(),  // id
        //                 tuple[2].read<double>(),  //speed
        //                 tuple[3].read<double>(),  // latitude
        //                 tuple[4].read<double>());  // longitude
        //         }
        //     }

        exportToJson(actualBuffers, "query8_out.json");
        exportToCsv(actualBuffers, "query8_out.csv");

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
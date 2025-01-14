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
#include <nlohmann/json.hpp> 


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
            row["window_start"] = tuple[0].read<uint64_t>();
            row["window_end"]   = tuple[1].read<uint64_t>();
            row["speed_sum"]    = tuple[2].read<double>();
            jsonData.push_back(row);
        }
    }
    std::ofstream outFile(outputPath);
    outFile << jsonData.dump(2);
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

        auto csvSourceType = CSVSourceType::create("sncb", "sncbmerged");
        csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "selected_columns_df.csv");
        csvSourceType->setNumberOfTuplesToProducePerBuffer(20);// Read 2 tuples per buffer
        csvSourceType->setNumberOfBuffersToProduce(40);       // Produce 10 buffers
        csvSourceType->setSkipHeader(true);                   // Skip the header

        /*
        Location-Based Noise Monitoring
        The query constantly monitors the sound levels outside the train using speed and break pressure data.
        */
        auto query =
            Query::from("sncb")
                .filter(
                    // Check if train is in a specific geographic area
                    tpointatstbox(Attribute("longitude", BasicType::FLOAT64),
                        Attribute("latitude", BasicType::FLOAT64),
                        Attribute("timestamp", BasicType::UINT64)) == 1
                    && 
                    // Only process records with valid speed and pressure
                    Attribute("speed") > 0 && Attribute("PCFA_mbar") > 0
                    &&
                    //Show records with the highest noise level
                    Attribute("speed") * 0.5 + Attribute("PCFA_mbar") * 0.5 > 2
                )
                .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(500)))
                .apply(Max(Attribute("PCFA_mbar")));

        // Create the test harness and attach the CSV source
        auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                               .addLogicalSource("sncb", gpsSchema)
                               .attachWorkerWithLambdaSourceToCoordinator(csvSourceType, workerConfiguration);

        testHarness.validate().setupTopology();

        // Define expected output
        const auto expectedOutput =  "1722520000, 1722520500, 5.043\n"
                                                        "1722520500, 1722521000, 5.051\n"
                                                        "1722521000, 1722521500, 5.047\n"
                                                        "1722521500, 1722522000, 4.991\n"
                                                        "1722522000, 1722522500, 5.388\n"
                                                        "1722522500, 1722523000, 5.152\n"
                                                        "1722523000, 1722523500, 5.021\n"
                                                        "1722523500, 1722524000, 5.017\n";

                            
        // Run the query and get the actual dynamic buffers
        auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput), "TopDown").getOutput();


        for (const auto& buffer : actualBuffers) {
            size_t numTuples = buffer.getNumberOfTuples();
            for (size_t i = 0; i < numTuples; ++i) {
                auto tuple = buffer[i];
                NES_INFO("The result is : {}, {}, {}",
                    tuple[0].read<uint64_t>(), // window_start
                    tuple[1].read<uint64_t>(), // window_end
                    tuple[2].read<double>()); // speed            
            }
        }

        // Export the actual output to a JSON file
        exportToJson(actualBuffers, "query2.json");

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
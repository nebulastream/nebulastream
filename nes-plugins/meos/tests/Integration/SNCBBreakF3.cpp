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


void exportToCsv(const std::vector<NES::Runtime::MemoryLayouts::TestTupleBuffer>& actualBuffers,
                 const std::string& outputPath) {
    std::ofstream outFile(outputPath);
    
    // Write CSV header
    outFile << "timestamp, end, id, minPCFF, maxPCFF, minPCFA, maxPCFA, variPCFF, variPCFA\n";
    
    for (const auto& buffer : actualBuffers) {
        size_t numTuples = buffer.getNumberOfTuples();
        for (size_t i = 0; i < numTuples; ++i) {
            auto tuple = buffer[i];
            outFile << tuple[0].read<uint64_t>() << ","  // window start timestamp
                    << tuple[1].read<uint64_t>() << ","  // end
                    << tuple[2].read<uint64_t>() << ","  //id
                    << tuple[3].read<double>() << ","  // min
                    << tuple[4].read<double>() << ","  // max
                    << tuple[5].read<double>() << ","  // min
                    << tuple[6].read<double>() << ","  // max
                    << tuple[7].read<double>() << ","  // vari
                    << tuple[8].read<double>() << "\n";  // vari
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
TEST_F(ReadSNCB, testF3) {
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
        csvSourceType->setNumberOfTuplesToProducePerBuffer(36);    
        csvSourceType->setGatheringInterval(0);                      
        csvSourceType->setNumberOfBuffersToProduce(10000);
        csvSourceType->setSkipHeader(true);                 // Skip the header

        /* Monitoring relais 2 */
        auto cfaCheckQuery = Query::from("gps")
        .window(SlidingWindow::of(EventTime(Attribute("timestamp", BasicType::UINT64)), Minutes(15), Seconds(30)))
        .byKey(Attribute("id"))
        .apply(Min(Attribute("PCF2_mbar"))->as(Attribute("PCF2_min_value")),
                Max(Attribute("PCF2_mbar"))->as(Attribute("PCF2_max_value")),
                Min(Attribute("PCFF_mbar"))->as(Attribute("PCFF_min_value")),
                Max(Attribute("PCFF_mbar"))->as(Attribute("PCFF_max_value")))
        .map(Attribute("timestamp") = Attribute("gps$start"))
        .map(Attribute("variationPCF2") = Attribute("PCF2_max_value") - Attribute("PCF2_min_value"))
        .filter(Attribute("variationPCF2") > 0.1)
        .map(Attribute("variationPCFF") = Attribute("PCFF_max_value") - Attribute("PCFF_min_value"));
        //.filter(Attribute("variationPCFF") > 1);


        auto pcffVariationQueryNot = Query::from("gps")
        .window(SlidingWindow::of(EventTime(Attribute("timestamp", BasicType::UINT64)), Minutes(25), Seconds(30)))
        .byKey(Attribute("id"))
        .apply(Min(Attribute("PCF2_mbar"))->as(Attribute("PCFF_min_value_f")),
              Max(Attribute("PCF2_mbar"))->as(Attribute("PCFF_max_value_f")))
        .map(Attribute("timestamp") = Attribute("gps$start"))
        .map(Attribute("variationPCF2_f") = Attribute("PCFF_max_value_f") - Attribute("PCFF_min_value_f"))
        .filter(Attribute("variationPCF2_f") < 0.1)
        .project(Attribute("gps$start"),
                 Attribute("id"),
                 Attribute("timestamp"),
                 Attribute("variationPCF2_f"));


        auto noEventAfter5s = cfaCheckQuery
        .andWith(pcffVariationQueryNot)
        .window(TumblingWindow::of(EventTime(Attribute("timestamp", BasicType::UINT64)), Seconds(15)))
        .project(Attribute("id"),
                Attribute("$timestamp"),
                Attribute("variationPCF2"),
                Attribute("variationPCFF"),
                Attribute("variationPCF2_f"));
        
           
        // Create the Test Harness and Attach CSV Sources
        auto testHarness = TestHarness(cfaCheckQuery, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
            .addLogicalSource("gps", gpsSchema)
            .attachWorkerWithLambdaSourceToCoordinator(csvSourceType, workerConfiguration1);

        testHarness.validate().setupTopology();

        // Define expected output
        const auto expectedOutput =  "4,1723680000, 5.516, 3.791, 0.045\n"
                                                        "4,1723680000, 5.463, 3.915, 0.045\n"
                                                        "4,1723680000, 3.933, 5.452, 0.045\n"
                                                        "4,1723680000, 3.933, 5.452, 0.045\n";

        // Run the query and get the actual dynamic buffers
        auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput), "TopDown").getOutput();


        // //Iterate Through Buffers and Log Results
        for (const auto& buffer : actualBuffers) {
                size_t numTuples = buffer.getNumberOfTuples();
                for (size_t i = 0; i < numTuples; ++i) {
                    auto tuple = buffer[i];
                    NES_INFO(" Result - id: {}, timestamp: {}, variationPCFA: {}, variationPCFF: {}, variationPCFF_f: {}",
                        tuple[0].read<uint64_t>(), //id
                        tuple[1].read<uint64_t>(), //timestamp
                        tuple[2].read<double>(), //variationPCFA
                        tuple[3].read<double>(), //variationPCFF
                        tuple[4].read<double>()); //variationPCFF_f
                }
            }

        // // exportToJson(actualBuffers, "queryF1_out.json");
        // exportToCsv(actualBuffers, "queryF1_out.csv");

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
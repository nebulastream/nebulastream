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
    double PCFA_mbar;
    double PCFF_mbar;
    double PCF1_mbar;
    double PCF2_mbar;
    double T1_mbar;
    double T2_mbar;
    uint64_t Code1;
    uint64_t Code2;
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
        auto workerConfiguration3 = WorkerConfiguration::create();

        auto pressSchema = Schema::create()
                        ->addField("timestamp", BasicType::UINT64)
                        ->addField("id", BasicType::UINT64)
                        ->addField("PCFA_mbar", BasicType::FLOAT64)
                        ->addField("PCFF_mbar", BasicType::FLOAT64)
                        ->addField("PCF1_mbar", BasicType::FLOAT64)
                        ->addField("PCF2_mbar", BasicType::FLOAT64)
                        ->addField("T1_mbar", BasicType::FLOAT64)
                        ->addField("T2_mbar", BasicType::FLOAT64)
                        ->addField("Code1", BasicType::UINT64)
                        ->addField("Code2", BasicType::UINT64);
                            

        auto csvSourceType = CSVSourceType::create("press", "dfpress_time");
        csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "dfpress_time.csv");
        csvSourceType->setNumberOfTuplesToProducePerBuffer(10); // Read 10 tuples per buffer
        csvSourceType->setNumberOfBuffersToProduce(20);        // Produce 20 buffers
        csvSourceType->setSkipHeader(true);                    // Skip the header


        auto batterySchema = Schema::create()
                ->addField("timestamp", BasicType::UINT64)
                ->addField("id", BasicType::UINT64)
                ->addField("Vbat", BasicType::FLOAT64);


        auto csvSourceType2 = CSVSourceType::create("battery", "battery_time");
        csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "dfbats_vbat_time.csv");
        csvSourceType2->setNumberOfTuplesToProducePerBuffer(10); // Read 10 tuples per buffer
        csvSourceType2->setNumberOfBuffersToProduce(20);        // Produce 20 buffers
        csvSourceType2->setSkipHeader(true);                    // Skip the header

        auto gpsSchema = Schema::create()
                ->addField("timestamp", BasicType::UINT64)
                ->addField("id", BasicType::UINT64)
                ->addField("speed", BasicType::FLOAT64)
                ->addField("latitude", BasicType::FLOAT64)
                ->addField("longitude", BasicType::FLOAT64);

        auto csvSourceType3 = CSVSourceType::create("gps", "gps_time");
        csvSourceType3->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "dfgps_time.csv");
        csvSourceType3->setNumberOfTuplesToProducePerBuffer(100);// Read 2 tuples per buffer
        csvSourceType3->setNumberOfBuffersToProduce(100);       // Produce 10 buffers
        csvSourceType3->setSkipHeader(true);                   // Skip the header


        // auto subQueryB = Query::from("press2").filter(lessExpression); // B in query

        auto subQueryB = Query::from("gps").filter(teintersects(Attribute("longitude", BasicType::FLOAT64),
                                                        Attribute("latitude", BasicType::FLOAT64),
                                                        Attribute("timestamp", BasicType::UINT64)) > 0
                                                        && Attribute("speed") > 0); // B in query

        // Define query
        auto query =
            Query::from("press")
                .andWith(subQueryB)
                .window(SlidingWindow::of(EventTime(Attribute("timestamp", BasicType::UINT64)), Seconds(10), Seconds(10)));

        // Create the test harness and attach the CSV sources
        auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                            .addLogicalSource("press", pressSchema)
                            .addLogicalSource("battery", batterySchema) // Added battery logical source
                            .addLogicalSource("gps", gpsSchema) // Added gps logical source
                            .attachWorkerWithLambdaSourceToCoordinator(csvSourceType, workerConfiguration1)
                            .attachWorkerWithLambdaSourceToCoordinator(csvSourceType2, workerConfiguration2)
                            .attachWorkerWithLambdaSourceToCoordinator(csvSourceType3, workerConfiguration3); // Attached battery source

        testHarness.validate().setupTopology();

        // Define expected output
        const auto expectedOutput =  "";
                            
        // Run the query and get the actual dynamic buffers
        auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput), "TopDown").getOutput();


        for (const auto& buffer : actualBuffers) {
            size_t numTuples = buffer.getNumberOfTuples();
            for (size_t i = 0; i < numTuples; ++i) {
                auto tuple = buffer[i];
                NES_INFO("The result is : {}, {}, {}",
                    tuple[0].read<uint64_t>(), // window_start
                    tuple[1].read<uint64_t>(), // window_end
                    tuple[2].read<double>()); // PCF1_mbar            
            }
        }

        const auto outputSchema = testHarness.getOutputSchema();
        auto tmpBuffers =
            TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
        auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);


        EXPECT_FALSE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));


    } catch (const std::exception& e) {
        FAIL() << "Caught exception: " << e.what();
    }
}

} // namespace NES
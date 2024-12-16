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

        auto batterySchema = Schema::create()
                      ->addField("timestamp", BasicType::UINT64)
                      ->addField("id", BasicType::UINT64)
                      ->addField("Vbat", BasicType::FLOAT64);

        ASSERT_EQ(sizeof(InputValue), batterySchema->getSchemaSizeInBytes());

        auto csvSourceType = CSVSourceType::create("sncb", "dfbats_vbat_time");
        csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "dfbats_vbat_time.csv");
        csvSourceType->setNumberOfTuplesToProducePerBuffer(100);// Read 2 tuples per buffer
        csvSourceType->setNumberOfBuffersToProduce(100);       // Produce 10 buffers
        csvSourceType->setSkipHeader(true);                   // Skip the header

        // Define query
        auto query =
            Query::from("sncb")
                .filter(READ(Attribute("Vbat",BasicType::UINT64)) > 0)
                .window(SlidingWindow::of(EventTime(Attribute("timestamp", BasicType::UINT64)), Seconds(10), Seconds(10)))
                .apply(Max(Attribute("Vbat",BasicType::UINT64)));


        // Create the test harness and attach the CSV source
        auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                               .addLogicalSource("sncb", batterySchema)
                               .attachWorkerWithLambdaSourceToCoordinator(csvSourceType, workerConfiguration);

        testHarness.validate().setupTopology();

        // Define expected output
        const auto expectedOutput =  "1722520000, 1722530000, 30\n"
                                                        "1722530000, 1722540000, 30\n";
                            
        // Run the query and get the actual dynamic buffers
        auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput), "TopDown").getOutput();


        for (const auto& buffer : actualBuffers) {
            size_t numTuples = buffer.getNumberOfTuples();
            for (size_t i = 0; i < numTuples; ++i) {
                auto tuple = buffer[i];
                NES_INFO("The result is : {}, {}, {}",
                    tuple[0].read<uint64_t>(), // window_start
                    tuple[1].read<uint64_t>(), // window_end
                    tuple[2].read<double>()); // vbat            
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
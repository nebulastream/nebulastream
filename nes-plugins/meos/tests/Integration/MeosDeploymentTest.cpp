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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <API/TestSchemas.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <chrono>

using namespace std;

struct InputValue {
    _Float64 timestamp;
    uint64_t mmsi;
    _Float32 latitude;
    _Float32 longitude;
    _Float32 sog;
};


namespace NES {
using namespace Configurations;

class MeosDeploy : public Testing::BaseIntegrationTest,
      public testing::WithParamInterface<std::tuple<std::string, SchemaPtr, std::string, std::string>>  {
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

    // static auto createFloatTestData() {
    //     return std::make_tuple("Float",
    //                            Schema::create()
    //                                 ->addField("timestamp", BasicType::FLOAT64)
    //              s                   ->addField("mmsi", BasicType::UINT64)
    //                                 ->addField("latitude", BasicType::FLOAT32)
    //                                 ->addField("longitude", BasicType::FLOAT32)
    //                                 ->addField("sog", BasicType::FLOAT32),
    //                            "aisinput.csv",
    //                            "1610060400.0,265513270,57.059,12.272388,0.0\n"
    //                            "1610060401.0,219027804,55.94244,11.866278,0.0\n"
    //                          );
    // }

};


PhysicalSourceTypePtr createSimpleInputStream(std::string logicalSourceName,
                                              std::string physicalSourceName,
                                              uint64_t numberOfBuffers,
                                              uint64_t numberOfKeys = 1) {
    return LambdaSourceType::create(
        logicalSourceName,
        physicalSourceName,
        [numberOfKeys](Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
            auto inputValue = (InputValue*) buffer.getBuffer();
            for (uint64_t i = 0; i < numberOfTuplesToProduce; i++) {
                inputValue[i].mmsi = i % numberOfKeys;
                inputValue[i].timestamp = 1610060400.0;
                inputValue[i].latitude = 57.059 + i % 10;
                inputValue[i].longitude = 12.272388 + i % 10;
                inputValue[i].sog = 0.0;
            }
            buffer.setNumberOfTuples(numberOfTuplesToProduce);

        },
        numberOfBuffers,
        0,
        GatheringMode::INTERVAL_MODE);
}


TEST_F(MeosDeploy, testSimpleWindowEventTime) {
    try {
        auto meosSchema = Schema::create()
            ->addField("timestamp", BasicType::FLOAT64)
            ->addField("mmsi", BasicType::UINT64)
            ->addField("latitude",  BasicType::FLOAT64)
            ->addField("longitude", BasicType::FLOAT32)
            ->addField("sog", BasicType::FLOAT32);

        ASSERT_EQ(sizeof(InputValue), meosSchema->getSchemaSizeInBytes());


        auto csvSourceType = CSVSourceType::create("ais", "aisinput");
        csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "aisinput.csv");
        csvSourceType->setNumberOfTuplesToProducePerBuffer(2);
        csvSourceType->setNumberOfBuffersToProduce(10);
        csvSourceType->setSkipHeader(true);

        auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Seconds(10)))
                     .byKey(Attribute("mmsi"))
                     .apply(Sum(Attribute("sog")));

        auto lambdaSource = createSimpleInputStream("window", "window1", 1);
        auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("window", meosSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(lambdaSource, workerConfiguration);

        ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
        testHarness.validate().setupTopology();

    const auto expectedOutput = "0, 1000, 0, 170\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));

    } catch (const std::exception& e) {
        FAIL() << "Caught exception: " << e.what();
    }
}

} // namespace NES

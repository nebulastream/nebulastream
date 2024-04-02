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

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-copy-dtor"
#include <BaseIntegrationTest.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#pragma clang diagnostic pop
#include <API/QueryAPI.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Identifiers.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>
#include <API/TestSchemas.hpp>

using namespace std;

namespace NES {

using namespace Configurations;

/**
 * @brief In this test we assess the correctness of the non-keyed sliding window
 */
class NonKeyedSlidingWindowTests : public Testing::BaseIntegrationTest {
  public:
    WorkerConfigurationPtr workerConfiguration;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NonKeyedSlidingWindowTests.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NonKeyedSlidingWindowTests test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        workerConfiguration = WorkerConfiguration::create();
        workerConfiguration->queryCompiler.windowingStrategy = QueryCompilation::WindowingStrategy::SLICING;
        workerConfiguration->queryCompiler.compilationStrategy = QueryCompilation::CompilationStrategy::DEBUG;
    }
};

struct InputValue {
    uint64_t id;
    uint64_t value;
    uint64_t timestamp;
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
                inputValue[i].value = 1;
                inputValue[i].id = i % numberOfKeys;
                inputValue[i].timestamp = 1;
            }
            buffer.setNumberOfTuples(numberOfTuplesToProduce);
        },
        numberOfBuffers,
        0,
        GatheringMode::INTERVAL_MODE);
}

class DataGenerator {
  public:
    DataGenerator(std::string logicalSourceName, std::string physicalSourceName, uint64_t numberOfBuffers)
        : logicalSourceName(std::move(logicalSourceName)), physicalSourceName(std::move(physicalSourceName)),
          numberOfBuffers(numberOfBuffers){};
    PhysicalSourceTypePtr getSource() {
        return LambdaSourceType::create(
            logicalSourceName,
            physicalSourceName,
            [this](Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
                auto inputValue = (InputValue*) buffer.getBuffer();
                for (uint64_t i = 0; i < numberOfTuplesToProduce; i++) {
                    inputValue[i].value = 1;
                    inputValue[i].id = 1;
                    inputValue[i].timestamp = (counter * numberOfTuplesToProduce) + i;
                }
                counter++;
                NES_DEBUG("Counter: {}", counter)
                buffer.setNumberOfTuples(numberOfTuplesToProduce);
            },
            numberOfBuffers,
            0,
            GatheringMode::INTERVAL_MODE);
    }

  private:
    std::string logicalSourceName;
    std::string physicalSourceName;
    uint64_t numberOfBuffers;
    std::atomic_uint64_t counter = 0;
};

TEST_F(NonKeyedSlidingWindowTests, testSingleSlidingWindowSingleBufferSameLength) {
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Seconds(1)))
                     .apply(Sum(Attribute("value")));

    auto lambdaSource = createSimpleInputStream("window", "window1", 1);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(lambdaSource, workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();

    // Expected output
    auto expectedOutput = "0, 1000, 170\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

TEST_F(NonKeyedSlidingWindowTests, testSingleSlidingWindowSingleBuffer) {
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(100)))
                     .apply(Sum(Attribute("value")));

    auto lambdaSource = createSimpleInputStream("window", "window1", 1);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(lambdaSource, workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();

    // Expected output
    auto expectedOutput = "0, 1000, 170\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

TEST_F(NonKeyedSlidingWindowTests, testSingleSlidingWindowMultiBuffer) {
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(100)))
                     .apply(Sum(Attribute("value")));
    auto lambdaSource = createSimpleInputStream("window", "window1", 100);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(lambdaSource, workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();

    // Expected output
    auto expectedOutput = "0, 1000, 17000\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

TEST_F(NonKeyedSlidingWindowTests, testMultipleSldingWindowMultiBuffer) {
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(100)))
                     .apply(Sum(Attribute("value")));
    auto dg = DataGenerator("window", "window1", 100);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(dg.getSource(), workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();

    // Expected output
    std::stringstream expectedOutput;
    for (uint64_t windowStart = 0; windowStart <= 16000; windowStart = windowStart + 100) {
        expectedOutput << windowStart << ", " << (windowStart + 1000) << ", 1000\n";
    }
    expectedOutput << "16100, 17100, 900\n"
                   << "16200, 17200, 800\n"
                   << "16300, 17300, 700\n"
                   << "16400, 17400, 600\n"
                   << "16500, 17500, 500\n"
                   << "16600, 17600, 400\n"
                   << "16700, 17700, 300\n"
                   << "16800, 17800, 200\n"
                   << "16900, 17900, 100\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromStream(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

TEST_F(NonKeyedSlidingWindowTests, testMultipleSldingWindowIrigularSlide) {
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(300)))
                     .apply(Sum(Attribute("value")));
    auto dg = DataGenerator("window", "window1", 100);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(dg.getSource(), workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();

    // Expected output
    std::stringstream expectedOutput;
    expectedOutput << "0, 1000, 1000\n";
    for (uint64_t windowStart = 300; windowStart <= 16000; windowStart = windowStart + 300) {
        expectedOutput << windowStart << ", " << (windowStart + 1000) << ", 1000\n";
    }
    expectedOutput << "16200, 17200, 800\n"
                   << "16500, 17500, 500\n"
                   << "16800, 17800, 200\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromStream(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

}// namespace NES

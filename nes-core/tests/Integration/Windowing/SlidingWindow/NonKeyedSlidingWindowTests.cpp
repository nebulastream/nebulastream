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
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <Common/Identifiers.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>

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
        workerConfiguration->queryCompiler.windowingStrategy =
            QueryCompilation::QueryCompilerOptions::WindowingStrategy::SLICING;
        workerConfiguration->queryCompiler.compilationStrategy =
            QueryCompilation::QueryCompilerOptions::CompilationStrategy::DEBUG;
    }
};

struct InputValue {
    uint64_t value;
    uint64_t id;
    uint64_t timestamp;
};

struct Output {
    uint64_t start;
    uint64_t end;
    uint64_t value;
    bool operator==(Output const& rhs) const { return (start == rhs.start && end == rhs.end && value == rhs.value); }
    friend ostream& operator<<(ostream& os, const Output& output) {
        os << "start: " << output.start << " end: " << output.end << " value: " << output.value;
        return os;
    }
};

struct OutputMultiAgg {
    uint64_t start;
    uint64_t end;
    uint64_t value1;
    uint64_t value2;
    uint64_t value3;
    uint64_t value4;
    uint64_t value5;
    bool operator==(const OutputMultiAgg& rhs) const {
        return start == rhs.start && end == rhs.end && value1 == rhs.value1 && value2 == rhs.value2 && value3 == rhs.value3
            && value4 == rhs.value4 && value5 == rhs.value5;
    }
    bool operator!=(const OutputMultiAgg& rhs) const { return !(rhs == *this); }

    friend ostream& operator<<(ostream& os, const OutputMultiAgg& agg) {
        os << "start: " << agg.start << " end: " << agg.end << " value1: " << agg.value1 << " value2: " << agg.value2
           << " value3: " << agg.value3 << " value4: " << agg.value4 << " value5: " << agg.value5;
        return os;
    }
};

PhysicalSourceTypePtr createSimpleInputStream(uint64_t numberOfBuffers, uint64_t numberOfKeys = 1) {
    return LambdaSourceType::create(
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
    DataGenerator(uint64_t numberOfBuffers) : numberOfBuffers(numberOfBuffers){};
    PhysicalSourceTypePtr getSource() {
        return LambdaSourceType::create(
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
    uint64_t numberOfBuffers;
    std::atomic_uint64_t counter = 0;
};

TEST_F(NonKeyedSlidingWindowTests, testSingleSlidingWindowSingleBufferSameLength) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Seconds(1)))
                     .apply(Sum(Attribute("value")));

    auto lambdaSource = createSimpleInputStream(1);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .enableNautilus()
                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator("window", lambdaSource, workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.validate().setupTopology();

    std::vector<Output> expectedOutput = {{0, 1000, 170}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    ASSERT_EQ(actualOutput.size(), expectedOutput.size());
    ASSERT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(NonKeyedSlidingWindowTests, testSingleSlidingWindowSingleBuffer) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(100)))
                     .apply(Sum(Attribute("value")));
    // R"(Query::from("window"))";

    auto lambdaSource = createSimpleInputStream(1);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .enableNautilus()
                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator("window", lambdaSource, workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.validate().setupTopology();

    std::vector<Output> expectedOutput = {{0, 1000, 170}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    ASSERT_EQ(actualOutput.size(), expectedOutput.size());
    ASSERT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(NonKeyedSlidingWindowTests, testSingleSlidingWindowMultiBuffer) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(100)))
                     .apply(Sum(Attribute("value")));
    auto lambdaSource = createSimpleInputStream(100);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .enableNautilus()
                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator("window", lambdaSource, workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();
    std::vector<Output> expectedOutput = {{0, 1000, 17000}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");
    ASSERT_EQ(actualOutput.size(), expectedOutput.size());
    ASSERT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(NonKeyedSlidingWindowTests, testMultipleSldingWindowMultiBuffer) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(100)))
                     .apply(Sum(Attribute("value")));
    auto dg = DataGenerator(100);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .enableNautilus()
                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator("window", dg.getSource(), workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();

    std::vector<Output> expectedOutput = {};

    for (uint64_t windowStart = 0; windowStart <= 16000; windowStart = windowStart + 100) {
        expectedOutput.push_back({windowStart, windowStart + 1000, 1000});
    }
    expectedOutput.push_back({16100, 17100, 900});
    expectedOutput.push_back({16200, 17200, 800});
    expectedOutput.push_back({16300, 17300, 700});
    expectedOutput.push_back({16400, 17400, 600});
    expectedOutput.push_back({16500, 17500, 500});
    expectedOutput.push_back({16600, 17600, 400});
    expectedOutput.push_back({16700, 17700, 300});
    expectedOutput.push_back({16800, 17800, 200});
    expectedOutput.push_back({16900, 17900, 100});

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");
    ASSERT_EQ(actualOutput.size(), expectedOutput.size());
    ASSERT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(NonKeyedSlidingWindowTests, testMultipleSldingWindowIrigularSlide) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(300)))
                     .apply(Sum(Attribute("value")));
    auto dg = DataGenerator(100);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .enableNautilus()
                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator("window", dg.getSource(), workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();

    std::vector<Output> expectedOutput = {};
    expectedOutput.push_back({0, 1000, 1000});
    for (uint64_t windowStart = 300; windowStart <= 16000; windowStart = windowStart + 300) {
        expectedOutput.push_back({windowStart, windowStart + 1000, 1000});
    }
    expectedOutput.push_back({16200, 17200, 800});
    expectedOutput.push_back({16500, 17500, 500});
    expectedOutput.push_back({16800, 17800, 200});

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");
    ASSERT_EQ(actualOutput.size(), expectedOutput.size());
    ASSERT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

}// namespace NES

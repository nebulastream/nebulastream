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
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

/**
 * @brief In this test we assess the correctness of the thread local tumbling window
 */
class KeyedSlidingWindowTests : public Testing::BaseIntegrationTest, public ::testing::WithParamInterface<int> {
  public:
    WorkerConfigurationPtr workerConfiguration;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("KeyedSlidingWindowTests.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup KeyedSlidingWindowTests test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        workerConfiguration = WorkerConfiguration::create();
        workerConfiguration->queryCompiler.windowingStrategy = QueryCompilation::WindowingStrategy::SLICING;
        workerConfiguration->queryCompiler.compilationStrategy = QueryCompilation::CompilationStrategy::DEBUG;
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
    uint64_t id;
    uint64_t value;
    bool operator==(Output const& rhs) const {
        return (start == rhs.start && end == rhs.end && id == rhs.id && value == rhs.value);
    }
    friend ostream& operator<<(ostream& os, const Output& output) {
        os << "start: " << output.start << " end: " << output.end << " id: " << output.id << " value: " << output.value;
        return os;
    }
};

struct OutputMultiAgg {
    uint64_t start;
    uint64_t end;
    uint64_t id;
    uint64_t value1;
    uint64_t value2;
    uint64_t value3;
    uint64_t value4;
    uint64_t value5;
    bool operator==(const OutputMultiAgg& rhs) const {
        return start == rhs.start && end == rhs.end && id == rhs.id && value1 == rhs.value1 && value2 == rhs.value2
            && value3 == rhs.value3 && value4 == rhs.value4 && value5 == rhs.value5;
    }
    bool operator!=(const OutputMultiAgg& rhs) const { return !(rhs == *this); }

    friend ostream& operator<<(ostream& os, const OutputMultiAgg& agg) {
        os << "start: " << agg.start << " end: " << agg.end << " id: " << agg.id << " value1: " << agg.value1
           << " value2: " << agg.value2 << " value3: " << agg.value3 << " value4: " << agg.value4 << " value5: " << agg.value5;
        return os;
    }
};

struct InputValueMultiKeys {
    uint64_t value;
    uint64_t key1;
    uint64_t key2;
    uint64_t key3;
    uint64_t timestamp;
};

struct OutputMultiKeys {
    uint64_t start;
    uint64_t end;
    uint64_t key1;
    uint64_t key2;
    uint64_t key3;
    uint64_t value;
    bool operator==(const OutputMultiKeys& rhs) const {
        return start == rhs.start && end == rhs.end && key1 == rhs.key1 && key2 == rhs.key2 && key3 == rhs.key3
            && value == rhs.value;
    }
    bool operator!=(const OutputMultiKeys& rhs) const { return !(rhs == *this); }
    friend ostream& operator<<(ostream& os, const OutputMultiKeys& keys) {
        os << "start: " << keys.start << " end: " << keys.end << " key1: " << keys.key1 << " key2: " << keys.key2
           << " key3: " << keys.key3 << " value: " << keys.value;
        return os;
    }
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

class DataGeneratorMultiKey {
  public:
    DataGeneratorMultiKey(std::string logicalSourceName,
                          std::string physicalSourceName,
                          uint64_t numberOfBuffers,
                          uint64_t numberOfKeys = 1)
        : logicalSourceName(std::move(logicalSourceName)), physicalSourceName(std::move(physicalSourceName)),
          numberOfBuffers(numberOfBuffers), numberOfKeys(numberOfKeys){};
    PhysicalSourceTypePtr getSource() {
        return LambdaSourceType::create(
            logicalSourceName,
            physicalSourceName,
            [this](Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
                auto inputValue = (InputValueMultiKeys*) buffer.getBuffer();
                for (uint64_t i = 0; i < numberOfTuplesToProduce; i++) {
                    inputValue[i].value = 1;
                    inputValue[i].key1 = i % numberOfKeys;
                    inputValue[i].key2 = i % numberOfKeys;
                    inputValue[i].key3 = i % numberOfKeys;
                    inputValue[i].timestamp = (counter * numberOfTuplesToProduce) + i;
                }
                counter++;
            },
            numberOfBuffers,
            0,
            GatheringMode::INTERVAL_MODE);
    }

  private:
    std::string logicalSourceName;
    std::string physicalSourceName;
    uint64_t numberOfBuffers;
    uint64_t numberOfKeys;
    std::atomic_uint64_t counter = 0;
};

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

TEST_F(KeyedSlidingWindowTests, testSingleSlidingWindowSingleBufferSameLength) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Seconds(1)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")));

    auto lambdaSource = createSimpleInputStream("window", "window1", 1);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(lambdaSource, workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.validate().setupTopology();

    std::vector<Output> expectedOutput = {{0, 1000, 0, 170}};

    std::vector<Output> actualOutput = testHarness.runQuery(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY").getOutput<Output>();

    ASSERT_EQ(actualOutput.size(), expectedOutput.size());
    ASSERT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(KeyedSlidingWindowTests, testSingleSlidingWindowSingleBuffer) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(100)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")));

    auto lambdaSource = createSimpleInputStream("window", "window1", 1);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("window", testSchema)

                           .attachWorkerWithLambdaSourceToCoordinator(lambdaSource, workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.validate().setupTopology();

    std::vector<Output> expectedOutput = {{0, 1000, 0, 170}};

    std::vector<Output> actualOutput = testHarness.runQuery(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY").getOutput<Output>();

    ASSERT_EQ(actualOutput.size(), expectedOutput.size());
    ASSERT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(KeyedSlidingWindowTests, testSingleSlidingWindowMultiBuffer) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(100)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")));
    auto lambdaSource = createSimpleInputStream("window", "window1", 100);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("window", testSchema)

                           .attachWorkerWithLambdaSourceToCoordinator(lambdaSource, workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();
    std::vector<Output> expectedOutput = {{0, 1000, 0, 17000}};
    std::vector<Output> actualOutput = testHarness.runQuery(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY").getOutput<Output>();
    ASSERT_EQ(actualOutput.size(), expectedOutput.size());
    ASSERT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(KeyedSlidingWindowTests, testMultipleSlidingWindowMultiBuffer) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(100)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")));
    auto dg = DataGenerator("window", "window1", 100);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(dg.getSource(), workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();

    std::vector<Output> expectedOutput = {};

    for (uint64_t windowStart = 0; windowStart <= 16000; windowStart = windowStart + 100) {
        expectedOutput.push_back({windowStart, windowStart + 1000, 1, 1000});
    }
    expectedOutput.push_back({16100, 17100, 1, 900});
    expectedOutput.push_back({16200, 17200, 1, 800});
    expectedOutput.push_back({16300, 17300, 1, 700});
    expectedOutput.push_back({16400, 17400, 1, 600});
    expectedOutput.push_back({16500, 17500, 1, 500});
    expectedOutput.push_back({16600, 17600, 1, 400});
    expectedOutput.push_back({16700, 17700, 1, 300});
    expectedOutput.push_back({16800, 17800, 1, 200});
    expectedOutput.push_back({16900, 17900, 1, 100});

    std::vector<Output> actualOutput = testHarness.runQuery(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY").getOutput<Output>();
    ASSERT_EQ(actualOutput.size(), expectedOutput.size());
    ASSERT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(KeyedSlidingWindowTests, testMultipleSldingWindowIrigularSlide) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(300)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")));
    auto dg = DataGenerator("window", "window1", 100);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(dg.getSource(), workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();

    std::vector<Output> expectedOutput = {};
    expectedOutput.push_back({0, 1000, 1, 1000});
    for (uint64_t windowStart = 300; windowStart <= 16000; windowStart = windowStart + 300) {
        expectedOutput.push_back({windowStart, windowStart + 1000, 1, 1000});
    }
    expectedOutput.push_back({16200, 17200, 1, 800});
    expectedOutput.push_back({16500, 17500, 1, 500});
    expectedOutput.push_back({16800, 17800, 1, 200});

    std::vector<Output> actualOutput = testHarness.runQuery(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY").getOutput<Output>();
    ASSERT_EQ(actualOutput.size(), expectedOutput.size());
    ASSERT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(KeyedSlidingWindowTests, testSingleMultiKeySlidingWindow) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("key1", DataTypeFactory::createUInt64())
                          ->addField("key2", DataTypeFactory::createUInt64())
                          ->addField("key3", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValueMultiKeys), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Seconds(1)))
                     .byKey(Attribute("key1"), Attribute("key2"), Attribute("key3"))
                     .apply(Sum(Attribute("value")));
    auto dg = DataGeneratorMultiKey("window", "window1", 1, 102);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(dg.getSource(), workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();
    std::vector<OutputMultiKeys> expectedOutput;
    for (uint64_t k = 0; k < 102; k++) {
        expectedOutput.push_back({0, 1000, k, k, k, 1});
    }
    std::vector<OutputMultiKeys> actualOutput = testHarness.runQuery(102, "BottomUp", "NONE", "IN_MEMORY").getOutput<OutputMultiKeys>();
    ASSERT_EQ(actualOutput.size(), expectedOutput.size());
    ASSERT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}
}// namespace NES

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

#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <iostream>
#include <utility>

using namespace std;

namespace NES {

using namespace Configurations;

/**
 * @brief In this test we assess the correctness of the non keyed tumbling window
 */
class NonKeyedTumblingWindowTests : public Testing::BaseIntegrationTest {
  public:
    WorkerConfigurationPtr workerConfiguration;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NonKeyedTumblingWindowTests.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NonKeyedTumblingWindowTests test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        workerConfiguration = WorkerConfiguration::create();
        workerConfiguration->queryCompiler.windowingStrategy = QueryCompilation::WindowingStrategy::SLICING;
        workerConfiguration->queryCompiler.compilationStrategy = QueryCompilation::CompilationStrategy::OPTIMIZE;
        this->numberOfGeneratedBuffers = 100;
    }

  protected:
    uint64_t numberOfGeneratedBuffers;
};

struct InputValue {
    uint64_t value;
    uint64_t id;
    uint64_t timestamp;
};

struct MultipleInputValues {
    uint64_t value3;
    uint64_t value2;
    uint64_t value1;
    uint64_t timestamp;
};

struct GlobalOutput {
    uint64_t start;
    uint64_t end;
    uint64_t value;
    bool operator==(GlobalOutput const& rhs) const { return (start == rhs.start && end == rhs.end && value == rhs.value); }
    friend ostream& operator<<(ostream& os, const GlobalOutput& GlobalOutput) {
        os << "start: " << GlobalOutput.start << " end: " << GlobalOutput.end << " value: " << GlobalOutput.value;
        return os;
    }
};

struct GlobalOutputMultiAgg {
    uint64_t start;
    uint64_t end;
    uint64_t value1;
    uint64_t value2;
    uint64_t value3;
    uint64_t value4;
    uint64_t value5;
    bool operator==(const GlobalOutputMultiAgg& rhs) const {
        return start == rhs.start && end == rhs.end && value1 == rhs.value1 && value2 == rhs.value2 && value3 == rhs.value3
            && value4 == rhs.value4 && value5 == rhs.value5;
    }
    bool operator!=(const GlobalOutputMultiAgg& rhs) const { return !(rhs == *this); }

    friend ostream& operator<<(ostream& os, const GlobalOutputMultiAgg& agg) {
        os << "start: " << agg.start << " end: " << agg.end << " value1: " << agg.value1 << " value2: " << agg.value2
           << " value3: " << agg.value3 << " value4: " << agg.value4 << " value5: " << agg.value5;
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

struct GlobalOutputMultiKeys {
    uint64_t start;
    uint64_t end;
    uint64_t key1;
    uint64_t key2;
    uint64_t key3;
    uint64_t value;
    bool operator==(const GlobalOutputMultiKeys& rhs) const {
        return start == rhs.start && end == rhs.end && key1 == rhs.key1 && key2 == rhs.key2 && key3 == rhs.key3
            && value == rhs.value;
    }
    bool operator!=(const GlobalOutputMultiKeys& rhs) const { return !(rhs == *this); }
    friend ostream& operator<<(ostream& os, const GlobalOutputMultiKeys& keys) {
        os << "start: " << keys.start << " end: " << keys.end << " key1: " << keys.key1 << " key2: " << keys.key2
           << " key3: " << keys.key3 << " value: " << keys.value;
        return os;
    }
};

struct GlobalOutputMultiAggSameKey {
    uint64_t start;
    uint64_t end;
    uint64_t value1;
    uint64_t value2;
    uint64_t value3;
    uint64_t value4;
    bool operator==(const GlobalOutputMultiAggSameKey& rhs) const {
        return start == rhs.start && end == rhs.end && value1 == rhs.value1 && value2 == rhs.value2 && value3 == rhs.value3
            && value4 == rhs.value4;
    }
    bool operator!=(const GlobalOutputMultiAggSameKey& rhs) const { return !(rhs == *this); }
    friend ostream& operator<<(ostream& os, const GlobalOutputMultiAggSameKey& keys) {
        os << "start: " << keys.start << " end: " << keys.end << " value1: " << keys.value1 << " value2: " << keys.value2
           << " value3:" << keys.value3 << " value4:" << keys.value4;
        return os;
    }
};

PhysicalSourceTypePtr createSimpleInputStream(std::string logicalSourceName,
                                              std::string physicalSourceName,
                                              uint64_t numberOfBuffers,
                                              uint64_t numberOfKeys = 1) {
    return LambdaSourceType::create(
        std::move(logicalSourceName),
        std::move(physicalSourceName),
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

class DataGeneratorMultiValue {
  public:
    DataGeneratorMultiValue(std::string logicalSourceName, std::string physicalSourceName, uint64_t numberOfBuffers)
        : logicalSourceName(std::move(logicalSourceName)), physicalSourceName(std::move(physicalSourceName)),
          numberOfBuffers(numberOfBuffers){};
    PhysicalSourceTypePtr getSource() {
        return LambdaSourceType::create(
            logicalSourceName,
            physicalSourceName,
            [this](Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
                auto inputValue = (MultipleInputValues*) buffer.getBuffer();
                for (uint64_t i = 0; i < numberOfTuplesToProduce; i++) {
                    inputValue[i].value1 = 1;
                    inputValue[i].value2 = 1;
                    inputValue[i].value3 = 1;
                    inputValue[i].timestamp = (counter * numberOfTuplesToProduce) + i;
                }
                counter++;
                NES_DEBUG("numberOfTuplesToProduce: {}", numberOfTuplesToProduce)
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
                NES_DEBUG("Counter: {}", std::to_string(counter));
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

TEST_F(NonKeyedTumblingWindowTests, testSingleGlobalTumblingWindowSingleBuffer) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                     .apply(Sum(Attribute("value")));

    auto lambdaSource = createSimpleInputStream("window", "window1", 1);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(lambdaSource, workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.validate().setupTopology();

    std::vector<GlobalOutput> expectedGlobalOutput = {{0, 1000, 170}};

    std::vector<GlobalOutput> actualGlobalOutput =
        testHarness.runQuery(expectedGlobalOutput.size(), "BottomUp", "NONE", "IN_MEMORY").getOutput<GlobalOutput>();

    ASSERT_EQ(actualGlobalOutput.size(), expectedGlobalOutput.size());
    ASSERT_THAT(actualGlobalOutput, ::testing::UnorderedElementsAreArray(expectedGlobalOutput));
}

TEST_F(NonKeyedTumblingWindowTests, testSingleGlobalTumblingWindowMultiBuffer) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                     .apply(Sum(Attribute("value")));
    auto lambdaSource = createSimpleInputStream("window", "window1", 100);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(lambdaSource, workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();
    std::vector<GlobalOutput> expectedGlobalOutput = {{0, 1000, 17000}};
    std::vector<GlobalOutput> actualGlobalOutput =
        testHarness.runQuery(expectedGlobalOutput.size(), "BottomUp", "NONE", "IN_MEMORY").getOutput<GlobalOutput>();
    ASSERT_EQ(actualGlobalOutput.size(), expectedGlobalOutput.size());
    ASSERT_THAT(actualGlobalOutput, ::testing::UnorderedElementsAreArray(expectedGlobalOutput));
}

TEST_F(NonKeyedTumblingWindowTests, testMultipleGlobalTumblingWindowMultiBuffer) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                     .apply(Sum(Attribute("value")));
    auto dg = DataGenerator("window", "window1", 100);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(dg.getSource(), workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();
    std::vector<GlobalOutput> expectedGlobalOutput = {{0, 1000, 1000},
                                                      {1000, 2000, 1000},
                                                      {2000, 3000, 1000},
                                                      {3000, 4000, 1000},
                                                      {4000, 5000, 1000},
                                                      {5000, 6000, 1000},
                                                      {6000, 7000, 1000},
                                                      {7000, 8000, 1000},
                                                      {8000, 9000, 1000},
                                                      {9000, 10000, 1000},
                                                      {10000, 11000, 1000},
                                                      {11000, 12000, 1000},
                                                      {12000, 13000, 1000},
                                                      {13000, 14000, 1000},
                                                      {14000, 15000, 1000},
                                                      {15000, 16000, 1000},
                                                      {16000, 17000, 1000}};
    std::vector<GlobalOutput> actualGlobalOutput =
        testHarness.runQuery(expectedGlobalOutput.size(), "BottomUp", "NONE", "IN_MEMORY").getOutput<GlobalOutput>();
    ASSERT_EQ(actualGlobalOutput.size(), expectedGlobalOutput.size());
    ASSERT_THAT(actualGlobalOutput, ::testing::UnorderedElementsAreArray(expectedGlobalOutput));
}

TEST_F(NonKeyedTumblingWindowTests, testSingleTumblingWindowMultiBufferMultipleKeys) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                     .apply(Sum(Attribute("value")));
    auto lambdaSource = createSimpleInputStream("window", "window1", 100, 100);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(lambdaSource, workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();
    std::vector<GlobalOutput> expectedGlobalOutput;
    expectedGlobalOutput.push_back({0, 1000, 17000});
    std::vector<GlobalOutput> actualGlobalOutput =
        testHarness.runQuery(expectedGlobalOutput.size(), "BottomUp", "NONE", "IN_MEMORY").getOutput<GlobalOutput>();
    ASSERT_EQ(actualGlobalOutput.size(), expectedGlobalOutput.size());
    ASSERT_THAT(actualGlobalOutput, ::testing::UnorderedElementsAreArray(expectedGlobalOutput));
}

TEST_F(NonKeyedTumblingWindowTests, testTumblingWindowCount) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).apply(Count());
    auto dg = DataGenerator("window", "window1", 100);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(dg.getSource(), workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();
    std::vector<GlobalOutput> expectedGlobalOutput = {{0, 1000, 1000},
                                                      {1000, 2000, 1000},
                                                      {2000, 3000, 1000},
                                                      {3000, 4000, 1000},
                                                      {4000, 5000, 1000},
                                                      {5000, 6000, 1000},
                                                      {6000, 7000, 1000},
                                                      {7000, 8000, 1000},
                                                      {8000, 9000, 1000},
                                                      {9000, 10000, 1000},
                                                      {10000, 11000, 1000},
                                                      {11000, 12000, 1000},
                                                      {12000, 13000, 1000},
                                                      {13000, 14000, 1000},
                                                      {14000, 15000, 1000},
                                                      {15000, 16000, 1000},
                                                      {16000, 17000, 1000}};
    std::vector<GlobalOutput> actualGlobalOutput =
        testHarness.runQuery(expectedGlobalOutput.size(), "BottomUp", "NONE", "IN_MEMORY").getOutput<GlobalOutput>();
    ASSERT_EQ(actualGlobalOutput.size(), expectedGlobalOutput.size());
    ASSERT_THAT(actualGlobalOutput, ::testing::UnorderedElementsAreArray(expectedGlobalOutput));
}

TEST_F(NonKeyedTumblingWindowTests, testTumblingWindowMin) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                     .apply(Min(Attribute("value")));
    auto dg = DataGenerator("window", "window1", 100);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(dg.getSource(), workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();
    std::vector<GlobalOutput> expectedGlobalOutput = {{0, 1000, 1},
                                                      {1000, 2000, 1},
                                                      {2000, 3000, 1},
                                                      {3000, 4000, 1},
                                                      {4000, 5000, 1},
                                                      {5000, 6000, 1},
                                                      {6000, 7000, 1},
                                                      {7000, 8000, 1},
                                                      {8000, 9000, 1},
                                                      {9000, 10000, 1},
                                                      {10000, 11000, 1},
                                                      {11000, 12000, 1},
                                                      {12000, 13000, 1},
                                                      {13000, 14000, 1},
                                                      {14000, 15000, 1},
                                                      {15000, 16000, 1},
                                                      {16000, 17000, 1}};
    std::vector<GlobalOutput> actualGlobalOutput =
        testHarness.runQuery(expectedGlobalOutput.size(), "BottomUp", "NONE", "IN_MEMORY").getOutput<GlobalOutput>();
    ASSERT_EQ(actualGlobalOutput.size(), expectedGlobalOutput.size());
    ASSERT_THAT(actualGlobalOutput, ::testing::UnorderedElementsAreArray(expectedGlobalOutput));
}

TEST_F(NonKeyedTumblingWindowTests, testTumblingWindowMax) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                     .apply(Max(Attribute("value")));
    auto dg = DataGenerator("window", "window1", 100);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(dg.getSource(), workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();
    std::vector<GlobalOutput> expectedGlobalOutput = {{0, 1000, 1},
                                                      {1000, 2000, 1},
                                                      {2000, 3000, 1},
                                                      {3000, 4000, 1},
                                                      {4000, 5000, 1},
                                                      {5000, 6000, 1},
                                                      {6000, 7000, 1},
                                                      {7000, 8000, 1},
                                                      {8000, 9000, 1},
                                                      {9000, 10000, 1},
                                                      {10000, 11000, 1},
                                                      {11000, 12000, 1},
                                                      {12000, 13000, 1},
                                                      {13000, 14000, 1},
                                                      {14000, 15000, 1},
                                                      {15000, 16000, 1},
                                                      {16000, 17000, 1}};
    std::vector<GlobalOutput> actualGlobalOutput =
        testHarness.runQuery(expectedGlobalOutput.size(), "BottomUp", "NONE", "IN_MEMORY").getOutput<GlobalOutput>();
    ASSERT_EQ(actualGlobalOutput.size(), expectedGlobalOutput.size());
    ASSERT_THAT(actualGlobalOutput, ::testing::UnorderedElementsAreArray(expectedGlobalOutput));
}

TEST_F(NonKeyedTumblingWindowTests, testTumblingWindowAVG) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                     .apply(Avg(Attribute("value")));
    auto dg = DataGenerator("window", "window1", 100);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(dg.getSource(), workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();
    std::vector<GlobalOutput> expectedGlobalOutput = {{0, 1000, 1},
                                                      {1000, 2000, 1},
                                                      {2000, 3000, 1},
                                                      {3000, 4000, 1},
                                                      {4000, 5000, 1},
                                                      {5000, 6000, 1},
                                                      {6000, 7000, 1},
                                                      {7000, 8000, 1},
                                                      {8000, 9000, 1},
                                                      {9000, 10000, 1},
                                                      {10000, 11000, 1},
                                                      {11000, 12000, 1},
                                                      {12000, 13000, 1},
                                                      {13000, 14000, 1},
                                                      {14000, 15000, 1},
                                                      {15000, 16000, 1},
                                                      {16000, 17000, 1}};
    std::vector<GlobalOutput> actualGlobalOutput =
        testHarness.runQuery(expectedGlobalOutput.size(), "BottomUp", "NONE", "IN_MEMORY").getOutput<GlobalOutput>();
    ASSERT_EQ(actualGlobalOutput.size(), expectedGlobalOutput.size());
    ASSERT_THAT(actualGlobalOutput, ::testing::UnorderedElementsAreArray(expectedGlobalOutput));
}

TEST_F(NonKeyedTumblingWindowTests, testTumblingWindowMultiAggregate) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                     .apply(Sum(Attribute("value"))->as(Attribute("sum_value")),
                            Count()->as(Attribute("count_value")),
                            Min(Attribute("value"))->as(Attribute("min_value")),
                            Max(Attribute("value"))->as(Attribute("max_value")),
                            Avg(Attribute("value"))->as(Attribute("avg_value")));
    auto dg = DataGenerator("window", "window1", 100);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(dg.getSource(), workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();
    std::vector<GlobalOutputMultiAgg> expectedGlobalOutput = {{0, 1000, 1000, 1000, 1, 1, 1},
                                                              {1000, 2000, 1000, 1000, 1, 1, 1},
                                                              {2000, 3000, 1000, 1000, 1, 1, 1},
                                                              {3000, 4000, 1000, 1000, 1, 1, 1},
                                                              {4000, 5000, 1000, 1000, 1, 1, 1},
                                                              {5000, 6000, 1000, 1000, 1, 1, 1},
                                                              {6000, 7000, 1000, 1000, 1, 1, 1},
                                                              {7000, 8000, 1000, 1000, 1, 1, 1},
                                                              {8000, 9000, 1000, 1000, 1, 1, 1},
                                                              {9000, 10000, 1000, 1000, 1, 1, 1},
                                                              {10000, 11000, 1000, 1000, 1, 1, 1},
                                                              {11000, 12000, 1000, 1000, 1, 1, 1},
                                                              {12000, 13000, 1000, 1000, 1, 1, 1},
                                                              {13000, 14000, 1000, 1000, 1, 1, 1},
                                                              {14000, 15000, 1000, 1000, 1, 1, 1},
                                                              {15000, 16000, 1000, 1000, 1, 1, 1},
                                                              {16000, 17000, 1000, 1000, 1, 1, 1}};
    std::vector<GlobalOutputMultiAgg> actualGlobalOutput =
        testHarness.runQuery(expectedGlobalOutput.size(), "BottomUp", "NONE", "IN_MEMORY").getOutput<GlobalOutputMultiAgg>();
    ASSERT_EQ(actualGlobalOutput.size(), expectedGlobalOutput.size());
    ASSERT_THAT(actualGlobalOutput, ::testing::UnorderedElementsAreArray(expectedGlobalOutput));
}

/**
 * @brief: This test replicates the angle-turning query from KTM in Elegant.
 * The average of 3 readings on top of a motorcycle are used to determine
 * how "dangerously" someone drives. The count is work-around meta-info about
 * the duration of the dangerous behavior.
 *
 * In this case, we emulate that by using multi-averages on top of one value
 * and the line count. We then calculate and compare them against each window.
 */
TEST_F(NonKeyedTumblingWindowTests, testTumblingWindowMultiAverageAndCount) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                     .apply(Avg(Attribute("value"))->as(Attribute("avg_value_3")),
                            Avg(Attribute("value"))->as(Attribute("avg_value_2")),
                            Avg(Attribute("value"))->as(Attribute("avg_value_1")),
                            Count()->as(Attribute("count_value")));
    auto dg = DataGenerator("window", "window1", this->numberOfGeneratedBuffers);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(dg.getSource(), workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();
    std::vector<GlobalOutputMultiAggSameKey> expectedGlobalOutput = {{0, 1000, 1, 1, 1, 1000},
                                                                     {1000, 2000, 1, 1, 1, 1000},
                                                                     {2000, 3000, 1, 1, 1, 1000},
                                                                     {3000, 4000, 1, 1, 1, 1000},
                                                                     {4000, 5000, 1, 1, 1, 1000},
                                                                     {5000, 6000, 1, 1, 1, 1000},
                                                                     {6000, 7000, 1, 1, 1, 1000},
                                                                     {7000, 8000, 1, 1, 1, 1000},
                                                                     {8000, 9000, 1, 1, 1, 1000},
                                                                     {9000, 10000, 1, 1, 1, 1000},
                                                                     {10000, 11000, 1, 1, 1, 1000},
                                                                     {11000, 12000, 1, 1, 1, 1000},
                                                                     {12000, 13000, 1, 1, 1, 1000},
                                                                     {13000, 14000, 1, 1, 1, 1000},
                                                                     {14000, 15000, 1, 1, 1, 1000},
                                                                     {15000, 16000, 1, 1, 1, 1000},
                                                                     {16000, 17000, 1, 1, 1, 1000}};
    std::vector<GlobalOutputMultiAggSameKey> actualGlobalOutput =
        testHarness.runQuery(expectedGlobalOutput.size(), "BottomUp", "NONE", "IN_MEMORY")
            .getOutput<GlobalOutputMultiAggSameKey>();
    EXPECT_EQ(actualGlobalOutput.size(), expectedGlobalOutput.size());
    EXPECT_THAT(actualGlobalOutput, ::testing::UnorderedElementsAreArray(expectedGlobalOutput));
}

/**
 * @brief: This test replicates the angle-turning query from KTM in Elegant.
 * The average of 3 readings on top of a motorcycle are used to determine
 * how "dangerously" someone drives. The count is work-around meta-info about
 * the duration of the dangerous behavior.
 *
 * In this case, we emulate that by using multiple values
 * and the line count. We then calculate and compare them against each window.
 */
TEST_F(NonKeyedTumblingWindowTests, testTumblingWindowMultiplePhysicalValuesAndCount) {
    auto testSchema = Schema::create()
                          ->addField("value3", DataTypeFactory::createUInt64())
                          ->addField("value2", DataTypeFactory::createUInt64())
                          ->addField("value1", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(MultipleInputValues), testSchema->getSchemaSizeInBytes());
    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                     .apply(Avg(Attribute("value1"))->as(Attribute("avg_value_1")),
                            Avg(Attribute("value2"))->as(Attribute("avg_value_2")),
                            Avg(Attribute("value3"))->as(Attribute("avg_value_3")),
                            Count()->as(Attribute("count_value")));
    std::vector<GlobalOutputMultiAggSameKey> expectedGlobalOutput = {{0, 1000, 1, 1, 1, 1000},
                                                                     {1000, 2000, 1, 1, 1, 1000},
                                                                     {2000, 3000, 1, 1, 1, 1000},
                                                                     {3000, 4000, 1, 1, 1, 1000},
                                                                     {4000, 5000, 1, 1, 1, 1000},
                                                                     {5000, 6000, 1, 1, 1, 1000},
                                                                     {6000, 7000, 1, 1, 1, 1000},
                                                                     {7000, 8000, 1, 1, 1, 1000},
                                                                     {8000, 9000, 1, 1, 1, 1000},
                                                                     {9000, 10000, 1, 1, 1, 1000},
                                                                     {10000, 11000, 1, 1, 1, 1000},
                                                                     {11000, 12000, 1, 1, 1, 1000},
                                                                     {12000, 13000, 1, 1, 1, 800}};
    auto dg = DataGeneratorMultiValue("window", "window1", this->numberOfGeneratedBuffers);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator(dg.getSource(), workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();
    std::vector<GlobalOutputMultiAggSameKey> actualGlobalOutput =
        testHarness.runQuery(expectedGlobalOutput.size(), "BottomUp", "NONE", "IN_MEMORY")
            .getOutput<GlobalOutputMultiAggSameKey>();
    ASSERT_EQ(actualGlobalOutput.size(), expectedGlobalOutput.size());
    ASSERT_THAT(actualGlobalOutput, ::testing::UnorderedElementsAreArray(expectedGlobalOutput));
}

}// namespace NES

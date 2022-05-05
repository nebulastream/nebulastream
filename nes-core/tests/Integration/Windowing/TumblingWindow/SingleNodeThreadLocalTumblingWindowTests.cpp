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
#include "../util/NesBaseTest.hpp"
#include <NesBaseTest.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#pragma clang diagnostic pop
#include <Catalogs/Source/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>

#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

/**
 * @brief In this test we assess the correctness of the thread local tumbling window
 */
class SingleNodeThreadLocalTumblingWindowTests : public Testing::NESBaseTest, public ::testing::WithParamInterface<int> {
  public:
    WorkerConfigurationPtr workerConfiguration;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SingleNodeThreadLocalTumblingWindowTests.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SingleNodeThreadLocalTumblingWindowTests test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        workerConfiguration = WorkerConfiguration::create();
        workerConfiguration->queryCompiler.windowingStrategy =
            QueryCompilation::QueryCompilerOptions::WindowingStrategy::THREAD_LOCAL;
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
    uint64_t id;
    uint64_t value;
    bool operator==(Output const& rhs) const {
        return (start == rhs.start && end == rhs.end && id == rhs.id && value == rhs.value);
    }
};

PhysicalSourceTypePtr createSimpleInputStream(uint64_t numberOfBuffers) {
    return LambdaSourceType::create(
        [](Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
            auto inputValue = (InputValue*) buffer.getBuffer();
            for (uint64_t i = 0; i < numberOfTuplesToProduce; i++) {
                inputValue[i].value = 1;
                inputValue[i].id = 1;
                inputValue[i].timestamp = 1;
            }
            buffer.setNumberOfTuples(numberOfTuplesToProduce);
        },
        numberOfBuffers,
        0,
        GatheringMode ::INTERVAL_MODE);
}
/*
TEST_F(SingleNodeThreadLocalTumblingWindowTests, testSingleTumblingWindowSingleBuffer) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    std::string query =
        R"(Query::from("window").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value"))))";
    // R"(Query::from("window"))";

    auto lambdaSource = createSimpleInputStream(1);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator("window", lambdaSource, workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    testHarness.validate().setupTopology();

    std::vector<Output> expectedOutput = {{0, 1000, 1, 170}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    ASSERT_EQ(actualOutput.size(), expectedOutput.size());
    ASSERT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(SingleNodeThreadLocalTumblingWindowTests, testSingleTumblingWindowMultiBuffer) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    std::string query =
        R"(Query::from("window").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value"))))";
    auto lambdaSource = createSimpleInputStream(100);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator("window", lambdaSource, workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();
    std::vector<Output> expectedOutput = {{0, 1000, 1, 17000}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");
    ASSERT_EQ(actualOutput.size(), expectedOutput.size());
    ASSERT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}
 */

TEST_P(SingleNodeThreadLocalTumblingWindowTests, testSingleTumblingWindowMultiBufferConcurrent) {
    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(InputValue), testSchema->getSchemaSizeInBytes());
    std::string query =
        R"(Query::from("window").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value"))))";
    workerConfiguration->numWorkerThreads = GetParam();
    ;
    auto lambdaSource = createSimpleInputStream(100);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithLambdaSourceToCoordinator("window", lambdaSource, workerConfiguration);

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.validate().setupTopology();
    std::vector<Output> expectedOutput = {{0, 1000, 1, 17000}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");
    ASSERT_EQ(actualOutput.size(), expectedOutput.size());
    ASSERT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

INSTANTIATE_TEST_CASE_P(testSingleTumblingWindowMultiBufferConcurrentTest,
                        SingleNodeThreadLocalTumblingWindowTests,
                        ::testing::Values(1, 2, 4),
                        [](const testing::TestParamInfo<SingleNodeThreadLocalTumblingWindowTests::ParamType>& info) {
                            std::string name = std::to_string(info.param) + "Worker";
                            return name;
                        });

}// namespace NES

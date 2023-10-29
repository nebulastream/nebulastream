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
#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Operators/LogicalOperators/Windows/Types/ThresholdWindow.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/TestSourceDescriptor.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <iostream>
#include <utility>

using namespace NES;
using Runtime::TupleBuffer;

const static uint64_t windowSize = 10;
const static uint64_t recordsPerBuffer = 100;

// Dump IR
constexpr auto dumpMode = NES::QueryCompilation::DumpMode::NONE;

class WindowAggregationFunctionTest : public Testing::BaseUnitTest,
                                      public ::testing::WithParamInterface<QueryCompilation::QueryCompilerType> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("WindowAggregationFunctionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("WindowAggregationFunctionTest: Setup WindowAggregationFunctionTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        auto queryCompiler = this->GetParam();
        executionEngine = std::make_shared<Testing::TestExecutionEngine>(queryCompiler, dumpMode);
        sourceSchema = Schema::create()->addField("test$ts", BasicType::UINT64)->addField("test$value", BasicType::INT64);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("WindowAggregationFunctionTest: Tear down WindowAggregationFunctionTest test case.");
        ASSERT_TRUE(executionEngine->stop());
        Testing::BaseUnitTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_DEBUG("WindowAggregationFunctionTest: Tear down WindowAggregationFunctionTest test class.");
    }

    SchemaPtr sourceSchema;
    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;

    void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf, uint64_t ts) {
        for (int64_t recordIndex = 0; recordIndex < (int64_t) recordsPerBuffer; recordIndex++) {
            buf[recordIndex][0].write<uint64_t>(ts);
            buf[recordIndex][1].write<int64_t>(recordIndex);
        }
        buf.setNumberOfTuples(recordsPerBuffer);
    }

    Runtime::Execution::ExecutableQueryPlanPtr executeQuery(Query query) {
        auto plan = executionEngine->submitQuery(query.getQueryPlan());
        auto source = executionEngine->getDataSource(plan, 0);
        // create data for five windows
        for (uint64_t ts = 1; ts < 30; ts = ts + windowSize) {
            auto inputBuffer = executionEngine->getBuffer(sourceSchema);
            fillBuffer(inputBuffer, ts);
            source->emitBuffer(inputBuffer);
        }
        return plan;
    }
};

TEST_P(WindowAggregationFunctionTest, testSumAggregation) {
    struct ResultRecord {
        uint64_t start_ts;
        uint64_t end_ts;
        uint64_t value;
    };
    auto sinkSchema = Schema::create()->addField("value", BasicType::INT64);
    auto collector = executionEngine->createCollectSink<ResultRecord>(sinkSchema);
    auto testSourceDescriptor = executionEngine->createDataSource(sourceSchema);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(collector);

    auto query = TestQuery::from(testSourceDescriptor)
                     .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(windowSize)))
                     .apply(Sum(Attribute("value", BasicType::INT64))->as(Attribute("value")))
                     .project(Attribute("start"), Attribute("end"), Attribute("value"))
                     .sink(testSinkDescriptor);

    auto plan = executeQuery(query);

    collector->waitTillCompleted(/*wait for two records*/ 2);
    auto& results = collector->getResult();
    EXPECT_EQ(results.size(), 2u);
    EXPECT_EQ(results[0].start_ts, 0UL);
    EXPECT_EQ(results[0].end_ts, 10UL);
    EXPECT_EQ(results[0].value, 4950UL);

    EXPECT_EQ(results[1].start_ts, 10UL);
    EXPECT_EQ(results[1].end_ts, 20UL);
    EXPECT_EQ(results[1].value, 4950UL);
}

TEST_P(WindowAggregationFunctionTest, testAvgAggregation) {
    struct ResultRecord {
        uint64_t start_ts;
        uint64_t end_ts;
        uint64_t value;
    };
    auto sinkSchema = Schema::create()->addField("value", BasicType::INT64);
    auto collector = executionEngine->createCollectSink<ResultRecord>(sinkSchema);
    auto testSourceDescriptor = executionEngine->createDataSource(sourceSchema);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(collector);

    auto query = TestQuery::from(testSourceDescriptor)
                     .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(windowSize)))
                     .apply(Avg(Attribute("value", BasicType::INT64))->as(Attribute("value")))
                     .project(Attribute("start"), Attribute("end"), Attribute("value"))
                     .sink(testSinkDescriptor);

    auto plan = executeQuery(query);

    collector->waitTillCompleted(/*wait for two records*/ 2);
    auto& results = collector->getResult();
    EXPECT_EQ(results.size(), 2u);
    EXPECT_EQ(results[0].start_ts, 0UL);
    EXPECT_EQ(results[0].end_ts, 10UL);
    EXPECT_EQ(results[0].value, 49UL);

    EXPECT_EQ(results[1].start_ts, 10UL);
    EXPECT_EQ(results[1].end_ts, 20UL);
    EXPECT_EQ(results[1].value, 49UL);
}

TEST_P(WindowAggregationFunctionTest, testMinAggregation) {
    struct ResultRecord {
        uint64_t start_ts;
        uint64_t end_ts;
        uint64_t value;
    };
    auto sinkSchema = Schema::create()->addField("value", BasicType::INT64);
    auto collector = executionEngine->createCollectSink<ResultRecord>(sinkSchema);
    auto testSourceDescriptor = executionEngine->createDataSource(sourceSchema);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(collector);

    auto query = TestQuery::from(testSourceDescriptor)
                     .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(windowSize)))
                     .apply(Min(Attribute("value", BasicType::INT64))->as(Attribute("value")))
                     .project(Attribute("start"), Attribute("end"), Attribute("value"))
                     .sink(testSinkDescriptor);

    auto plan = executeQuery(query);

    collector->waitTillCompleted(/*wait for two records*/ 2);
    auto& results = collector->getResult();
    EXPECT_EQ(results.size(), 2u);
    EXPECT_EQ(results[0].start_ts, 0UL);
    EXPECT_EQ(results[0].end_ts, 10UL);
    EXPECT_EQ(results[0].value, 0UL);

    EXPECT_EQ(results[1].start_ts, 10UL);
    EXPECT_EQ(results[1].end_ts, 20UL);
    EXPECT_EQ(results[1].value, 0UL);
}

TEST_P(WindowAggregationFunctionTest, testMaxAggregation) {
    struct ResultRecord {
        uint64_t start_ts;
        uint64_t end_ts;
        uint64_t value;
    };
    auto sinkSchema = Schema::create()->addField("value", BasicType::INT64);
    auto collector = executionEngine->createCollectSink<ResultRecord>(sinkSchema);
    auto testSourceDescriptor = executionEngine->createDataSource(sourceSchema);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(collector);

    auto query = TestQuery::from(testSourceDescriptor)
                     .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(windowSize)))
                     .apply(Max(Attribute("value", BasicType::INT64))->as(Attribute("value")))
                     .project(Attribute("start"), Attribute("end"), Attribute("value"))
                     .sink(testSinkDescriptor);

    auto plan = executeQuery(query);

    collector->waitTillCompleted(/*wait for two records*/ 2);
    auto& results = collector->getResult();
    EXPECT_EQ(results.size(), 2u);
    EXPECT_EQ(results[0].start_ts, 0UL);
    EXPECT_EQ(results[0].end_ts, 10UL);
    EXPECT_EQ(results[0].value, 99UL);

    EXPECT_EQ(results[1].start_ts, 10UL);
    EXPECT_EQ(results[1].end_ts, 20UL);
    EXPECT_EQ(results[1].value, 99UL);
}

TEST_P(WindowAggregationFunctionTest, testMultiAggregationFunctions) {
    struct ResultRecord {
        uint64_t start_ts;
        uint64_t end_ts;
        uint64_t sum;
        uint64_t min;
        uint64_t max;
        uint64_t avg;
    };
    auto sinkSchema = Schema::create()
                          ->addField("start", BasicType::INT64)
                          ->addField("end", BasicType::INT64)
                          ->addField("sum", BasicType::INT64)
                          ->addField("test$count", BasicType::UINT64);
    auto collector = executionEngine->createCollectSink<ResultRecord>(sinkSchema);
    auto testSourceDescriptor = executionEngine->createDataSource(sourceSchema);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(collector);

    auto query =
        TestQuery::from(testSourceDescriptor)
            .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(windowSize)))
            .apply(Sum(Attribute("value", BasicType::INT64))->as(Attribute("sum")),
                   Min(Attribute("value", BasicType::INT64))->as(Attribute("min")),
                   Max(Attribute("value", BasicType::INT64))->as(Attribute("max")),
                   Avg(Attribute("value", BasicType::INT64))->as(Attribute("avg")))
            .project(Attribute("start"), Attribute("end"), Attribute("sum"), Attribute("min"), Attribute("max"), Attribute("avg"))
            .sink(testSinkDescriptor);

    auto plan = executeQuery(query);

    collector->waitTillCompleted(/*wait for two records*/ 2);
    auto& results = collector->getResult();
    EXPECT_EQ(results.size(), 2u);
    EXPECT_EQ(results[0].start_ts, 0UL);
    EXPECT_EQ(results[0].end_ts, 10UL);
    EXPECT_EQ(results[0].sum, 4950UL);
    EXPECT_EQ(results[0].min, 0UL);
    EXPECT_EQ(results[0].max, 99UL);
    EXPECT_EQ(results[0].avg, 49UL);
}

INSTANTIATE_TEST_CASE_P(testGlobalTumblingWindow,
                        WindowAggregationFunctionTest,
                        ::testing::Values(QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER),
                        [](const testing::TestParamInfo<WindowAggregationFunctionTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(info.param));
                        });
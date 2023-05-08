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
// clang-format: off
// clang-format: on
#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/TestSourceDescriptor.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <iostream>
#include <utility>

using namespace NES;
using Runtime::TupleBuffer;

// Dump IR
constexpr auto dumpMode = NES::QueryCompilation::QueryCompilerOptions::DumpMode::NONE;

class MapQueryExecutionTest : public Testing::TestWithErrorHandling,
                              public ::testing::WithParamInterface<QueryCompilation::QueryCompilerOptions::QueryCompiler> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MapQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("QueryExecutionTest: Setup QueryCatalogServiceTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling::SetUp();
        auto queryCompiler = this->GetParam();
        executionEngine = std::make_shared<Testing::TestExecutionEngine>(queryCompiler, dumpMode);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("QueryExecutionTest: Tear down MapQueryExecutionTest test case.");
        ASSERT_TRUE(executionEngine->stop());
        Testing::TestWithErrorHandling::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("MapQueryExecutionTest: Tear down QueryExecutionTest test class."); }

    void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf) {
        for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
            buf[recordIndex][0].write<int64_t>(recordIndex);
            buf[recordIndex][1].write<int64_t>(1);
        }
        buf.setNumberOfTuples(10);
    }

    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
};

TEST_P(MapQueryExecutionTest, MapQueryArithmetic) {
    auto schema = Schema::create()->addField("test$id", BasicType::INT64)->addField("test$one", BasicType::INT64);
    auto testSink = executionEngine->createDataSink(schema);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor).map(Attribute("id") = Attribute("id") * 2).sink(testSinkDescriptor);
    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    auto source = executionEngine->getDataSource(plan, 0);
    ASSERT_TRUE(!!source);
    auto inputBuffer = executionEngine->getBuffer(schema);
    fillBuffer(inputBuffer);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10u);
    for (uint32_t recordIndex = 0u; recordIndex < 10u; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex][0].read<int64_t>(), recordIndex * 2);
        EXPECT_EQ(resultBuffer[recordIndex][1].read<int64_t>(), 1LL);
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_P(MapQueryExecutionTest, MapLogarithmicFunctions) {
    auto schema = Schema::create()->addField("test$id", BasicType::FLOAT64);

    auto resultSchema = Schema::create()
                            ->addField("test$id", BasicType::FLOAT64)
                            ->addField("test$log10", BasicType::FLOAT64)
                            ->addField("test$log2", BasicType::FLOAT64)
                            ->addField("test$ln", BasicType::FLOAT64);
    auto testSink = executionEngine->createDataSink(resultSchema);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .map(Attribute("log10") = LOG10(Attribute("id")))
                     .map(Attribute("log2") = LOG2(Attribute("id")))
                     .map(Attribute("ln") = LN(Attribute("id")))
                     .sink(testSinkDescriptor);
    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    auto source = executionEngine->getDataSource(plan, 0);
    ASSERT_TRUE(!!source);
    // add buffer
    auto inputBuffer = executionEngine->getBuffer(schema);
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        inputBuffer[recordIndex][0].write<double>(recordIndex);
    }
    inputBuffer.setNumberOfTuples(10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    // compare results
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10u);
    for (uint32_t recordIndex = 0u; recordIndex < 10u; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex]["test$log10"].read<double>(), std::log10(recordIndex));
        EXPECT_EQ(resultBuffer[recordIndex]["test$log2"].read<double>(), std::log2(recordIndex));
        EXPECT_EQ(resultBuffer[recordIndex]["test$ln"].read<double>(), std::log(recordIndex));
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_P(MapQueryExecutionTest, TwoMapQuery) {
    auto schema = Schema::create()->addField("test$id", BasicType::INT64);

    auto resultSchema = Schema::create()
                            ->addField("test$id", BasicType::INT64)
                            ->addField("test$new1", BasicType::INT64)
                            ->addField("test$new2", BasicType::INT64);
    auto testSink = executionEngine->createDataSink(resultSchema);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .map(Attribute("test$new1") = Attribute("test$id") * 2)
                     .map(Attribute("test$new2") = Attribute("test$id") + 2)
                     .sink(testSinkDescriptor);
    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    auto source = executionEngine->getDataSource(plan, 0);
    ASSERT_TRUE(!!source);
    // add buffer
    auto inputBuffer = executionEngine->getBuffer(schema);
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        inputBuffer[recordIndex][0].write<int64_t>(recordIndex);
    }
    inputBuffer.setNumberOfTuples(10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    // compare results
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10u);
    for (uint32_t recordIndex = 0u; recordIndex < 10u; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex]["test$new1"].read<int64_t>(), recordIndex * 2);
        EXPECT_EQ(resultBuffer[recordIndex]["test$new2"].read<int64_t>(), recordIndex + 2);
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_P(MapQueryExecutionTest, MapAbsFunction) {
    auto schema = Schema::create()->addField("test$id", BasicType::FLOAT64);

    auto resultSchema = Schema::create()
                            ->addField("test$id", BasicType::FLOAT64)
                            ->addField("test$abs", BasicType::FLOAT64);
    auto testSink = executionEngine->createDataSink(resultSchema);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .map(Attribute("abs") = ABS(Attribute("id")))
                     .sink(testSinkDescriptor);
    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    auto source = executionEngine->getDataSource(plan, 0);
    ASSERT_TRUE(!!source);
    // add buffer
    auto inputBuffer = executionEngine->getBuffer(schema);
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        inputBuffer[recordIndex]["test$id"].write<double>((double)-recordIndex);
    }
    inputBuffer.setNumberOfTuples(10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    // compare results
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10u);
    for (uint32_t recordIndex = 0u; recordIndex < 10u; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex]["test$abs"].read<double>(), std::fabs(recordIndex));
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_P(MapQueryExecutionTest, MapPowerFunction) {
    auto schema = Schema::create()
                      ->addField("test$left$id", BasicType::FLOAT64)
                      ->addField("test$right$id", BasicType::FLOAT64);

    auto resultSchema = Schema::create()
                            ->addField("test$left$id", BasicType::FLOAT64)
                            ->addField("test$right$id", BasicType::FLOAT64)
                            ->addField("test$power", BasicType::FLOAT64);
    auto testSink = executionEngine->createDataSink(resultSchema);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .map(Attribute("power") = POWER(Attribute("left$id"), Attribute("right$id")))
                     .sink(testSinkDescriptor);
    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    auto source = executionEngine->getDataSource(plan, 0);
    ASSERT_TRUE(!!source);
    // add buffer
    auto inputBuffer = executionEngine->getBuffer(schema);
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        inputBuffer[recordIndex]["test$left$id"].write<double>((double)2 * recordIndex);
        inputBuffer[recordIndex]["test$right$id"].write<double>((double)recordIndex);
    }
    inputBuffer.setNumberOfTuples(10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    // compare results
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10u);
    for (uint32_t recordIndex = 0u; recordIndex < 10u; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex]["test$power"].read<double>(), std::pow(2 * recordIndex, recordIndex));
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_P(MapQueryExecutionTest, MapTrigonometricFunctions) {
    auto schema = Schema::create()->addField("test$id", BasicType::FLOAT64);

    auto resultSchema = Schema::create()
                            ->addField("test$id", BasicType::FLOAT64)
                            ->addField("test$sin", BasicType::FLOAT64)
                            ->addField("test$cos", BasicType::FLOAT64)
                            ->addField("test$radians", BasicType::FLOAT64);
    auto testSink = executionEngine->createDataSink(resultSchema);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .map(Attribute("sin") = SIN(Attribute("id")))
                     .map(Attribute("cos") = COS(Attribute("id")))
                     .map(Attribute("radians") = RADIANS(Attribute("id")))
                     .sink(testSinkDescriptor);
    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    auto source = executionEngine->getDataSource(plan, 0);
    ASSERT_TRUE(!!source);
    // add buffer
    auto inputBuffer = executionEngine->getBuffer(schema);
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        inputBuffer[recordIndex][0].write<double>(recordIndex);
    }
    inputBuffer.setNumberOfTuples(10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    // compare results
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10u);
    for (uint32_t recordIndex = 0u; recordIndex < 10u; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex]["test$sin"].read<double>(), std::sin(recordIndex));
        EXPECT_EQ(resultBuffer[recordIndex]["test$cos"].read<double>(), std::cos(recordIndex));
        EXPECT_EQ(resultBuffer[recordIndex]["test$radians"].read<double>(), (recordIndex * M_PI) / 180);
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

INSTANTIATE_TEST_CASE_P(testMapQueries,
                        MapQueryExecutionTest,
                        ::testing::Values(QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER),
                        [](const testing::TestParamInfo<MapQueryExecutionTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(info.param));
                        });

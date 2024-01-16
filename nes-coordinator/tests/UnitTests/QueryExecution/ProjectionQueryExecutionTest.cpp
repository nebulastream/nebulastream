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
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
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
constexpr auto dumpMode = NES::QueryCompilation::DumpMode::NONE;

class ProjectionQueryExecutionTest : public Testing::BaseUnitTest,
                                     public ::testing::WithParamInterface<QueryCompilation::QueryCompilerType> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ProjectionQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("QueryCatalogServiceTest: Setup QueryCatalogServiceTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        executionEngine = std::make_shared<Testing::TestExecutionEngine>(dumpMode);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("QueryCatalogServiceTest: Tear down QueryCatalogServiceTest test case.");
        ASSERT_TRUE(executionEngine->stop());
        Testing::BaseUnitTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("QueryCatalogServiceTest: Tear down QueryCatalogServiceTest test class."); }

    void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf) {
        int numberOfTuples = 10;
        for (int recordIndex = 0; recordIndex < numberOfTuples; recordIndex++) {
            buf[recordIndex][0].write<int64_t>(recordIndex);
            buf[recordIndex][1].write<int64_t>(1);
            buf[recordIndex][2].write<int64_t>(42);
        }
        buf.setNumberOfTuples(numberOfTuples);
    }

    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
    static constexpr uint64_t defaultDecomposedQueryPlanId = 0;
    static constexpr uint64_t defaultSharedQueryId = 0;
};

TEST_F(ProjectionQueryExecutionTest, projectField) {
    const auto expectedNumberOfTuples = 10;
    auto schema = Schema::create()
                      ->addField("test$id", BasicType::INT64)
                      ->addField("test$one", BasicType::INT64)
                      ->addField("test$value", BasicType::INT64);
    auto outputSchema = Schema::create()->addField("id", BasicType::INT64);
    auto testSink = executionEngine->createDataSink(outputSchema, expectedNumberOfTuples);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor).project(Attribute("id")).sink(testSinkDescriptor);
    auto decomposedQueryPlan =
        DecomposedQueryPlan::create(defaultDecomposedQueryPlanId, defaultSharedQueryId, query.getQueryPlan()->getRootOperators());
    auto plan = executionEngine->submitQuery(decomposedQueryPlan);
    auto source = executionEngine->getDataSource(plan, 0);
    auto inputBuffer = executionEngine->getBuffer(schema);
    fillBuffer(inputBuffer);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), expectedNumberOfTuples);
    for (uint32_t recordIndex = 0u; recordIndex < resultBuffer.getNumberOfTuples(); ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex][0].read<int64_t>(), recordIndex);
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_F(ProjectionQueryExecutionTest, projectTwoFields) {
    const auto expectedNumberOfTuples = 10;
    auto schema = Schema::create()
                      ->addField("test$id", BasicType::INT64)
                      ->addField("test$one", BasicType::INT64)
                      ->addField("test$value", BasicType::INT64);
    auto outputSchema = Schema::create()->addField("id", BasicType::INT64)->addField("value", BasicType::INT64);
    auto testSink = executionEngine->createDataSink(outputSchema, expectedNumberOfTuples);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor).project(Attribute("id"), Attribute("value")).sink(testSinkDescriptor);
    auto decomposedQueryPlan =
        DecomposedQueryPlan::create(defaultDecomposedQueryPlanId, defaultSharedQueryId, query.getQueryPlan()->getRootOperators());
    auto plan = executionEngine->submitQuery(decomposedQueryPlan);
    auto source = executionEngine->getDataSource(plan, 0);
    auto inputBuffer = executionEngine->getBuffer(schema);
    fillBuffer(inputBuffer);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), expectedNumberOfTuples);
    for (uint32_t recordIndex = 0u; recordIndex < expectedNumberOfTuples; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex][0].read<int64_t>(), recordIndex);
        EXPECT_EQ(resultBuffer[recordIndex][1].read<int64_t>(), 42);
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_F(ProjectionQueryExecutionTest, projectNonExistingFields) {
    auto schema = Schema::create()
                      ->addField("test$id", BasicType::INT64)
                      ->addField("test$one", BasicType::INT64)
                      ->addField("test$value", BasicType::INT64);
    auto outputSchema = Schema::create()->addField("id", BasicType::INT64)->addField("value", BasicType::INT64);
    auto testSink = executionEngine->createDataSink(outputSchema);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor).project(Attribute("x")).sink(testSinkDescriptor);
    auto decomposedQueryPlan =
        DecomposedQueryPlan::create(defaultDecomposedQueryPlanId, defaultSharedQueryId, query.getQueryPlan()->getRootOperators());
    ASSERT_ANY_THROW(executionEngine->submitQuery(decomposedQueryPlan));
}

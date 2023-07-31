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
#include "Operators/LogicalOperators/Sources/SourceDescriptor.hpp"
#include "Plans/Query/QueryPlan.hpp"
#include "Runtime/MemoryLayout/DynamicTupleBuffer.hpp"
#include "Runtime/MemoryLayout/RowLayout.hpp"
#include "Util/TestQuery.hpp"
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

//Todo: rename test class
// Dump IR
constexpr auto dumpMode = NES::QueryCompilation::QueryCompilerOptions::DumpMode::NONE;

class UnionQueryExecutionTest : public Testing::TestWithErrorHandling,
                                 public ::testing::WithParamInterface<QueryCompilation::QueryCompilerOptions::QueryCompiler> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("FilterQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("FilterQueryExecutionTest: Setup FilterQueryExecutionTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling::SetUp();
        auto queryCompiler = this->GetParam();
        executionEngine = std::make_shared<Testing::TestExecutionEngine>(queryCompiler, dumpMode);

        schema = Schema::create()
                ->addField("test$id", BasicType::INT64)
                ->addField("test$one", BasicType::INT64)
                ->addField("test$timestamp", BasicType::UINT64);
        testSink = executionEngine->createDataSink(schema, 2);
        testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("FilterQueryExecutionTest: Tear down FilterQueryExecutionTest test case.");
        ASSERT_TRUE(executionEngine->stop());
        Testing::TestWithErrorHandling::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("FilterQueryExecutionTest: Tear down FilterQueryExecutionTest test class."); }

    void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf) {
        for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
            buf[recordIndex][0].write<int64_t>(recordIndex);
            buf[recordIndex][1].write<int64_t>(1);
            buf[recordIndex][2].write<uint64_t>(1000 + 1000 * recordIndex);
        }
        buf.setNumberOfTuples(10);
    }

    Runtime::MemoryLayouts::DynamicTupleBuffer 
    runQuery(std::shared_ptr<Runtime::Execution::ExecutableQueryPlan> queryPlan) {
        // Setup first source and emit buffer
        auto source1 = executionEngine->getDataSource(queryPlan, 0);
        auto inputBuffer1 = executionEngine->getBuffer(schema);
        fillBuffer(inputBuffer1);
        source1->emitBuffer(inputBuffer1);

        // Setup second source and emit buffer
        // auto source2 = executionEngine->getDataSource(queryPlan, 1);
        // auto inputBuffer2 = executionEngine->getBuffer(schema);
        // fillBuffer(inputBuffer2);
        // source2->emitBuffer(inputBuffer2);

        // Wait until the sink processed all tuples
        //Todo: #4043 this blocks indefinitely if no tuples pass the filter
        testSink->waitTillCompleted();
        auto numResultBuffers = 2U;
        EXPECT_EQ(testSink->getNumberOfResultBuffers(), numResultBuffers);

        // Merge the result buffers, create a dynamic result buffer and check whether it contains the unified tuples.
        auto mergedBuffer = 
            TestUtils::mergeBuffers(testSink->resultBuffers, schema, executionEngine->getBufferManager());
        return Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(mergedBuffer, schema);
    }

    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
    SchemaPtr schema;
    std::shared_ptr<TestSink> testSink;
    std::shared_ptr<TestUtils::TestSinkDescriptor> testSinkDescriptor;
};

// TEST_P(UnionQueryExecutionTest, unionOperatorWithFilterOnUnionResult) {
//     auto testSourceDescriptor1 = executionEngine->createDataSource(schema);
//     auto testSourceDescriptor2 = executionEngine->createDataSource(schema);

//     Query query = TestQuery::from(testSourceDescriptor1)
//                       .unionWith(TestQuery::from(testSourceDescriptor2))
//                       .filter(Attribute("test$id") > 5)
//                       .sink(testSinkDescriptor);

//     auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    
//     auto resultBuffer = runQuery(queryPlan);
    
//     EXPECT_EQ(resultBuffer.getNumberOfTuples(), 8);
//     for (uint32_t recordIndex = 0u; recordIndex < 8u; ++recordIndex) {
//         NES_DEBUG("Result: {} at Index: {}", resultBuffer[recordIndex][0].read<int64_t>(), recordIndex);
//         EXPECT_EQ(resultBuffer[recordIndex][0].read<int64_t>(), (recordIndex % 4) + 6);
//         EXPECT_EQ(resultBuffer[recordIndex][1].read<int64_t>(), 1);
//     }

//     ASSERT_TRUE(executionEngine->stopQuery(queryPlan));
// }

// TEST_P(UnionQueryExecutionTest, unionOperatorWithFilterOnSources) {
//     auto testSourceDescriptor1 = executionEngine->createDataSource(schema);
//     auto testSourceDescriptor2 = executionEngine->createDataSource(schema);

//     Query subQuery = TestQuery::from(testSourceDescriptor2)
//                         .filter(Attribute("test$id") > 3);
//     Query query = TestQuery::from(testSourceDescriptor1)
//                       .filter(Attribute("test$id") > 7)
//                       .unionWith(subQuery)
//                       .sink(testSinkDescriptor);
//     auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());

//     auto resultBuffer = runQuery(queryPlan);
    
//     EXPECT_EQ(resultBuffer.getNumberOfTuples(), 8);
//     for (uint32_t recordIndex = 0u; recordIndex < 2u; ++recordIndex) {
//         EXPECT_EQ(resultBuffer[recordIndex][0].read<int64_t>(), recordIndex + 8);
//         EXPECT_EQ(resultBuffer[recordIndex][1].read<int64_t>(), 1);
//     }
//     for (uint32_t recordIndex = 2u; recordIndex < 8u; ++recordIndex) {
//         EXPECT_EQ(resultBuffer[recordIndex][0].read<int64_t>(), recordIndex + 2);
//         EXPECT_EQ(resultBuffer[recordIndex][1].read<int64_t>(), 1);
//     }

//     ASSERT_TRUE(executionEngine->stopQuery(queryPlan));
// }

/*
Query::from("windTurbines")
.unionWith(Query::from("solarPanels"))
    .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Hours(1)))
    .byKey(Attribute("producerId"))
    .apply(Sum(Attribute("producedPower")))
    .joinWith(Query::from("consumers")
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Hours(1))).byKey(Attribute("producerId"))
        .apply(Sum(Attribute("consumedPower")))
        )
    .where(Attribute("producerId"))
    .equalsTo(Attribute("producerId"))
    .window(TumblingWindow::of(EventTime(Attribute("start")), Hours(1)))
.map(Attribute("DifferenceProducedConsumedPower") = Attribute("producedPower") - Attribute("consumedPower"))
.project(Attribute("DifferenceProducedConsumedPower"), Attribute("producedPower"), Attribute("consumedPower"), Attribute("start").as("timestamp"))
.sink(MQTTSinkDescriptor::create("ws://172.31.0.11:9001","energyConsumption", "user", 100, MQTTSinkDescriptor::TimeUnits::milliseconds,0, MQTTSinkDescriptor::ServiceQualities::atLeastOnce, false, "frontend"));

*/
TEST_P(UnionQueryExecutionTest, demoQueryWithUnions) {
    auto testSourceDescriptor1 = executionEngine->createDataSource(schema);
    auto testSourceDescriptor2 = executionEngine->createDataSource(schema);

    // Query subQuery = TestQuery::from(testSourceDescriptor2)
    //                     .filter(Attribute("test$id") > 3);
    // Query query = TestQuery::from(testSourceDescriptor1)
    //                   .filter(Attribute("test$id") > 7)
    //                   .unionWith(subQuery)
    //                   .sink(testSinkDescriptor);

    // Test
    auto query = 
        TestQuery::from(testSourceDescriptor1)
            // .unionWith(TestQuery::from(testSourceDescriptor2))
                .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(5000UL)))
                .byKey(Attribute("test$id"))
                .apply(Sum(Attribute("test$one")))
                .sink(testSinkDescriptor);
                      
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());

    auto resultBuffer = runQuery(queryPlan);
    
    //Todo: Adapt output schema!
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 9);
    for (uint32_t recordIndex = 0u; recordIndex < 9u; ++recordIndex) {
        NES_DEBUG("Result: {}, at Index: {}", resultBuffer[recordIndex][0].read<int64_t>(), recordIndex);
        // EXPECT_EQ(resultBuffer[recordIndex][0].read<int64_t>(), recordIndex + 8);
        // EXPECT_EQ(resultBuffer[recordIndex][1].read<int64_t>(), 1);
    }

    ASSERT_TRUE(executionEngine->stopQuery(queryPlan));
}

TEST_P(UnionQueryExecutionTest, DISABLED_unionOperatorWithoutExecution) {
    auto testSourceDescriptor1 = executionEngine->createDataSource(schema);
    auto testSourceDescriptor2 = executionEngine->createDataSource(schema);

    Query query = TestQuery::from(testSourceDescriptor1)
                      .unionWith(TestQuery::from(testSourceDescriptor2))
                      .sink(testSinkDescriptor);

    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    
    auto resultBuffer = runQuery(queryPlan);

    for (uint32_t recordIndex = 0u; recordIndex < 20u; ++recordIndex) {
        NES_DEBUG("Result: {} at Index: {}", resultBuffer[recordIndex][0].read<int64_t>(), recordIndex);
        EXPECT_EQ(resultBuffer[recordIndex][0].read<int64_t>(), recordIndex % 10);
        EXPECT_EQ(resultBuffer[recordIndex][1].read<int64_t>(), 1);
    }

    ASSERT_TRUE(executionEngine->stopQuery(queryPlan));
}

//Todo: #4043: Currently, we cannot use the TestSink to check whether a query does not produce any results
// -> could call writeData with a nullptr to signalize that no TupleBuffer was produced
TEST_P(UnionQueryExecutionTest, DISABLED_unionOperatorWithoutResults) {
    auto testSourceDescriptor1 = executionEngine->createDataSource(schema);
    auto testSourceDescriptor2 = executionEngine->createDataSource(schema);

    Query query = TestQuery::from(testSourceDescriptor1)
                      .unionWith(TestQuery::from(testSourceDescriptor2))
                      .filter(Attribute("test$id") > 9)
                      .sink(testSinkDescriptor);

    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    
    auto resultBuffer = runQuery(queryPlan);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 0u);

    ASSERT_TRUE(executionEngine->stopQuery(queryPlan));
}

INSTANTIATE_TEST_CASE_P(testFilterQueries,
                        UnionQueryExecutionTest,
                        ::testing::Values(
                                        //   QueryCompilation::QueryCompilerOptions::QueryCompiler::DEFAULT_QUERY_COMPILER,
                                          QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER),
                        [](const testing::TestParamInfo<UnionQueryExecutionTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(info.param));
                        });

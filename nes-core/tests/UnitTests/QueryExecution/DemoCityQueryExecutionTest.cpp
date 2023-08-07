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
#include <cstdint>
#include <iostream>
#include <utility>

using namespace NES;
using Runtime::TupleBuffer;

//Todo: rename test class
// Dump IR
constexpr auto dumpMode = NES::QueryCompilation::QueryCompilerOptions::DumpMode::NONE;

class DemoCityQueryExecutionTest : public Testing::TestWithErrorHandling,
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
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("FilterQueryExecutionTest: Tear down FilterQueryExecutionTest test case.");
        ASSERT_TRUE(executionEngine->stop());
        Testing::TestWithErrorHandling::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("FilterQueryExecutionTest: Tear down FilterQueryExecutionTest test class."); }

    void createSchemaAndSink(const uint64_t numResultBuffers) {
        schema = Schema::create()
                ->addField("test$producerId", BasicType::INT64)
                ->addField("test$producedPower", BasicType::INT64)
                // ->addField("test$consumedPower", BasicType::INT64)
                ->addField("test$timestamp", BasicType::UINT64);      
        // schema = createSchema("test", {("producerId", BasicType::INT64), ("producedPower", BasicType::INT64)});
        resultSchema = Schema::create()
                ->addField("test$start", BasicType::UINT64)
                ->addField("test$end", BasicType::UINT64)
                ->addField("test$key", BasicType::INT64)
                ->addField("test$leftId", BasicType::INT64);
                // ->addField("test$leftValue", BasicType::INT64)
                // ->addField("test$timestamp", BasicType::UINT64)
                // ->addField("test$rightValue", BasicType::INT64)
                // ->addField("test$rightValue", BasicType::INT64)
                // ->addField("test$timestamp", BasicType::UINT64);
                // ->addField("test$value2", BasicType::INT64);
        expectedResultBuffers = numResultBuffers;
        testSink = executionEngine->createDataSink(schema, expectedResultBuffers);
        testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    }

    void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf, const uint64_t numInputTuples) {
        for (uint64_t recordIndex = 0; recordIndex < numInputTuples; recordIndex++) {
            buf[recordIndex][0].write<int64_t>(1);                      //id
            buf[recordIndex][1].write<int64_t>(recordIndex * 2);        //producedPower
            // buf[recordIndex][1].write<int64_t>(recordIndex * 2);        //producedPower
            // buf[recordIndex][2].write<int64_t>(recordIndex);            //consumedPower
            buf[recordIndex][2].write<uint64_t>(3600000 * recordIndex); //timestamps
        }
        buf.setNumberOfTuples(numInputTuples);
    }

    void fillBufferConsumer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf, const uint64_t numInputTuples) {
        for (uint64_t recordIndex = 0; recordIndex < numInputTuples; recordIndex++) {
            buf[recordIndex][0].write<int64_t>(1);                      //id
            buf[recordIndex][1].write<int64_t>(recordIndex);        //consumedPower
            buf[recordIndex][2].write<uint64_t>(3600000 * recordIndex); //timestamps
        }
        buf.setNumberOfTuples(numInputTuples);
    }

    Runtime::MemoryLayouts::DynamicTupleBuffer 
    runQuery(std::shared_ptr<Runtime::Execution::ExecutableQueryPlan> queryPlan, const uint64_t numInputTuples = 10, 
             uint64_t timeoutInMilliseconds = 500) {
        // Setup first source and emit buffer
        auto source1 = executionEngine->getDataSource(queryPlan, 0);
        auto inputBuffer1 = executionEngine->getBuffer(schema);
        fillBuffer(inputBuffer1, numInputTuples);
        source1->emitBuffer(inputBuffer1);

        // Setup first source and emit buffer
        auto source2 = executionEngine->getDataSource(queryPlan, 1);
        auto inputBuffer2 = executionEngine->getBuffer(schema);
        fillBuffer(inputBuffer2, numInputTuples);
        source1->emitBuffer(inputBuffer2);

        // Setup second source and emit buffer
        // auto source3 = executionEngine->getDataSource(queryPlan, 2);
        // if(!schema2) {
        //     auto inputBuffer3 = executionEngine->getBuffer(schema); 
        //     fillBufferConsumer(inputBuffer3, numInputTuples);
        //     source2->emitBuffer(inputBuffer3);
        // } else {
        //     auto inputBuffer3 = executionEngine->getBuffer(schema2); 
        //     fillBufferConsumer(inputBuffer3, numInputTuples);
        //     source2->emitBuffer(inputBuffer3);
        // }

        // Wait until the sink processed all tuples
        testSink->waitTillCompletedOrTimeout(timeoutInMilliseconds);
        EXPECT_EQ(testSink->getNumberOfResultBuffers(), expectedResultBuffers);

        // Merge the result buffers, create a dynamic result buffer and check whether it contains the unified tuples.
        auto mergedBuffer = 
            TestUtils::mergeBuffers(testSink->resultBuffers, resultSchema, executionEngine->getBufferManager());
        return Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(mergedBuffer, resultSchema);
    }

    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
    SchemaPtr schema;
    SchemaPtr resultSchema;
    std::shared_ptr<TestSink> testSink;
    uint64_t expectedResultBuffers;
    std::shared_ptr<TestUtils::TestSinkDescriptor> testSinkDescriptor;
};

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
TEST_P(DemoCityQueryExecutionTest, demoQueryWithUnions) {
    // Setup test parameters.
    constexpr uint64_t numResultBuffers = 2;
    constexpr uint64_t numInputTuples = 13;
    constexpr uint64_t numResultTuples = 2;
    constexpr uint64_t timeoutInMilliseconds = 500;

    createSchemaAndSink(numResultBuffers);
    auto windTurbines = executionEngine->createDataSource(schema);
    auto solarPanels = executionEngine->createDataSource(schema);

    auto query = 
        TestQuery::from(windTurbines)
        .unionWith(
            TestQuery::from(solarPanels)
        )
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Hours(1)))
            .byKey(Attribute("producerId"))
            .apply(Sum(Attribute("producedPower")))
        .sink(testSinkDescriptor);
                      
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());

    auto resultBuffer = runQuery(queryPlan, numInputTuples, timeoutInMilliseconds);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), numResultTuples);
    for (uint64_t recordIndex = 0u; recordIndex < resultBuffer.getNumberOfTuples(); ++recordIndex) {
        NES_DEBUG("Start: {} Hours, at Index: {}", resultBuffer[recordIndex][0].read<uint64_t>() / 3600000, recordIndex); // / 3600000
        NES_DEBUG("End: {} Hours, at Index: {}", resultBuffer[recordIndex][1].read<uint64_t>() / 3600000, recordIndex);
        NES_DEBUG("Id: {}, at Index: {}", resultBuffer[recordIndex][2].read<int64_t>(), recordIndex);
        NES_DEBUG("Power: {}, at Index: {}", resultBuffer[recordIndex][3].read<int64_t>(), recordIndex);
        NES_DEBUG("--------------------------------------")
    }

    ASSERT_TRUE(executionEngine->stopQuery(queryPlan));
}

INSTANTIATE_TEST_CASE_P(testFilterQueries,
                        DemoCityQueryExecutionTest,
                        ::testing::Values(
                                          QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER),
                        [](const testing::TestParamInfo<DemoCityQueryExecutionTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(info.param));
                        });

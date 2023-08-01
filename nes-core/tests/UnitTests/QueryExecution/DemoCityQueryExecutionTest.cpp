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
                ->addField("test$id", BasicType::INT64)
                ->addField("test$one", BasicType::INT64)
                ->addField("test$timestamp", BasicType::UINT64);        
        resultSchema = Schema::create()
                ->addField("test$start", BasicType::UINT64)
                ->addField("test$end", BasicType::UINT64)
                ->addField("test$windowid", BasicType::UINT64)
                ->addField("test$value", BasicType::INT64);
        expectedResultBuffers = numResultBuffers;
        testSink = executionEngine->createDataSink(schema, expectedResultBuffers);
        testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    }

    void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf, const uint64_t numInputTuples) {
        for (uint64_t recordIndex = 0; recordIndex < numInputTuples; recordIndex++) {
            buf[recordIndex][0].write<int64_t>(recordIndex);
            buf[recordIndex][1].write<int64_t>(1);
            buf[recordIndex][2].write<uint64_t>(1000 * recordIndex);
        }
        buf.setNumberOfTuples(numInputTuples);
    }

    Runtime::MemoryLayouts::DynamicTupleBuffer 
    runQuery(std::shared_ptr<Runtime::Execution::ExecutableQueryPlan> queryPlan, const uint64_t numInputTuples = 10) {
        // Setup first source and emit buffer
        auto source1 = executionEngine->getDataSource(queryPlan, 0);
        auto inputBuffer1 = executionEngine->getBuffer(schema);
        fillBuffer(inputBuffer1, numInputTuples);
        source1->emitBuffer(inputBuffer1);

        // Setup second source and emit buffer
        // auto source2 = executionEngine->getDataSource(queryPlan, 1);
        // auto inputBuffer2 = executionEngine->getBuffer(schema);
        // fillBuffer(inputBuffer2);
        // source2->emitBuffer(inputBuffer2);

        // Wait until the sink processed all tuples
        //Todo: #4043 this blocks indefinitely if no tuples pass the filter. In general stalls, if not enough 
        //            are produced.
        testSink->waitTillCompleted();
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

TEST_P(DemoCityQueryExecutionTest, demoQueryWithUnions) {
    constexpr uint64_t numResultBuffers = 2;
    constexpr uint64_t numInputTuples = 13;
    constexpr uint64_t numResultTuples = 2;
    createSchemaAndSink(numResultBuffers);
    auto testSourceDescriptor1 = executionEngine->createDataSource(schema);
    auto testSourceDescriptor2 = executionEngine->createDataSource(schema);

    // To trigger a TumblingWindow, the 'end' timestamp of the window must appear, which also starts the new window.
    auto query = 
        TestQuery::from(testSourceDescriptor1)
            // .unionWith(TestQuery::from(testSourceDescriptor2))
                .window(TumblingWindow::of(EventTime(Attribute("timestamp")), 
                    Milliseconds(6000UL)))
                .byKey(Attribute("test$one"))
                .apply(Sum(Attribute("test$id")))
                .sink(testSinkDescriptor);
                      
    auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());

    auto resultBuffer = runQuery(queryPlan, numInputTuples);
    
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), numResultTuples);
    for (uint64_t recordIndex = 0u; recordIndex < numResultTuples; ++recordIndex) {
        NES_DEBUG("Start: {}, at Index: {}", resultBuffer[recordIndex][0].read<uint64_t>(), recordIndex);
        NES_DEBUG("End: {}, at Index: {}", resultBuffer[recordIndex][1].read<uint64_t>(), recordIndex);
        NES_DEBUG("Id: {}, at Index: {}", resultBuffer[recordIndex][2].read<uint64_t>(), recordIndex);
        NES_DEBUG("Result: {}, at Index: {}", resultBuffer[recordIndex][3].read<int64_t>(), recordIndex);
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

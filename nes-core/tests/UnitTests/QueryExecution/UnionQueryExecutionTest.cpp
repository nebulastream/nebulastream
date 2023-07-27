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

class FilterQueryExecutionTest : public Testing::TestWithErrorHandling,
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

    void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf) {
        for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
            buf[recordIndex][0].write<int64_t>(recordIndex);
            buf[recordIndex][1].write<int64_t>(1);
        }
        buf.setNumberOfTuples(10);
    }

    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
};

// Todo: Currently this test fails when emitting the bufer
TEST_P(FilterQueryExecutionTest, unionOperatorFailing) {
    auto schema = Schema::create()->addField("test$id", BasicType::INT64)->addField("test$one", BasicType::INT64);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);
    auto testSink = executionEngine->createDataSink(schema, 2);
    auto schema2 = Schema::create()->addField("test$id", BasicType::INT64)->addField("test$one", BasicType::INT64);
    auto testSourceDescriptor2 = executionEngine->createDataSource(schema2);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query query = TestQuery::from(testSourceDescriptor)
                      .unionWith(TestQuery::from(testSourceDescriptor2))
                    //   .map(Attribute("id") = Attribute("id"))
                      .sink(testSinkDescriptor);

    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    // Setup first source
    auto source = executionEngine->getDataSource(plan, 0);
    auto inputBuffer = executionEngine->getBuffer(schema);
    fillBuffer(inputBuffer);

    // Setup second source
    auto source2 = executionEngine->getDataSource(plan, 1);
    auto inputBuffer2 = executionEngine->getBuffer(schema);
    fillBuffer(inputBuffer2);

    // Emit buffers for sources
    source->emitBuffer(inputBuffer);
    source2->emitBuffer(inputBuffer2);

    // Wait until the sink processed all tuples
    testSink->waitTillCompleted(); //Todo: this probably blocks if no tuples pass the filter
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 2u);

    auto resultBuffer = TestUtils::mergeBuffers(testSink->resultBuffers, schema, executionEngine->getBufferManager());
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 20u);
    ASSERT_TRUE(executionEngine->stopQuery(plan));
}

INSTANTIATE_TEST_CASE_P(testFilterQueries,
                        FilterQueryExecutionTest,
                        ::testing::Values(
                                        //   QueryCompilation::QueryCompilerOptions::QueryCompiler::DEFAULT_QUERY_COMPILER,
                                          QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER),
                        [](const testing::TestParamInfo<FilterQueryExecutionTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(info.param));
                        });

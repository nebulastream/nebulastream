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

#include <API/Schema.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/TestSourceDescriptor.hpp>
#include <Util/magicenum/magic_enum.hpp>

using namespace NES;
using Runtime::TupleBuffer;

// Dump IR
constexpr auto dumpMode = NES::QueryCompilation::QueryCompilerOptions::DumpMode::NONE;

class MapGPUQueryExecutionTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MapGPUQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("MapGPUQueryExecutionTest: Setup MapGPUQueryExecutionTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        auto queryCompiler = QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER;
        executionEngine = std::make_shared<Testing::TestExecutionEngine>(queryCompiler, dumpMode);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("MapGPUQueryExecutionTest: Tear down MapGPUQueryExecutionTest test case.");
        ASSERT_TRUE(executionEngine->stop());
        Testing::BaseUnitTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("MapGPUQueryExecutionTest: Tear down MapGPUQueryExecutionTest test class."); }

    static void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf, const uint64_t tuples) {
        for (uint64_t recordIndex = 0; recordIndex < tuples; recordIndex++) {
            buf[recordIndex][0].write<int64_t>(recordIndex);
        }
        buf.setNumberOfTuples(tuples);
    }

    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
};

#define N_TUPLES 128

TEST_F(MapGPUQueryExecutionTest, vectorizedMapOnGPU) {
    auto schema = Schema::create()->addField("test$id", BasicType::INT64);
    auto testSink = executionEngine->createDataSink(schema);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query =
        TestQuery::from(testSourceDescriptor).map(Attribute("test$id") = Attribute("test$id") * 2).sink(testSinkDescriptor);
    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    auto source = executionEngine->getDataSource(plan, 0);
    auto inputBuffer = executionEngine->getBuffer(schema);
    fillBuffer(inputBuffer, N_TUPLES);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), N_TUPLES);
    for (uint32_t recordIndex = 0u; recordIndex < N_TUPLES; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex][0].read<int64_t>(), recordIndex * 2);
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}
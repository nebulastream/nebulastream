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

#ifdef ENABLE_JNI

#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Operators/LogicalOperators/UDFs/JavaUDFDescriptor.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Util/JavaUDFDescriptorBuilder.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <iostream>
#include <jni.h>
#include <utility>

using namespace NES;
using Runtime::TupleBuffer;

class FlatMapJavaUDFQueryExecutionTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("FlatMapJavaUDFQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("QueryExecutionTest: Setup FlatMapJavaUDFQueryExecutionTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        executionEngine = std::make_shared<NES::Testing::TestExecutionEngine>();
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        Testing::BaseUnitTest::TearDown();
        NES_DEBUG("QueryExecutionTest: Tear down FlatMapJavaUDFQueryExecutionTest test case.");
        ASSERT_TRUE(executionEngine->stop());
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("FlatMapJavaUDFQueryExecutionTest: Tear down QueryExecutionTest test class."); }

    std::shared_ptr<NES::Testing::TestExecutionEngine> executionEngine;
    static constexpr DecomposedQueryPlanId defaultDecomposedQueryPlanId = INVALID_DECOMPOSED_QUERY_PLAN_ID;
    static constexpr SharedQueryId defaultSharedQueryId = INVALID_SHARED_QUERY_ID;
};

constexpr auto numberOfRecords = 10;
constexpr auto UDF_INCREMENT = 10;

/**
 * This helper function fills a buffer with test data
 */
void fillBuffer(Runtime::MemoryLayouts::TestTupleBuffer& buf) {
    for (int recordIndex = 0; recordIndex < numberOfRecords; recordIndex++) {
        buf[recordIndex][0].write<int32_t>(recordIndex);
    }
    buf.setNumberOfTuples(numberOfRecords);
}

/**
 * @brief Test simple UDF with integer objects as input and output (IntegerMapFunction<Integer, Integer>)
 * The UDF increments incoming tuples by its current state starting from 10 and
*/
TEST_F(FlatMapJavaUDFQueryExecutionTest, FlatMapJavaUdf) {
    auto fqSchema = Schema::create()->addField("s$id", BasicType::INT32);
    auto udfSchema = Schema::create()->addField("id", BasicType::INT32);
    auto testSink = executionEngine->createDataSink(fqSchema, numberOfRecords);
    auto testSourceDescriptor = executionEngine->createDataSource(fqSchema);

    auto javaUDFDescriptor =
        Catalogs::UDF::JavaUDFDescriptorBuilder{}
            .setClassName("stream.nebula.IntegerFlatMapFunction")
            .setMethodName("flatMap")
            .setInstance({})
            .setByteCodeList({{"stream.nebula.FlatMapFunction", {}}, {"stream.nebula.IntegerFlatMapFunction", {}}})
            .setInputSchema(udfSchema)
            .setOutputSchema(udfSchema)
            .setInputClassName("java.lang.Integer")
            .setOutputClassName("java.util.Collection")
            .loadByteCodeFrom(JAVA_UDF_TEST_DATA)
            .build();
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor).flatMapUDF(javaUDFDescriptor).sink(testSinkDescriptor);
    auto decomposedQueryPlan = DecomposedQueryPlan::create(defaultDecomposedQueryPlanId,
                                                           defaultSharedQueryId,
                                                           INVALID_WORKER_NODE_ID,
                                                           query.getQueryPlan()->getRootOperators());
    auto plan = executionEngine->submitQuery(decomposedQueryPlan);
    auto source = executionEngine->getDataSource(plan, 0);
    ASSERT_TRUE(!!source);
    auto inputBuffer = executionEngine->getBuffer(fqSchema);
    fillBuffer(inputBuffer);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), numberOfRecords);
    for (uint32_t recordIndex = 0u; recordIndex < numberOfRecords; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex][0].read<int32_t>(), recordIndex + UDF_INCREMENT);
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

#endif// ENABLE_JNI
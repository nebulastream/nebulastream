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
#ifdef NAUTILUS_PYTHON_UDF_ENABLED

#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PythonUDFDescriptorBuilder.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/magicenum/magic_enum.hpp>

using namespace NES;
using Runtime::TupleBuffer;

// Dump IR
constexpr auto dumpMode = NES::QueryCompilation::DumpMode::NONE;

class MapPythonUDFQueryExecutionTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MapPythonUDFQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("QueryExecutionTest: Setup MapPythonUDFQueryExecutionTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        NES_DEBUG("Setting up Nautilus Compiler");
        executionEngine =
            std::make_shared<NES::Testing::TestExecutionEngine>(dumpMode);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        Testing::BaseIntegrationTest::TearDown();
        NES_DEBUG("QueryExecutionTest: Tear down MapPythonUDFQueryExecutionTest test case.");
        ASSERT_TRUE(executionEngine->stop());
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("MapPythonUDFQueryExecutionTest: Tear down QueryExecutionTest test class."); }

    std::shared_ptr<NES::Testing::TestExecutionEngine> executionEngine;
};

constexpr auto numberOfRecords = 10;
constexpr auto udfIncrement = 10;

/**
* This helper function fills a buffer with test data
*/
void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf) {
    NES_DEBUG("Filling tuple buffer with test data")
    for (int recordIndex = 0; recordIndex < numberOfRecords; recordIndex++) {
        buf[recordIndex][0].write<int32_t>(recordIndex);
    }
    buf.setNumberOfTuples(numberOfRecords);
}

/**
* @brief Test simple UDF with integer objects as input and output (IntegerMapFunction<Integer, Integer>)
* The UDF increments incoming tuples by 10.
*/
TEST_F(MapPythonUDFQueryExecutionTest, MapPythonUdf) {
    auto schema = Schema::create()->addField("id", BasicType::INT32);
    auto outputSchema = Schema::create()->addField("id", BasicType::INT32);
    auto testSink = executionEngine->createDataSink(schema);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto functionName = "integer_test";
    auto functionString = "def integer_test(x):\n\ty = x + 10\n\treturn y\n";

    auto pythonUDFDescriptor = Catalogs::UDF::PythonUDFDescriptorBuilder{}
                                   .setFunctionName(functionName)
                                   .setFunctionString(functionString)
                                   .setOutputSchema(outputSchema)
                                   .build();

    NES_DEBUG("Set up Descriptor");
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor).mapUDF(pythonUDFDescriptor).sink(testSinkDescriptor);
    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    auto source = executionEngine->getDataSource(plan, 0);
    NES_DEBUG("submitted query and got source");
    ASSERT_TRUE(!!source);
    auto inputBuffer = executionEngine->getBuffer(schema);
    fillBuffer(inputBuffer);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), numberOfRecords);
    for (uint32_t recordIndex = 0u; recordIndex < numberOfRecords; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex][0].read<int32_t>(), recordIndex + udfIncrement);
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

#endif// NAUTILUS_PYTHON_UDF_ENABLED
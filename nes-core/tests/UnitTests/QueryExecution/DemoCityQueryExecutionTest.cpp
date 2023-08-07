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

    template<typename InputRecord>
    void generateAndEmitInputBuffers(const std::shared_ptr<Runtime::Execution::ExecutableQueryPlan>& queryPlan, 
            const std::vector<SchemaPtr>& sourceSchemas, const uint64_t numInputTuples = 10) {

        for(size_t sourceSchemaIdx = 0; sourceSchemaIdx < sourceSchemas.size(); ++sourceSchemaIdx) {
            auto source = executionEngine->getDataSource(queryPlan, sourceSchemaIdx);
            auto inputBuffer = executionEngine->getBuffer(sourceSchemas.at(sourceSchemaIdx));

            // Get a pointer to the buffer and fill it with values.
            auto bufferPtr = inputBuffer.getBuffer().getBuffer<InputRecord>();
            for(uint64_t recordIndex = 0; recordIndex < numInputTuples; ++recordIndex) {
                bufferPtr[recordIndex].producerId = 1;
                bufferPtr[recordIndex].power = recordIndex;
                bufferPtr[recordIndex].timestamp = 3600000 * recordIndex;
            }
            inputBuffer.setNumberOfTuples(numInputTuples);

            source->emitBuffer(inputBuffer);
        }
    }

    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
    std::shared_ptr<TestUtils::TestSinkDescriptor> testSinkDescriptor;
};

TEST_P(DemoCityQueryExecutionTest, demoQueryWithUnions) {
    // Setup test parameters.
    constexpr uint64_t numResultRecords = 12;
    constexpr uint64_t numInputTuples = 13;
    constexpr uint64_t timeoutInMilliseconds = 2000;

    // Declare the structure of the input records.
    struct __attribute__((packed)) InputRecord {
        int64_t producerId;
        int64_t power;
        uint64_t timestamp;
    };

    // Declare the structure of the result records.
    struct __attribute__((packed)) ResultRecord {
        int64_t difference;
        int64_t produced;
        int64_t consumed;
        uint64_t start;
    };

    // Define the first two sources 'windTurbines' and 'solarPanels'.
    const auto producerSchema = Schema::create()
                ->addField("test$producerId", BasicType::INT64)
                ->addField("test$producedPower", BasicType::INT64)
                ->addField("test$timestamp", BasicType::UINT64);
    const auto windTurbines = executionEngine->createDataSource(producerSchema);
    const auto solarPanels = executionEngine->createDataSource(producerSchema);

    // Define the third source 'consumers'.
    const auto consumerSchema = Schema::create()
                ->addField("test2$producerId", BasicType::INT64)
                ->addField("test2$consumedPower", BasicType::INT64)
                ->addField("test2$timestamp", BasicType::UINT64);    
    const auto consumers = executionEngine->createDataSource(consumerSchema);
    
    // Define the sink.
    const auto sinkSchema = Schema::create()
                ->addField("test$difference", BasicType::INT64)
                ->addField("test$produced", BasicType::INT64)
                ->addField("test$consumed", BasicType::INT64)
                ->addField("test$start", BasicType::UINT64);
    const auto testSink = executionEngine->createCollectSink<ResultRecord>(sinkSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    // Define the demo query.
    const auto query = 
        TestQuery::from(windTurbines)
        .unionWith(
            TestQuery::from(solarPanels)
        )
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Hours(1UL)))
        .byKey(Attribute("producerId"))
        .apply(Sum(Attribute("producedPower")))
        .joinWith(
            TestQuery::from(consumers)
            .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Hours(1UL))).byKey(Attribute("producerId"))
            .apply(Sum(Attribute("consumedPower")))
        )
        .where(Attribute("producerId"))
        .equalsTo(Attribute("producerId"))
        .window(TumblingWindow::of(EventTime(Attribute("start")), Hours(1)))
        .map(Attribute("DifferenceProducedConsumedPower") = Attribute("producedPower") - Attribute("consumedPower"))
        //Todo: #4068: Use 'Attribute("start").as("timestamp")' instead of 'Attribute("start")' 
        //              -> Currently not possible, because renaming and reducing the number of fields leads to an error.
        .project(Attribute("DifferenceProducedConsumedPower"), Attribute("producedPower"), Attribute("consumedPower"), Attribute("start"))
        .sink(testSinkDescriptor);
                      
    // Submit the query, retrieve the query plan and run it.
    const auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    generateAndEmitInputBuffers<InputRecord>(queryPlan, {producerSchema, producerSchema, consumerSchema}, numInputTuples);

    // Wait until the sink processed all tuples or timeout is reached.
    testSink->waitTillCompletedOrTimeout(numResultRecords, timeoutInMilliseconds);
    const auto resultRecords = testSink->getResult();

    EXPECT_EQ(resultRecords.size(), numResultRecords);
    for(size_t recordIdx = 0; recordIdx < resultRecords.size(); ++recordIdx) {
        EXPECT_EQ(resultRecords.at(recordIdx).difference, recordIdx);
        EXPECT_EQ(resultRecords.at(recordIdx).produced, recordIdx * 2);
        EXPECT_EQ(resultRecords.at(recordIdx).consumed, recordIdx);
        EXPECT_EQ(resultRecords.at(recordIdx).start, recordIdx * 3600000);
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

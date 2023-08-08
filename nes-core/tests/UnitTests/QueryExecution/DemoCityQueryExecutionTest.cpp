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
#include <gtest/gtest.h>
#include <iostream>
#include <utility>

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

    void generateAndEmitInputBuffers(const std::shared_ptr<Runtime::Execution::ExecutableQueryPlan>& queryPlan, 
            const std::vector<SchemaPtr>& sourceSchemas, 
            std::vector<std::function<void(Runtime::MemoryLayouts::DynamicTupleBuffer&)>> inputDataGenerators) {
        // Make sure that each source schema has one corresponding input data generator.
        EXPECT_EQ(sourceSchemas.size(), inputDataGenerators.size());

        // For each source schema, create a source and an input buffer. Fill the input buffer using the corresponding
        // input data generator and finally use the source to emit the input buffer.
        for(size_t sourceSchemaIdx = 0; sourceSchemaIdx < sourceSchemas.size(); ++sourceSchemaIdx) {
            auto source = executionEngine->getDataSource(queryPlan, sourceSchemaIdx);
            auto inputBuffer = executionEngine->getBuffer(sourceSchemas.at(sourceSchemaIdx));

            inputDataGenerators.at(sourceSchemaIdx)(inputBuffer);

            source->emitBuffer(inputBuffer);
        }
    }

    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
    std::shared_ptr<TestUtils::TestSinkDescriptor> testSinkDescriptor;
};

TEST_P(DemoCityQueryExecutionTest, demoQueryWithUnions) {
    //==---------------------------------------==//
    //==-------- SETUP TEST PARAMETERS --------==//
    //==---------------------------------------==//
    constexpr uint64_t numInputRecords = 13;
    constexpr uint64_t numResultRecords = 12;
    constexpr uint64_t timeoutInMilliseconds = 2000;
    constexpr uint64_t milliSecondsToHours = 3600000;

    // Define the input data generator functions.
    std::function<void(Runtime::MemoryLayouts::DynamicTupleBuffer&)> windTurbineDataGenerator;
    windTurbineDataGenerator = [](Runtime::MemoryLayouts::DynamicTupleBuffer& buffer) {
        for(size_t recordIdx = 0; recordIdx < numInputRecords; ++recordIdx) {
            buffer[recordIdx][0].write<int64_t>(1);
            buffer[recordIdx][1].write<int64_t>(recordIdx);
            buffer[recordIdx][2].write<uint64_t>(milliSecondsToHours * recordIdx);
        }
        buffer.setNumberOfTuples(numInputRecords);
    };
    auto solarPanelDataGenerator = windTurbineDataGenerator;
    auto consumersDataGenerator = windTurbineDataGenerator;

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

    //==---------------------------------------==//
    //==-------- DEFINE THE DEMO QUERY --------==//
    //==---------------------------------------==//
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
                      
    //==-------------------------------------------------------==//
    //==-------- GENERATE INPUT DATA AND RUN THE QUERY --------==//
    //==-------------------------------------------------------==//
    const auto queryPlan = executionEngine->submitQuery(query.getQueryPlan());
    generateAndEmitInputBuffers(queryPlan, {producerSchema, producerSchema, consumerSchema}, 
    {solarPanelDataGenerator, windTurbineDataGenerator, consumersDataGenerator});

    // Wait until the sink processed all records or timeout is reached.
    testSink->waitTillCompletedOrTimeout(numResultRecords, timeoutInMilliseconds);
    const auto resultRecords = testSink->getResult();

    //==--------------------------------------==//
    //==-------- COMPARE TEST RESULTS --------==//
    //==--------------------------------------==//
    EXPECT_EQ(resultRecords.size(), numResultRecords);
    for(size_t recordIdx = 0; recordIdx < resultRecords.size(); ++recordIdx) {
        EXPECT_EQ(resultRecords.at(recordIdx).difference, recordIdx);
        EXPECT_EQ(resultRecords.at(recordIdx).produced, recordIdx * 2);
        EXPECT_EQ(resultRecords.at(recordIdx).consumed, recordIdx);
        EXPECT_EQ(resultRecords.at(recordIdx).start, recordIdx * milliSecondsToHours);
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

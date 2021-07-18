/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include "gtest/gtest.h"

#include <API/Query.hpp>
#include <API/Schema.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayout.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayoutBuffer.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayoutField.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <future>
#include <iostream>
#include <utility>

#include "../../util/DummySink.hpp"
#include "../../util/SchemaSourceDescriptor.hpp"
#include "../../util/TestQuery.hpp"
#include "../../util/TestQueryCompiler.hpp"
#include <Catalogs/StreamCatalog.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Topology/TopologyNode.hpp>

#include <Optimizer/QueryRewrite/DistributeWindowRule.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>

#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>

using namespace NES;
using Runtime::TupleBuffer;

class QueryExecutionTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("QueryExecutionTest.log", NES::LOG_DEBUG);
        NES_DEBUG("QueryExecutionTest: Setup QueryCatalogTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        // create test input buffer
        windowSchema = Schema::create()
                           ->addField("test$key", BasicType::INT64)
                           ->addField("test$value", BasicType::INT64)
                           ->addField("test$ts", BasicType::UINT64);
        testSchema = Schema::create()
                         ->addField("test$id", BasicType::INT64)
                         ->addField("test$one", BasicType::INT64)
                         ->addField("test$value", BasicType::INT64);
        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
        nodeEngine = Runtime::create("127.0.0.1", 31337, streamConf);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("QueryExecutionTest: Tear down QueryExecutionTest test case.");
        nodeEngine->stop();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("QueryExecutionTest: Tear down QueryExecutionTest test class."); }

    SchemaPtr testSchema;
    SchemaPtr windowSchema;
    Runtime::NodeEnginePtr nodeEngine;
};

/**
 * @brief A window source, which generates data consisting of (key, value, ts).
 * Key = 1||2
 * Value = 1
 * Ts = #Iteration
 */
class WindowSource : public NES::DefaultSource {
  public:
    uint64_t runCnt = 0;
    int64_t timestamp;
    bool varyWatermark;
    bool decreaseTime;
    WindowSource(SchemaPtr schema,
                 Runtime::BufferManagerPtr bufferManager,
                 Runtime::QueryManagerPtr queryManager,
                 const uint64_t numbersOfBufferToProduce,
                 uint64_t frequency,
                 bool varyWatermark,
                 bool decreaseTime,
                 int64_t timestamp,
                 std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
        : DefaultSource(std::move(schema),
                        std::move(bufferManager),
                        std::move(queryManager),
                        numbersOfBufferToProduce,
                        frequency,
                        1,
                        12,
                        std::move(successors)),
          timestamp(timestamp), varyWatermark(varyWatermark), decreaseTime(decreaseTime) {}

    std::optional<TupleBuffer> receiveData() override {
        auto buffer = bufferManager->getBufferBlocking();
        auto rowLayout = NES::Runtime::DynamicMemoryLayout::DynamicRowLayout::create(schema, true);

        auto bindedRowLayout = rowLayout->bind(buffer);

        for (int i = 0; i < 10; i++) {
            Runtime::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(0, bindedRowLayout)[i] = 1;
            Runtime::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(1, bindedRowLayout)[i] = 1;

            if (varyWatermark) {
                if (!decreaseTime) {
                    Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(2, bindedRowLayout)[i] =
                        timestamp++;
                } else {
                    if (runCnt == 0) {
                        /**
                         *in this run, we create normal tuples and one tuples that triggers a very large watermark
                         * first buffer
                         * |key:INT64|value:INT64|ts:UINT64|
                            +----------------------------------------------------+
                            |1|1|30|
                            |1|1|31|
                            |1|1|32|
                            |1|1|33|
                            |1|1|34|
                            |1|1|35|
                            |1|1|36|
                            |1|1|37|
                            |1|1|38|
                            |1|1|59|
                            +----------------------------------------------------+
                         */
                        if (i < 9) {
                            Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(2, bindedRowLayout)[i] =
                                timestamp++;
                        } else {
                            Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(2, bindedRowLayout)[i] =
                                timestamp + 20;
                        }
                    } else {
                        /**
                         * in this run we add ts below the current watermark to see if they are part of the result
                         * |key:INT64|value:INT64|ts:UINT64|
                            +----------------------------------------------------+
                            |1|1|48|
                            |1|1|47|
                            |1|1|46|
                            |1|1|45|
                            |1|1|44|
                            |1|1|43|
                            |1|1|42|
                            |1|1|41|
                            |1|1|40|
                            |1|1|39|
                            +----------------------------------------------------+
                         */
                        timestamp = timestamp - 1 <= 0 ? 0 : timestamp - 1;
                        Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(2, bindedRowLayout)[i] =
                            timestamp;
                    }
                }
            } else {
                Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(2, bindedRowLayout)[i] = timestamp;
            }
        }
        buffer.setNumberOfTuples(10);
        timestamp = timestamp + 10;
        runCnt++;

        NES_DEBUG("QueryExecutionTest: source buffer=" << UtilityFunctions::prettyPrintTupleBuffer(buffer, schema));
        return buffer;
    };

    static DataSourcePtr create(const SchemaPtr& schema,
                                const Runtime::BufferManagerPtr& bufferManager,
                                const Runtime::QueryManagerPtr& queryManager,
                                const uint64_t numbersOfBufferToProduce,
                                uint64_t frequency,
                                const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors,
                                const bool varyWatermark = false,
                                bool decreaseTime = false,
                                int64_t timestamp = 5) {

        return std::make_shared<WindowSource>(schema,
                                              bufferManager,
                                              queryManager,
                                              numbersOfBufferToProduce,
                                              frequency,
                                              varyWatermark,
                                              decreaseTime,
                                              timestamp,
                                              successors);
    }
};

using DefaultSourcePtr = std::shared_ptr<DefaultSource>;

class TestSink : public SinkMedium {
  public:
    TestSink(uint64_t expectedBuffer, const SchemaPtr& schema, const Runtime::BufferManagerPtr& bufferManager)
        : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager), 0), expectedBuffer(expectedBuffer){};

    static std::shared_ptr<TestSink>
    create(uint64_t expectedBuffer, const SchemaPtr& schema, const Runtime::BufferManagerPtr& bufferManager) {
        return std::make_shared<TestSink>(expectedBuffer, schema, bufferManager);
    }

    bool writeData(TupleBuffer& input_buffer, Runtime::WorkerContext&) override {
        std::unique_lock lock(m);
        NES_DEBUG("QueryExecutionTest: TestSink: got buffer " << input_buffer);
        NES_DEBUG("QueryExecutionTest: PrettyPrintTupleBuffer"
                  << UtilityFunctions::prettyPrintTupleBuffer(input_buffer, getSchemaPtr()));

        resultBuffers.emplace_back(std::move(input_buffer));
        if (resultBuffers.size() == expectedBuffer) {
            completed.set_value(true);
        } else if (resultBuffers.size() > expectedBuffer) {
            EXPECT_TRUE(false);
        }
        return true;
    }

    /**
     * @brief Factory to create a new TestSink.
     * @param expectedBuffer number of buffers expected this sink should receive.
     * @return
     */

    TupleBuffer get(uint64_t index) {
        std::unique_lock lock(m);
        return resultBuffers[index];
    }

    void setup() override{};

    std::string toString() const override { return "Test_Sink"; }

    void shutdown() override {}

    ~TestSink() override {
        NES_DEBUG("~TestSink()");
        std::unique_lock lock(m);
        cleanupBuffers();
    };

    uint32_t getNumberOfResultBuffers() {
        std::unique_lock lock(m);
        return resultBuffers.size();
    }

    SinkMediumTypes getSinkMediumType() override { return SinkMediumTypes::PRINT_SINK; }

    void cleanupBuffers() { resultBuffers.clear(); }

    mutable std::recursive_mutex m;
    uint64_t expectedBuffer;

    std::promise<bool> completed;
    std::vector<TupleBuffer> resultBuffers;
};

void fillBuffer(TupleBuffer& buf, const Runtime::DynamicMemoryLayout::DynamicRowLayoutPtr& memoryLayout) {

    auto bindedRowLayout = memoryLayout->bind(buf);
    auto recordIndexFields = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(0, bindedRowLayout);
    auto fields01 = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(1, bindedRowLayout);
    auto fields02 = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(2, bindedRowLayout);

    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        recordIndexFields[recordIndex] = recordIndex;
        fields01[recordIndex] = 1;
        fields02[recordIndex] = recordIndex % 2;
    }
    buf.setNumberOfTuples(10);
}

TEST_F(QueryExecutionTest, filterQuery) {
    // creating query plan

    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchema,
        [&](OperatorId id,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createDefaultDataSourceWithSchemaForOneBuffer(testSchema,
                                                                 nodeEngine->getBufferManager(),
                                                                 nodeEngine->getQueryManager(),
                                                                 id,
                                                                 numSourceLocalBuffers,
                                                                 std::move(successors));
        });

    auto outputSchema = Schema::create()->addField("id", BasicType::INT64);
    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor).filter(Attribute("id") < 5).sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();
    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 1u);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    auto memoryLayout = Runtime::DynamicMemoryLayout::DynamicRowLayout::create(testSchema, true);
    fillBuffer(buffer, memoryLayout);
    plan->setup();
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Deployed);
    plan->start(nodeEngine->getStateManager());
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
    Runtime::WorkerContext workerContext{1};
    plan->getPipelines()[0]->execute(buffer, workerContext);

    // This plan should produce one output buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);

    auto resultBuffer = testSink->get(0);
    // The output buffer should contain 5 tuple;
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 5u);

    auto bindedRowLayoutResult = memoryLayout->bind(resultBuffer);
    auto resultRecordIndexFields =
        Runtime::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(0, bindedRowLayoutResult);
    for (uint32_t recordIndex = 0u; recordIndex < 5u; ++recordIndex) {
        // id
        EXPECT_EQ(resultRecordIndexFields[recordIndex], recordIndex);
    }

    testSink->cleanupBuffers();
    buffer.release();
    plan->stop();
}

TEST_F(QueryExecutionTest, projectionQuery) {
    auto streamConf = PhysicalStreamConfig::createEmpty();

    // creating query plan
    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchema,
        [&](OperatorId id,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createDefaultDataSourceWithSchemaForOneBuffer(testSchema,
                                                                 nodeEngine->getBufferManager(),
                                                                 nodeEngine->getQueryManager(),
                                                                 id,
                                                                 numSourceLocalBuffers,
                                                                 std::move(successors));
        });

    auto outputSchema = Schema::create()->addField("id", BasicType::INT64);
    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor).project(Attribute("id")).sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();

    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 1U);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    auto memoryLayout = Runtime::DynamicMemoryLayout::DynamicRowLayout::create(testSchema, true);
    fillBuffer(buffer, memoryLayout);
    plan->setup();
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Deployed);
    plan->start(nodeEngine->getStateManager());
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
    Runtime::WorkerContext workerContext{1};
    plan->getPipelines()[0]->execute(buffer, workerContext);

    // This plan should produce one output buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1U);

    auto resultBuffer = testSink->get(0);
    // The output buffer should contain 5 tuple;
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10UL);

    auto resultLayout = Runtime::DynamicMemoryLayout::DynamicRowLayout::create(outputSchema, true);
    auto bindedRowLayoutResult = resultLayout->bind(resultBuffer);
    auto resultRecordIndexFields =
        Runtime::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(0, bindedRowLayoutResult);

    for (uint32_t recordIndex = 0UL; recordIndex < 10UL; ++recordIndex) {
        // id
        EXPECT_EQ(resultRecordIndexFields[recordIndex], recordIndex);
    }

    testSink->cleanupBuffers();
    buffer.release();
    plan->stop();
}

// @ Philipp & reviewers: query to test strategies. have a look at the code with a breakpoint at Compiler.cpp:79, then --> console or filesystem. (Expected content wont match though)
// TODO delete before merge
TEST_F(QueryExecutionTest, mapQuery) {
    // creating query plan

    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchema,
        [&](OperatorId id,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createDefaultDataSourceWithSchemaForOneBuffer(testSchema,
                                                                 nodeEngine->getBufferManager(),
                                                                 nodeEngine->getQueryManager(),
                                                                 id,
                                                                 numSourceLocalBuffers,
                                                                 std::move(successors));
        });

    auto outputSchema = Schema::create()
                            ->addField("one", BasicType::INT64)
                            ->addField("id", BasicType::INT64)
                            ->addField("value", BasicType::INT64)
//                            ->addField("new", BasicType::INT64)
                            ;

    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor)
//                     .filter(Attribute("id") < 6)
//                    .map(Attribute("new") = Attribute("id") + Attribute("one"))
//                    .map(Attribute("id") = Attribute("new") + Attribute("one"))
//                    .map(Attribute("myBool") = Attribute("id") <= Attribute("one"))
//                    .project(Attribute("one"), Attribute("id"))
//                    .project(Attribute("id"), Attribute("one"))
            .map(Attribute("id") = Attribute("id") + Attribute("one"))
                     .sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();
    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 1U);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    auto memoryLayout = Runtime::DynamicMemoryLayout::DynamicRowLayout::create(testSchema, true);
    fillBuffer(buffer, memoryLayout);
    plan->setup();
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Deployed);
    plan->start(nodeEngine->getStateManager());
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
    Runtime::WorkerContext workerContext{1};
    plan->getPipelines()[0]->execute(buffer, workerContext);

    // This plan should produce one output buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1U);

    std::string expectedContent = "+----------------------------------------------------+\n"
                                  "|id:INT64|one:INT64|value:INT64|new:INT64|\n"
                                  "+----------------------------------------------------+\n"
                                  "|0|1|0|1|\n"
                                  "|1|1|1|2|\n"
                                  "|2|1|0|3|\n"
                                  "|3|1|1|4|\n"
                                  "|4|1|0|5|\n"
                                  "|5|1|1|6|\n"
                                  "+----------------------------------------------------+";

    auto resultBuffer = testSink->get(0);

    EXPECT_EQ(expectedContent, UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, outputSchema));

    buffer.release();
    testSink->cleanupBuffers();
    plan->stop();
}



TEST_F(QueryExecutionTest, arithmeticOperatorsQuery) {
    // creating query plan

    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchema,
        [&](OperatorId id,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createDefaultDataSourceWithSchemaForOneBuffer(testSchema,
                                                                 nodeEngine->getBufferManager(),
                                                                 nodeEngine->getQueryManager(),
                                                                 id,
                                                                 numSourceLocalBuffers,
                                                                 std::move(successors));
        });

    auto outputSchema = Schema::create()
                            ->addField("id", BasicType::INT64)
                            ->addField("one", BasicType::INT64)
                            ->addField("value", BasicType::INT64)
                            ->addField("result_pow_int", BasicType::INT64)
                            ->addField("result_pow_float", BasicType::FLOAT64)
                            ->addField("result_mod_int", BasicType::INT64)
                            ->addField("result_mod_float", BasicType::FLOAT64)
                            ->addField("result_ceil", BasicType::FLOAT64)
                            ->addField("result_exp", BasicType::FLOAT64)
                            ->addField("result_batch_test", BasicType::BOOLEAN);

    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor)
                     .filter(Attribute("id") < 6)
                     .map(Attribute("result_pow_int") = POWER(2, Attribute("one") + Attribute("id")))    // int
                     .map(Attribute("result_pow_float") = POWER(2.0, Attribute("one") + Attribute("id")))// float
                     .map(Attribute("result_mod_int") = 20 % (Attribute("id") + 1))                      // int
                     .map(Attribute("result_mod_float") = MOD(-20.0, Attribute("id") + Attribute("one")))// float
                     .map(Attribute("result_ceil") = ABS(ROUND(FLOOR(CEIL((Attribute("id")) / 2.0)))))// more detailed tests below
                     .map(Attribute("result_exp") = EXP(Attribute("result_ceil")))
                     .map(Attribute("result_batch_test") =// batch test of many arithmetic operators, should always be 1/TRUE:
                          // test functionality of many functions at once through combination with their inverse function:
                          Attribute("result_ceil") == LOGN(EXP(Attribute("result_ceil")))
                              && Attribute("result_ceil") == SQRT(POWER(Attribute("result_ceil"), 2))
                              && Attribute("result_ceil") == LOG10(POWER(10, Attribute("result_ceil")))
                              // test FLOOR, ROUND, CEIL, ABS
                              && FLOOR(Attribute("id") / 2.0) <= Attribute("id") / 2.0
                              && Attribute("id") / 2.0 <= CEIL(Attribute("id") / 2.0)
                              && FLOOR(Attribute("id") / 2.0) <= ROUND(Attribute("id") / 2.0)
                              && ROUND(Attribute("id") / 2.0) <= CEIL(Attribute("id") / 2.0)
                              && ABS(1 - Attribute("id")) == ABS(Attribute("id") - 1))
                     .sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();
    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 1U);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    auto memoryLayout = Runtime::DynamicMemoryLayout::DynamicRowLayout::create(testSchema, true);
    fillBuffer(buffer, memoryLayout);
    plan->setup();
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Deployed);
    plan->start(nodeEngine->getStateManager());
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
    Runtime::WorkerContext workerContext{1};
    plan->getPipelines()[0]->execute(buffer, workerContext);

    // This plan should produce one output buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1U);

    std::string expectedContent =
        "+----------------------------------------------------+\n"
        "|id:INT64|one:INT64|value:INT64|result_pow_int:INT64|result_pow_float:FLOAT64|result_mod_int:INT64|result_mod_float:"
        "FLOAT64|result_ceil:FLOAT64|result_exp:FLOAT64|result_batch_test:BOOLEAN|\n"
        "+----------------------------------------------------+\n"
        "|0|1|0|2|2.000000|0|-0.000000|0.000000|1.000000|1|\n"
        "|1|1|1|4|4.000000|0|-0.000000|1.000000|2.718282|1|\n"
        "|2|1|0|8|8.000000|2|-2.000000|1.000000|2.718282|1|\n"
        "|3|1|1|16|16.000000|0|-0.000000|2.000000|7.389056|1|\n"
        "|4|1|0|32|32.000000|0|-0.000000|2.000000|7.389056|1|\n"
        "|5|1|1|64|64.000000|2|-2.000000|3.000000|20.085537|1|\n"
        "+----------------------------------------------------+";

    auto resultBuffer = testSink->get(0);

    EXPECT_EQ(expectedContent, UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, outputSchema));

    buffer.release();
    testSink->cleanupBuffers();
    plan->stop();
}

/**
 * @brief This test verify that the watermark assigned correctly.
 * WindowSource -> WatermarkAssignerOperator -> TestSink
 *
 */
TEST_F(QueryExecutionTest, watermarkAssignerTest) {
    uint64_t millisecondOfallowedLateness = 2U; /*milliseconds of allowedLateness*/

    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        windowSchema,
        [&](OperatorId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return WindowSource::create(windowSchema,
                                        nodeEngine->getBufferManager(),
                                        nodeEngine->getQueryManager(),
                                        /*bufferCnt*/ 2,
                                        /*frequency*/ 1000,
                                        successors,
                                        /*varyWatermark*/ true);
        });
    auto query = TestQuery::from(windowSourceDescriptor);

    // 2. add window operator:
    // 2.1 add Tumbling window of size 10s and a sum aggregation on the value. Use window of 5ms to ensure that it is closed.
    auto windowType = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(5));

    // add a watermark assigner operator with the specified allowedLateness
    query = query.assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute("ts"),
                                                                               Milliseconds(millisecondOfallowedLateness),
                                                                               Milliseconds()));

    query = query.window(windowType).byKey(Attribute("key", INT64)).apply(Sum(Attribute("value", INT64)));
    // add a watermark assigner operator with allowedLateness of 1 millisecond

    // 3. add sink. We expect that this sink will receive one buffer
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("_$start", UINT64))
                                  ->addField(createField("_$end", UINT64))
                                  ->addField(createField("test$key", INT64))
                                  ->addField("test$value", INT64);

    // each source buffer produce 1 result buffer, totalling 2 buffers
    auto testSink = TestSink::create(/*expected result buffer*/ 2, windowResultSchema, nodeEngine->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    query.sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    Optimizer::DistributeWindowRulePtr distributeWindowRule = Optimizer::DistributeWindowRule::create();
    queryPlan = distributeWindowRule->apply(queryPlan);
    queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    std::cout << " plan=" << queryPlan->toString() << std::endl;
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();

    nodeEngine->registerQueryInNodeEngine(plan);
    nodeEngine->startQuery(0);

    // wait till all buffers have been produced
    testSink->completed.get_future().get();

    auto resultBuffer = testSink->get(0);

    // 10 records, starting at ts=5 with 1ms difference each record, hence ts of the last record=14
    EXPECT_EQ(resultBuffer.getWatermark(), 14 - millisecondOfallowedLateness);

    nodeEngine->stopQuery(0);
    testSink->cleanupBuffers();
}

/**
 * @brief This tests creates a windowed query.
 * WindowSource -> windowOperator -> windowScan -> TestSink
 * The source generates 2. buffers.
 */
TEST_F(QueryExecutionTest, tumblingWindowQueryTest) {
    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        windowSchema,
        [&](OperatorId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return WindowSource::create(windowSchema,
                                        nodeEngine->getBufferManager(),
                                        nodeEngine->getQueryManager(),
                                        /*bufferCnt*/ 2,
                                        /*frequency*/ 1000,
                                        std::move(successors));
        });
    auto query = TestQuery::from(windowSourceDescriptor);

    // 2. dd window operator:
    // 2.1 add Tumbling window of size 10s and a sum aggregation on the value.
    auto windowType = TumblingWindow::of(EventTime(Attribute("test$ts")), Milliseconds(10));

    query = query.window(windowType).byKey(Attribute("key", INT64)).apply(Sum(Attribute("value", INT64)));

    // 3. add sink. We expect that this sink will receive one buffer
    //    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("_$start", UINT64))
                                  ->addField(createField("_$end", UINT64))
                                  ->addField(createField("test$key", INT64))
                                  ->addField("test$value", INT64);

    auto testSink = TestSink::create(/*expected result buffer*/ 1, windowResultSchema, nodeEngine->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    query.sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    Optimizer::DistributeWindowRulePtr distributeWindowRule = Optimizer::DistributeWindowRule::create();
    queryPlan = distributeWindowRule->apply(queryPlan);
    queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    std::cout << " plan=" << queryPlan->toString() << std::endl;
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();

    nodeEngine->registerQueryInNodeEngine(plan);
    nodeEngine->startQuery(0);

    // wait till all buffers have been produced
    testSink->completed.get_future().get();

    // get result buffer, which should contain two results.
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1UL);
    auto resultBuffer = testSink->get(0);

    NES_DEBUG("QueryExecutionTest: buffer=" << UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, windowResultSchema));
    //TODO 1 Tuple im result buffer in 312 2 results?
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1UL);

    auto resultLayout = Runtime::DynamicMemoryLayout::DynamicRowLayout::create(windowResultSchema, true);
    auto bindedRowLayoutResult = resultLayout->bind(resultBuffer);

    auto startFields = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(0, bindedRowLayoutResult);
    auto endFields = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(1, bindedRowLayoutResult);
    auto keyFields = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(2, bindedRowLayoutResult);
    auto valueFields = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(3, bindedRowLayoutResult);

    for (int recordIndex = 0; recordIndex < 1; recordIndex++) {
        // start
        EXPECT_EQ(startFields[recordIndex], 0UL);
        // end
        EXPECT_EQ(endFields[recordIndex], 10UL);
        // key
        EXPECT_EQ(keyFields[recordIndex], 1UL);
        // value
        EXPECT_EQ(valueFields[recordIndex], 10UL);
    }
    nodeEngine->stopQuery(0);
    testSink->cleanupBuffers();
}

/**
 * @brief This tests creates a windowed query.
 * WindowSource -> windowOperator -> windowScan -> TestSink
 * The source generates 2. buffers.
 */
TEST_F(QueryExecutionTest, tumblingWindowQueryTestWithOutOfOrderBuffer) {

    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        windowSchema,
        [&](OperatorId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return WindowSource::create(windowSchema,
                                        nodeEngine->getBufferManager(),
                                        nodeEngine->getQueryManager(),
                                        /*bufferCnt*/ 2,
                                        /*frequency*/ 1000,
                                        successors,
                                        /*varyWatermark*/ true,
                                        true,
                                        30);
        });
    auto query = TestQuery::from(windowSourceDescriptor);
    // 2. dd window operator:
    // 2.1 add Tumbling window of size 10s and a sum aggregation on the value.
    auto windowType = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10));

    query = query.window(windowType).byKey(Attribute("key", INT64)).apply(Sum(Attribute("value", INT64)));

    // 3. add sink. We expect that this sink will receive one buffer
    //    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField(createField("key", INT64))
                                  ->addField("value", INT64);

    auto testSink = TestSink::create(/*expected result buffer*/ 1, windowResultSchema, nodeEngine->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    query.sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    Optimizer::DistributeWindowRulePtr distributeWindowRule = Optimizer::DistributeWindowRule::create();
    queryPlan = distributeWindowRule->apply(queryPlan);
    queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    std::cout << " plan=" << queryPlan->toString() << std::endl;
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();
    nodeEngine->registerQueryInNodeEngine(plan);
    nodeEngine->startQuery(0);

    // wait till all buffers have been produced
    testSink->completed.get_future().get();

    // get result buffer, which should contain two results.
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1UL);
    auto resultBuffer = testSink->get(0);
    NES_DEBUG("QueryExecutionTest: buffer=" << UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, windowResultSchema));
    //TODO 1 Tuple im result buffer in 312 2 results?
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1UL);

    auto resultLayout = Runtime::DynamicMemoryLayout::DynamicRowLayout::create(windowResultSchema, true);
    auto bindedRowLayoutResult = resultLayout->bind(resultBuffer);

    auto startFields = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(0, bindedRowLayoutResult);
    auto endFields = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(1, bindedRowLayoutResult);
    auto keyFields = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(2, bindedRowLayoutResult);
    auto valueFields = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(3, bindedRowLayoutResult);

    for (int recordIndex = 0; recordIndex < 1; recordIndex++) {
        // start
        EXPECT_EQ(startFields[recordIndex], 30UL);
        // end
        EXPECT_EQ(endFields[recordIndex], 40UL);
        // key
        EXPECT_EQ(keyFields[recordIndex], 1UL);
        // value
        EXPECT_EQ(valueFields[recordIndex], 9UL);
    }

    nodeEngine->stopQuery(0);
    testSink->cleanupBuffers();
}

TEST_F(QueryExecutionTest, SlidingWindowQueryWindowSourcesize10slide5) {
    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        windowSchema,
        [&](OperatorId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return WindowSource::create(windowSchema,
                                        nodeEngine->getBufferManager(),
                                        nodeEngine->getQueryManager(),
                                        /*bufferCnt*/ 2,
                                        /*frequency*/ 1000,
                                        std::move(successors));
        });
    auto query = TestQuery::from(windowSourceDescriptor);

    // 2. dd window operator:
    // 2.1 add Sliding window of size 10ms and with Slide 2ms and a sum aggregation on the value.
    auto windowType = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(10), Milliseconds(5));

    auto aggregation = Sum(Attribute("value"));
    query = query.window(windowType).byKey(Attribute("key")).apply(aggregation);

    // 3. add sink. We expect that this sink will receive one buffer
    //    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField(createField("key", INT64))
                                  ->addField("value", INT64);

    auto testSink = TestSink::create(/*expected result buffer*/ 1, windowResultSchema, nodeEngine->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    query.sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    Optimizer::DistributeWindowRulePtr distributeWindowRule = Optimizer::DistributeWindowRule::create();
    queryPlan = distributeWindowRule->apply(queryPlan);
    queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    //    std::cout << " plan=" << queryPlan->toString() << std::endl;
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();
    nodeEngine->registerQueryInNodeEngine(plan);
    nodeEngine->startQuery(0);

    // wait till all buffers have been produced
    testSink->completed.get_future().get();
    NES_INFO("QueryExecutionTest: The test sink contains " << testSink->getNumberOfResultBuffers() << " result buffers.");

    // get result buffer
    auto resultBuffer = testSink->get(0);

    NES_INFO("QueryExecutionTest: The result buffer contains " << resultBuffer.getNumberOfTuples() << " tuples.");
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 2UL);
    NES_INFO("QueryExecutionTest: buffer=" << UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, windowResultSchema));
    std::string expectedContent = "+----------------------------------------------------+\n"
                                  "|start:UINT64|end:UINT64|key:INT64|value:INT64|\n"
                                  "+----------------------------------------------------+\n"
                                  "|0|10|1|10|\n"
                                  "|5|15|1|10|\n"
                                  "+----------------------------------------------------+";
    EXPECT_EQ(expectedContent, UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, windowResultSchema));
    nodeEngine->stopQuery(0);
    testSink->cleanupBuffers();
}

TEST_F(QueryExecutionTest, SlidingWindowQueryWindowSourceSize15Slide5) {
    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        windowSchema,
        [&](OperatorId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return WindowSource::create(windowSchema,
                                        nodeEngine->getBufferManager(),
                                        nodeEngine->getQueryManager(),
                                        /*bufferCnt*/ 3,
                                        /*frequency*/ 0,
                                        successors);
        });
    auto query = TestQuery::from(windowSourceDescriptor);

    // 2. dd window operator:
    // 2.1 add Sliding window of size 10ms and with Slide 2ms and a sum aggregation on the value.
    auto windowType = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(15), Milliseconds(5));

    auto aggregation = Sum(Attribute("value"));
    query = query.window(windowType).byKey(Attribute("key")).apply(aggregation);

    // 3. add sink. We expect that this sink will receive one buffer
    //    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField(createField("key", INT64))
                                  ->addField("value", INT64);

    auto testSink = TestSink::create(/*expected result buffer*/ 2, windowResultSchema, nodeEngine->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    query.sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    Optimizer::DistributeWindowRulePtr distributeWindowRule = Optimizer::DistributeWindowRule::create();
    queryPlan = distributeWindowRule->apply(queryPlan);
    queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    std::cout << " plan=" << queryPlan->toString() << std::endl;
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();
    nodeEngine->registerQueryInNodeEngine(plan);
    nodeEngine->startQuery(0);

    // wait till all buffers have been produced
    testSink->completed.get_future().get();
    NES_INFO("QueryExecutionTest: The test sink contains " << testSink->getNumberOfResultBuffers() << " result buffers.");
    // get result buffer

    auto resultBuffer = testSink->get(0);
    NES_INFO("QueryExecutionTest: The result buffer contains " << resultBuffer.getNumberOfTuples() << " tuples.");
    NES_INFO("QueryExecutionTest: buffer=" << UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, windowResultSchema));
    std::string expectedContent = "+----------------------------------------------------+\n"
                                  "|start:UINT64|end:UINT64|key:INT64|value:INT64|\n"
                                  "+----------------------------------------------------+\n"
                                  "|0|15|1|10|\n"
                                  "+----------------------------------------------------+";
    EXPECT_EQ(expectedContent, UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, windowResultSchema));

    auto resultBuffer2 = testSink->get(1);
    NES_INFO("QueryExecutionTest: The result buffer contains " << resultBuffer2.getNumberOfTuples() << " tuples.");
    NES_INFO("QueryExecutionTest: buffer=" << UtilityFunctions::prettyPrintTupleBuffer(resultBuffer2, windowResultSchema));
    std::string expectedContent2 = "+----------------------------------------------------+\n"
                                   "|start:UINT64|end:UINT64|key:INT64|value:INT64|\n"
                                   "+----------------------------------------------------+\n"
                                   "|5|20|1|20|\n"
                                   "|10|25|1|10|\n"
                                   "+----------------------------------------------------+";
    EXPECT_EQ(expectedContent2, UtilityFunctions::prettyPrintTupleBuffer(resultBuffer2, windowResultSchema));

    nodeEngine->stopQuery(0);
    testSink->cleanupBuffers();
}

/*
 * In this test we use a slide size that is equivalent to the slice size, to test if slicing works proper for small slides as well
 */
TEST_F(QueryExecutionTest, SlidingWindowQueryWindowSourcesize4slide2) {
    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        windowSchema,
        [&](OperatorId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return WindowSource::create(windowSchema,
                                        nodeEngine->getBufferManager(),
                                        nodeEngine->getQueryManager(),
                                        /*bufferCnt*/ 2,
                                        /*frequency*/ 0,
                                        successors);
        });
    auto query = TestQuery::from(windowSourceDescriptor);

    // 2. dd window operator:
    // 2.1 add Sliding window of size 10ms and with Slide 2ms and a sum aggregation on the value.
    auto windowType = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(4), Milliseconds(2));

    auto aggregation = Sum(Attribute("value"));
    query = query.window(windowType).byKey(Attribute("key")).apply(aggregation);

    // 3. add sink. We expect that this sink will receive one buffer
    //    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField(createField("key", INT64))
                                  ->addField("value", INT64);

    auto testSink = TestSink::create(/*expected result buffer*/ 1, windowResultSchema, nodeEngine->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    query.sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    Optimizer::DistributeWindowRulePtr distributeWindowRule = Optimizer::DistributeWindowRule::create();
    queryPlan = distributeWindowRule->apply(queryPlan);
    queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    std::cout << " plan=" << queryPlan->toString() << std::endl;
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();
    nodeEngine->registerQueryInNodeEngine(plan);
    nodeEngine->startQuery(0);

    // wait till all buffers have been produced
    testSink->completed.get_future().get();
    NES_INFO("QueryExecutionTest: The test sink contains " << testSink->getNumberOfResultBuffers() << " result buffers.");
    // get result buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1UL);

    auto resultBuffer = testSink->get(0);

    NES_INFO("QueryExecutionTest: The result buffer contains " << resultBuffer.getNumberOfTuples() << " tuples.");
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 2UL);
    NES_INFO("QueryExecutionTest: buffer=" << UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, windowResultSchema));
    std::string expectedContent = "+----------------------------------------------------+\n"
                                  "|start:UINT64|end:UINT64|key:INT64|value:INT64|\n"
                                  "+----------------------------------------------------+\n"
                                  "|2|6|1|10|\n"
                                  "|4|8|1|10|\n"
                                  "+----------------------------------------------------+";
    EXPECT_EQ(expectedContent, UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, windowResultSchema));
    nodeEngine->stopQuery(0);
    testSink->cleanupBuffers();
}

// P1 = Source1 -> filter1
// P2 = Source2 -> filter2
// P3 = [P1|P2] -> merge -> SINK
// So, merge is a blocking window_scan with two children.
TEST_F(QueryExecutionTest, DISABLED_mergeQuery) {
    // created buffer per source * number of sources
    uint64_t expectedBuf = 20;

    //auto testSource1 = createDefaultDataSourceWithSchemaForOneBuffer(testSchema, nodeEngine->getBufferManager(),
    //                                                                 nodeEngine->getQueryManager(), 1, 12);

    auto query1 = TestQuery::from(testSchema);

    query1 = query1.filter(Attribute("id") < 5);

    // creating P2
    // auto testSource2 = createDefaultDataSourceWithSchemaForOneBuffer(testSchema, nodeEngine->getBufferManager(),
    //                                                                 nodeEngine->getQueryManager(), 1, 12);
    auto query2 = TestQuery::from(testSchema).filter(Attribute("id") <= 5);

    // creating P3
    // merge does not change schema
    SchemaPtr ptr = std::make_shared<Schema>(testSchema);
    auto mergedQuery = query2.unionWith(&query1).sink(DummySink::create());

    auto testSink = std::make_shared<TestSink>(expectedBuf, testSchema, nodeEngine->getBufferManager());

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(mergedQuery.getQueryPlan());
    // auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    // auto generatableOperators = translatePhase->transform(queryPlan->getRootOperators()[0]);

    // nodeEngine->getQueryManager()->registerQuery(plan);

    // The plan should have three pipeline
    // EXPECT_EQ(plan->getNumberOfPipelines(), 3);

    // TODO switch to event time if that is ready to remove sleep
    auto memoryLayout = Runtime::DynamicMemoryLayout::DynamicRowLayout::create(testSchema, true);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    fillBuffer(buffer, memoryLayout);
    // TODO do not rely on sleeps
    // ingest test data
    //plan->setup();
    // plan->start(nodeEngine->getStateManager());
    Runtime::WorkerContext workerContext{1};
    //auto stage_0 = plan->getPipeline(0);
    //auto stage_1 = plan->getPipeline(1);
    for (int i = 0; i < 10; i++) {

        //  stage_0->execute(buffer, workerContext);// P1
        //  stage_1->execute(buffer, workerContext);// P2
        // Contfext -> Context 1 and Context 2;
        //
        // P1 -> P2 -> P3
        // P1 -> 10 tuples -> sum=10;
        // P2 -> 10 tuples -> sum=10;
        // P1 -> 10 tuples -> P2 -> sum =10;
        // P2 -> 20 tuples -> sum=20;
        // TODO why sleep here?
        sleep(1);
    }
    testSink->completed.get_future().get();
    //plan->stop();

    auto resultBuffer = testSink->get(0);
    // The output buffer should contain 5 tuple;
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 5UL);// how to interpret this?

    auto bindedRowLayoutResult = memoryLayout->bind(resultBuffer);
    auto recordIndexFields = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(0, bindedRowLayoutResult);

    for (auto recordIndex = 0L; recordIndex < 5L; ++recordIndex) {
        EXPECT_EQ(recordIndexFields[recordIndex], recordIndex);
    }

    testSink->cleanupBuffers();
}

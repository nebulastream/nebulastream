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

#include "../../../util/TestQuery.hpp"
#include "../../../util/TestQueryCompiler.hpp"
#include "gtest/gtest.h"
//#include <E2EBase.hpp>
#include <API/Query.hpp>
#include <API/Schema.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayout.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayoutBuffer.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayoutField.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <future>
#include <iostream>
#include <utility>

#include "../tests/util/DummySink.hpp"
#include "../tests/util/SchemaSourceDescriptor.hpp"
#include "../tests/util/TestQuery.hpp"
#include "../tests/util/TestQueryCompiler.hpp"
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

#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>

using namespace NES;
using Runtime::TupleBuffer;



class E2EBMExecutionTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("E2EBMExecutionTest.log", NES::LOG_DEBUG);
        NES_DEBUG("E2EBMExecutionTest: Setup QueryCatalogTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        // create test input buffer
        testSchema = Schema::create()
                         ->addField("test$id", BasicType::INT64)
                         ->addField("test$one", BasicType::INT64)
                         ->addField("test$value", BasicType::INT64);
        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
        nodeEngine = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 31337, streamConf);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("E2EBMExecutionTest: Tear down E2EBMExecutionTest test case.");
        nodeEngine->stop();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("E2EBMExecutionTest: Tear down E2EBMExecutionTest test class."); }

    SchemaPtr testSchema;
    Runtime::NodeEnginePtr nodeEngine;
    std::chrono::nanoseconds runtime;
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

        NES_DEBUG("E2EBMExecutionTest: source buffer=" << UtilityFunctions::prettyPrintTupleBuffer(buffer, schema));
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
        NES_DEBUG("E2EBMExecutionTest: TestSink: got buffer " << input_buffer);
        NES_DEBUG("E2EBMExecutionTest: PrettyPrintTupleBuffer"
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

TEST_F(E2EBMExecutionTest, filterQuery) {
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

    auto query = TestQuery::from(testSourceDescriptor).filter(Attribute("id") < 9).sink(testSinkDescriptor);

    //auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    //NES_DEBUG("Input Query Plan: " + (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    auto updatedQueryPlan = typeInferencePhase->execute(updatedPlan);
    //NES_DEBUG("Updated Query Plan: " + (updatedPlan)->toString());

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
    std::vector<Runtime::QueryStatistics*> statisticsVec;
    for (uint64_t i = 0; i < 1 + 1; ++i) {
        int64_t nextPeriodStartTime = 3 * 1000
            + std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        auto queryStatisticsPtrs = nodeEngine->getQueryStatistics(1);
        for (auto it : queryStatisticsPtrs) {

            auto* statistics = new Runtime::QueryStatistics(0, 0);
            statistics->setProcessedBuffers(it->getProcessedBuffers());
            statistics->setProcessedTasks(it->getProcessedTasks());
            statistics->setProcessedTuple(it->getProcessedTuple());

            statisticsVec.push_back(statistics);
            NES_WARNING("Statistic: " << it->getQueryStatisticsAsString());
        }
        auto curTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        while (curTime < nextPeriodStartTime) {
            curTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                          .count();
        }
    }
    NES_DEBUG("dump output=" << &statisticsVec << &nodeEngine);
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
    Runtime::WorkerContext workerContext{1, nodeEngine->getBufferManager()};
    NES_DEBUG("dump output=" << &statisticsVec << &nodeEngine);
    plan->getPipelines()[0]->execute(buffer, workerContext);
    NES_DEBUG("dump output=" << &statisticsVec << &nodeEngine);

    // This plan should produce one output buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    NES_DEBUG("dump output=" << &statisticsVec << &nodeEngine);
    std::cout << " Query Executed" << std::endl;

    auto resultBuffer = testSink->get(0);
    // The output buffer should contain 5 tuple;
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 9u);

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
TEST_F(E2EBMExecutionTest, filterQueryWoUpdate) {
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

    auto query = TestQuery::from(testSourceDescriptor).filter(Attribute("id") < 9).sink(testSinkDescriptor);

    //auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    // Execute
    //auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    //NES_DEBUG("Input Query Plan: " + (queryPlan)->toString());
    //const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    //auto updatedQueryPlan = typeInferencePhase->execute(updatedPlan);
    //NES_DEBUG("Updated Query Plan: " + (updatedPlan)->toString());

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
    std::vector<Runtime::QueryStatistics*> statisticsVec;

    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
    for (uint64_t i = 0; i < 1 + 1; ++i) {
        int64_t nextPeriodStartTime = 3 * 1000
            + std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        auto queryStatisticsPtrs = nodeEngine->getQueryStatistics(0);
        for (auto it : queryStatisticsPtrs) {

            auto* statistics = new Runtime::QueryStatistics(0, 0);
            statistics->setProcessedBuffers(it->getProcessedBuffers());
            statistics->setProcessedTasks(it->getProcessedTasks());
            statistics->setProcessedTuple(it->getProcessedTuple());

            statisticsVec.push_back(statistics);
            NES_WARNING("Statistic: " << it->getQueryStatisticsAsString());
        }
        auto curTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        while (curTime < nextPeriodStartTime) {
            curTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                          .count();
        }
    }
    Runtime::WorkerContext workerContext{1, nodeEngine->getBufferManager()};
    plan->getPipelines()[0]->execute(buffer, workerContext);

    // This plan should produce one output buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    NES_DEBUG("dump output=" << &statisticsVec << &nodeEngine);
    std::cout << " Query Executed" << std::endl;

    auto resultBuffer = testSink->get(0);
    // The output buffer should contain 5 tuple;
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 9u);

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

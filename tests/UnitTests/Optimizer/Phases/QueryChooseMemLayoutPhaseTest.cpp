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
#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Schema.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryChooseMemLayoutPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Runtime/MemoryLayout/DynamicColumnLayoutField.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayoutField.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>

using namespace NES;
using NES::Runtime::TupleBuffer;

namespace NES {

class QueryChooseMemLayoutPhaseTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("QueryChooseMemLayoutPhaseTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryChooseMemLayoutPhaseTest test case.");
    }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        NES_INFO("Setup QueryChooseMemLayoutPhaseTest test case.");

        testSchema = Schema::create()
                ->addField("test$id", BasicType::INT64)
                ->addField("test$one", BasicType::INT64)
                ->addField("test$value", BasicType::INT64);

        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
        nodeEngine = Runtime::create("127.0.0.1", 31337, streamConf);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("Tear down QueryChooseMemLayoutPhaseTest test case.");
        nodeEngine->stop();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down QueryChooseMemLayoutPhaseTest test class."); }

    SchemaPtr testSchema;
    Runtime::NodeEnginePtr nodeEngine;
};

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


void fillBufferRowLayout(TupleBuffer& buf, const Runtime::DynamicMemoryLayout::DynamicRowLayoutPtr& memoryLayout, uint64_t numberOfTuples) {

    auto bindedRowLayout = memoryLayout->bind(buf);
    auto recordIndexFields = RowLayoutField(int64_t, 0, bindedRowLayout);
    auto fields01 = RowLayoutField(int64_t, 1, bindedRowLayout);
    auto fields02 = RowLayoutField(int64_t, 2, bindedRowLayout);

    for (size_t recordIndex = 0; recordIndex < numberOfTuples; recordIndex++) {
        recordIndexFields[recordIndex] = recordIndex;
        fields01[recordIndex] = 100 + recordIndex;
        fields02[recordIndex] = 200 + recordIndex;
    }
    buf.setNumberOfTuples(numberOfTuples);
}

void fillBufferColLayout(TupleBuffer& buf, const Runtime::DynamicMemoryLayout::DynamicColumnLayoutPtr& memoryLayout, uint64_t numberOfTuples) {

    auto bindedColLayout = memoryLayout->bind(buf);
    auto recordIndexFields = ColLayoutField(int64_t, 0, bindedColLayout);
    auto fields01 = ColLayoutField(int64_t, 1, bindedColLayout);
    auto fields02 = ColLayoutField(int64_t, 2, bindedColLayout);

    for (size_t recordIndex = 0; recordIndex < numberOfTuples; recordIndex++) {
        recordIndexFields[recordIndex] = recordIndex;
        fields01[recordIndex] = 100 + recordIndex;
        fields02[recordIndex] = 200 + recordIndex;
    }
    buf.setNumberOfTuples(numberOfTuples);
}

TEST_F(QueryChooseMemLayoutPhaseTest, executeQueryChooseMemLayoutSimpleColLayout) {
    const uint64_t numbersOfBufferToProduce = 1000;
    const uint64_t frequency = 1000;

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(QueryParsingServicePtr());
    streamCatalog->removeLogicalStream("default_logical");
    streamCatalog->addLogicalStream("default_logical", inputSchema);

    auto source = LogicalOperatorFactory::createSourceOperator(DefaultSourceDescriptor::create(inputSchema,
                                                                                               numbersOfBufferToProduce,
                                                                                               frequency));
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f3") = Attribute("f1") * 42);
    auto sink = LogicalOperatorFactory::createSinkOperator(FileSinkDescriptor::create(""));

    auto plan = QueryPlan::create(source);
    plan->appendOperatorAsNewRoot(map);
    plan->appendOperatorAsNewRoot(sink);

    auto phase = Optimizer::QueryChooseMemLayoutPhase::create(Schema::COL_LAYOUT);
    phase->execute(plan);

    // Check that the source has a col layout
    for (auto& source : plan->getSourceOperators()) {

        source->inferSchema();
        if (source->getInputSchema()->getLayoutType() !=  Schema::COL_LAYOUT) {
            NES_ERROR("QueryChooseMemLayoutPhaseTest: source input schema = " << source->getInputSchema()->getLayoutTypeAsString());
            EXPECT_TRUE(false);
        } else {
            EXPECT_TRUE(true);
        }
    }
}

TEST_F(QueryChooseMemLayoutPhaseTest, executeQueryChooseMemLayoutSimpleRowLayout) {
    const uint64_t numbersOfBufferToProduce = 1000;
    const uint64_t frequency = 1000;

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(QueryParsingServicePtr());
    streamCatalog->removeLogicalStream("default_logical");
    streamCatalog->addLogicalStream("default_logical", inputSchema);

    auto source = LogicalOperatorFactory::createSourceOperator(DefaultSourceDescriptor::create(inputSchema,
                                                                                               numbersOfBufferToProduce,
                                                                                               frequency));
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f3") = Attribute("f1") * 42);
    auto sink = LogicalOperatorFactory::createSinkOperator(FileSinkDescriptor::create(""));

    auto plan = QueryPlan::create(source);
    plan->appendOperatorAsNewRoot(map);
    plan->appendOperatorAsNewRoot(sink);

    auto phase = Optimizer::QueryChooseMemLayoutPhase::create(Schema::ROW_LAYOUT);
    phase->execute(plan);

    // Check that the source has a row layout
    EXPECT_EQ(source->getInputSchema()->getLayoutType(), Schema::ROW_LAYOUT);
}

TEST_F(QueryChooseMemLayoutPhaseTest, queryChoooseMemLayoutPushBuffersColLayout) {
    const size_t numberOfTuples = 10;

    // Needing to set this layoutType to
    testSchema->setLayoutType(Schema::MemoryLayoutType::COL_LAYOUT);

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

    auto outputSchema = Schema::create(Schema::COL_LAYOUT)
                            ->addField("id", BasicType::INT64)
                            ->addField("field1", BasicType::INT64)
                            ->addField("field2", BasicType::INT64);
    auto testSink = std::make_shared<TestSink>(numberOfTuples, outputSchema, nodeEngine->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    // This filter does not exclude any tuple
    auto query = TestQuery::from(testSourceDescriptor).filter(Attribute("id") >= -10).sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto phase = Optimizer::QueryChooseMemLayoutPhase::create(Schema::COL_LAYOUT);

    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    phase->execute(queryPlan);

    NES_DEBUG("QueryChooseMemLayoutPhaseTest: queryPlan = " << queryPlan->toString());

    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();

    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 1U);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    auto memoryLayout = Runtime::DynamicMemoryLayout::DynamicColumnLayout::create(testSchema, true);
    fillBufferColLayout(buffer, memoryLayout, numberOfTuples);

    plan->setup();
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Deployed);
    plan->start(nodeEngine->getStateManager());
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
    Runtime::WorkerContext workerContext{1};
    plan->getPipelines()[0]->execute(buffer, workerContext);

    // This plan should produce one output buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1U);

    auto resultBuffer = testSink->get(0);
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), numberOfTuples);

    auto bindedColLayoutResult = memoryLayout->bind(resultBuffer);
    auto resultRecordIndexFields = ColLayoutField(int64_t, 0, bindedColLayoutResult);
    auto fields01 = ColLayoutField(int64_t, 1, bindedColLayoutResult);
    auto fields02 = ColLayoutField(int64_t, 2, bindedColLayoutResult);

    for (uint32_t recordIndex = 0U; recordIndex < numberOfTuples; ++recordIndex) {
        EXPECT_EQ(resultRecordIndexFields[recordIndex], recordIndex);
        EXPECT_EQ(fields01[recordIndex],  100 + recordIndex);
        EXPECT_EQ(fields02[recordIndex], 200 + recordIndex);
    }

    testSink->cleanupBuffers();
    buffer.release();
    plan->stop();

}

TEST_F(QueryChooseMemLayoutPhaseTest, queryChoooseMemLayoutPushBuffersRowLayout) {
    // TODO test here sending data in row layout and check if query works
}

}
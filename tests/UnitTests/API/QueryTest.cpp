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

#include <API/QueryAPI.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <gmock/gmock-matchers.h>
#include <iostream>

namespace NES {

class QueryTest : public testing::Test {
  public:
    SourceConfigPtr sourceConfig;

    static void SetUpTestCase() {
        NES::setupLogging("QueryTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {

        sourceConfig = SourceConfig::create();
        sourceConfig->setSourceConfig("");
        sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
        sourceConfig->setNumberOfBuffersToProduce(3);
        sourceConfig->setPhysicalStreamName("test2");
        sourceConfig->setLogicalStreamName("test_stream");
    }

    static void TearDownTestCase() { NES_INFO("Tear down QueryTest test class."); }

    void TearDown() override {}
};

TEST_F(QueryTest, testQueryFilter) {

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(QueryParsingServicePtr());
    streamCatalog->addPhysicalStream("default_logical", sce);

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical").filter(lessExpression).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1U);

    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalStreamSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);

    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);
}

TEST_F(QueryTest, testQueryProjection) {
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(QueryParsingServicePtr());
    streamCatalog->addPhysicalStream("default_logical", sce);

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto lessExpression = Attribute("id");
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical").project(Attribute("id"), Attribute("value")).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1U);

    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalStreamSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);

    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);
}

TEST_F(QueryTest, testQueryTumblingWindow) {
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(QueryParsingServicePtr());
    streamCatalog->addPhysicalStream("default_logical", sce);

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .window(TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)))
                      .byKey(Attribute("id"))
                      .apply(Sum(Attribute("value")))
                      .sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1U);

    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalStreamSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);

    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);
}

TEST_F(QueryTest, testQuerySlidingWindow) {
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(QueryParsingServicePtr());
    streamCatalog->addPhysicalStream("default_logical", sce);

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .window(SlidingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10), Seconds(2)))
                      .byKey(Attribute("id"))
                      .apply(Sum(Attribute("value")))
                      .sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1U);

    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalStreamSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);

    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);
}

/**
 * Merge two input stream: one with filter and one without filter.
 */
TEST_F(QueryTest, testQueryMerge) {
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);
    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(QueryParsingServicePtr());
    streamCatalog->addPhysicalStream("default_logical", sce);
    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto subQuery = Query::from("default_logical").filter(lessExpression);
    auto query = Query::from("default_logical").unionWith(&subQuery).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 2U);
    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalStreamSourceDescriptor>());
    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);
    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);
}

/**
 * Join two input stream: one with filter and one without filter.
 */
TEST_F(QueryTest, testQueryJoin) {
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);
    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(QueryParsingServicePtr());
    streamCatalog->addPhysicalStream("default_logical", sce);
    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto subQuery = Query::from("default_logical").filter(lessExpression);

    auto query = Query::from("default_logical")
                     .joinWith(subQuery)
                     .where(Attribute("id"))
                     .equalsTo(Attribute("id"))
                     .window(TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)))
                     .sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 2U);
    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalStreamSourceDescriptor>());
    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);
    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);
}

TEST_F(QueryTest, testQueryExpression) {
    auto andExpression = Attribute("f1") && 10;
    EXPECT_TRUE(andExpression->instanceOf<AndExpressionNode>());

    auto orExpression = Attribute("f1") || 45;
    EXPECT_TRUE(orExpression->instanceOf<OrExpressionNode>());

    auto lessExpression = Attribute("f1") < 45;
    EXPECT_TRUE(lessExpression->instanceOf<LessExpressionNode>());

    auto lessThenExpression = Attribute("f1") <= 45;
    EXPECT_TRUE(lessThenExpression->instanceOf<LessEqualsExpressionNode>());

    auto equalsExpression = Attribute("f1") == 45;
    EXPECT_TRUE(equalsExpression->instanceOf<EqualsExpressionNode>());

    auto greaterExpression = Attribute("f1") > 45;
    EXPECT_TRUE(greaterExpression->instanceOf<GreaterExpressionNode>());

    auto greaterThenExpression = Attribute("f1") >= 45;
    EXPECT_TRUE(greaterThenExpression->instanceOf<GreaterEqualsExpressionNode>());

    auto notEqualExpression = Attribute("f1") != 45;
    EXPECT_TRUE(notEqualExpression->instanceOf<NegateExpressionNode>());
    auto equals = notEqualExpression->as<NegateExpressionNode>()->child();
    EXPECT_TRUE(equals->instanceOf<EqualsExpressionNode>());

    auto assignmentExpression = Attribute("f1") = --Attribute("f1")++ + 10;
    ConsoleDumpHandler::create(std::cout)->dump(assignmentExpression);
    EXPECT_TRUE(assignmentExpression->instanceOf<FieldAssignmentExpressionNode>());
}

/**
 * @brief Test if the custom field set for aggregation using "as()" is set in the sink output schema
 */
TEST_F(QueryTest, windowAggregationWithAs) {
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);
    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(QueryParsingServicePtr());
    streamCatalog->addPhysicalStream("default_logical", sce);
    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    // create a query with "as" in the aggregation
    auto query = Query::from("default_logical")
                     .window(TumblingWindow::of(EventTime(Attribute("value")), Milliseconds(10)))
                     .byKey(Attribute("id", INT64))
                     .apply(Sum(Attribute("value", INT64))->as(Attribute("MY_OUTPUT_FIELD_NAME")))
                     .filter(Attribute("MY_OUTPUT_FIELD_NAME") > 1)
                     .sink(PrintSinkDescriptor::create());

    // only perform type inference phase to check if the modified aggregation field name is set in the output schema of the sink
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    // get the output schema of the sink
    const auto outputSchemaString = query.getQueryPlan()->getSinkOperators()[0]->getOutputSchema()->toString();
    NES_DEBUG("QueryExecutionTest:: WindowAggWithAs outputSchema: " << outputSchemaString);

    EXPECT_THAT(outputSchemaString, ::testing::HasSubstr("MY_OUTPUT_FIELD_NAME"));
}

}// namespace NES

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

#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Operators/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/Types/SlidingWindow.hpp>
#include <Operators/LogicalOperators/Windows/Types/ThresholdWindow.hpp>
#include <Operators/LogicalOperators/Windows/Types/TumblingWindow.hpp>
#include <Operators/LogicalOperators/Windows/Types/WindowType.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/DumpHandler/ConsoleDumpHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
#include <iostream>

namespace NES {

using namespace Configurations;

class QueryAPITest : public Testing::BaseUnitTest {
  public:
    PhysicalSourcePtr physicalSource;
    LogicalSourcePtr logicalSource;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        auto defaultSourceType = DefaultSourceType::create("test2", "test_source");
        physicalSource = PhysicalSource::create(defaultSourceType);
        logicalSource = LogicalSource::create("test2", Schema::create());
    }
};

TEST_F(QueryAPITest, testQueryFilter) {

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical").filter(lessExpression).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1U);

    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);

    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);
}

TEST_F(QueryAPITest, testQueryProjection) {

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto lessExpression = Attribute("id");
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical").project(Attribute("id"), Attribute("value")).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1U);

    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);

    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);
}

TEST_F(QueryAPITest, testQueryTumblingWindow) {

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
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);

    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_TRUE(sinkOptr->getSinkDescriptor()->instanceOf<PrintSinkDescriptor>());
}

TEST_F(QueryAPITest, testQuerySlidingWindow) {

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
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);

    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);
}

/**
 * Merge two input source: one with filter and one without filter.
 */
TEST_F(QueryAPITest, testQueryMerge) {

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto subQuery = Query::from("default_logical").filter(lessExpression);
    auto query = Query::from("default_logical").unionWith(subQuery).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 2U);
    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());
    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);
    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);
}

/**
 * Join two input source: one with filter and one without filter.
 */
TEST_F(QueryAPITest, testQueryJoin) {

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
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());
    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);
    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);
}

TEST_F(QueryAPITest, testQueryExpression) {
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
TEST_F(QueryAPITest, windowAggregationWithAs) {
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4, properties);
    Catalogs::Source::SourceCatalogEntryPtr sce =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);

    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    sourceCatalog->addPhysicalSource("default_logical", sce);

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    // create a query with "as" in the aggregation
    auto query =
        Query::from("default_logical")
            .window(TumblingWindow::of(EventTime(Attribute("value")), Milliseconds(10)))
            .byKey(Attribute("id", BasicType::INT64))
            .apply(Sum(Attribute("value", BasicType::INT64))->as(FieldAccessExpressionNode::create("MY_OUTPUT_FIELD_NAME")))
            .filter(Attribute("MY_OUTPUT_FIELD_NAME") > 1)
            .sink(PrintSinkDescriptor::create());

    Catalogs::UDF::UDFCatalogPtr udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
    // only perform type inference phase to check if the modified aggregation field name is set in the output schema of the sink
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    // get the output schema of the sink
    const auto outputSchemaString = query.getQueryPlan()->getSinkOperators()[0]->getOutputSchema()->toString();
    NES_DEBUG("QueryExecutionTest:: WindowAggWithAs outputSchema: {}", outputSchemaString);

    EXPECT_THAT(outputSchemaString, ::testing::HasSubstr("MY_OUTPUT_FIELD_NAME"));
}

/**
 * @brief Test if the system can create a logical query plan with a Threshold Window Operator
 */
TEST_F(QueryAPITest, ThresholdWindowQueryTest) {

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();

    // without by key
    auto query = Query::from("default_logical")
                     .window(ThresholdWindow::of(Attribute("f1") < 45))
                     .apply(Sum(Attribute("value", BasicType::INT64))->as(Attribute("MY_OUTPUT_FIELD_NAME")))
                     .sink(PrintSinkDescriptor::create());

    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1U);

    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);

    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);

    // with by key
    auto query2 = Query::from("default_logical")
                      .window(ThresholdWindow::of(Attribute("f1") < 45))
                      .byKey(Attribute("id", BasicType::INT64))
                      .apply(Sum(Attribute("value", BasicType::INT64))->as(Attribute("MY_OUTPUT_FIELD_NAME")))
                      .sink(PrintSinkDescriptor::create());

    auto plan2 = query2.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators2 = plan2->getSourceOperators();
    EXPECT_EQ(sourceOperators2.size(), 1U);

    SourceLogicalOperatorNodePtr srcOptr2 = sourceOperators2[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators2 = plan2->getSinkOperators();
    EXPECT_EQ(sinkOperators2.size(), 1U);

    SinkLogicalOperatorNodePtr sinkOptr2 = sinkOperators2[0];
    EXPECT_EQ(sinkOperators2.size(), 1U);
}

/**
 * @brief Test if the system can create a logical query plan with a Threshold Window Operator with minuium count
 */
TEST_F(QueryAPITest, ThresholdWindowQueryTestWithMinSupport) {

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();

    // without by key
    auto query = Query::from("default_logical")
                     .window(ThresholdWindow::of(Attribute("f1") < 45, 5))
                     .apply(Sum(Attribute("value", BasicType::INT64))->as(Attribute("MY_OUTPUT_FIELD_NAME")))
                     .sink(PrintSinkDescriptor::create());

    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1U);

    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);

    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);

    // with by key
    auto query2 = Query::from("default_logical")
                      .window(ThresholdWindow::of(Attribute("f1") < 45))
                      .byKey(Attribute("id", BasicType::INT64))
                      .apply(Sum(Attribute("value", BasicType::INT64))->as(Attribute("MY_OUTPUT_FIELD_NAME")))
                      .sink(PrintSinkDescriptor::create());

    auto plan2 = query2.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators2 = plan2->getSourceOperators();
    EXPECT_EQ(sourceOperators2.size(), 1U);

    SourceLogicalOperatorNodePtr srcOptr2 = sourceOperators2[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators2 = plan2->getSinkOperators();
    EXPECT_EQ(sinkOperators2.size(), 1U);

    SinkLogicalOperatorNodePtr sinkOptr2 = sinkOperators2[0];
    EXPECT_EQ(sinkOperators2.size(), 1U);
}

/**
 * @brief Test if the system can create a logical query plan with a Threshold Window Operator and minium count
 */
TEST_F(QueryAPITest, ThresholdWindowQueryTestwithKeyAndMinCount) {

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();

    // without by key
    auto query = Query::from("default_logical")
                     .window(ThresholdWindow::of(Attribute("f1") < 45, 5))
                     .apply(Sum(Attribute("value", BasicType::INT64))->as(Attribute("MY_OUTPUT_FIELD_NAME")))
                     .sink(PrintSinkDescriptor::create());

    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1U);

    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);

    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);

    // with by key
    auto query2 = Query::from("default_logical")
                      .window(ThresholdWindow::of(Attribute("f1") < 45, 5))
                      .byKey(Attribute("id", BasicType::INT64))
                      .apply(Sum(Attribute("value", BasicType::INT64))->as(Attribute("MY_OUTPUT_FIELD_NAME")))
                      .sink(PrintSinkDescriptor::create());

    auto plan2 = query2.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators2 = plan2->getSourceOperators();
    EXPECT_EQ(sourceOperators2.size(), 1U);

    SourceLogicalOperatorNodePtr srcOptr2 = sourceOperators2[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators2 = plan2->getSinkOperators();
    EXPECT_EQ(sinkOperators2.size(), 1U);

    SinkLogicalOperatorNodePtr sinkOptr2 = sinkOperators2[0];
    EXPECT_EQ(sinkOperators2.size(), 1U);
}

}// namespace NES

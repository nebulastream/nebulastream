#include "gtest/gtest.h"

#include <API/Query.hpp>
#include <API/Window/TimeCharacteristic.hpp>
#include <API/Window/WindowAggregation.hpp>
#include <API/Window/WindowType.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <iostream>

namespace NES {

class QueryTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("QueryTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryTest test class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down QueryTest test class." << std::endl;
    }

    void TearDown() {
    }
};

TEST_F(QueryTest, testQueryFilter) {

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(/**Source Type**/ "DefaultSource", /**Source Config**/ "",
        /**Source Frequence**/ 1, /**Number Of Tuples To Produce Per Buffer**/ 0,
        /**Number of Buffers To Produce**/ 3, /**Physical Stream Name**/ "test2",
        /**Logical Stream Name**/ "test_stream");

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->addPhysicalStream("default_logical", sce);

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical").filter(lessExpression).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1);

    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalStreamSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1);

    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1);
}

TEST_F(QueryTest, testQueryWindow) {
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(/**Source Type**/ "DefaultSource", /**Source Config**/ "",
        /**Source Frequence**/ 1, /**Number Of Tuples To Produce Per Buffer**/ 0,
        /**Number of Buffers To Produce**/ 3, /**Physical Stream Name**/ "test2",
        /**Logical Stream Name**/ "test_stream");

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->addPhysicalStream("default_logical", sce);

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .window(
                          TumblingWindow::of(TimeCharacteristic::createProcessingTime(), Seconds(10)),
                          Sum::on(Attribute("value")))
                      .sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1);

    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalStreamSourceDescriptor>());

    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1);

    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1);
}

/**
 * Merge two input stream: one with filter and one without filter.
 */
TEST_F(QueryTest, testQueryMerge) {
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(/**Source Type**/ "DefaultSource", /**Source Config**/ "",
        /**Source Frequence**/ 1, /**Number Of Tuples To Produce Per Buffer**/ 0,
        /**Number of Buffers To Produce**/ 3, /**Physical Stream Name**/ "test2",
        /**Logical Stream Name**/ "test_stream");
    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->addPhysicalStream("default_logical", sce);
    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto subQuery = Query::from("default_logical").filter(lessExpression);
    auto query = Query::from("default_logical").merge(&subQuery).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 2);
    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalStreamSourceDescriptor>());
    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1);
    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1);
}

TEST_F(QueryTest, testQueryExpression) {
    auto andExpression = Attribute("f1") && 10;
    ASSERT_TRUE(andExpression->instanceOf<AndExpressionNode>());

    auto orExpression = Attribute("f1") || 45;
    ASSERT_TRUE(orExpression->instanceOf<OrExpressionNode>());

    auto lessExpression = Attribute("f1") < 45;
    ASSERT_TRUE(lessExpression->instanceOf<LessExpressionNode>());

    auto lessThenExpression = Attribute("f1") <= 45;
    ASSERT_TRUE(lessThenExpression->instanceOf<LessEqualsExpressionNode>());

    auto equalsExpression = Attribute("f1") == 45;
    ASSERT_TRUE(equalsExpression->instanceOf<EqualsExpressionNode>());

    auto greaterExpression = Attribute("f1") > 45;
    ASSERT_TRUE(greaterExpression->instanceOf<GreaterExpressionNode>());

    auto greaterThenExpression = Attribute("f1") >= 45;
    ASSERT_TRUE(greaterThenExpression->instanceOf<GreaterEqualsExpressionNode>());

    auto notEqualExpression = Attribute("f1") != 45;
    ASSERT_TRUE(notEqualExpression->instanceOf<NegateExpressionNode>());
    auto equals = notEqualExpression->as<NegateExpressionNode>()->child();
    ASSERT_TRUE(equals->instanceOf<EqualsExpressionNode>());

    auto assignmentExpression = Attribute("f1") = --Attribute("f1")++ + 10;
    ConsoleDumpHandler::create()->dump(assignmentExpression, std::cout);
    ASSERT_TRUE(assignmentExpression->instanceOf<FieldAssignmentExpressionNode>());
}

}// namespace NES

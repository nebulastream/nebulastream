#include "gtest/gtest.h"

#include <iostream>
#include <Util/Logger.hpp>
#include <QueryCompiler/HandCodedQueryExecutionPlan.hpp>

#include <API/Query.hpp>
#include <API/Types/DataTypes.hpp>
#include <Nodes/Expressions/FieldReadExpressionNode.hpp>
#include <Nodes/Expressions/BinaryExpressions/LessThenExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>

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

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField(
        "value", BasicType::UINT64);

    Stream def = Stream("default_logical", schema);

    auto fieldRead = FieldReadExpressionNode::create(createDataType(INT64), "field_1");
    auto constant = ConstantValueExpressionNode::create(createBasicTypeValue(BasicType::INT64, "10"));
    auto filterPredicate = LessThenExpressionNode::create(fieldRead, constant);
    Query& query = Query::from(def).filter(filterPredicate).print(std::cout);

    const std::vector<SourceLogicalOperatorNodePtr>& sourceOperators = query.getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1);

    SourceLogicalOperatorNodePtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getDataSource()->getSchema()->equals(schema));

    const std::vector<SinkLogicalOperatorNodePtr>& sinkOperators = query.getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1);
    
    SinkLogicalOperatorNodePtr sinkOptr = sinkOperators[0];

    const std::vector<NodePtr>& children = sinkOptr->getChildren();
    EXPECT_EQ(sinkOperators.size(), 1);
}

}  // namespace NES


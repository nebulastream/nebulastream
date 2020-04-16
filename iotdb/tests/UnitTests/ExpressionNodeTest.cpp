#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>

namespace NES {

class ExpressionNodeTest : public testing::Test {
  public:
    void SetUp() {

    }

    void TearDown() {
        NES_DEBUG("Tear down ExpressionNodeTest Test.")
    }

  protected:

    static void setupLogging() {
        NES::setupLogging("ExpressionNodeTest.log", NES::LOG_DEBUG);
        NES_DEBUG("Setup ExpressionNodeTest test class.");
    }
};

TEST_F(ExpressionNodeTest, predicateConstruction) {
    auto left = ConstantValueExpressionNode::create(createBasicTypeValue(BasicType::INT64, "10"));
    ASSERT_FALSE(left->isPredicate());
    auto right = ConstantValueExpressionNode::create(createBasicTypeValue(BasicType::INT64, "11"));
    ASSERT_FALSE(right->isPredicate());
    auto expression = EqualsExpressionNode::create(left, right);

    // check if expression is a predicate
    ASSERT_TRUE(expression->isPredicate());
    auto fieldRead = FieldAccessExpressionNode::create(createDataType(INT64), "field_1");
    auto constant = ConstantValueExpressionNode::create(createBasicTypeValue(BasicType::INT64, "10"));
    auto lessThen = LessEqualsExpressionNode::create(fieldRead, constant);
    ASSERT_TRUE(lessThen->isPredicate());

    auto andExpression = AndExpressionNode::create(expression, lessThen);
    ConsoleDumpHandler::create()->dump(andExpression, std::cout);
    ASSERT_TRUE(andExpression->isPredicate());
}

} // namespace NES

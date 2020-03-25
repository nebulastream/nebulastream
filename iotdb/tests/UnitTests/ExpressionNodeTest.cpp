#include <gtest/gtest.h>
#include <iostream>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Util/Logger.hpp>
#include <memory>
#include <Nodes/Expressions/BinaryExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/BinaryExpressions/LessThenExpressionNode.hpp>
#include <Nodes/Expressions/FieldReadExpressionNode.hpp>
#include <Nodes/Expressions/BinaryExpressions/AndExpressionNode.hpp>

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
    auto fieldRead = FieldReadExpressionNode::create(createDataType(INT64), "field_1");
    auto constant = ConstantValueExpressionNode::create(createBasicTypeValue(BasicType::INT64, "10"));
    auto lessThen = LessThenExpressionNode::create(fieldRead, constant);
    ASSERT_TRUE(lessThen->isPredicate());

    auto andExpression = AndExpressionNode::create(expression, lessThen);
    andExpression->prettyPrint(std::cout);
    ASSERT_TRUE(andExpression->isPredicate());
}

} // namespace NES

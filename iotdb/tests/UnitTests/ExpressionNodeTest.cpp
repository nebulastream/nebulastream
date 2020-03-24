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
        NES_DEBUG("Tear down LogicalOperatorNode Test.")
    }

  protected:

    static void setupLogging() {
        // create PatternLayout
        log4cxx::LayoutPtr layoutPtr(
            new log4cxx::PatternLayout(
                "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

        // create FileAppender
        LOG4CXX_DECODE_CHAR(fileName, "ExpressionNodeTest.log");
        log4cxx::FileAppenderPtr file(
            new log4cxx::FileAppender(layoutPtr, fileName));

        // create ConsoleAppender
        log4cxx::ConsoleAppenderPtr console(
            new log4cxx::ConsoleAppender(layoutPtr));

        // set log level
        NESLogger->setLevel(log4cxx::Level::getDebug());

        // add appenders and other will inherit the settings
        NESLogger->addAppender(file);
        NESLogger->addAppender(console);
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

#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <API/Query.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
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
        NES_DEBUG("Tear down ExpressionNodeTest Test.");
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

TEST_F(ExpressionNodeTest, attributeStampInference) {
    auto schema = Schema::create()
        ->addField("f1", INT8);

    auto attribute = Attribute("f1").getExpressionNode();
    // check if the attribute field is initially undefined
    ASSERT_TRUE(attribute->getStamp()->isUndefined());

    // infer stamp using schema
    attribute->inferStamp(schema);
    ASSERT_TRUE(attribute->getStamp()->isEqual(createDataType(INT8)));

    // test inference with undefined attribute
    auto notValidAttribute = Attribute("f2").getExpressionNode();

    ASSERT_TRUE(notValidAttribute->getStamp()->isUndefined());
    // we expect that this call throws an exception
    ASSERT_ANY_THROW(notValidAttribute->inferStamp(schema));
}

TEST_F(ExpressionNodeTest, inferenceExpressionTest) {
    auto schema = Schema::create()
        ->addField("f1", INT8)
        ->addField("f2", INT64)
        ->addField("f3", FLOAT64)
        ->addField("f3", createArrayDataType(BOOLEAN, 10));

    auto addExpression = Attribute("f1") + 10;
    ASSERT_TRUE(addExpression->getStamp()->isUndefined());
    addExpression->inferStamp(schema);
    ASSERT_TRUE(addExpression->getStamp()->isEqual(createDataType(INT32)));

    auto mulExpression = Attribute("f2")*10;
    ASSERT_TRUE(mulExpression->getStamp()->isUndefined());
    mulExpression->inferStamp(schema);
    ASSERT_TRUE(mulExpression->getStamp()->isEqual(createDataType(INT64)));

    auto increment = Attribute("f3")++;
    ASSERT_TRUE(increment->getStamp()->isUndefined());
    increment->inferStamp(schema);
    ASSERT_TRUE(increment->getStamp()->isEqual(createDataType(FLOAT64)));

    // We expect that you can't increment an array
    auto incrementArray = Attribute("f4")++;
    ASSERT_TRUE(incrementArray->getStamp()->isUndefined());
    ASSERT_ANY_THROW(incrementArray->inferStamp(schema));
}

TEST_F(ExpressionNodeTest, inferPredicateTest) {
    auto schema = Schema::create()
        ->addField("f1", INT8)
        ->addField("f2", INT64)
        ->addField("f3", BOOLEAN)
        ->addField("f3", createArrayDataType(BOOLEAN, 10));

    auto equalsExpression = Attribute("f1") == 10;
    equalsExpression->inferStamp(schema);
    ASSERT_TRUE(equalsExpression->isPredicate());

    auto lessExpression = Attribute("f2") < 10;
    lessExpression->inferStamp(schema);
    ASSERT_TRUE(lessExpression->isPredicate());

    auto negateBoolean = !Attribute("f3");
    negateBoolean->inferStamp(schema);
    ASSERT_TRUE(negateBoolean->isPredicate());

    // you cant negate non boolean.
    auto negateInteger = !Attribute("f1");
    ASSERT_ANY_THROW(negateInteger->inferStamp(schema));

    auto andExpression = Attribute("f3") && true;
    andExpression->inferStamp(schema);
    ASSERT_TRUE(andExpression->isPredicate());

    auto orExpression = Attribute("f3") || Attribute("f3");
    orExpression->inferStamp(schema);
    ASSERT_TRUE(orExpression->isPredicate());
    // you cant make a logical expression between non boolean.
    auto orIntegerExpression = Attribute("f1") || Attribute("f2");
    ASSERT_ANY_THROW(negateInteger->inferStamp(schema));
}

TEST_F(ExpressionNodeTest, inferAssertionTest) {
    auto schema = Schema::create()
        ->addField("f1", INT8)
        ->addField("f2", INT64)
        ->addField("f3", BOOLEAN)
        ->addField("f3", createArrayDataType(BOOLEAN, 10));

    auto assertion = Attribute("f1") = 10*(33 + Attribute("f1"));
    assertion->inferStamp(schema);
    ASSERT_TRUE(assertion->getField()->getStamp()->isEqual(createDataType(INT32)));
}

} // namespace NES

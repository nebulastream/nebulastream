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
#include <API/Schema.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <NesBaseTest.hpp>
#include <iostream>
#include <memory>
#include <stdint.h>

namespace NES {

class ExpressionNodeTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() { setupLogging(); }

    void SetUp() override {}

    void TearDown() override { NES_DEBUG("Tear down ExpressionNodeTest Test."); }

  protected:
    static void setupLogging() {
        NES::setupLogging("ExpressionNodeTest.log", NES::LOG_DEBUG);
        NES_DEBUG("Setup ExpressionNodeTest test class.");
    }
};

TEST_F(ExpressionNodeTest, predicateConstruction) {
    auto left = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT64, "10"));
    ASSERT_FALSE(left->isPredicate());
    auto right = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT64, "11"));
    ASSERT_FALSE(right->isPredicate());
    auto expression = EqualsExpressionNode::create(left, right);

    // check if expression is a predicate
    EXPECT_TRUE(expression->isPredicate());
    auto fieldRead = FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "field_1");
    auto constant = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT64, "10"));
    auto lessThen = LessEqualsExpressionNode::create(fieldRead, constant);
    EXPECT_TRUE(lessThen->isPredicate());

    auto andExpression = AndExpressionNode::create(expression, lessThen);
    ConsoleDumpHandler::create(std::cout)->dump(andExpression);
    EXPECT_TRUE(andExpression->isPredicate());
}

TEST_F(ExpressionNodeTest, attributeStampInference) {
    auto schema = Schema::create()->addField("test$f1", INT8);

    auto attribute = Attribute("f1").getExpressionNode();
    // check if the attribute field is initially undefined
    EXPECT_TRUE(attribute->getStamp()->isUndefined());

    // infer stamp using schema
    attribute->inferStamp(schema);
    EXPECT_TRUE(attribute->getStamp()->isEquals(DataTypeFactory::createInt8()));

    // test inference with undefined attribute
    auto notValidAttribute = Attribute("f2").getExpressionNode();

    EXPECT_TRUE(notValidAttribute->getStamp()->isUndefined());
    // we expect that this call throws an exception
    ASSERT_ANY_THROW(notValidAttribute->inferStamp(schema));
}

TEST_F(ExpressionNodeTest, inferenceExpressionTest) {
    auto schema = Schema::create()
                      ->addField("test$f1", INT8)
                      ->addField("test$f2", INT64)
                      ->addField("test$f3", FLOAT64)
                      ->addField("test$f4", DataTypeFactory::createArray(10, DataTypeFactory::createBoolean()));

    auto addExpression = Attribute("f1") + 10;
    EXPECT_TRUE(addExpression->getStamp()->isUndefined());
    addExpression->inferStamp(schema);
    EXPECT_TRUE(addExpression->getStamp()->isEquals(DataTypeFactory::createType(INT32)));

    auto mulExpression = Attribute("f2") * 10;
    EXPECT_TRUE(mulExpression->getStamp()->isUndefined());
    mulExpression->inferStamp(schema);
    EXPECT_TRUE(mulExpression->getStamp()->isEquals(DataTypeFactory::createType(INT64)));

    auto increment = Attribute("f3")++;
    EXPECT_TRUE(increment->getStamp()->isUndefined());
    increment->inferStamp(schema);
    EXPECT_TRUE(increment->getStamp()->isEquals(DataTypeFactory::createType(FLOAT64)));

    // We expect that you can't increment an array
    auto incrementArray = Attribute("f4")++;
    EXPECT_TRUE(incrementArray->getStamp()->isUndefined());
    ASSERT_ANY_THROW(incrementArray->inferStamp(schema));
}

TEST_F(ExpressionNodeTest, inferPredicateTest) {
    auto schema = Schema::create()
                      ->addField("test$f1", INT8)
                      ->addField("test$f2", INT64)
                      ->addField("test$f3", BOOLEAN)
                      ->addField("test$f4", DataTypeFactory::createArray(10, DataTypeFactory::createBoolean()));

    auto equalsExpression = Attribute("f1") == 10;
    equalsExpression->inferStamp(schema);
    EXPECT_TRUE(equalsExpression->isPredicate());

    auto lessExpression = Attribute("f2") < 10;
    lessExpression->inferStamp(schema);
    EXPECT_TRUE(lessExpression->isPredicate());

    auto negateBoolean = !Attribute("f3");
    negateBoolean->inferStamp(schema);
    EXPECT_TRUE(negateBoolean->isPredicate());

    // you cant negate non boolean.
    auto negateInteger = !Attribute("f1");
    ASSERT_ANY_THROW(negateInteger->inferStamp(schema));

    auto andExpression = Attribute("f3") && true;
    andExpression->inferStamp(schema);
    EXPECT_TRUE(andExpression->isPredicate());

    auto orExpression = Attribute("f3") || Attribute("f3");
    orExpression->inferStamp(schema);
    EXPECT_TRUE(orExpression->isPredicate());
    // you cant make a logical expression between non boolean.
    auto orIntegerExpression = Attribute("f1") || Attribute("f2");
    ASSERT_ANY_THROW(negateInteger->inferStamp(schema));
}

TEST_F(ExpressionNodeTest, inferAssertionTest) {
    auto schema = Schema::create()
                      ->addField("test$f1", INT8)
                      ->addField("test$f2", INT64)
                      ->addField("test$f3", BOOLEAN)
                      ->addField("test$f4", DataTypeFactory::createArray(10, DataTypeFactory::createBoolean()));

    auto assertion = Attribute("f1") = 10 * (33 + Attribute("f1"));
    assertion->inferStamp(schema);
    EXPECT_TRUE(assertion->getField()->getStamp()->isEquals(DataTypeFactory::createType(INT8)));
}

TEST_F(ExpressionNodeTest, multiplicationInferStampTest) {
    auto schema = Schema::create()->addField("test$left", UINT32)->addField("test$right", INT16);

    auto multiplicationNode = Attribute("left") * Attribute("right");
    multiplicationNode->inferStamp(schema);
    ASSERT_TRUE(multiplicationNode->getStamp()->isInteger());
    auto intStamp = DataType::as<Integer>(multiplicationNode->getStamp());
    int bits = intStamp->getBits();
    int64_t lowerBound = intStamp->lowerBound;
    int64_t upperBound = intStamp->upperBound;
    EXPECT_TRUE(bits == 32);
    EXPECT_TRUE(lowerBound == INT16_MIN);
    EXPECT_TRUE(upperBound == UINT32_MAX);
    //TODO: question about the bits in stamps: Does this result of our stamp inference make sense? A 32 bit integer can not store all values between [INT16_MIN,UINT32_MAX]
}

/**
 * @brief Test behaviour of special ModExpressionNode::inferStamp function. (integers)
 */
TEST_F(ExpressionNodeTest, moduloIntegerInferStampTest) {
    auto schema = Schema::create()->addField("test$left", UINT32)->addField("test$right", INT16);

    auto moduloNode = MOD(Attribute("left"), Attribute("right"));
    moduloNode->inferStamp(schema);
    ASSERT_TRUE(moduloNode->getStamp()->isInteger());

    auto intStamp = DataType::as<Integer>(moduloNode->getStamp());
    int bits = intStamp->getBits();
    int64_t lowerBound = intStamp->lowerBound;
    int64_t upperBound = intStamp->upperBound;
    NES_INFO(upperBound);
    EXPECT_EQ(bits, 32);
    EXPECT_EQ(lowerBound, 0);
    EXPECT_EQ(
        upperBound,
        -INT16_MIN
            - 1);// e.g. when calculating MOD(..., -128) the result will always be in range [-127, 127]. And no other INT8 divisor will yield a wider range than -128 (=INT8_MIN).
    EXPECT_EQ(upperBound, INT16_MAX);// equivalent to above
};

/**
 * @brief Test behaviour of special ModExpressionNode::inferStamp function. (float)
 */
TEST_F(ExpressionNodeTest, moduloFloatInferStampTest) {
    auto schema = Schema::create()->addField("test$left", UINT32)->addField("test$right", FLOAT32);

    auto moduloNode = MOD(Attribute("left"), Attribute("right"));
    moduloNode->inferStamp(schema);
    ASSERT_TRUE(moduloNode->getStamp()->isFloat());

    auto floatStamp = DataType::as<Float>(moduloNode->getStamp());
    int bits = floatStamp->getBits();
    int64_t lowerBound = floatStamp->lowerBound;
    int64_t upperBound = floatStamp->upperBound;
    NES_INFO(upperBound);
    EXPECT_EQ(bits, 32);
    EXPECT_EQ(lowerBound, 0);         // == lower bound of UINT32, as it is is higher than range spanned by Float divisor
    EXPECT_EQ(upperBound, UINT32_MAX);// == upper bound of UINT32, as it  is lower than range spanned by Float divisor
}

}// namespace NES

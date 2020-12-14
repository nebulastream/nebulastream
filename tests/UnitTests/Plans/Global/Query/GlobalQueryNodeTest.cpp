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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>

using namespace NES;

class GlobalQueryNodeTest : public testing::Test {

  public:
    /* Will be called before a test is executed. */
    void SetUp() {
        setupLogging("GlobalQueryNodeTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup GlobalQueryNodeTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() { NES_INFO("Setup GlobalQueryNodeTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down GlobalQueryNodeTest test class."); }
};

/**
 * @brief This test is for validating different behaviour for an empty global query node
 */
TEST_F(GlobalQueryNodeTest, testCreateEmptyGlobalQueryNode) {
    GlobalQueryNodePtr emptyGlobalQueryNode = GlobalQueryNode::createEmpty(1);

    auto expression = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
    auto filter = LogicalOperatorFactory::createFilterOperator(expression);
    NES_DEBUG("GlobalQueryNodeTest: Empty global query node should not have any operator");
    EXPECT_TRUE(emptyGlobalQueryNode->hasOperator(filter) == nullptr);
}

/**
 * @brief This test is for validating different behaviours of a regular global query node
 */
TEST_F(GlobalQueryNodeTest, testCreateRegularGlobalQueryNode) {

    auto expression = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
    auto filter = LogicalOperatorFactory::createFilterOperator(expression);
    uint64_t globalQueryNodeId = 1;
    GlobalQueryNodePtr globalQueryNode = GlobalQueryNode::create(globalQueryNodeId, filter);

    NES_DEBUG(
        "GlobalQueryNodeTest: A newly created  global query node should return non null object when asked if it has operator");
    EXPECT_TRUE(globalQueryNode->hasOperator(filter) != nullptr);
}

/**
 * @brief This test is for validating different behaviours of a regular global query node for new operator addition
 */
TEST_F(GlobalQueryNodeTest, testCreateRegularGlobalQueryNodeAndCheckWithOtherEqualOperator) {

    auto expression = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
    auto filter = LogicalOperatorFactory::createFilterOperator(expression);
    GlobalQueryNodePtr globalQueryNode = GlobalQueryNode::create(1, filter);

    NES_DEBUG(
        "GlobalQueryNodeTest: A newly created  global query node should return non null object when asked if it has operator");
    EXPECT_TRUE(globalQueryNode->hasOperator(filter) != nullptr);

    auto expression2 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
    auto filter2 = LogicalOperatorFactory::createFilterOperator(expression2);
    EXPECT_TRUE(globalQueryNode->hasOperator(filter2) != nullptr);
}

/**
 * @brief This test is for validating different behaviours of a regular global query node
 */
TEST_F(GlobalQueryNodeTest, testCreateRegularGlobalQueryNodeAndCheckWithOtherUnequalOperator) {

    auto expression = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
    auto filter = LogicalOperatorFactory::createFilterOperator(expression);
    GlobalQueryNodePtr globalQueryNode = GlobalQueryNode::create(1, filter);

    NES_DEBUG(
        "GlobalQueryNodeTest: A newly created  global query node should return non null object when asked if it has operator");
    EXPECT_TRUE(globalQueryNode->hasOperator(filter) != nullptr);

    auto expression2 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "6"));
    auto filter2 = LogicalOperatorFactory::createFilterOperator(expression2);
    EXPECT_TRUE(globalQueryNode->hasOperator(filter2) == nullptr);
}

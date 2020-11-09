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
    void TearDown() {
        NES_INFO("Setup GlobalQueryNodeTest test case.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_INFO("Tear down GlobalQueryNodeTest test class.");
    }
};

/**
 * @brief This test is for validating different behaviour for an empty global query node
 */
TEST_F(GlobalQueryNodeTest, testCreateEmptyGlobalQueryNode) {
    GlobalQueryNodePtr emptyGlobalQueryNode = GlobalQueryNode::createEmpty(1);
    NES_DEBUG("GlobalQueryNodeTest: Empty global query node should return true when asked if it is empty");
    EXPECT_TRUE(emptyGlobalQueryNode->isEmpty());
    NES_DEBUG("GlobalQueryNodeTest: Empty global query node should have nothing to update");
    EXPECT_FALSE(emptyGlobalQueryNode->hasNewUpdate());
}

/**
 * @brief This test is for validating different behaviours of a regular global query node
 */
TEST_F(GlobalQueryNodeTest, testCreateRegularGlobalQueryNode) {

    const ExpressionNodePtr& ptr = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
    const LogicalOperatorNodePtr& filterOptr = LogicalOperatorFactory::createFilterOperator(ptr);
    uint64_t globalQueryNodeId = 1;
    GlobalQueryNodePtr globalQueryNode = GlobalQueryNode::create(globalQueryNodeId, 1, filterOptr);
    NES_DEBUG("GlobalQueryNodeTest: A newly created  global query node should return false when asked if it is empty");
    EXPECT_FALSE(globalQueryNode->isEmpty());
    NES_DEBUG("GlobalQueryNodeTest: A newly created global query node should have something to update");
    EXPECT_TRUE(globalQueryNode->hasNewUpdate());

    NES_DEBUG("GlobalQueryNodeTest: Find the list of global query node that have a logical operator of type FilterLogicalOperatorNode");
    std::vector<GlobalQueryNodePtr> globalQueryNodes;
    globalQueryNode->getNodesWithTypeHelper<FilterLogicalOperatorNode>(globalQueryNodes);
    EXPECT_TRUE(globalQueryNodes.size() == 1);
    EXPECT_TRUE(globalQueryNodes[0]->getId() == globalQueryNodeId);
}

/**
 * @brief This test is for validating different behaviours of a regular global query node for new operator addition
 */
TEST_F(GlobalQueryNodeTest, testCreateRegularGlobalQueryNodeAndAddOperator) {

    const ExpressionNodePtr ptr1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
    const LogicalOperatorNodePtr filterOptr1 = LogicalOperatorFactory::createFilterOperator(ptr1);
    GlobalQueryNodePtr globalQueryNode = GlobalQueryNode::create(1, 1, filterOptr1);
    NES_DEBUG("GlobalQueryNodeTest: A newly created  global query node should return false when asked if it is empty");
    EXPECT_FALSE(globalQueryNode->isEmpty());
    NES_DEBUG("GlobalQueryNodeTest: A newly created global query node should have something to update");
    EXPECT_TRUE(globalQueryNode->hasNewUpdate());
    globalQueryNode->markAsUpdated();
    NES_DEBUG("GlobalQueryNodeTest: A global query node should no have anything to update once it is marked as updated");
    EXPECT_FALSE(globalQueryNode->hasNewUpdate());

    const ExpressionNodePtr ptr2 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
    const LogicalOperatorNodePtr& filterOptr2 = LogicalOperatorFactory::createFilterOperator(ptr2);
    globalQueryNode->addQueryAndOperator(2, filterOptr2);
    NES_DEBUG("GlobalQueryNodeTest: Global query node should have something to update after a new query and operator is added");
    EXPECT_TRUE(globalQueryNode->hasNewUpdate());
}

/**
 * @brief This test is for validating different behaviours of a regular global query node for deletion of query
 */
TEST_F(GlobalQueryNodeTest, testCreateRegularGlobalQueryNodeAndRemoveQuery) {

    const ExpressionNodePtr ptr1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
    const LogicalOperatorNodePtr filterOptr1 = LogicalOperatorFactory::createFilterOperator(ptr1);
    GlobalQueryNodePtr globalQueryNode = GlobalQueryNode::create(1, 1, filterOptr1);
    NES_DEBUG("GlobalQueryNodeTest: A newly created  global query node should return false when asked if it is empty");
    EXPECT_FALSE(globalQueryNode->isEmpty());
    NES_DEBUG("GlobalQueryNodeTest: A newly created global query node should have something to update");
    EXPECT_TRUE(globalQueryNode->hasNewUpdate());
    globalQueryNode->markAsUpdated();
    NES_DEBUG("GlobalQueryNodeTest: A global query node should no have anything to update once it is marked as updated");
    EXPECT_FALSE(globalQueryNode->hasNewUpdate());

    bool success = globalQueryNode->removeQuery(1);
    EXPECT_TRUE(success);
    NES_DEBUG("GlobalQueryNodeTest: Global query node should have something to update after a query deletion");
    EXPECT_TRUE(globalQueryNode->hasNewUpdate());
    EXPECT_TRUE(globalQueryNode->isEmpty());
}

/**
 * @brief This test is for validating different behaviours of a regular global query node for deletion of query
 */
TEST_F(GlobalQueryNodeTest, testCreateRegularGlobalQueryNodeAndAddAnewQueryAndRemoveTheAddedQuery) {

    const ExpressionNodePtr ptr1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
    const LogicalOperatorNodePtr filterOptr1 = LogicalOperatorFactory::createFilterOperator(ptr1);
    GlobalQueryNodePtr globalQueryNode = GlobalQueryNode::create(1, 1, filterOptr1);
    NES_DEBUG("GlobalQueryNodeTest: A newly created  global query node should return false when asked if it is empty");
    EXPECT_FALSE(globalQueryNode->isEmpty());
    NES_DEBUG("GlobalQueryNodeTest: A newly created global query node should have something to update");
    EXPECT_TRUE(globalQueryNode->hasNewUpdate());
    globalQueryNode->markAsUpdated();
    NES_DEBUG("GlobalQueryNodeTest: A global query node should no have anything to update once it is marked as updated");
    EXPECT_FALSE(globalQueryNode->hasNewUpdate());

    globalQueryNode->addQueryAndOperator(2, filterOptr1);
    NES_DEBUG("GlobalQueryNodeTest: Global query node should have something to update after a new query is added");
    bool success = globalQueryNode->removeQuery(2);
    EXPECT_TRUE(success);
    NES_DEBUG("GlobalQueryNodeTest: Global query node should not be empty");
    EXPECT_FALSE(globalQueryNode->isEmpty());
}

/**
 * @brief This test is for validating different behaviours of a regular global query node for deletion of query
 */
TEST_F(GlobalQueryNodeTest, testCreateRegularGlobalQueryNodeAndAddAnewQueryAndRemoveAllQueries) {

    const ExpressionNodePtr ptr1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
    const LogicalOperatorNodePtr filterOptr1 = LogicalOperatorFactory::createFilterOperator(ptr1);
    GlobalQueryNodePtr globalQueryNode = GlobalQueryNode::create(1, 1, filterOptr1);
    NES_DEBUG("GlobalQueryNodeTest: A newly created  global query node should return false when asked if it is empty");
    EXPECT_FALSE(globalQueryNode->isEmpty());
    NES_DEBUG("GlobalQueryNodeTest: A newly created global query node should have something to update");
    EXPECT_TRUE(globalQueryNode->hasNewUpdate());
    globalQueryNode->markAsUpdated();
    NES_DEBUG("GlobalQueryNodeTest: A global query node should no have anything to update once it is marked as updated");
    EXPECT_FALSE(globalQueryNode->hasNewUpdate());

    globalQueryNode->addQueryAndOperator(2, filterOptr1);
    NES_DEBUG("GlobalQueryNodeTest: Global query node should have something to update after a new query is added");
    bool success = globalQueryNode->removeQuery(2);
    EXPECT_TRUE(success);
    success = globalQueryNode->removeQuery(1);
    EXPECT_TRUE(success);
    NES_DEBUG("GlobalQueryNodeTest: Global query node should be empty after removing all queries");
    EXPECT_TRUE(globalQueryNode->isEmpty());
}

/**
 * @brief This test is for validating different behaviours of a regular global query node for addition and checking if the operator already exists
 */
TEST_F(GlobalQueryNodeTest, testCreateRegularGlobalQueryNodeAndCheckIfSimilarOperatorExists) {

    const ExpressionNodePtr ptr1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
    const LogicalOperatorNodePtr filterOptr1 = LogicalOperatorFactory::createFilterOperator(ptr1);
    GlobalQueryNodePtr globalQueryNode = GlobalQueryNode::create(1, 1, filterOptr1);
    NES_DEBUG("GlobalQueryNodeTest: A newly created  global query node should return false when asked if it is empty");
    EXPECT_FALSE(globalQueryNode->isEmpty());
    NES_DEBUG("GlobalQueryNodeTest: A newly created global query node should have something to update");
    EXPECT_TRUE(globalQueryNode->hasNewUpdate());
    globalQueryNode->markAsUpdated();
    NES_DEBUG("GlobalQueryNodeTest: A global query node should no have anything to update once it is marked as updated");
    EXPECT_FALSE(globalQueryNode->hasNewUpdate());

    const ExpressionNodePtr ptr2 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
    const LogicalOperatorNodePtr filterOptr2 = LogicalOperatorFactory::createFilterOperator(ptr1);

    OperatorNodePtr existingOptr = globalQueryNode->hasOperator(filterOptr2);
    EXPECT_TRUE(existingOptr != nullptr);
    globalQueryNode->addQueryAndOperator(2, existingOptr);
    NES_DEBUG("GlobalQueryNodeTest: Global query node should have something to update after a new query is added");
    bool success = globalQueryNode->removeQuery(2);
    EXPECT_TRUE(success);
    success = globalQueryNode->removeQuery(1);
    EXPECT_TRUE(success);
    NES_DEBUG("GlobalQueryNodeTest: Global query node should be empty after removing all queries");
    EXPECT_TRUE(globalQueryNode->isEmpty());
}

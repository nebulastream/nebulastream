#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
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
    const LogicalOperatorNodePtr& filterOptr = createFilterLogicalOperatorNode(ptr);
    uint64_t globalQueryNodeId = 1;
    GlobalQueryNodePtr globalQueryNode = GlobalQueryNode::create(globalQueryNodeId, "Q1", filterOptr);
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
 * @brief This test is for validating different behaviours of a regular global query node
 */
TEST_F(GlobalQueryNodeTest, testCreateRegularGlobalQueryNodeAndAddOperator) {

    const ExpressionNodePtr& ptr = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
    const LogicalOperatorNodePtr& filterOptr = createFilterLogicalOperatorNode(ptr);
    GlobalQueryNodePtr globalQueryNode = GlobalQueryNode::create(1, "Q1", filterOptr);
    NES_DEBUG("GlobalQueryNodeTest: A newly created  global query node should return false when asked if it is empty");
    EXPECT_FALSE(globalQueryNode->isEmpty());
    NES_DEBUG("GlobalQueryNodeTest: A newly created global query node should have something to update");
    EXPECT_TRUE(globalQueryNode->hasNewUpdate());

    globalQueryNode->addQuery("2");
}

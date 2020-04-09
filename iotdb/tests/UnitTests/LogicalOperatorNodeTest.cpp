#include <gtest/gtest.h>
#include <iostream>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/SourceLogicalOperatorNode.hpp>
#include <Nodes/Util/DumpContext.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Util/Logger.hpp>

#include <QueryCompiler/HandCodedQueryExecutionPlan.hpp>
#include <API/InputQuery.hpp>
#include <API/UserAPIExpression.hpp>
#include <Operators/Operator.hpp>
#include <Operators/Impl/FilterOperator.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <SourceSink/DefaultSource.hpp>
#include <memory>

#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <Nodes/Phases/TranslateToLegacyPlanPhase.hpp>
#include <Nodes/Expressions/FieldReadExpressionNode.hpp>
#include <Nodes/Expressions/BinaryExpressions/EqualsExpressionNode.hpp>

namespace NES {

class LogicalOperatorNodeTest : public testing::Test {
  public:
    void SetUp() {
        dumpContext = DumpContext::create();
        dumpContext->registerDumpHandler(ConsoleDumpHandler::create());

        sPtr = StreamCatalog::instance().getStreamForLogicalStreamOrThrowException("default_logical");
        source = std::make_shared<DefaultSource>(sPtr->getSchema(), 0, 0);

        pred1 = ConstantValueExpressionNode::create(createBasicTypeValue(BasicType::INT8, "1"));
        pred2 = ConstantValueExpressionNode::create(createBasicTypeValue(BasicType::INT8, "2"));
        pred3 = ConstantValueExpressionNode::create(createBasicTypeValue(BasicType::INT8, "3"));
        pred4 = ConstantValueExpressionNode::create(createBasicTypeValue(BasicType::INT8, "4"));
        pred5 = ConstantValueExpressionNode::create(createBasicTypeValue(BasicType::INT8, "5"));
        pred6 = ConstantValueExpressionNode::create(createBasicTypeValue(BasicType::INT8, "6"));
        pred7 = ConstantValueExpressionNode::create(createBasicTypeValue(BasicType::INT8, "7"));

        sourceOp = createSourceLogicalOperatorNode(source);
        filterOp1 = createFilterLogicalOperatorNode(pred1);
        filterOp3 = createFilterLogicalOperatorNode(pred3);
        filterOp2 = createFilterLogicalOperatorNode(pred2);

        filterOp4 = createFilterLogicalOperatorNode(pred4);
        filterOp5 = createFilterLogicalOperatorNode(pred5);
        filterOp6 = createFilterLogicalOperatorNode(pred6);
        filterOp7 = createFilterLogicalOperatorNode(pred7);

        filterOp1Copy = createFilterLogicalOperatorNode(pred1);
        filterOp2Copy = createFilterLogicalOperatorNode(pred2);
        filterOp3Copy = createFilterLogicalOperatorNode(pred3);
        filterOp4Copy = createFilterLogicalOperatorNode(pred4);
        filterOp5Copy = createFilterLogicalOperatorNode(pred5);
        filterOp6Copy = createFilterLogicalOperatorNode(pred6);
        filterOp7Copy = createFilterLogicalOperatorNode(pred7);

        removed = false;
        replaced = false;
        children.clear();
        parents.clear();
    }

    void TearDown() {
        NES_DEBUG("Tear down LogicalOperatorNode Test.")
    }

  protected:
    bool removed;
    bool replaced;
    StreamPtr sPtr;
    DataSourcePtr source;
    DumpContextPtr dumpContext;

    ExpressionNodePtr pred1, pred2, pred3, pred4, pred5, pred6, pred7;
    NodePtr sourceOp;

    NodePtr filterOp1, filterOp2, filterOp3, filterOp4, filterOp5, filterOp6, filterOp7;
    NodePtr filterOp1Copy, filterOp2Copy, filterOp3Copy, filterOp4Copy, filterOp5Copy, filterOp6Copy, filterOp7Copy;

    std::vector<NodePtr> children{};
    std::vector<NodePtr> parents{};

    static void setupLogging() {
        // create PatternLayout
        log4cxx::LayoutPtr layoutPtr(
            new log4cxx::PatternLayout(
                "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

        // create FileAppender
        LOG4CXX_DECODE_CHAR(fileName, "WindowManager.log");
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

TEST_F(LogicalOperatorNodeTest, getSuccessors) {
    // std::cout << "filterOp1's use counts: " << filterOp1.use_count() << std::endl;
    // auto x = filterOp1->getptr();
    // std::cout << filterOp1.use_count() << std::endl;
    // std::cout << "x's use counts: " << x.use_count() << std::endl;

    sourceOp->addChild(filterOp1);
    sourceOp->addChild(filterOp2);

    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 2);

    children.push_back(filterOp3);
    EXPECT_EQ(children.size(), 3);

    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 2);
}

TEST_F(LogicalOperatorNodeTest, getPredecessors) {
    filterOp3->addParent(filterOp1);

    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1);

    parents.push_back(filterOp2);
    EXPECT_EQ(parents.size(), 2);

    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1);
}

TEST_F(LogicalOperatorNodeTest, addSelfAsSuccessor) {
    sourceOp->addChild(filterOp1);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1);

    sourceOp->addChild(sourceOp);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1);
}

TEST_F(LogicalOperatorNodeTest, addAndRemoveSingleSuccessor) {
    sourceOp->addChild(filterOp1);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1);

    removed = sourceOp->removeChild(filterOp1);
    EXPECT_TRUE(removed);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 0);
}

TEST_F(LogicalOperatorNodeTest, addAndRemoveMultipleSuccessors) {
    sourceOp->addChild(filterOp1);
    sourceOp->addChild(filterOp2);
    sourceOp->addChild(filterOp3);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 3);

    removed = sourceOp->removeChild(filterOp1);
    EXPECT_TRUE(removed);
    removed = sourceOp->removeChild(filterOp2);
    EXPECT_TRUE(removed);
    removed = sourceOp->removeChild(filterOp3);
    EXPECT_TRUE(removed);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 0);
}

TEST_F(LogicalOperatorNodeTest, addAndRmoveDuplicatedSuccessors) {
    sourceOp->addChild(filterOp1);
    sourceOp->addChild(filterOp1Copy);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1);

    removed = sourceOp->removeChild(filterOp1);
    EXPECT_TRUE(removed);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 0);

    removed = sourceOp->removeChild(filterOp1Copy);
    EXPECT_FALSE(removed);
    sourceOp->addChild(filterOp1Copy);
    EXPECT_EQ(children.size(), 0);
}

TEST_F(LogicalOperatorNodeTest, DISABLED_addAndRemoveNullSuccessor) {
    // assertion fail due to nullptr
    sourceOp->addChild(filterOp1);
    EXPECT_EQ(children.size(), 1);
    sourceOp->addChild(nullptr);
    removed = sourceOp->removeChild(nullptr);
}

TEST_F(LogicalOperatorNodeTest, addSelfAsPredecessor) {
    filterOp3->addParent(filterOp1);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1);

    filterOp3->addParent(filterOp3);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1);
}

TEST_F(LogicalOperatorNodeTest, addAndRemoveSinglePredecessor) {
    filterOp3->addParent(filterOp1);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1);

    removed = filterOp3->removeParent(filterOp1);
    EXPECT_TRUE(removed);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 0);
}

TEST_F(LogicalOperatorNodeTest, addAndRemoveMultiplePredecessors) {
    filterOp3->addParent(filterOp1);
    filterOp3->addParent(filterOp2);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 2);

    removed = filterOp3->removeParent(filterOp1);
    EXPECT_TRUE(removed);
    removed = filterOp3->removeParent(filterOp2);
    EXPECT_TRUE(removed);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 0);

}

TEST_F(LogicalOperatorNodeTest, addAndRemoveDuplicatedPredecessors) {
    filterOp3->addParent(filterOp1);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1);

    filterOp3->addParent(filterOp1Copy);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1);

    removed = filterOp3->removeParent(filterOp1);
    EXPECT_TRUE(removed);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 0);

    removed = filterOp3->removeParent(filterOp1Copy);
    EXPECT_FALSE(removed);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 0);
}

TEST_F(LogicalOperatorNodeTest, consistencyBetweenSuccessorPredecesorRelation1) {
    filterOp3->addParent(filterOp1);
    filterOp3->addParent(filterOp2);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 2);
    children = filterOp1->getChildren();
    EXPECT_EQ(children.size(), 1);
    children = filterOp2->getChildren();
    EXPECT_EQ(children.size(), 1);

    filterOp3->removeParent(filterOp1);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1);

    children = filterOp1->getChildren();
    std::cout << "children of filterOp1" << std::endl;
    for (auto&& op : children) {
        std::cout << op->toString() << std::endl;
    }
    std::cout << "================================================================================\n";
    EXPECT_EQ(children.size(), 0);

    children = filterOp2->getChildren();
    EXPECT_EQ(children.size(), 1);

    filterOp3->removeParent(filterOp2);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 0);

    children = filterOp1->getChildren();
    EXPECT_EQ(children.size(), 0);
    children = filterOp2->getChildren();
    EXPECT_EQ(children.size(), 0);
}

TEST_F(LogicalOperatorNodeTest, consistencyBetweenSuccessorPredecesorRelation2) {
    filterOp3->addChild(filterOp1);
    filterOp3->addChild(filterOp2);
    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 2);
    parents = filterOp1->getParents();
    EXPECT_EQ(parents.size(), 1);
    parents = filterOp2->getParents();
    EXPECT_EQ(parents.size(), 1);

    filterOp3->removeChild(filterOp1);
    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 1);
    parents = filterOp1->getParents();
    EXPECT_EQ(parents.size(), 0);
    parents = filterOp2->getParents();
    EXPECT_EQ(parents.size(), 1);

    filterOp3->removeChild(filterOp2);
    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 0);
    parents = filterOp1->getParents();
    EXPECT_EQ(parents.size(), 0);
    parents = filterOp2->getParents();
    EXPECT_EQ(parents.size(), 0);
}

TEST_F(LogicalOperatorNodeTest, DISABLED_addAndRemoveNullPredecessor) {
    // assertion failed due to nullptr
    filterOp3->addParent(filterOp1);
    filterOp3->addParent(nullptr);
    filterOp3->removeParent(nullptr);
}

/**
 * @brief replace filterOp1 with filterOp3
 * original: sourceOp -> filterOp1 -> filterOp2
 *           filterOp3 -> filterOp4
 * replaced: sourceOp -> filterOp3 -> filterOp2
 *                                |-> filterOp4
 */
TEST_F(LogicalOperatorNodeTest, replaceSuccessor) {
    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 0);
    filterOp3->addChild(filterOp4);

    sourceOp->addChild(filterOp1);
    filterOp1->addChild(filterOp2);

    sourceOp->replace(filterOp3, filterOp1);

    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1);
    EXPECT_TRUE(children[0]->equal(filterOp3));

    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 2);
    EXPECT_TRUE(children[0]->equal(filterOp4));
    EXPECT_TRUE(children[1]->equal(filterOp2));
}

/**
 * @brief replace filterOp1 with filterOp1Copy
 * original: sourceOp -> filterOp1 -> filterOp2
 * replaced: sourceOp -> filterOp1Copy -> filterOp2
 */

TEST_F(LogicalOperatorNodeTest, replaceWithEqualSuccessor) {
    children = filterOp1Copy->getChildren();
    EXPECT_EQ(children.size(), 0);

    sourceOp->addChild(filterOp1);
    filterOp1->addChild(filterOp2);

    sourceOp->replace(filterOp1Copy, filterOp1);

    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1);

    children = filterOp1Copy->getChildren();
    EXPECT_EQ(children.size(), 1);
}

/**
 * @brief replace filterOp1 with filterOp3
 * original: sourceOp -> filterOp1 -> filterOp2
 *                   |-> filterOp3 -> filterOp4
 * replaced: sourceOp -> filterOp1 -> filterOp2
 *                   |-> filterOp3 -> filterOp4
 */

TEST_F(LogicalOperatorNodeTest, replaceWithExistedSuccessor) {
    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 0);

    sourceOp->addChild(filterOp1);
    sourceOp->addChild(filterOp3);
    filterOp1->addChild(filterOp2);
    filterOp3->addChild(filterOp4);

    replaced = sourceOp->replace(filterOp3, filterOp1);
    EXPECT_FALSE(replaced);

    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 2);
    EXPECT_TRUE(children[0]->equal(filterOp1));
    EXPECT_TRUE(children[1]->equal(filterOp3));

    children = filterOp1->getChildren();
    EXPECT_EQ(children.size(), 1);
    EXPECT_TRUE(children[0]->equal(filterOp2));

    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 1);
    EXPECT_TRUE(children[0]->equal(filterOp4));
}

/**
 * @brief replace filterOp1 with filterOp1
 * original: sourceOp -> filterOp1 -> filterOp2
 * replaced: sourceOp -> filterOp1 -> filterOp2
 */
TEST_F(LogicalOperatorNodeTest, replaceWithIdenticalSuccessor) {
    children = filterOp1->getChildren();
    EXPECT_EQ(children.size(), 0);

    sourceOp->addChild(filterOp1);
    filterOp1->addChild(filterOp2);

    sourceOp->replace(filterOp1, filterOp1);

    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1);

    children = filterOp1->getChildren();
    EXPECT_EQ(children.size(), 1);
}

/**
 * @brief replace filterOp1 with filterOp2
 * original: sourceOp -> filterOp1 -> filterOp2
 * replaced: sourceOp -> filterOp2
 */
TEST_F(LogicalOperatorNodeTest, replaceWithSubSuccessor) {
    children = filterOp2->getChildren();
    EXPECT_EQ(children.size(), 0);

    sourceOp->addChild(filterOp1);
    filterOp1->addChild(filterOp2);

    sourceOp->replace(filterOp2, filterOp1);

    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1);
    EXPECT_TRUE(children[0]->equal(filterOp2));

    children = filterOp2->getChildren();
    EXPECT_EQ(children.size(), 0);
}

/**
 * @brief replace filterOp3 (not existed) with filterOp3
 * original: sourceOp -> filterOp1 -> filterOp2
 * replaced: sourceOp -> filterOp1 -> filterOp2
 */
TEST_F(LogicalOperatorNodeTest, replaceWithNoneSuccessor) {
    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 0);

    sourceOp->addChild(filterOp1);
    filterOp1->addChild(filterOp2);

    sourceOp->replace(filterOp3, filterOp3);

    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1);
    EXPECT_TRUE(children[0]->equal(filterOp1));

    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 0);
}

TEST_F(LogicalOperatorNodeTest, DISABLED_replaceSuccessorInvalidOldOperator) {
    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 0);

    sourceOp->addChild(filterOp1);
    filterOp1->addChild(filterOp2);

    sourceOp->replace(filterOp3, nullptr);
}

TEST_F(LogicalOperatorNodeTest, DISABLED_replaceWithWithInvalidNewOperator) {
    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 0);

    sourceOp->addChild(filterOp1);
    filterOp1->addChild(filterOp2);

    sourceOp->replace(nullptr, filterOp1);
}

/**
 * @brief replace filterOp1 with filterOp3
 * original: sourceOp <- filterOp1 <- filterOp2
 * replaced: sourceOp <- filterOp3 <- filterOp2
 */
TEST_F(LogicalOperatorNodeTest, replacePredecessor) {
    parents = filterOp3->getParents();

    EXPECT_EQ(parents.size(), 0);

    filterOp2->addParent(filterOp1);
    filterOp1->addParent(sourceOp);

    filterOp2->replace(filterOp3, filterOp1);

    parents = filterOp2->getParents();
    EXPECT_EQ(parents.size(), 1);
    EXPECT_EQ(parents[0].get(), filterOp3.get());

    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1);
}

/**
 * @brief replace filterOp1 with filterOp1Copy
 * original: sourceOp -> filterOp1 -> filterOp2
 * replaced: sourceOp -> filterOp1Copy -> filterOp2
 */
TEST_F(LogicalOperatorNodeTest, replaceWithEqualPredecessor) {
    parents = filterOp1Copy->getParents();
    EXPECT_EQ(parents.size(), 0);

    filterOp2->addParent(filterOp1);
    filterOp1->addParent(sourceOp);

    parents = filterOp1->getParents();
    EXPECT_EQ(parents.size(), 1);

    filterOp2->replace(filterOp1Copy, filterOp1);

    parents = filterOp2->getParents();
    EXPECT_EQ(parents.size(), 1);
    EXPECT_NE(parents[0].get(), filterOp1.get());
    EXPECT_EQ(parents[0].get(), filterOp1Copy.get());

    parents = filterOp1Copy->getParents();
    EXPECT_EQ(parents.size(), 1);
    EXPECT_EQ(parents[0].get(), sourceOp.get());
}
/**
 * @brief replace filterOp1 with filterOp1
 * original: sourceOp -> filterOp1 -> filterOp2
 * replaced: sourceOp -> filterOp1 -> filterOp2
 */
TEST_F(LogicalOperatorNodeTest, replaceWithIdenticalPredecessor) {
    parents = filterOp1->getParents();
    EXPECT_EQ(parents.size(), 0);

    filterOp2->addParent(filterOp1);
    filterOp1->addParent(sourceOp);

    parents = filterOp1->getParents();
    EXPECT_EQ(parents.size(), 1);

    filterOp2->replace(filterOp1, filterOp1);

    parents = filterOp2->getParents();
    EXPECT_EQ(parents.size(), 1);
    EXPECT_EQ(parents[0].get(), filterOp1.get());

    parents = filterOp1->getParents();
    EXPECT_EQ(parents.size(), 1);
    EXPECT_EQ(parents[0].get(), sourceOp.get());
}

/**
 * @brief remove filterOp1 from topology
 * original: sourceOp -> filterOp1 -> filterOp2
 *                                |-> filterOp3
 * desired: sourceOp -> filterOp2
 *                  |-> filterOp3
 */
TEST_F(LogicalOperatorNodeTest, removeExistedAndLevelUpSuccessors) {
    sourceOp->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp3);

    sourceOp->removeAndLevelUpChildren(filterOp1);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 2);
    EXPECT_TRUE(children[0]->equal(filterOp2));
    EXPECT_TRUE(children[1]->equal(filterOp3));
}

/**
 * @brief remove filterOp4 from topology
 * original: sourceOp -> filterOp1 -> filterOp2
 *                                |-> filterOp3
 *
 * desired: sourceOp -> filterOp1 -> filterOp2
 *                               |-> filterOp3
 */
TEST_F(LogicalOperatorNodeTest, removeNotExistedAndLevelUpSuccessors) {
    sourceOp->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp3);

    removed = sourceOp->removeAndLevelUpChildren(filterOp4);
    EXPECT_FALSE(removed);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1);
    EXPECT_TRUE(children[0]->equal(filterOp1));
}

/**
 * @brief remove filterOp1 from topology
 *
 *                                /-> filterOp3'
 * original: sourceOp -> filterOp1 -> filterOp2
 *                   \-> filterOp3
 *
 * desired:  unchanged
 *
 *
 */
TEST_F(LogicalOperatorNodeTest, removeExistedSblingAndLevelUpSuccessors) {
    sourceOp->addChild(filterOp1);
    sourceOp->addChild(filterOp3);

    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp3Copy);

    removed = sourceOp->removeAndLevelUpChildren(filterOp1);
    EXPECT_FALSE(removed);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 2);
    EXPECT_TRUE(children[0]->equal(filterOp1));
    EXPECT_TRUE(children[1]->equal(filterOp3));
}

TEST_F(LogicalOperatorNodeTest, remove) {
    filterOp2->addParent(filterOp1);
    // filterOp3 neither in filterOp2's children nor parents
    removed = filterOp2->remove(filterOp3);
    EXPECT_FALSE(removed);
    // filterOp1 in filterOp2's parents
    removed = filterOp2->remove(filterOp1);
    EXPECT_TRUE(removed);

    filterOp2->addChild(filterOp3);
    removed = filterOp2->remove(filterOp3);
    EXPECT_TRUE(removed);
}

TEST_F(LogicalOperatorNodeTest, clear) {
    filterOp2->addParent(filterOp1);
    filterOp2->addChild(filterOp3);
    parents = filterOp2->getParents();
    children = filterOp2->getChildren();
    EXPECT_EQ(children.size(), 1);
    EXPECT_EQ(parents.size(), 1);

    filterOp2->clear();
    parents = filterOp2->getParents();
    children = filterOp2->getChildren();
    EXPECT_EQ(children.size(), 0);
    EXPECT_EQ(parents.size(), 0);
}

/**
 *
 *                                  /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *                     \-> filterOp3
 *
 *
 *                                    /-> filterOp4'
 * topology2: filterOp6' -> filterOp1' -> filterOp2'
 *                      \-> filterOp3'
 *
 */
TEST_F(LogicalOperatorNodeTest, equalWithAllSuccessors1) {
    bool same = false;

    EXPECT_TRUE(filterOp1->equal(filterOp1Copy));
    EXPECT_TRUE(filterOp2->equal(filterOp2Copy));
    EXPECT_TRUE(filterOp3->equal(filterOp3Copy));
    EXPECT_TRUE(filterOp4->equal(filterOp4Copy));
    EXPECT_TRUE(filterOp6->equal(filterOp6Copy));

    // topology1
    filterOp6->addChild(filterOp1);
    filterOp6->addChild(filterOp3);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);

    // topology2
    filterOp6Copy->addChild(filterOp1Copy);
    filterOp6Copy->addChild(filterOp3Copy);
    filterOp1Copy->addChild(filterOp4Copy);
    filterOp1Copy->addChild(filterOp2Copy);

    same = filterOp6->equalWithAllChildren(filterOp6Copy);
    EXPECT_TRUE(same);
}

/**
 *
 *                                  /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *                     \-> filterOp3
 *
 *                                  /-> filterOp4'
 * topology3: sourceOp -> filterOp1' -> filterOp2'
 *                     \-> filterOp3'
 *
 */
TEST_F(LogicalOperatorNodeTest, equalWithAllSuccessors2) {
    bool same = true;
    // topology1
    filterOp6->addChild(filterOp1);
    filterOp6->addChild(filterOp3);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);

    // topology3
    sourceOp->addChild(filterOp1Copy);
    sourceOp->addChild(filterOp3Copy);
    filterOp1Copy->addChild(filterOp4Copy);
    filterOp1Copy->addChild(filterOp2Copy);

    same = filterOp6->equalWithAllChildren(sourceOp);
    EXPECT_FALSE(same);
}

/**
 *
 *                                  /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *                     \-> filterOp3
 *
 *                                    /-> filterOp5'
 * topology4: filterOp6' -> filterOp1' -> filterOp2'
 *                      \-> filterOp3'
 *
 */
TEST_F(LogicalOperatorNodeTest, equalWithAllSuccessors3) {
    bool same = true;
    // topology1
    filterOp6->addChild(filterOp3);
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);

    // topology4
    filterOp6Copy->addChild(filterOp1Copy);
    filterOp6Copy->addChild(filterOp3Copy);
    filterOp1Copy->addChild(filterOp2Copy);
    filterOp1Copy->addChild(filterOp5Copy);

    same = filterOp6->equalWithAllChildren(filterOp6Copy);
    EXPECT_FALSE(same);
}


/**
 *
 *                                  /<- filterOp4
 * topology1: filterOp6 <- filterOp1 <- filterOp2
 *                     \<- filterOp3
 *
 *
 *                                    /<- filterOp4'
 * topology2: filterOp6' <- filterOp1' <- filterOp2'
 *                      \<- filterOp3'
 *
 */
TEST_F(LogicalOperatorNodeTest, equalWithAllPredecessors1) {
    bool same = false;

    // topology1
    filterOp2->addParent(filterOp1);
    filterOp4->addParent(filterOp1);
    filterOp1->addParent(filterOp6);
    filterOp3->addParent(filterOp6);

    // topology2
    filterOp2Copy->addParent(filterOp1Copy);
    filterOp4Copy->addParent(filterOp1Copy);
    filterOp3Copy->addParent(filterOp6Copy);
    filterOp1Copy->addParent(filterOp6Copy);

    same = filterOp4->equalWithAllParents(filterOp4Copy);
    EXPECT_TRUE(same);
    same = filterOp2->equalWithAllParents(filterOp2Copy);
    EXPECT_TRUE(same);
    same = filterOp3->equalWithAllParents(filterOp3Copy);
    EXPECT_TRUE(same);
}

/**
 *
 *                                  /<- filterOp4
 * topology1: filterOp6 <- filterOp1 <- filterOp2
 *                     \<- filterOp3
 *
 *                                  /<- filterOp4'
 * topology3: sourceOp <- filterOp1' <- filterOp2'
 *                     \<- filterOp3'
 *
 */
TEST_F(LogicalOperatorNodeTest, equalWithAllPredecessors2) {
    bool same = false;

    // topology1
    filterOp2->addParent(filterOp1);
    filterOp4->addParent(filterOp1);
    filterOp1->addParent(filterOp6);
    filterOp3->addParent(filterOp6);

    // topology3
    filterOp4Copy->addParent(filterOp1Copy);
    filterOp2Copy->addParent(filterOp1Copy);
    filterOp1Copy->addParent(sourceOp);
    filterOp3Copy->addParent(sourceOp);

    same = filterOp4->equalWithAllParents(filterOp4Copy);
    EXPECT_FALSE(same);
    same = filterOp2->equalWithAllParents(filterOp2Copy);
    EXPECT_FALSE(same);
    same = filterOp3->equalWithAllParents(filterOp3Copy);
    EXPECT_FALSE(same);
}

/**
 *                                  /<- filterOp4
 * topology1: filterOp6 <- filterOp1 <- filterOp2
 *                     \<- filterOp3
 *
 *
 *
 *                                    /<- filterOp5'
 * topology4: filterOp6' <- filterOp1' <- filterOp2'
 *                      \<- filterOp3'
 */
TEST_F(LogicalOperatorNodeTest, equalWithAllPredecessors3) {
    bool same = false;
    // topology1
    filterOp2->addParent(filterOp1);
    filterOp4->addParent(filterOp1);
    filterOp1->addParent(filterOp6);
    filterOp3->addParent(filterOp6);

    // topology4
    filterOp5Copy->addParent(filterOp1Copy);
    filterOp2Copy->addParent(filterOp1Copy);
    filterOp3Copy->addParent(filterOp6Copy);
    filterOp1Copy->addParent(filterOp6Copy);

    same = filterOp5->equalWithAllParents(filterOp4Copy);
    EXPECT_FALSE(same);

    same = filterOp2->equalWithAllParents(filterOp2Copy);
    EXPECT_TRUE(same);
}


// TODO: add more operator casting
TEST_F(LogicalOperatorNodeTest, as) {
    NodePtr base2 = filterOp1;
    FilterLogicalOperatorNodePtr _filterOp1 = base2->as<FilterLogicalOperatorNode>();
}

TEST_F(LogicalOperatorNodeTest, asBadCast) {
    NodePtr base2 = sourceOp;
    try {
        FilterLogicalOperatorNodePtr _filterOp1 = base2->as<FilterLogicalOperatorNode>();
        FAIL();
    } catch (const std::bad_cast& e) {
        SUCCEED();
    }
}

/**
 *
 *                                  /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *                     \-> filterOp3 -> filterOp5
 *
 */
// TEST_F(LogicalOperatorNodeTest, DISABLED_findRecurisivelyOperatorNotExists) {

//     // topology1
//     filterOp6->addChild(filterOp3);
//     filterOp6->addChild(filterOp1);
//     filterOp1->addChild(filterOp2);
//     filterOp1->addChild(filterOp4);
//     filterOp3->addChild(filterOp5);

//     NodePtr x = nullptr;
//     // case 1: filterOp7 not in this graph
//     x = filterOp6->findRecursively(filterOp6, filterOp7);
//     EXPECT_TRUE(x == nullptr);
//     // case 2: filterOp6 is in this graph, but not the
//     // children of filterOp1
//     x = filterOp6->findRecursively(filterOp1, filterOp6);
//     EXPECT_TRUE(x == nullptr);
// }

/**
 *                                  /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *            ^        \-> filterOp3
 *            |<---------------|
 */
TEST_F(LogicalOperatorNodeTest, isCyclic) {
    // topology1
    filterOp6->addChild(filterOp3);
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);
    filterOp3->addChild(filterOp6);

    EXPECT_TRUE(filterOp6->isCyclic());
    EXPECT_TRUE(filterOp3->isCyclic());
    EXPECT_FALSE(filterOp1->isCyclic());
    EXPECT_FALSE(filterOp2->isCyclic());
    EXPECT_FALSE(filterOp4->isCyclic());
}
/**
 *                                  /-> filterOp4 --|
 * topology1: filterOp6 -> filterOp1 -> filterOp2 <-
 *                    \-> filterOp3
 */
TEST_F(LogicalOperatorNodeTest, isNotCyclic) {
    filterOp6->addChild(filterOp3);
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);
    filterOp4->addChild(filterOp2);

    EXPECT_FALSE(filterOp6->isCyclic());
    EXPECT_FALSE(filterOp3->isCyclic());
    EXPECT_FALSE(filterOp1->isCyclic());
}

/**
 *                                  /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *            ^        \-> filterOp3
 *            |<---------------|
 */
TEST_F(LogicalOperatorNodeTest, getAndFlattenAllSuccessorsNoCycle) {
    filterOp6->addChild(filterOp3);
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);

    std::vector<NodePtr> expected{};
    expected.push_back(filterOp3);
    expected.push_back(filterOp1);
    expected.push_back(filterOp2);
    expected.push_back(filterOp4);

    children = filterOp6->getAndFlattenAllChildren();
    EXPECT_EQ(children.size(), expected.size());

    for (int i = 0; i < children.size(); i++) {
        EXPECT_TRUE(children[i]->equal(expected[i]));
    }
}

/**
 *                                  /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *            ^        \-> filterOp3
 *            |<---------------|
 */
TEST_F(LogicalOperatorNodeTest, getAndFlattenAllSuccessorsForCycle) {
    filterOp6->addChild(filterOp3);
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);
    filterOp3->addChild(filterOp6);

    std::vector<NodePtr> expected{};
    expected.push_back(filterOp3);
    expected.push_back(filterOp1);
    expected.push_back(filterOp2);
    expected.push_back(filterOp4);

    children = filterOp6->getAndFlattenAllChildren();
    EXPECT_EQ(children.size(), expected.size());
    for (int i = 0; i < children.size(); i++) {
        EXPECT_TRUE(children[i]->equal(expected[i]));
    }
}

TEST_F(LogicalOperatorNodeTest, prettyPrint) {
    filterOp6->addChild(filterOp3);
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);

    std::stringstream ss1;
    ss1 << std::string(0, ' ') << filterOp6->toString() << std::endl;
    ss1 << std::string(2, ' ') << filterOp3->toString() << std::endl;
    ss1 << std::string(2, ' ') << filterOp1->toString() << std::endl;
    ss1 << std::string(4, ' ') << filterOp2->toString() << std::endl;
    ss1 << std::string(4, ' ') << filterOp4->toString() << std::endl;

    std::stringstream ss;
    dumpContext->dump(filterOp6, ss);
    EXPECT_EQ(ss.str(), ss1.str());
}

TEST_F(LogicalOperatorNodeTest, instanceOf) {
    bool inst = false;
    inst = filterOp1->instanceOf<FilterLogicalOperatorNode>();
    EXPECT_TRUE(inst);
    inst = filterOp1->instanceOf<SourceLogicalOperatorNode>();
    EXPECT_FALSE(inst);
}

TEST_F(LogicalOperatorNodeTest, getOperatorByType) {
    filterOp1->addChild(filterOp2);
    filterOp2->addChild(filterOp3);
    filterOp3->addChild(filterOp4);
    filterOp4->addChild(filterOp4);
    dumpContext->dump(filterOp6, std::cout);
    std::vector<NodePtr> expected{};
    expected.push_back(filterOp1);
    expected.push_back(filterOp2);
    expected.push_back(filterOp3);
    expected.push_back(filterOp4);
    const vector<FilterLogicalOperatorNodePtr> children = filterOp1->getNodesByType<FilterLogicalOperatorNode>();;
    // EXPECT_EQ(children.size(), expected.size());

    for (int i = 0; i < children.size(); i++) {
        std::cout << i << std::endl;
        // both reference to the same pointer
        EXPECT_TRUE(children[i]->isIdentical(expected[i]));
        // EXPECT_TRUE(children[i] == (expected[i]));
        // both have same values
        EXPECT_TRUE(children[i]->equal(expected[i]));
    }
}

/**
 *  swap filterOp1 by filterOp3
 *                                 /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *
 *             filterOp3 -> filterOp5
 *                     \-> filterOp2
 */
TEST_F(LogicalOperatorNodeTest, swap1) {
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);

    filterOp3->addChild(filterOp2);
    filterOp3->addChild(filterOp5);

    filterOp6->swap(filterOp3, filterOp1);
    stringstream expected;
    expected << std::string(0, ' ') << filterOp6->toString() << std::endl;
    expected << std::string(2, ' ') << filterOp3->toString() << std::endl;
    expected << std::string(4, ' ') << filterOp2->toString() << std::endl;
    expected << std::string(4, ' ') << filterOp5->toString() << std::endl;

    stringstream ss;
    dumpContext->dump(filterOp6, ss);
    // std::cout << ss.str();
    EXPECT_EQ(ss.str(), expected.str());
}

/**
 *  swap filterOp4 by filterOp3
 *                                 /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *
 *             filterOp3 -> filterOp5
 *                     \-> filterOp2
 */
TEST_F(LogicalOperatorNodeTest, swap2) {
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);

    filterOp3->addChild(filterOp2);
    filterOp3->addChild(filterOp5);

    filterOp6->swap(filterOp3, filterOp4);
    stringstream expected;
    expected << std::string(0, ' ') << filterOp6->toString() << std::endl;
    expected << std::string(2, ' ') << filterOp1->toString() << std::endl;
    expected << std::string(4, ' ') << filterOp2->toString() << std::endl;
    expected << std::string(4, ' ') << filterOp3->toString() << std::endl;
    expected << std::string(6, ' ') << filterOp2->toString() << std::endl;
    expected << std::string(6, ' ') << filterOp5->toString() << std::endl;

    stringstream ss;
    dumpContext->dump(filterOp6, ss);
    // std::cout << ss.str();
    EXPECT_EQ(ss.str(), expected.str());
}

/**
 *  swap filterOp2 by filterOp3
 *                    /-> filterOp4 ->\
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *
 *             filterOp3 -> filterOp5
 *                     \-> filterOp7
 */
TEST_F(LogicalOperatorNodeTest, swap3) {
    filterOp6->addChild(filterOp1);
    filterOp6->addChild(filterOp4);
    filterOp1->addChild(filterOp2);
    filterOp4->addChild(filterOp2);

    filterOp3->addChild(filterOp7);
    filterOp3->addChild(filterOp5);

    filterOp6->swap(filterOp3, filterOp2);
    stringstream expected;
    expected << std::string(0, ' ') << filterOp6->toString() << std::endl;
    expected << std::string(2, ' ') << filterOp1->toString() << std::endl;
    expected << std::string(4, ' ') << filterOp3->toString() << std::endl;
    expected << std::string(6, ' ') << filterOp7->toString() << std::endl;
    expected << std::string(6, ' ') << filterOp5->toString() << std::endl;

    expected << std::string(2, ' ') << filterOp4->toString() << std::endl;
    expected << std::string(4, ' ') << filterOp3->toString() << std::endl;
    expected << std::string(6, ' ') << filterOp7->toString() << std::endl;
    expected << std::string(6, ' ') << filterOp5->toString() << std::endl;

    stringstream ss;
    dumpContext->dump(filterOp6, ss);
    std::cout << ss.str();
    EXPECT_EQ(ss.str(), expected.str());
}

/**
 *  swap filterOp2 by filterOp3
 *                    /-> filterOp4 ->\
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *
 *             filterOp3 -> filterOp5
 *                     \-> filterOp7
 */
TEST_F(LogicalOperatorNodeTest, swap4) {
    filterOp6->addChild(filterOp1);
    filterOp6->addChild(filterOp4);
    filterOp1->addChild(filterOp2);
    filterOp4->addChild(filterOp2);

    filterOp3->addChild(filterOp7);
    filterOp3->addChild(filterOp5);

    filterOp6->swap(filterOp3, filterOp2);
    stringstream expected;
    expected << std::string(0, ' ') << filterOp6->toString() << std::endl;
    expected << std::string(2, ' ') << filterOp1->toString() << std::endl;
    expected << std::string(4, ' ') << filterOp3->toString() << std::endl;
    expected << std::string(6, ' ') << filterOp7->toString() << std::endl;
    expected << std::string(6, ' ') << filterOp5->toString() << std::endl;

    expected << std::string(2, ' ') << filterOp4->toString() << std::endl;
    expected << std::string(4, ' ') << filterOp3->toString() << std::endl;
    expected << std::string(6, ' ') << filterOp7->toString() << std::endl;
    expected << std::string(6, ' ') << filterOp5->toString() << std::endl;

    stringstream ss;
    dumpContext->dump(filterOp6, ss);
    std::cout << ss.str();
    EXPECT_EQ(ss.str(), expected.str());
}


/**
 *  swap filterOp2 by filterOp3
 *                        /-> filterOp4 -->|
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *
 *             filterOp3 -> filterOp5
 *                     \-> filterOp2
 */
TEST_F(LogicalOperatorNodeTest, swap5) {
    filterOp6->addChild(filterOp1);
    filterOp6->addChild(filterOp4);
    filterOp1->addChild(filterOp2);
    filterOp4->addChild(filterOp2);

    filterOp3->addChild(filterOp2);
    filterOp3->addChild(filterOp5);

    filterOp6->swap(filterOp3, filterOp2);
    stringstream expected;
    expected << std::string(0, ' ') << filterOp6->toString() << std::endl;
    expected << std::string(2, ' ') << filterOp1->toString() << std::endl;
    expected << std::string(4, ' ') << filterOp3->toString() << std::endl;
    expected << std::string(6, ' ') << filterOp2->toString() << std::endl;
    expected << std::string(6, ' ') << filterOp5->toString() << std::endl;

    expected << std::string(2, ' ') << filterOp4->toString() << std::endl;
    expected << std::string(4, ' ') << filterOp3->toString() << std::endl;
    expected << std::string(6, ' ') << filterOp2->toString() << std::endl;
    expected << std::string(6, ' ') << filterOp5->toString() << std::endl;

    stringstream ss;
    dumpContext->dump(filterOp6, ss);
    std::cout << ss.str();
    EXPECT_EQ(ss.str(), expected.str());
}

/**
 *  swap filterOp4 by filterOp3
 *                    /-> filterOp4 -->|
 * topology1: filterOp6 -> filterOp3 -> filterOp2
 *
 *             filterOp3 -> filterOp5
 *                     \-> filterOp2
 */
TEST_F(LogicalOperatorNodeTest, swap6) {
    filterOp6->addChild(filterOp3);
    filterOp6->addChild(filterOp4);
    filterOp1->addChild(filterOp2);
    filterOp4->addChild(filterOp2);

    filterOp3->addChild(filterOp2);
    filterOp3->addChild(filterOp5);

    bool swapped = filterOp6->swap(filterOp3, filterOp4);
    EXPECT_FALSE(swapped);

}

/**
 * split at filterOp2
 * topology1: filterOp6 -> filterOp1 -> filterOp2 -> filterOp3
 */
TEST_F(LogicalOperatorNodeTest, splitWithSinglePredecessor) {
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp2->addChild(filterOp3);
    std::vector<NodePtr> expected{};
    expected.push_back(filterOp1);
    expected.push_back(filterOp2);

    auto vec = filterOp6->split(filterOp2);
    EXPECT_EQ(vec.size(), expected.size());
    for (int i = 0; i < vec.size(); i++) {
        EXPECT_TRUE(vec[i]->equal(expected[i]));
    }
}


/**
 * split at filterOp3
 * topology1: filterOp6 -> filterOp1 -> filterOp2 -> filterOp3
 */
TEST_F(LogicalOperatorNodeTest, splitWithAtLastSuccessor) {
    filterOp7->addChild(filterOp1);
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp2->addChild(filterOp3);
    std::vector<NodePtr> expected{};
    expected.push_back(filterOp2);
    expected.push_back(filterOp3);

    auto vec = filterOp6->split(filterOp3);
    EXPECT_EQ(vec.size(), expected.size());
    for (int i = 0; i < vec.size(); i++) {
        EXPECT_TRUE(vec[i]->equal(expected[i]));
    }
}
/**
 * split at filterOp6
 * topology1: filterOp6 -> filterOp1 -> filterOp2 -> filterOp3
 */
TEST_F(LogicalOperatorNodeTest, splitWithAtRoot) {
    filterOp7->addChild(filterOp1);
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp2->addChild(filterOp3);
    std::vector<NodePtr> expected{};
    expected.push_back(filterOp6);

    auto vec = filterOp6->split(filterOp6);
    EXPECT_EQ(vec.size(), expected.size());
    for (int i = 0; i < vec.size(); i++) {
        EXPECT_TRUE(vec[i]->equal(expected[i]));
    }
}
/**
 *
 *             filterOp7->\
 * topology1: filterOp6 -> filterOp1 -> filterOp2 -> filterOp3
 */
TEST_F(LogicalOperatorNodeTest, splitWithMultiplePredecessors) {
    filterOp7->addChild(filterOp1);
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp2->addChild(filterOp3);
    std::vector<NodePtr> expected{};
    expected.push_back(filterOp7);
    expected.push_back(filterOp6);
    expected.push_back(filterOp1);

    auto vec = filterOp6->split(filterOp1);
    EXPECT_EQ(vec.size(), expected.size());
    for (int i = 0; i < vec.size(); i++) {
        EXPECT_TRUE(vec[i]->equal(expected[i]));
    }
}


TEST_F(LogicalOperatorNodeTest, bfIterator) {
    /**
     * 1 -> 2 -> 5
     * 1 -> 2 -> 6
     * 1 -> 3
     * 1 -> 4 -> 7
     *
     */
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp3);
    filterOp1->addChild(filterOp4);
    filterOp2->addChild(filterOp5);
    filterOp2->addChild(filterOp6);
    filterOp4->addChild(filterOp7);

    ConsoleDumpHandler::create()->dump(filterOp1, std::cout);

    auto bfNodeIterator = BreadthFirstNodeIterator(filterOp1);
    auto iterator = bfNodeIterator.begin();

    ASSERT_EQ(*iterator, filterOp1);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp2);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp3);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp4);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp5);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp6);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp7);
}

TEST_F(LogicalOperatorNodeTest, dfIterator) {
    /**
     * 1 -> 2 -> 5
     * 1 -> 2 -> 6
     * 1 -> 3
     * 1 -> 4 -> 7
     *
     */
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp3);
    filterOp1->addChild(filterOp4);
    filterOp2->addChild(filterOp5);
    filterOp2->addChild(filterOp6);
    filterOp4->addChild(filterOp7);

    ConsoleDumpHandler::create()->dump(filterOp1, std::cout);

    auto dfNodeIterator = DepthFirstNodeIterator(filterOp1);
    auto iterator = dfNodeIterator.begin();

    ASSERT_EQ(*iterator, filterOp1);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp4);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp7);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp3);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp2);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp6);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp5);

}

TEST_F(LogicalOperatorNodeTest, translateToLagacyOperatorTree) {
    /**
     * Sink -> Filter -> Source
     */
    auto schema = Schema::create();
    auto printSink = createPrintSinkWithSchema(schema, std::cout);
    auto sinkOperator = createSinkLogicalOperatorNode(printSink);
    auto constValue = ConstantValueExpressionNode::create(createBasicTypeValue(BasicType::INT8, "1"));
    auto fieldRead = FieldReadExpressionNode::create(createDataType(BasicType::INT8), "FieldName");
    auto andNode = EqualsExpressionNode::create(constValue, fieldRead);
    auto filter = createFilterLogicalOperatorNode(andNode);
    sinkOperator->addChild(filter);
    filter->addChild(sourceOp);

    ConsoleDumpHandler::create()->dump(sinkOperator, std::cout);
    auto translatePhase = TranslateToLegacyPlanPhase::create();
    auto legacySink = translatePhase->transform(sinkOperator->as<OperatorNode>());
    std::cout << legacySink->toString() << std::endl;
    ASSERT_EQ(legacySink->getOperatorType(), SINK_OP);
    auto legacyFilter = legacySink->getChildren()[0];
    ASSERT_EQ(legacyFilter->getOperatorType(), FILTER_OP);
    auto legacyPredicate = std::dynamic_pointer_cast<FilterOperator>(legacyFilter)->getPredicate();
    std::cout << legacyPredicate->toString() << std::endl;
    ASSERT_EQ(legacyPredicate->toString(), "(1 == FieldName:INT8 )");
    auto legacySource = legacyFilter->getChildren()[0];
    ASSERT_EQ(legacySource->getOperatorType(), SOURCE_OP);

}

} // namespace NES

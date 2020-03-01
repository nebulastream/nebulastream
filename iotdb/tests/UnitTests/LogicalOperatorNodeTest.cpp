#include <gtest/gtest.h>
#include <cassert>
#include <iostream>
#include <OperatorNodes/LogicalOperatorPlan.hpp>

#include <Util/Logger.hpp>

#include <QueryCompiler/HandCodedQueryExecutionPlan.hpp>

#include <SourceSink/SourceCreator.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <NodeEngine/Dispatcher.hpp>

#include <API/InputQuery.hpp>
#include <API/UserAPIExpression.hpp>
#include <API/Environment.hpp>
#include <API/Types/DataTypes.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Operators/Operator.hpp>

#include <API/InputQuery.hpp>
#include <API/Config.hpp>
#include <API/Schema.hpp>
#include <SourceSink/DataSource.hpp>
#include <API/InputQuery.hpp>
#include <API/Environment.hpp>
#include <API/UserAPIExpression.hpp>
#include <Catalogs/StreamCatalog.hpp>


namespace NES {

class LogicalOperatorNodeTest : public testing::Test {
public:
    void SetUp() {
        sPtr = StreamCatalog::instance().getStreamForLogicalStreamOrThrowException("default_logical");
        source = createTestDataSourceWithSchema(sPtr->getSchema());

        pred1 = createPredicate((*sPtr.get())["value"]!=1);
        pred2 = createPredicate((*sPtr.get())["value"]!=2);
        pred3 = createPredicate((*sPtr.get())["value"]!=3);
        pred4 = createPredicate((*sPtr.get())["value"]!=4);
        pred5 = createPredicate((*sPtr.get())["value"]!=5);
        pred6 = createPredicate((*sPtr.get())["value"]!=6);

        sourceOp = createSourceLogicalOperatorNode(source);
        filterOp1 = createFilterLogicalOperatorNode(pred1);
        filterOp2 = createFilterLogicalOperatorNode(pred2);
        filterOp3 = createFilterLogicalOperatorNode(pred3);
        filterOp4 = createFilterLogicalOperatorNode(pred4);
        filterOp5 = createFilterLogicalOperatorNode(pred5);
        filterOp6 = createFilterLogicalOperatorNode(pred6);

        filterOp1Copy = createFilterLogicalOperatorNode(pred1);
        filterOp2Copy = createFilterLogicalOperatorNode(pred2);
        filterOp3Copy = createFilterLogicalOperatorNode(pred3);
        filterOp4Copy = createFilterLogicalOperatorNode(pred4);
        filterOp5Copy = createFilterLogicalOperatorNode(pred5);
        filterOp6Copy = createFilterLogicalOperatorNode(pred6);

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

    PredicatePtr pred1, pred2, pred3, pred4, pred5, pred6;
    BaseOperatorNodePtr sourceOp;

    BaseOperatorNodePtr filterOp1, filterOp2, filterOp3, filterOp4, filterOp5, filterOp6;
    BaseOperatorNodePtr filterOp1Copy, filterOp2Copy, filterOp3Copy, filterOp4Copy, filterOp5Copy, filterOp6Copy;

    std::vector<BaseOperatorNodePtr> children {};
    std::vector<BaseOperatorNodePtr> parents {};

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
    sourceOp->addSuccessor(filterOp1);
    sourceOp->addSuccessor(filterOp2);

    children = sourceOp->getSuccessors();
    EXPECT_EQ(children.size(), 2);

    children.push_back(filterOp3);
    EXPECT_EQ(children.size(), 3);

    children = sourceOp->getSuccessors();
    EXPECT_EQ(children.size(), 2);
}

TEST_F(LogicalOperatorNodeTest, getPredecessors) {
    filterOp3->addPredecessor(filterOp1);

    parents = filterOp3->getPredecessors();
    EXPECT_EQ(parents.size(), 1);

    parents.push_back(filterOp2);
    EXPECT_EQ(parents.size(), 2);

    parents = filterOp3->getPredecessors();
    EXPECT_EQ(parents.size(), 1);
}

TEST_F(LogicalOperatorNodeTest, addSelfAsSuccessor) {
    sourceOp->addSuccessor(filterOp1);
    children = sourceOp->getSuccessors();
    EXPECT_EQ(children.size(), 1);

    sourceOp->addSuccessor(sourceOp);
    children = sourceOp->getSuccessors();
    EXPECT_EQ(children.size(), 1);
}

TEST_F(LogicalOperatorNodeTest, addAndRemoveSingleSuccessor) {
    sourceOp->addSuccessor(filterOp1);
    children = sourceOp->getSuccessors();
    EXPECT_EQ(children.size(), 1);

    removed = sourceOp->removeSuccessor(filterOp1);
    EXPECT_TRUE(removed);
    children = sourceOp->getSuccessors();
    EXPECT_EQ(children.size(), 0);
}

TEST_F(LogicalOperatorNodeTest, addAndRemoveMultipleSuccessors) {
    sourceOp->addSuccessor(filterOp1);
    sourceOp->addSuccessor(filterOp2);
    sourceOp->addSuccessor(filterOp3);
    children = sourceOp->getSuccessors();
    EXPECT_EQ(children.size(), 3);

    removed = sourceOp->removeSuccessor(filterOp1);
    EXPECT_TRUE(removed);
    removed = sourceOp->removeSuccessor(filterOp2);
    EXPECT_TRUE(removed);
    removed = sourceOp->removeSuccessor(filterOp3);
    EXPECT_TRUE(removed);
    children = sourceOp->getSuccessors();
    EXPECT_EQ(children.size(), 0);
}

TEST_F(LogicalOperatorNodeTest, addAndRmoveDuplicatedSuccessors) {
    sourceOp->addSuccessor(filterOp1);
    sourceOp->addSuccessor(filterOp1Copy);
    children = sourceOp->getSuccessors();
    EXPECT_EQ(children.size(), 1);

    removed = sourceOp->removeSuccessor(filterOp1);
    EXPECT_TRUE(removed);
    children = sourceOp->getSuccessors();
    EXPECT_EQ(children.size(), 0);

    removed = sourceOp->removeSuccessor(filterOp1Copy);
    EXPECT_FALSE(removed);
    sourceOp->addSuccessor(filterOp1Copy);
    EXPECT_EQ(children.size(), 0);
}

TEST_F(LogicalOperatorNodeTest, DISABLED_addAndRemoveNullSuccessor) {
    // assertion fail due to nullptr
    sourceOp->addSuccessor(filterOp1);
    EXPECT_EQ(children.size(), 1);
    sourceOp->addSuccessor(nullptr);
    removed = sourceOp->removeSuccessor(nullptr);
}

TEST_F(LogicalOperatorNodeTest, addSelfAsPredecessor) {
    filterOp3->addPredecessor(filterOp1);
    parents = filterOp3->getPredecessors();
    EXPECT_EQ(parents.size(), 1);

    filterOp3->addPredecessor(filterOp3);
    parents = filterOp3->getPredecessors();
    EXPECT_EQ(parents.size(), 1);
}

TEST_F(LogicalOperatorNodeTest, addAndRemoveSinglePredecessor) {
    filterOp3->addPredecessor(filterOp1);
    parents = filterOp3->getPredecessors();
    EXPECT_EQ(parents.size(), 1);

    removed = filterOp3->removePredecessor(filterOp1);
    EXPECT_TRUE(removed);
    parents = filterOp3->getPredecessors();
    EXPECT_EQ(parents.size(), 0);
}

TEST_F(LogicalOperatorNodeTest, addAndRemoveMultiplePredecessors) {
    filterOp3->addPredecessor(filterOp1);
    filterOp3->addPredecessor(filterOp2);
    parents = filterOp3->getPredecessors();
    EXPECT_EQ(parents.size(), 2);

    removed = filterOp3->removePredecessor(filterOp1);
    EXPECT_TRUE(removed);
    removed = filterOp3->removePredecessor(filterOp2);
    EXPECT_TRUE(removed);
    parents = filterOp3->getPredecessors();
    EXPECT_EQ(parents.size(), 0);

}

TEST_F(LogicalOperatorNodeTest, addAndRemoveDuplicatedPredecessors) {
    filterOp3->addPredecessor(filterOp1);
    parents = filterOp3->getPredecessors();
    EXPECT_EQ(parents.size(), 1);

    filterOp3->addPredecessor(filterOp1Copy);
    parents = filterOp3->getPredecessors();
    EXPECT_EQ(parents.size(), 1);

    removed = filterOp3->removePredecessor(filterOp1);
    EXPECT_TRUE(removed);
    parents = filterOp3->getPredecessors();
    EXPECT_EQ(parents.size(), 0);

    removed = filterOp3->removePredecessor(filterOp1Copy);
    EXPECT_FALSE(removed);
    parents = filterOp3->getPredecessors();
    EXPECT_EQ(parents.size(), 0);
}

TEST_F(LogicalOperatorNodeTest, DISABLED_addAndRemoveNullPredecessor) {
    // assertion failed due to nullptr
    filterOp3->addPredecessor(filterOp1);
    filterOp3->addPredecessor(nullptr);
    filterOp3->removePredecessor(nullptr);
}

/**
 * @brief replace filterOp1 with filterOp3
 * original: sourceOp -> filterOp1 -> filterOp2
 *           filterOp3 -> filterOp4
 * replaced: sourceOp -> filterOp3 -> filterOp2
 *                                |-> filterOp4
 */
TEST_F(LogicalOperatorNodeTest, replaceSuccessor) {
    children = filterOp3->getSuccessors();
    EXPECT_EQ(children.size(), 0);
    filterOp3->addSuccessor(filterOp4);

    sourceOp->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);

    sourceOp->replace(filterOp3, filterOp1);

    children = sourceOp->getSuccessors();
    EXPECT_EQ(children.size(), 1);
    EXPECT_EQ(*children[0].get(), *filterOp3.get());

    children = filterOp3->getSuccessors();
    EXPECT_EQ(children.size(), 2);
    EXPECT_EQ(*children[0].get(), *filterOp4.get());
    EXPECT_EQ(*children[1].get(), *filterOp2.get());
}

/**
 * @brief replace filterOp1 with filterOp1Copy
 * original: sourceOp -> filterOp1 -> filterOp2
 * replaced: sourceOp -> filterOp1Copy -> filterOp2
 */

TEST_F(LogicalOperatorNodeTest, replaceWithEqualSuccessor) {
    children = filterOp1Copy->getSuccessors();
    EXPECT_EQ(children.size(), 0);

    sourceOp->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);

    sourceOp->replace(filterOp1Copy, filterOp1);

    children = sourceOp->getSuccessors();
    EXPECT_EQ(children.size(), 1);

    children = filterOp1Copy->getSuccessors();
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
    children = filterOp3->getSuccessors();
    EXPECT_EQ(children.size(), 0);

    sourceOp->addSuccessor(filterOp1);
    sourceOp->addSuccessor(filterOp3);
    filterOp1->addSuccessor(filterOp2);
    filterOp3->addSuccessor(filterOp4);

    replaced = sourceOp->replace(filterOp3, filterOp1);
    EXPECT_FALSE(replaced);

    children = sourceOp->getSuccessors();
    EXPECT_EQ(children.size(), 2);
    EXPECT_EQ(*children[0].get(), *filterOp1.get());
    EXPECT_EQ(*children[1].get(), *filterOp3.get());

    children = filterOp1->getSuccessors();
    EXPECT_EQ(children.size(), 1);
    EXPECT_EQ(*children[0].get(), *filterOp2.get());

    children = filterOp3->getSuccessors();
    EXPECT_EQ(children.size(), 1);
    EXPECT_EQ(*children[0].get(), *filterOp4.get());
}

/**
 * @brief replace filterOp1 with filterOp1
 * original: sourceOp -> filterOp1 -> filterOp2
 * replaced: sourceOp -> filterOp1 -> filterOp2
 */
TEST_F(LogicalOperatorNodeTest, replaceWithIdenticalSuccessor) {
    children = filterOp1->getSuccessors();
    EXPECT_EQ(children.size(), 0);

    sourceOp->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);

    sourceOp->replace(filterOp1, filterOp1);

    children = sourceOp->getSuccessors();
    EXPECT_EQ(children.size(), 1);

    children = filterOp1->getSuccessors();
    EXPECT_EQ(children.size(), 1);
}

/**
 * @brief replace filterOp1 with filterOp2
 * original: sourceOp -> filterOp1 -> filterOp2
 * replaced: sourceOp -> filterOp2
 */
TEST_F(LogicalOperatorNodeTest, replaceWithSubSuccessor) {
    children = filterOp2->getSuccessors();
    EXPECT_EQ(children.size(), 0);

    sourceOp->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);

    sourceOp->replace(filterOp2, filterOp1);

    children = sourceOp->getSuccessors();
    EXPECT_EQ(children.size(), 1);
    EXPECT_EQ(*children[0].get(), *filterOp2.get());

    children = filterOp2->getSuccessors();
    EXPECT_EQ(children.size(), 0);
}

/**
 * @brief replace filterOp3 (not existed) with filterOp3
 * original: sourceOp -> filterOp1 -> filterOp2
 * replaced: sourceOp -> filterOp1 -> filterOp2
 */
TEST_F(LogicalOperatorNodeTest, replaceWithNoneSuccessor) {
    children = filterOp3->getSuccessors();
    EXPECT_EQ(children.size(), 0);

    sourceOp->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);

    sourceOp->replace(filterOp3, filterOp3);

    children = sourceOp->getSuccessors();
    EXPECT_EQ(children.size(), 1);
    EXPECT_EQ(*children[0].get(), *filterOp1.get());

    children = filterOp3->getSuccessors();
    EXPECT_EQ(children.size(), 0);
}

TEST_F(LogicalOperatorNodeTest, DISABLED_replaceSuccessorInvalidOldOperator) {
    children = filterOp3->getSuccessors();
    EXPECT_EQ(children.size(), 0);

    sourceOp->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);

    sourceOp->replace(filterOp3, nullptr);
}

TEST_F(LogicalOperatorNodeTest, DISABLED_replaceWithWithInvalidNewOperator) {
    children = filterOp3->getSuccessors();
    EXPECT_EQ(children.size(), 0);

    sourceOp->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);

    sourceOp->replace(nullptr, filterOp1);
}

/**
 * @brief replace filterOp1 with filterOp3
 * original: sourceOp <- filterOp1 <- filterOp2
 * replaced: sourceOp <- filterOp3 <- filterOp2
 */
TEST_F(LogicalOperatorNodeTest, replacePredecessor) {
    parents = filterOp3->getPredecessors();

    EXPECT_EQ(parents.size(), 0);

    filterOp2->addPredecessor(filterOp1);
    filterOp1->addPredecessor(sourceOp);

    filterOp2->replace(filterOp3, filterOp1);

    parents = filterOp2->getPredecessors();
    EXPECT_EQ(parents.size(), 1);
    EXPECT_EQ(parents[0].get(), filterOp3.get());

    parents = filterOp3->getPredecessors();
    EXPECT_EQ(parents.size(), 1);
}

/**
 * @brief replace filterOp1 with filterOp1Copy
 * original: sourceOp -> filterOp1 -> filterOp2
 * replaced: sourceOp -> filterOp1Copy -> filterOp2
 */
TEST_F(LogicalOperatorNodeTest, replaceWithEqualPredecessor) {
    parents = filterOp1Copy->getPredecessors();
    EXPECT_EQ(parents.size(), 0);

    filterOp2->addPredecessor(filterOp1);
    filterOp1->addPredecessor(sourceOp);

    parents = filterOp1->getPredecessors();
    EXPECT_EQ(parents.size(), 1);

    filterOp2->replace(filterOp1Copy, filterOp1);

    parents = filterOp2->getPredecessors();
    EXPECT_EQ(parents.size(), 1);
    EXPECT_NE(parents[0].get(), filterOp1.get());
    EXPECT_EQ(parents[0].get(), filterOp1Copy.get());

    parents = filterOp1Copy->getPredecessors();
    EXPECT_EQ(parents.size(), 1);
    EXPECT_EQ(parents[0].get(), sourceOp.get());
}
/**
 * @brief replace filterOp1 with filterOp1
 * original: sourceOp -> filterOp1 -> filterOp2
 * replaced: sourceOp -> filterOp1 -> filterOp2
 */
TEST_F(LogicalOperatorNodeTest, replaceWithIdenticalPredecessor) {
    parents = filterOp1->getPredecessors();
    EXPECT_EQ(parents.size(), 0);

    filterOp2->addPredecessor(filterOp1);
    filterOp1->addPredecessor(sourceOp);

    parents = filterOp1->getPredecessors();
    EXPECT_EQ(parents.size(), 1);

    filterOp2->replace(filterOp1, filterOp1);

    parents = filterOp2->getPredecessors();
    EXPECT_EQ(parents.size(), 1);
    EXPECT_EQ(parents[0].get(), filterOp1.get());

    parents = filterOp1->getPredecessors();
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
    sourceOp->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);
    filterOp1->addSuccessor(filterOp3);

    sourceOp->removeAndLevelUpSuccessors(filterOp1);
    children = sourceOp->getSuccessors();
    EXPECT_EQ(children.size(), 2);
    EXPECT_EQ(*children[0].get(), *filterOp2.get());
    EXPECT_EQ(*children[1].get(), *filterOp3.get());
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
    sourceOp->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);
    filterOp1->addSuccessor(filterOp3);

    removed = sourceOp->removeAndLevelUpSuccessors(filterOp4);
    EXPECT_FALSE(removed);
    children = sourceOp->getSuccessors();
    EXPECT_EQ(children.size(), 1);
    EXPECT_EQ(*children[0].get(), *filterOp1.get());
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
    sourceOp->addSuccessor(filterOp1);
    sourceOp->addSuccessor(filterOp3);

    filterOp1->addSuccessor(filterOp2);
    filterOp1->addSuccessor(filterOp3Copy);

    removed = sourceOp->removeAndLevelUpSuccessors(filterOp1);
    EXPECT_FALSE(removed);
    children = sourceOp->getSuccessors();
    EXPECT_EQ(children.size(), 2);
    EXPECT_EQ(*children[0].get(), *filterOp1.get());
    EXPECT_EQ(*children[1].get(), *filterOp3.get());
}

TEST_F(LogicalOperatorNodeTest, remove) {
    filterOp2->addPredecessor(filterOp1);
    // filterOp3 neither in filterOp2's successors nor predecessors
    removed = filterOp2->remove(filterOp3);
    EXPECT_FALSE(removed);
    // filterOp1 in filterOp2's predecessors
    removed = filterOp2->remove(filterOp1);
    EXPECT_TRUE(removed);

    filterOp2->addSuccessor(filterOp3);
    removed = filterOp2->remove(filterOp3);
    EXPECT_TRUE(removed);
}

TEST_F(LogicalOperatorNodeTest, clear) {
    filterOp2->addPredecessor(filterOp1);
    filterOp2->addSuccessor(filterOp3);
    parents = filterOp2->getPredecessors();
    children = filterOp2->getSuccessors();
    EXPECT_EQ(children.size(), 1);
    EXPECT_EQ(parents.size(), 1);

    filterOp2->clear();
    parents = filterOp2->getPredecessors();
    children = filterOp2->getSuccessors();
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
    EXPECT_EQ(*filterOp1.get(), *filterOp1Copy.get());
    EXPECT_EQ(*filterOp2.get(), *filterOp2Copy.get());
    EXPECT_EQ(*filterOp3.get(), *filterOp3Copy.get());
    EXPECT_EQ(*filterOp4.get(), *filterOp4Copy.get());
    EXPECT_EQ(*filterOp6.get(), *filterOp6Copy.get());

    // topology1
    filterOp6->addSuccessor(filterOp1);
    filterOp6->addSuccessor(filterOp3);
    filterOp1->addSuccessor(filterOp2);
    filterOp1->addSuccessor(filterOp4);

    // topology2
    filterOp6Copy->addSuccessor(filterOp1Copy);
    filterOp6Copy->addSuccessor(filterOp3Copy);
    filterOp1Copy->addSuccessor(filterOp4Copy);
    filterOp1Copy->addSuccessor(filterOp2Copy);

    same = filterOp6->equalWithAllSuccessors(filterOp6Copy);
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
    filterOp6->addSuccessor(filterOp1);
    filterOp6->addSuccessor(filterOp3);
    filterOp1->addSuccessor(filterOp2);
    filterOp1->addSuccessor(filterOp4);

    // topology3
    sourceOp->addSuccessor(filterOp1Copy);
    sourceOp->addSuccessor(filterOp3Copy);
    filterOp1Copy->addSuccessor(filterOp4Copy);
    filterOp1Copy->addSuccessor(filterOp2Copy);

    same = filterOp6->equalWithAllSuccessors(sourceOp);
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
    filterOp6->addSuccessor(filterOp3);
    filterOp6->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);
    filterOp1->addSuccessor(filterOp4);

    // topology4
    filterOp6Copy->addSuccessor(filterOp1Copy);
    filterOp6Copy->addSuccessor(filterOp3Copy);
    filterOp1Copy->addSuccessor(filterOp2Copy);
    filterOp1Copy->addSuccessor(filterOp5Copy);

    same = filterOp6->equalWithAllSuccessors(filterOp6Copy);
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
    filterOp2->addPredecessor(filterOp1);
    filterOp4->addPredecessor(filterOp1);
    filterOp1->addPredecessor(filterOp6);
    filterOp3->addPredecessor(filterOp6);

    // topology2
    filterOp2Copy->addPredecessor(filterOp1Copy);
    filterOp4Copy->addPredecessor(filterOp1Copy);
    filterOp3Copy->addPredecessor(filterOp6Copy);
    filterOp1Copy->addPredecessor(filterOp6Copy);

    same = filterOp4->equalWithAllPredecessors(filterOp4Copy);
    EXPECT_TRUE(same);
    same = filterOp2->equalWithAllPredecessors(filterOp2Copy);
    EXPECT_TRUE(same);
    same = filterOp3->equalWithAllPredecessors(filterOp3Copy);
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
    filterOp2->addPredecessor(filterOp1);
    filterOp4->addPredecessor(filterOp1);
    filterOp1->addPredecessor(filterOp6);
    filterOp3->addPredecessor(filterOp6);

    // topology3
    filterOp4Copy->addPredecessor(filterOp1Copy);
    filterOp2Copy->addPredecessor(filterOp1Copy);
    filterOp1Copy->addPredecessor(sourceOp);
    filterOp3Copy->addPredecessor(sourceOp);

    same = filterOp4->equalWithAllPredecessors(filterOp4Copy);
    EXPECT_FALSE(same);
    same = filterOp2->equalWithAllPredecessors(filterOp2Copy);
    EXPECT_FALSE(same);
    same = filterOp3->equalWithAllPredecessors(filterOp3Copy);
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
    filterOp2->addPredecessor(filterOp1);
    filterOp4->addPredecessor(filterOp1);
    filterOp1->addPredecessor(filterOp6);
    filterOp3->addPredecessor(filterOp6);

    // topology4
    filterOp5Copy->addPredecessor(filterOp1Copy);
    filterOp2Copy->addPredecessor(filterOp1Copy);
    filterOp3Copy->addPredecessor(filterOp6Copy);
    filterOp1Copy->addPredecessor(filterOp6Copy);

    same = filterOp5->equalWithAllPredecessors(filterOp4Copy);
    EXPECT_FALSE(same);

    same = filterOp2->equalWithAllPredecessors(filterOp2Copy);
    EXPECT_TRUE(same);
}

} // namespace NES

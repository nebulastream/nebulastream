#include <gtest/gtest.h>
#include <cassert>
#include <iostream>
#include <sstream>
#include <OperatorNodes/LogicalOperatorPlan.hpp>
#include <OperatorNodes/Impl/FilterLogicalOperatorNode.hpp>
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
        pred7 = createPredicate((*sPtr.get())["value"]!=7);

        sourceOp = createSourceLogicalOperatorNode(source);
        filterOp1 = createFilterLogicalOperatorNode(pred1);
        filterOp2 = createFilterLogicalOperatorNode(pred2);
        filterOp3 = createFilterLogicalOperatorNode(pred3);
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

    PredicatePtr pred1, pred2, pred3, pred4, pred5, pred6, pred7;
    BaseOperatorNodePtr sourceOp;

    BaseOperatorNodePtr filterOp1, filterOp2, filterOp3, filterOp4, filterOp5, filterOp6, filterOp7;
    BaseOperatorNodePtr filterOp1Copy, filterOp2Copy, filterOp3Copy, filterOp4Copy, filterOp5Copy, filterOp6Copy, filterOp7Copy;

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

// TEST_F(LogicalOperatorNodeTest, getSuccessors) {
//     // std::cout << "filterOp1's use counts: " << filterOp1.use_count() << std::endl;
//     // auto x = filterOp1->getptr();
//     // std::cout << filterOp1.use_count() << std::endl;
//     // std::cout << "x's use counts: " << x.use_count() << std::endl;

//     sourceOp->addSuccessor(filterOp1);
//     sourceOp->addSuccessor(filterOp2);

//     children = sourceOp->getSuccessors();
//     EXPECT_EQ(children.size(), 2);

//     children.push_back(filterOp3);
//     EXPECT_EQ(children.size(), 3);

//     children = sourceOp->getSuccessors();
//     EXPECT_EQ(children.size(), 2);
// }

// TEST_F(LogicalOperatorNodeTest, getPredecessors) {
//     filterOp3->addPredecessor(filterOp1);

//     parents = filterOp3->getPredecessors();
//     EXPECT_EQ(parents.size(), 1);

//     parents.push_back(filterOp2);
//     EXPECT_EQ(parents.size(), 2);

//     parents = filterOp3->getPredecessors();
//     EXPECT_EQ(parents.size(), 1);
// }

// TEST_F(LogicalOperatorNodeTest, addSelfAsSuccessor) {
//     sourceOp->addSuccessor(filterOp1);
//     children = sourceOp->getSuccessors();
//     EXPECT_EQ(children.size(), 1);

//     sourceOp->addSuccessor(sourceOp);
//     children = sourceOp->getSuccessors();
//     EXPECT_EQ(children.size(), 1);
// }

// TEST_F(LogicalOperatorNodeTest, addAndRemoveSingleSuccessor) {
//     sourceOp->addSuccessor(filterOp1);
//     children = sourceOp->getSuccessors();
//     EXPECT_EQ(children.size(), 1);

//     removed = sourceOp->removeSuccessor(filterOp1);
//     EXPECT_TRUE(removed);
//     children = sourceOp->getSuccessors();
//     EXPECT_EQ(children.size(), 0);
// }

// TEST_F(LogicalOperatorNodeTest, addAndRemoveMultipleSuccessors) {
//     sourceOp->addSuccessor(filterOp1);
//     sourceOp->addSuccessor(filterOp2);
//     sourceOp->addSuccessor(filterOp3);
//     children = sourceOp->getSuccessors();
//     EXPECT_EQ(children.size(), 3);

//     removed = sourceOp->removeSuccessor(filterOp1);
//     EXPECT_TRUE(removed);
//     removed = sourceOp->removeSuccessor(filterOp2);
//     EXPECT_TRUE(removed);
//     removed = sourceOp->removeSuccessor(filterOp3);
//     EXPECT_TRUE(removed);
//     children = sourceOp->getSuccessors();
//     EXPECT_EQ(children.size(), 0);
// }

// TEST_F(LogicalOperatorNodeTest, addAndRmoveDuplicatedSuccessors) {
//     sourceOp->addSuccessor(filterOp1);
//     sourceOp->addSuccessor(filterOp1Copy);
//     children = sourceOp->getSuccessors();
//     EXPECT_EQ(children.size(), 1);

//     removed = sourceOp->removeSuccessor(filterOp1);
//     EXPECT_TRUE(removed);
//     children = sourceOp->getSuccessors();
//     EXPECT_EQ(children.size(), 0);

//     removed = sourceOp->removeSuccessor(filterOp1Copy);
//     EXPECT_FALSE(removed);
//     sourceOp->addSuccessor(filterOp1Copy);
//     EXPECT_EQ(children.size(), 0);
// }

// TEST_F(LogicalOperatorNodeTest, DISABLED_addAndRemoveNullSuccessor) {
//     // assertion fail due to nullptr
//     sourceOp->addSuccessor(filterOp1);
//     EXPECT_EQ(children.size(), 1);
//     sourceOp->addSuccessor(nullptr);
//     removed = sourceOp->removeSuccessor(nullptr);
// }

// TEST_F(LogicalOperatorNodeTest, addSelfAsPredecessor) {
//     filterOp3->addPredecessor(filterOp1);
//     parents = filterOp3->getPredecessors();
//     EXPECT_EQ(parents.size(), 1);

//     filterOp3->addPredecessor(filterOp3);
//     parents = filterOp3->getPredecessors();
//     EXPECT_EQ(parents.size(), 1);
// }

// TEST_F(LogicalOperatorNodeTest, addAndRemoveSinglePredecessor) {
//     filterOp3->addPredecessor(filterOp1);
//     parents = filterOp3->getPredecessors();
//     EXPECT_EQ(parents.size(), 1);

//     removed = filterOp3->removePredecessor(filterOp1);
//     EXPECT_TRUE(removed);
//     parents = filterOp3->getPredecessors();
//     EXPECT_EQ(parents.size(), 0);
// }

// TEST_F(LogicalOperatorNodeTest, addAndRemoveMultiplePredecessors) {
//     filterOp3->addPredecessor(filterOp1);
//     filterOp3->addPredecessor(filterOp2);
//     parents = filterOp3->getPredecessors();
//     EXPECT_EQ(parents.size(), 2);

//     removed = filterOp3->removePredecessor(filterOp1);
//     EXPECT_TRUE(removed);
//     removed = filterOp3->removePredecessor(filterOp2);
//     EXPECT_TRUE(removed);
//     parents = filterOp3->getPredecessors();
//     EXPECT_EQ(parents.size(), 0);

// }

// TEST_F(LogicalOperatorNodeTest, addAndRemoveDuplicatedPredecessors) {
//     filterOp3->addPredecessor(filterOp1);
//     parents = filterOp3->getPredecessors();
//     EXPECT_EQ(parents.size(), 1);

//     filterOp3->addPredecessor(filterOp1Copy);
//     parents = filterOp3->getPredecessors();
//     EXPECT_EQ(parents.size(), 1);

//     removed = filterOp3->removePredecessor(filterOp1);
//     EXPECT_TRUE(removed);
//     parents = filterOp3->getPredecessors();
//     EXPECT_EQ(parents.size(), 0);

//     removed = filterOp3->removePredecessor(filterOp1Copy);
//     EXPECT_FALSE(removed);
//     parents = filterOp3->getPredecessors();
//     EXPECT_EQ(parents.size(), 0);
// }

TEST_F(LogicalOperatorNodeTest, consistencyBetweenSuccessorPredecesorRelation1) {
    filterOp3->addPredecessor(filterOp1);
    filterOp3->addPredecessor(filterOp2);
    parents = filterOp3->getPredecessors();
    EXPECT_EQ(parents.size(), 2);
    children = filterOp1->getSuccessors();
    EXPECT_EQ(children.size(), 1);
    children = filterOp2->getSuccessors();
    EXPECT_EQ(children.size(), 1);

    filterOp3->removePredecessor(filterOp1);
    parents = filterOp3->getPredecessors();
    EXPECT_EQ(parents.size(), 1);

    children = filterOp1->getSuccessors();
    std::cout << "children of filterOp1" << std::endl;
    for (auto && op : children) {
        std::cout << op->toString() << std::endl;
    }
    std::cout << "================================================================================\n";
    EXPECT_EQ(children.size(), 0);

    children = filterOp2->getSuccessors();
    EXPECT_EQ(children.size(), 1);

    filterOp3->removePredecessor(filterOp2);
    parents = filterOp3->getPredecessors();
    EXPECT_EQ(parents.size(), 0);

    children = filterOp1->getSuccessors();
    EXPECT_EQ(children.size(), 0);
    children = filterOp2->getSuccessors();
    EXPECT_EQ(children.size(), 0);
}

TEST_F(LogicalOperatorNodeTest, consistencyBetweenSuccessorPredecesorRelation2) {
    filterOp3->addSuccessor(filterOp1);
    filterOp3->addSuccessor(filterOp2);
    children = filterOp3->getSuccessors();
    EXPECT_EQ(children.size(), 2);
    parents = filterOp1->getPredecessors();
    EXPECT_EQ(parents.size(), 1);
    parents = filterOp2->getPredecessors();
    EXPECT_EQ(parents.size(), 1);

    filterOp3->removeSuccessor(filterOp1);
    children = filterOp3->getSuccessors();
    EXPECT_EQ(children.size(), 1);
    parents = filterOp1->getPredecessors();
    EXPECT_EQ(parents.size(), 0);
    parents = filterOp2->getPredecessors();
    EXPECT_EQ(parents.size(), 1);

    filterOp3->removeSuccessor(filterOp2);
    children = filterOp3->getSuccessors();
    EXPECT_EQ(children.size(), 0);
    parents = filterOp1->getPredecessors();
    EXPECT_EQ(parents.size(), 0);
    parents = filterOp2->getPredecessors();
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


// TODO: add more operator casting
TEST_F(LogicalOperatorNodeTest, as) {
    BaseOperatorNodePtr base2 = filterOp1;
    FilterLogicalOperatorNode& _filterOp1 = base2->as<FilterLogicalOperatorNode>();
}

TEST_F(LogicalOperatorNodeTest, asBadCast) {
    BaseOperatorNodePtr base2 = sourceOp;
    try {
        FilterLogicalOperatorNode& _filterOp1 = base2->as<FilterLogicalOperatorNode>();
        FAIL();
    } catch (const std::bad_cast& e) {
        SUCCEED();
    }
}


/**
 *                                  /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *                     \-> filterOp3 -> filterOp5
 */
// TEST_F(LogicalOperatorNodeTest, DISABLED_findRecurisivelyOperatorExists) {
//     // topology1
//     filterOp6->addSuccessor(filterOp3);
//     filterOp6->addSuccessor(filterOp1);
//     filterOp1->addSuccessor(filterOp2);
//     filterOp1->addSuccessor(filterOp4);
//     filterOp3->addSuccessor(filterOp5);

//     BaseOperatorNodePtr x = nullptr;

//     x = filterOp6->findRecursively(filterOp6, filterOp1);
//     EXPECT_TRUE(x != nullptr);
//     EXPECT_TRUE(*x.get() == *filterOp1.get());
//     EXPECT_TRUE(*x.get() == *filterOp1Copy.get());

//     x = filterOp6->findRecursively(filterOp6, filterOp2);
//     std::cout << "x: " << x->toString() << std::endl;
//     std::cout << "filterOp2: " << filterOp2->toString() << std::endl;
//     EXPECT_TRUE(x != nullptr);
//     EXPECT_TRUE(*x.get() == *filterOp2.get());
//     EXPECT_TRUE(*x.get() == *filterOp2Copy.get());

//     x = filterOp6->findRecursively(filterOp6, filterOp3);
//     EXPECT_TRUE(x != nullptr);
//     EXPECT_TRUE(*x.get() == *filterOp3.get());
//     EXPECT_TRUE(*x.get() == *filterOp3Copy.get());

//     x = filterOp6->findRecursively(filterOp6, filterOp4);
//     EXPECT_TRUE(x != nullptr);
//     EXPECT_TRUE(*x.get() == *filterOp4.get());
//     EXPECT_TRUE(*x.get() == *filterOp4Copy.get());

//     x = filterOp6->findRecursively(filterOp6, filterOp5);
//     EXPECT_TRUE(x != nullptr);
//     EXPECT_TRUE(*x.get() == *filterOp5.get());
//     EXPECT_TRUE(*x.get() == *filterOp5Copy.get());

//     x = filterOp6->findRecursively(filterOp6, filterOp6);
//     EXPECT_TRUE(x != nullptr);
//     EXPECT_TRUE(*x.get() == *filterOp6.get());
//     EXPECT_TRUE(*x.get() == *filterOp6Copy.get());

//     x = filterOp6->findRecursively(filterOp1, filterOp2);
//     EXPECT_TRUE(x != nullptr);
//     EXPECT_TRUE(*x.get() == *filterOp2.get());
//     EXPECT_TRUE(*x.get() == *filterOp2Copy.get());

//     x = filterOp1->findRecursively(filterOp1, filterOp2);
//     EXPECT_TRUE(x != nullptr);
//     EXPECT_TRUE(*x.get() == *filterOp2.get());
//     EXPECT_TRUE(*x.get() == *filterOp2Copy.get());
// }

/**
 *
 *                                  /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *                     \-> filterOp3 -> filterOp5
 *
 */
// TEST_F(LogicalOperatorNodeTest, DISABLED_findRecurisivelyOperatorNotExists) {

//     // topology1
//     filterOp6->addSuccessor(filterOp3);
//     filterOp6->addSuccessor(filterOp1);
//     filterOp1->addSuccessor(filterOp2);
//     filterOp1->addSuccessor(filterOp4);
//     filterOp3->addSuccessor(filterOp5);

//     BaseOperatorNodePtr x = nullptr;
//     // case 1: filterOp7 not in this graph
//     x = filterOp6->findRecursively(filterOp6, filterOp7);
//     EXPECT_TRUE(x == nullptr);
//     // case 2: filterOp6 is in this graph, but not the
//     // successors of filterOp1
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
    filterOp6->addSuccessor(filterOp3);
    filterOp6->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);
    filterOp1->addSuccessor(filterOp4);
    filterOp3->addSuccessor(filterOp6);

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
    filterOp6->addSuccessor(filterOp3);
    filterOp6->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);
    filterOp1->addSuccessor(filterOp4);
    filterOp4->addSuccessor(filterOp2);

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
    filterOp6->addSuccessor(filterOp3);
    filterOp6->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);
    filterOp1->addSuccessor(filterOp4);

    std::vector<BaseOperatorNodePtr> expected {};
    expected.push_back(filterOp3);
    expected.push_back(filterOp1);
    expected.push_back(filterOp2);
    expected.push_back(filterOp4);

    children = filterOp6->getAndFlattenAllSuccessors();
    EXPECT_EQ(children.size(), expected.size());
    // for (auto && op_ : children) {
    //     std::cout << op_->toString() << std::endl;
    // }
    for (int i = 0; i < children.size(); i ++) {
        EXPECT_EQ(*children[i].get(), *expected[i].get());
    }

    // expected.push_back();
    // expected.push_back();
    // expected.push_back();
    // expected.push_back();

    // children = filterOp6->getAndFlattenAllSuccessors();
    // for (auto && op_ : children) {
    //     std::cout << op_->toString() << std::endl;
    // }
}

/**
 *                                  /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *            ^        \-> filterOp3
 *            |<---------------|
 */
TEST_F(LogicalOperatorNodeTest, getAndFlattenAllSuccessorsForCycle) {
    filterOp6->addSuccessor(filterOp3);
    filterOp6->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);
    filterOp1->addSuccessor(filterOp4);
    filterOp3->addSuccessor(filterOp6);

    std::vector<BaseOperatorNodePtr> expected {};
    expected.push_back(filterOp3);
    expected.push_back(filterOp1);
    expected.push_back(filterOp2);
    expected.push_back(filterOp4);

    children = filterOp6->getAndFlattenAllSuccessors();
    EXPECT_EQ(children.size(), expected.size());
    // for (auto && op_ : children) {
    //     std::cout << op_->toString() << std::endl;
    // }
    for (int i = 0; i < children.size(); i ++) {
        EXPECT_EQ(*children[i].get(), *expected[i].get());
    }
}

TEST_F(LogicalOperatorNodeTest, prettyPrint) {
    filterOp6->addSuccessor(filterOp3);
    filterOp6->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);
    filterOp1->addSuccessor(filterOp4);

    std::stringstream ss1;
    ss1 << std::string(0, ' ') << filterOp6->toString() << std::endl;
    ss1 << std::string(2, ' ') << filterOp3->toString() << std::endl;
    ss1 << std::string(2, ' ') << filterOp1->toString() << std::endl;
    ss1 << std::string(4, ' ') << filterOp2->toString() << std::endl;
    ss1 << std::string(4, ' ') << filterOp4->toString() << std::endl;

    std::stringstream ss;
    filterOp6->prettyPrint(ss);

    EXPECT_EQ(ss.str(), ss1.str());
}

TEST_F(LogicalOperatorNodeTest, instanceOf) {
    bool inst = false;
    inst = filterOp1->instanceOf(OperatorType::FILTER_OP);
    EXPECT_TRUE(inst);
    inst = filterOp1->instanceOf(OperatorType::SOURCE_OP);
    EXPECT_FALSE(inst);
}

TEST_F(LogicalOperatorNodeTest, getOperatorByType) {
    filterOp6->addSuccessor(filterOp3);
    filterOp6->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);
    filterOp1->addSuccessor(filterOp4);
    filterOp3->addSuccessor(filterOp6);
    std::vector<BaseOperatorNodePtr> expected {};
    expected.push_back(filterOp6);
    expected.push_back(filterOp3);
    expected.push_back(filterOp1);
    expected.push_back(filterOp2);
    expected.push_back(filterOp4);
    children = filterOp6->getOperatorsByType(OperatorType::FILTER_OP);
    EXPECT_EQ(children.size(), 5);

    for (int i = 0; i < children.size(); i ++) {
        EXPECT_EQ(*children[i].get(), *expected[i].get());
    }

    children = filterOp6->getOperatorsByType(OperatorType::SOURCE_OP);
    EXPECT_EQ(children.size(), 0);

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
    filterOp6->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);
    filterOp1->addSuccessor(filterOp4);

    filterOp3->addSuccessor(filterOp2);
    filterOp3->addSuccessor(filterOp5);

    filterOp6->swap(filterOp3, filterOp1);
    stringstream expected;
    expected << std::string(0, ' ') << filterOp6->toString() << std::endl;
    expected << std::string(2, ' ') << filterOp3->toString() << std::endl;
    expected << std::string(4, ' ') << filterOp2->toString() << std::endl;
    expected << std::string(4, ' ') << filterOp5->toString() << std::endl;

    stringstream ss;
    filterOp6->prettyPrint(ss);
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
    filterOp6->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);
    filterOp1->addSuccessor(filterOp4);

    filterOp3->addSuccessor(filterOp2);
    filterOp3->addSuccessor(filterOp5);

    filterOp6->swap(filterOp3, filterOp4);
    stringstream expected;
    expected << std::string(0, ' ') << filterOp6->toString() << std::endl;
    expected << std::string(2, ' ') << filterOp1->toString() << std::endl;
    expected << std::string(4, ' ') << filterOp2->toString() << std::endl;
    expected << std::string(4, ' ') << filterOp3->toString() << std::endl;
    expected << std::string(6, ' ') << filterOp2->toString() << std::endl;
    expected << std::string(6, ' ') << filterOp5->toString() << std::endl;

    stringstream ss;
    filterOp6->prettyPrint(ss);
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
    filterOp6->addSuccessor(filterOp1);
    filterOp6->addSuccessor(filterOp4);
    filterOp1->addSuccessor(filterOp2);
    filterOp4->addSuccessor(filterOp2);

    filterOp3->addSuccessor(filterOp7);
    filterOp3->addSuccessor(filterOp5);

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
    filterOp6->prettyPrint(ss);
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
    filterOp6->addSuccessor(filterOp1);
    filterOp6->addSuccessor(filterOp4);
    filterOp1->addSuccessor(filterOp2);
    filterOp4->addSuccessor(filterOp2);

    filterOp3->addSuccessor(filterOp7);
    filterOp3->addSuccessor(filterOp5);

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
    filterOp6->prettyPrint(ss);
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
    filterOp6->addSuccessor(filterOp1);
    filterOp6->addSuccessor(filterOp4);
    filterOp1->addSuccessor(filterOp2);
    filterOp4->addSuccessor(filterOp2);

    filterOp3->addSuccessor(filterOp2);
    filterOp3->addSuccessor(filterOp5);

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
    filterOp6->prettyPrint(ss);
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
    filterOp6->addSuccessor(filterOp3);
    filterOp6->addSuccessor(filterOp4);
    filterOp1->addSuccessor(filterOp2);
    filterOp4->addSuccessor(filterOp2);

    filterOp3->addSuccessor(filterOp2);
    filterOp3->addSuccessor(filterOp5);

    bool swapped = filterOp6->swap(filterOp3, filterOp4);
    EXPECT_FALSE(swapped);

}

/**
 * split at filterOp2
 * topology1: filterOp6 -> filterOp1 -> filterOp2 -> filterOp3
 */
TEST_F(LogicalOperatorNodeTest, splitWithSinglePredecessor) {
    filterOp6->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);
    filterOp2->addSuccessor(filterOp3);
    std::vector<BaseOperatorNodePtr> expected {};
    expected.push_back(filterOp1);
    expected.push_back(filterOp2);

    auto vec = filterOp6->split(filterOp2);
    // for (auto&& op : vec) {
    //     std::cout << op->toString() << std::endl;
    // }
    EXPECT_EQ(vec.size(), expected.size());
    for (int i = 0; i < vec.size(); i ++) {
        EXPECT_EQ(*vec[i].get(), *expected[i].get());
    }
}


/**
 * split at filterOp3
 * topology1: filterOp6 -> filterOp1 -> filterOp2 -> filterOp3
 */
TEST_F(LogicalOperatorNodeTest, splitWithAtLastSuccessor) {
    filterOp7->addSuccessor(filterOp1);
    filterOp6->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);
    filterOp2->addSuccessor(filterOp3);
    std::vector<BaseOperatorNodePtr> expected {};
    expected.push_back(filterOp2);
    expected.push_back(filterOp3);

    auto vec = filterOp6->split(filterOp3);
    // for (auto&& op : vec) {
    //     std::cout << op->toString() << std::endl;
    // }
    EXPECT_EQ(vec.size(), expected.size());
    for (int i = 0; i < vec.size(); i ++) {
        EXPECT_EQ(*vec[i].get(), *expected[i].get());
    }
}
/**
 * split at filterOp6
 * topology1: filterOp6 -> filterOp1 -> filterOp2 -> filterOp3
 */
TEST_F(LogicalOperatorNodeTest, splitWithAtRoot) {
    filterOp7->addSuccessor(filterOp1);
    filterOp6->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);
    filterOp2->addSuccessor(filterOp3);
    std::vector<BaseOperatorNodePtr> expected {};
    expected.push_back(filterOp6);

    auto vec = filterOp6->split(filterOp6);
    // for (auto&& op : vec) {
    //     std::cout << op->toString() << std::endl;
    // }
    EXPECT_EQ(vec.size(), expected.size());
    for (int i = 0; i < vec.size(); i ++) {
        EXPECT_EQ(*vec[i].get(), *expected[i].get());
    }
}
/**
 *
 *             filterOp7->\
 * topology1: filterOp6 -> filterOp1 -> filterOp2 -> filterOp3
 */
TEST_F(LogicalOperatorNodeTest, splitWithMultiplePredecessors) {
    filterOp7->addSuccessor(filterOp1);
    filterOp6->addSuccessor(filterOp1);
    filterOp1->addSuccessor(filterOp2);
    filterOp2->addSuccessor(filterOp3);
    std::vector<BaseOperatorNodePtr> expected {};
    expected.push_back(filterOp7);
    expected.push_back(filterOp6);
    expected.push_back(filterOp1);

    auto vec = filterOp6->split(filterOp1);
    // for (auto&& op : vec) {
    //     std::cout << op->toString() << std::endl;
    // }
    EXPECT_EQ(vec.size(), expected.size());
    for (int i = 0; i < vec.size(); i ++) {
        EXPECT_EQ(*vec[i].get(), *expected[i].get());
    }
}

} // namespace NES

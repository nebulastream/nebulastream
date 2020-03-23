#include <gtest/gtest.h>
#include <iostream>
#include <Nodes/Expressions/ConditionExpressionNode.hpp>
#include <Util/Logger.hpp>
#include <memory>
#include <Nodes/Expressions/BinaryOperationType.hpp>

namespace NES {

class PredicateNodeTest : public testing::Test {
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
        LOG4CXX_DECODE_CHAR(fileName, "PredicateNodeTest.log");
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

TEST_F(PredicateNodeTest, printPredicateOperatorTree) {

}

} // namespace NES

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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <NesBaseTest.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/DumpContext.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Plans/ChangeLog/ChangeLogEntry.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <gtest/gtest.h>
#include <set>

namespace NES {

class ChangeLogEntryTest : public Testing::TestWithErrorHandling {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ChangeLogEntryTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ChangeLogEntryTest test class.");
    }

    void SetUp() override {
        Testing::TestWithErrorHandling::SetUp();
        dumpContext = DumpContext::create();
        dumpContext->registerDumpHandler(ConsoleDumpHandler::create(std::cout));

        pred1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "1"));
        pred2 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "2"));
        pred3 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "3"));
        pred4 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "4"));
        pred5 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
        pred6 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "6"));
        pred7 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "7"));

        sourceOp1 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
        sourceOp2 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical2"));
        filterOp1 = LogicalOperatorFactory::createFilterOperator(pred1);
        filterOp2 = LogicalOperatorFactory::createFilterOperator(pred2);
        filterOp3 = LogicalOperatorFactory::createFilterOperator(pred3);
        filterOp4 = LogicalOperatorFactory::createFilterOperator(pred4);
        sinkOp1 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
        sinkOp2 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
        sinkOp3 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    }

  protected:
    DumpContextPtr dumpContext;

    ExpressionNodePtr pred1, pred2, pred3, pred4, pred5, pred6, pred7;
    LogicalOperatorNodePtr sourceOp1, sourceOp2;

    LogicalOperatorNodePtr filterOp1, filterOp2, filterOp3, filterOp4;
    LogicalOperatorNodePtr sinkOp1, sinkOp2, sinkOp3;
};

//Fetch change log entry and check PoSet
TEST_F(ChangeLogEntryTest, FetchPoSetOfChangeLogEntry) {

    // Compute Plan
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    NES_DEBUG("{}", queryPlan->toString());

    // Initialize change log entry
    auto changelogEntry = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {sinkOp1});

    EXPECT_EQ(changelogEntry->poSetOfSubQueryPlan.size(), 3);
    EXPECT_TRUE(changelogEntry->poSetOfSubQueryPlan.find(sourceOp1->getId()) != changelogEntry->poSetOfSubQueryPlan.end());
    EXPECT_TRUE(changelogEntry->poSetOfSubQueryPlan.find(filterOp1->getId()) != changelogEntry->poSetOfSubQueryPlan.end());
    EXPECT_TRUE(changelogEntry->poSetOfSubQueryPlan.find(sinkOp1->getId()) != changelogEntry->poSetOfSubQueryPlan.end());
}

/**
 * @brief Fetch poset for change log entry with multiple downstream operators
 *
 * Query:
 *
 * --- Sink 1 --- Filter ---
 *                          \
 *                           --- Filter --- Source 1
 *                          /
 *            --- Sink 2 ---
 *
 */
TEST_F(ChangeLogEntryTest, FetchPoSetOfChangeLogEntryWithMultipleDownStreamOptrs) {

    // Compute Plan
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp2);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    queryPlan->addRootOperator(sinkOp2);
    filterOp1->addParent(sinkOp2);

    // Initialize change log
    auto changelogEntry = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {sinkOp1, sinkOp2});

    EXPECT_EQ(changelogEntry->poSetOfSubQueryPlan.size(), 5);
    EXPECT_TRUE(changelogEntry->poSetOfSubQueryPlan.find(sourceOp1->getId()) != changelogEntry->poSetOfSubQueryPlan.end());
    EXPECT_TRUE(changelogEntry->poSetOfSubQueryPlan.find(filterOp1->getId()) != changelogEntry->poSetOfSubQueryPlan.end());
    EXPECT_TRUE(changelogEntry->poSetOfSubQueryPlan.find(filterOp2->getId()) != changelogEntry->poSetOfSubQueryPlan.end());
    EXPECT_TRUE(changelogEntry->poSetOfSubQueryPlan.find(sinkOp1->getId()) != changelogEntry->poSetOfSubQueryPlan.end());
    EXPECT_TRUE(changelogEntry->poSetOfSubQueryPlan.find(sinkOp2->getId()) != changelogEntry->poSetOfSubQueryPlan.end());
}

/**
 * @brief Fetch poset for change log entry with multiple downstream operators
 *
 * Query:
 *
 *                            --- Source 1
 *                          /
 * -- Sink 1 --- Filter ---
 *                          \
 *                            --- Filter --- Source 2
 *
 */
TEST_F(ChangeLogEntryTest, FetchPoSetOfChangeLogEntryWithMultipleUpStreamOptrs) {

    // Compute Plan
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    filterOp1->addChild(filterOp2);
    filterOp2->addChild(sourceOp2);

    // Initialize change log
    auto changelogEntry = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1, sourceOp2}, {sinkOp1});

    EXPECT_EQ(changelogEntry->poSetOfSubQueryPlan.size(), 5);
    EXPECT_TRUE(changelogEntry->poSetOfSubQueryPlan.find(sourceOp1->getId()) != changelogEntry->poSetOfSubQueryPlan.end());
    EXPECT_TRUE(changelogEntry->poSetOfSubQueryPlan.find(sourceOp2->getId()) != changelogEntry->poSetOfSubQueryPlan.end());
    EXPECT_TRUE(changelogEntry->poSetOfSubQueryPlan.find(filterOp1->getId()) != changelogEntry->poSetOfSubQueryPlan.end());
    EXPECT_TRUE(changelogEntry->poSetOfSubQueryPlan.find(filterOp2->getId()) != changelogEntry->poSetOfSubQueryPlan.end());
    EXPECT_TRUE(changelogEntry->poSetOfSubQueryPlan.find(sinkOp1->getId()) != changelogEntry->poSetOfSubQueryPlan.end());
}

}// namespace NES

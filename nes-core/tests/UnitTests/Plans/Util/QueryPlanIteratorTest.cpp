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
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/DumpContext.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryCompiler/CodeGenerator/TranslateToLegacyExpression.hpp>
#include <Util/Logger.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <gtest/gtest.h>
#include <iostream>

using namespace std;

namespace NES {

class QueryPlanIteratorTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("QueryPlanIteratorTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryPlanIteratorTest test class.");
    }

    void SetUp() override {
        dumpContext = DumpContext::create();
        dumpContext->registerDumpHandler(ConsoleDumpHandler::create(std::cout));

        pred1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "1"));
        pred2 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "2"));
        pred3 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "3"));
        pred4 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "4"));
        pred5 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
        pred6 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "6"));
        pred7 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "7"));

        sourceOp1 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
        sourceOp2 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical2"));
        filterOp1 = LogicalOperatorFactory::createFilterOperator(pred1);
        filterOp2 = LogicalOperatorFactory::createFilterOperator(pred2);
        filterOp3 = LogicalOperatorFactory::createFilterOperator(pred3);
        filterOp4 = LogicalOperatorFactory::createFilterOperator(pred4);
        sinkOp1 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
        sinkOp2 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
        sinkOp3 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());

        children.clear();
        parents.clear();
    }

    void TearDown() override { NES_DEBUG("Tear down QueryPlanIteratorTest Test."); }

  protected:
    DumpContextPtr dumpContext;

    ExpressionNodePtr pred1, pred2, pred3, pred4, pred5, pred6, pred7;
    LogicalOperatorNodePtr sourceOp1, sourceOp2;

    LogicalOperatorNodePtr filterOp1, filterOp2, filterOp3, filterOp4;
    LogicalOperatorNodePtr sinkOp1, sinkOp2, sinkOp3;

    std::vector<NodePtr> children{};
    std::vector<NodePtr> parents{};
};

/**
 * @brief Query:
 *
 * --- Sink 1 --- Filter --- Source 1
 *
 */
TEST_F(QueryPlanIteratorTest, iterateFilterQueryPlan) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG(queryPlan->toString());

    auto queryPlanIter = QueryPlanIterator(queryPlan).begin();
    ASSERT_EQ(sinkOp1, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(filterOp1, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(sourceOp1, *queryPlanIter);
}

/**
 * @brief Query:
 *
 * --- Sink 1 --- Filter ---
 *                          \
 *                           --- Filter --- Source 1
 *                          /
 *            --- Sink 2 ---
 *
 */
TEST_F(QueryPlanIteratorTest, iterateMultiSinkQueryPlan) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp2);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    queryPlan->addRootOperator(sinkOp2);
    filterOp1->addParent(sinkOp2);

    NES_DEBUG(queryPlan->toString());

    auto queryPlanIter = QueryPlanIterator(queryPlan).begin();
    ASSERT_EQ(sinkOp1, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(filterOp2, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(sinkOp2, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(filterOp1, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(sourceOp1, *queryPlanIter);
}

/**
 * @brief Query:
 *
 *                            --- Source 1
 *                          /
 * -- Sink 1 --- Filter ---
 *                          \
 *                            --- Filter --- Source 2
 *
 */
TEST_F(QueryPlanIteratorTest, iterateMultiSourceQueryPlan) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    filterOp1->addChild(filterOp2);
    filterOp2->addChild(sourceOp2);

    NES_DEBUG(queryPlan->toString());

    auto queryPlanIter = QueryPlanIterator(queryPlan).begin();
    ASSERT_EQ(sinkOp1, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(filterOp1, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(sourceOp1, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(filterOp2, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(sourceOp2, *queryPlanIter);
}

/**
 * @brief Query:
 *
 *                                        --- Filter3 --- Source 1
 *                                      /
 * --- Sink 1 --- Filter1 --- Filter2 ---
 *                         /            \
 *                        /               --- Filter4 --- Source 2
 *            --- Sink 2                              /
 *                                        --- Sink 3
 *
 */
TEST_F(QueryPlanIteratorTest, iterateMultiSinkMultiSourceQueryPlan) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp3);
    queryPlan->appendOperatorAsNewRoot(filterOp2);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    filterOp2->addParent(sinkOp2);
    queryPlan->addRootOperator(sinkOp2);
    filterOp2->addChild(filterOp4);
    filterOp4->addChild(sourceOp2);
    sourceOp2->addParent(sinkOp3);
    queryPlan->addRootOperator(sinkOp3);

    NES_DEBUG(queryPlan->toString());

    auto queryPlanIter = QueryPlanIterator(queryPlan).begin();
    ASSERT_EQ(sinkOp1, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(filterOp1, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(sinkOp2, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(filterOp2, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(filterOp3, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(sourceOp1, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(filterOp4, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(sinkOp3, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(sourceOp2, *queryPlanIter);
}

/**
 * @brief Query:
 *
 *                                        --- Filter3 ---
 *                                      /                 \
 * --- Sink 1 --- Filter1 --- Filter2 ---                  --- Source
 *                         /            \                 /
 *                        /               --- Filter4 ---
 *            --- Sink 2
 *
 *
 */
TEST_F(QueryPlanIteratorTest, iterateMultiSinkRemergeQueryPlan) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp3);
    queryPlan->appendOperatorAsNewRoot(filterOp2);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    filterOp2->addParent(sinkOp2);
    queryPlan->addRootOperator(sinkOp2);
    filterOp2->addChild(filterOp4);
    filterOp4->addChild(sourceOp1);

    NES_DEBUG(queryPlan->toString());

    auto queryPlanIter = QueryPlanIterator(queryPlan).begin();
    ASSERT_EQ(sinkOp1, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(filterOp1, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(sinkOp2, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(filterOp2, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(filterOp3, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(filterOp4, *queryPlanIter);
    ++queryPlanIter;
    ASSERT_EQ(sourceOp1, *queryPlanIter);
}

}// namespace NES

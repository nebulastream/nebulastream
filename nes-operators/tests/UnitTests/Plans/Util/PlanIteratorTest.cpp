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

#include <memory>
#include <vector>
#include <API/QueryAPI.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionConstantValue.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/Operator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

using namespace std;

namespace NES
{

class PlanIteratorTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("PlanIteratorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup PlanIteratorTest test class.");
    }

    void SetUp() override
    {
        Testing::BaseUnitTest::SetUp();

        pred1 = NodeFunctionConstantValue::create(DataTypeFactory::createInt8(), "1");
        pred2 = NodeFunctionConstantValue::create(DataTypeFactory::createInt8(), "2");
        pred3 = NodeFunctionConstantValue::create(DataTypeFactory::createInt8(), "3");
        pred4 = NodeFunctionConstantValue::create(DataTypeFactory::createInt8(), "4");
        pred5 = NodeFunctionConstantValue::create(DataTypeFactory::createInt8(), "5");
        pred6 = NodeFunctionConstantValue::create(DataTypeFactory::createInt8(), "6");
        pred7 = NodeFunctionConstantValue::create(DataTypeFactory::createInt8(), "7");

        sourceOp1 = std::make_shared<SourceNameLogicalOperator>("test_source_1", getNextOperatorId());
        sourceOp2 = std::make_shared<SourceNameLogicalOperator>("test_source_2", getNextOperatorId());
        filterOp1 = std::make_shared<LogicalSelectionOperator>(pred1, getNextOperatorId());
        filterOp2 = std::make_shared<LogicalSelectionOperator>(pred2, getNextOperatorId());
        filterOp3 = std::make_shared<LogicalSelectionOperator>(pred3, getNextOperatorId());
        filterOp4 = std::make_shared<LogicalSelectionOperator>(pred4, getNextOperatorId());
        sinkOp1 = std::make_shared<SinkLogicalOperator>("print_sink", getNextOperatorId());
        sinkOp2 = std::make_shared<SinkLogicalOperator>("print_sink", getNextOperatorId());
        sinkOp3 = std::make_shared<SinkLogicalOperator>("print_sink", getNextOperatorId());

        children.clear();
        parents.clear();
    }

protected:
    std::shared_ptr<NodeFunction> pred1, pred2, pred3, pred4, pred5, pred6, pred7;
    std::shared_ptr<LogicalOperator> sourceOp1, sourceOp2;

    std::shared_ptr<LogicalOperator> filterOp1, filterOp2, filterOp3, filterOp4;
    std::shared_ptr<LogicalOperator> sinkOp1, sinkOp2, sinkOp3;

    std::vector<std::shared_ptr<Node>> children;
    std::vector<std::shared_ptr<Node>> parents;
};

/**
 * @brief Query:
 *
 * --- Sink 1 --- Filter --- Source 1
 *
 */
TEST_F(PlanIteratorTest, iterateFilterQueryPlan)
{
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG("{}", queryPlan->toString());

    auto queryPlanIter = PlanIterator(*queryPlan).begin();
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
TEST_F(PlanIteratorTest, iterateMultiSinkQueryPlan)
{
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp2);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    queryPlan->addRootOperator(sinkOp2);
    filterOp1->addParent(sinkOp2);

    NES_DEBUG("{}", queryPlan->toString());

    auto queryPlanIter = PlanIterator(*queryPlan).begin();
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
TEST_F(PlanIteratorTest, iterateMultiSourceQueryPlan)
{
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    filterOp1->addChild(filterOp2);
    filterOp2->addChild(sourceOp2);

    NES_DEBUG("{}", queryPlan->toString());

    auto queryPlanIter = PlanIterator(*queryPlan).begin();
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
TEST_F(PlanIteratorTest, iterateMultiSinkMultiSourceQueryPlan)
{
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

    NES_DEBUG("{}", queryPlan->toString());

    auto queryPlanIter = PlanIterator(*queryPlan).begin();
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
TEST_F(PlanIteratorTest, iterateMultiSinkRemergeQueryPlan)
{
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp3);
    queryPlan->appendOperatorAsNewRoot(filterOp2);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    filterOp2->addParent(sinkOp2);
    queryPlan->addRootOperator(sinkOp2);
    filterOp2->addChild(filterOp4);
    filterOp4->addChild(sourceOp1);

    NES_DEBUG("{}", queryPlan->toString());

    auto queryPlanIter = PlanIterator(*queryPlan).begin();
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

}

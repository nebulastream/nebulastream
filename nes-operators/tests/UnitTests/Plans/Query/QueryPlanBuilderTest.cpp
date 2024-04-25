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

#include <API/Query.hpp>
#include <BaseIntegrationTest.hpp>
#include <Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/RenameSourceOperator.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <iostream>

using namespace NES;

class QueryPlanBuilderTest : public Testing::BaseUnitTest {

  public:
    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::Logger::setupLogging("QueryPlanBuilderTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryPlanTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Setup QueryPlanBuilderTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down QueryPlanBuilderTest test class."); }
};

TEST_F(QueryPlanBuilderTest, testHasOperator) {
    //test createQueryPlan
    auto queryPlan = QueryPlanBuilder::createQueryPlan("test_stream");
    EXPECT_EQ(queryPlan->getSourceConsumed(), "test_stream");
    //test addRename
    queryPlan = QueryPlanBuilder::addRename("testStream", queryPlan);
    EXPECT_TRUE(queryPlan->getOperatorByType<RenameSourceOperator>().size() == 1);
    EXPECT_EQ(queryPlan->getOperatorByType<RenameSourceOperator>()[0]->getNewSourceName(), "testStream");
    //test addFilter
    auto filterExpression = ExpressionNodePtr(
        LessExpressionNode::create(NES::Attribute("a").getExpressionNode(), NES::Attribute("b").getExpressionNode()));
    queryPlan = QueryPlanBuilder::addFilter(filterExpression, queryPlan);
    EXPECT_TRUE(queryPlan->getOperatorByType<LogicalFilterOperator>().size() == 1);
    EXPECT_EQ(queryPlan->getOperatorByType<LogicalFilterOperator>()[0]->getPredicate(), filterExpression);
    //test addProjection
    std::vector<ExpressionNodePtr> expressions;
    expressions.push_back(Attribute("id").getExpressionNode());
    queryPlan = QueryPlanBuilder::addProjection(expressions, queryPlan);
    EXPECT_TRUE(queryPlan->getOperatorByType<LogicalProjectionOperator>().size() == 1);
    EXPECT_EQ(queryPlan->getOperatorByType<LogicalProjectionOperator>()[0]->getExpressions(), expressions);
    //test addMap
    queryPlan = QueryPlanBuilder::addMap(Attribute("b") = 1, queryPlan);
    EXPECT_TRUE(queryPlan->getOperatorByType<LogicalMapOperator>().size() == 1);
    //test addUnion
    auto rightQueryPlan = QueryPlanBuilder::createQueryPlan("test_stream_b");
    queryPlan = QueryPlanBuilder::addUnion(queryPlan, rightQueryPlan);
    EXPECT_TRUE(queryPlan->getOperatorByType<LogicalUnionOperator>().size() == 1);
    // test addSink
    auto sinkDescriptorPtr = NES::PrintSinkDescriptor::create();
    queryPlan = QueryPlanBuilder::addSink(queryPlan, sinkDescriptorPtr);
    EXPECT_TRUE(queryPlan->getOperatorByType<SinkLogicalOperator>().size() == 1);
    EXPECT_TRUE(queryPlan->getOperatorByType<SinkLogicalOperator>()[0]->getSinkDescriptor()->equal(sinkDescriptorPtr));
}

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

#include <iostream>
#include <memory>
#include <vector>
#include <Functions/LogicalFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/QueryPlanBuilder.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
#include <Operators/MapLogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>

using namespace NES;

class QueryPlanBuilderTest : public Testing::BaseUnitTest
{
public:
    /* Will be called before a test is executed. */
    void SetUp() override
    {
        NES::Logger::setupLogging("QueryPlanBuilderTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryPlanTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Setup QueryPlanBuilderTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down QueryPlanBuilderTest test class."); }
};

TEST_F(QueryPlanBuilderTest, testHasOperator)
{
    ///test createQueryPlan
    auto queryPlan = QueryPlanBuilder::createQueryPlan("test_stream");
    ///test addSelection
    auto filterFunction = std::make_unique<EqualsLogicalFunction>(std::make_unique<FieldAccessLogicalFunction>("a"), std::make_unique<FieldAccessLogicalFunction>("b"));
    queryPlan = QueryPlanBuilder::addSelection(std::move(filterFunction), queryPlan);
    EXPECT_TRUE(queryPlan.getOperatorByType<SelectionLogicalOperator>().size() == 1);
    EXPECT_EQ(dynamic_cast<LogicalFunction*>(queryPlan.getOperatorByType<SelectionLogicalOperator>()[0]->getPredicate()), filterFunction.get());
    ///test addProjection
    std::vector<std::unique_ptr<LogicalFunction>> functions;
    functions.push_back(std::make_unique<FieldAccessLogicalFunction>("id"));
    queryPlan = QueryPlanBuilder::addProjection(std::move(functions), queryPlan);
    EXPECT_TRUE(queryPlan.getOperatorByType<ProjectionLogicalOperator>().size() == 1);
    EXPECT_EQ(queryPlan.getOperatorByType<ProjectionLogicalOperator>()[0]->getFunctions(), functions);
    ///test addMap
    queryPlan = QueryPlanBuilder::addMap(std::make_unique<EqualsLogicalFunction>(std::make_unique<FieldAccessLogicalFunction>("b"),
        std::make_unique<ConstantValueLogicalFunction>("1")), queryPlan);
    EXPECT_TRUE(queryPlan.getOperatorByType<MapLogicalOperator>().size() == 1);
    ///test addUnion
    auto rightQueryPlan = QueryPlanBuilder::createQueryPlan("test_stream_b");
    queryPlan = QueryPlanBuilder::addUnion(queryPlan, rightQueryPlan);
    EXPECT_TRUE(queryPlan.getOperatorByType<UnionLogicalOperator>().size() == 1);
    queryPlan = QueryPlanBuilder::addSink("print_source", queryPlan, WorkerId(0));
    EXPECT_TRUE(queryPlan.getOperatorByType<SinkLogicalOperator>().size() == 1);
}

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
#include <API/Functions/Functions.hpp>
#include <API/Query.hpp>
#include <Functions/LogicalFunctions/EqualsBinaryLogicalFunction.hpp>
#include <Operators/LogicalOperators/MapLogicalOperator.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperator.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperator.hpp>
#include <Operators/LogicalOperators/RenameSourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/SelectionLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>

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
    EXPECT_EQ(queryPlan->getSourceConsumed(), "test_stream");
    ///test addRename
    queryPlan = QueryPlanBuilder::addRename("testStream", queryPlan);
    EXPECT_TRUE(queryPlan->getOperatorByType<RenameSourceLogicalOperator>().size() == 1);
    EXPECT_EQ(queryPlan->getOperatorByType<RenameSourceLogicalOperator>()[0]->getNewSourceName(), "testStream");
    ///test addSelection
    auto filterFunction = std::shared_ptr<LogicalFunction>(
        EqualsBinaryLogicalFunction::create(NES::Attribute("a").getLogicalFunction(), NES::Attribute("b").getLogicalFunction()));
    queryPlan = QueryPlanBuilder::addSelection(filterFunction, queryPlan);
    EXPECT_TRUE(queryPlan->getOperatorByType<SelectionLogicalOperator>().size() == 1);
    EXPECT_EQ(queryPlan->getOperatorByType<SelectionLogicalOperator>()[0]->getPredicate(), filterFunction);
    ///test addProjection
    std::vector<std::shared_ptr<LogicalFunction>> functions;
    functions.push_back(Attribute("id").getLogicalFunction());
    queryPlan = QueryPlanBuilder::addProjection(functions, queryPlan);
    EXPECT_TRUE(queryPlan->getOperatorByType<ProjectionLogicalOperator>().size() == 1);
    EXPECT_EQ(queryPlan->getOperatorByType<ProjectionLogicalOperator>()[0]->getFunctions(), functions);
    ///test addMap
    queryPlan = QueryPlanBuilder::addMap(Attribute("b") = 1, queryPlan);
    EXPECT_TRUE(queryPlan->getOperatorByType<MapLogicalOperator>().size() == 1);
    ///test addUnion
    auto rightQueryPlan = QueryPlanBuilder::createQueryPlan("test_stream_b");
    queryPlan = QueryPlanBuilder::addUnion(queryPlan, rightQueryPlan);
    EXPECT_TRUE(queryPlan->getOperatorByType<UnionLogicalOperator>().size() == 1);
    queryPlan = QueryPlanBuilder::addSink("print_source", queryPlan, WorkerId(0));
    EXPECT_TRUE(queryPlan->getOperatorByType<SinkLogicalOperator>().size() == 1);
}

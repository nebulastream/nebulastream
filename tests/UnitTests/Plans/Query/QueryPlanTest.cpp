/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <gtest/gtest.h>

#include <API/Query.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <iostream>

using namespace NES;

class QueryPlanTest : public testing::Test {

  public:
    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("QueryPlanTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryPlanTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() { NES_INFO("Setup QueryPlanTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down QueryPlanTest test class."); }
};

TEST_F(QueryPlanTest, testHasOperator) {
    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("test_stream"));
    bool exists = queryPlan->hasOperatorWithId(op1->getId());
    EXPECT_FALSE(exists);

    queryPlan->appendOperatorAsNewRoot(op1);
    exists = queryPlan->hasOperatorWithId(op1->getId());
    EXPECT_TRUE(exists);
}

TEST_F(QueryPlanTest, testLeafOperators) {
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("test_stream"));
    QueryPlanPtr queryPlan = QueryPlan::create(op1);
    LogicalOperatorNodePtr op2 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op2);

    std::vector<OperatorNodePtr> leafOptrs = queryPlan->getLeafOperators();
    EXPECT_TRUE(std::find(leafOptrs.begin(), leafOptrs.end(), op1) != leafOptrs.end());
}

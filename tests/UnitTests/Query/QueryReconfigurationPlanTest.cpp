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

#include "gtest/gtest.h"
#include <Plans/Query/QueryReconfigurationPlan.hpp>
#include <Util/Logger.hpp>

using namespace NES;

class QueryReconfigurationPlanTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("QueryReconfigurationPlanTestTest.log", NES::LOG_DEBUG);
        NES_DEBUG("QueryReconfigurationPlanTest: Setup.");
    }
};

TEST_F(QueryReconfigurationPlanTest, serializationOfPlan) {
    auto plan = QueryReconfigurationPlan();
    plan.setId(1);
    plan.setQueryId(10);
    plan.setQuerySubPlanIdsToStart(std::vector<QuerySubPlanId>{111, 222, 333});
    plan.setQuerySubPlanIdsToStop(std::vector<QuerySubPlanId>{444, 555, 666});
    plan.setQuerySubPlanIdsToReplace(std::unordered_map<QuerySubPlanId, QuerySubPlanId>{{777, 888}, {999, 1111}});

    ASSERT_EQ("reconfigurationId:1|queryId:10|querySubPlanIdsToStart:111 222 333 |querySubPlanIdsToStop:444 555 666 "
              "|querySubPlanIdsToReplace:999 1111 777 888 |",
              plan.serializeToString());
}

TEST_F(QueryReconfigurationPlanTest, deserializationOfPlanFromString) {
    auto expectedToStart = std::vector<QuerySubPlanId>{111, 222, 333};
    auto expectedToStop = std::vector<QuerySubPlanId>{444, 555, 666};
    auto expectedToReplace = std::unordered_map<QuerySubPlanId, QuerySubPlanId>{{777, 888}, {999, 1111}};

    auto plan = QueryReconfigurationPlan::deserializeFromString(
        "reconfigurationId:1|queryId:10|querySubPlanIdsToStart:111 222 333 |querySubPlanIdsToStop:444 555 666 "
        "|querySubPlanIdsToReplace:999 1111 777 888 |");

    ASSERT_EQ(plan->getId(), 1);
    ASSERT_EQ(plan->getQueryId(), 10);
    ASSERT_TRUE(plan->getQuerySubPlanIdsToStart() == expectedToStart);
    ASSERT_TRUE(plan->getQuerySubPlanIdsToStop() == expectedToStop);
    ASSERT_TRUE(plan->getQuerySubPlanIdsToReplace() == expectedToReplace);
}

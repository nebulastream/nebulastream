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

#ifndef NES_NES_CORE_INCLUDE_PARSERS_QUERYPLANBUILDER_H_
#define NES_NES_CORE_INCLUDE_PARSERS_QUERYPLANBUILDER_H_

#include "API/Expressions/Expressions.hpp"
#include "Plans/Query/QueryPlan.hpp"
#include "Windowing/LogicalJoinDefinition.hpp"
#include <string>

class QueryPlanBuilder {
  public:
    static NES::QueryPlanPtr createQueryPlan(std::string sourceName);

    static NES::QueryPlanPtr addUnionOperatorNode(NES::QueryPlanPtr left, NES::QueryPlanPtr right, NES::QueryPlanPtr currentPlan);

    static NES::QueryPlanPtr addJoinOperatorNode(NES::QueryPlanPtr left, NES::QueryPlanPtr right, NES::QueryPlanPtr currentPlan,
                                                       NES::ExpressionItem onLeftKey,
                                                       NES::ExpressionItem onRightKey,
                                                       const NES::Windowing::WindowTypePtr& windowType,
                                                       NES::Join::LogicalJoinDefinition::JoinType joinType);
};

#endif//NES_NES_CORE_INCLUDE_PARSERS_QUERYPLANBUILDER_H_

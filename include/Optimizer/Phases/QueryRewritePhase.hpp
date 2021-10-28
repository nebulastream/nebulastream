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

#ifndef NES_INCLUDE_OPTIMIZER_PHASES_QUERY_REWRITE_PHASE_HPP_
#define NES_INCLUDE_OPTIMIZER_PHASES_QUERY_REWRITE_PHASE_HPP_

#include <memory>

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;
}// namespace NES

namespace NES::Optimizer {

class QueryRewritePhase;
using QueryRewritePhasePtr = std::shared_ptr<QueryRewritePhase>;

class FilterPushDownRule;
using FilterPushDownRulePtr = std::shared_ptr<FilterPushDownRule>;

class RenameStreamToProjectOperatorRule;
using RenameStreamToProjectOperatorRulePtr = std::shared_ptr<RenameStreamToProjectOperatorRule>;

class ProjectBeforeUnionOperatorRule;
using ProjectBeforeUnionOperatorRulePtr = std::shared_ptr<ProjectBeforeUnionOperatorRule>;

class AttributeSortRule;
using AttributeSortRulePtr = std::shared_ptr<AttributeSortRule>;

class BinaryOperatorSortRule;
using BinaryOperatorSortRulePtr = std::shared_ptr<BinaryOperatorSortRule>;

/**
 * @brief This phase is responsible for re-writing the query plan
 */
class QueryRewritePhase {
  public:
    static QueryRewritePhasePtr create(bool applyRulesImprovingSharingIdentification);

    /**
     * @brief Perform query plan re-write for the input query plan
     * @param queryPlan : the input query plan
     * @return updated query plan
     */
    QueryPlanPtr execute(const QueryPlanPtr& queryPlan);

  private:
    explicit QueryRewritePhase(bool applyRulesImprovingSharingIdentification);
    bool applyRulesImprovingSharingIdentification;
    FilterPushDownRulePtr filterPushDownRule;
    RenameStreamToProjectOperatorRulePtr renameStreamToProjectOperatorRule;
    ProjectBeforeUnionOperatorRulePtr projectBeforeUnionOperatorRule;
    AttributeSortRulePtr attributeSortRule;
    BinaryOperatorSortRulePtr binaryOperatorSortRule;
};
}// namespace NES::Optimizer
#endif// NES_INCLUDE_OPTIMIZER_PHASES_QUERY_REWRITE_PHASE_HPP_

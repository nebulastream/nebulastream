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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_QUERYREWRITEPHASE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_QUERYREWRITEPHASE_HPP_

#include <memory>

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

namespace Configurations {

class CoordinatorConfiguration;
using CoordinatorConfigurationPtr = std::shared_ptr<CoordinatorConfiguration>;

}// namespace Configurations

}// namespace NES

namespace NES::Optimizer {

class QueryRewritePhase;
using QueryRewritePhasePtr = std::shared_ptr<QueryRewritePhase>;

class AttributeSortRule;
using AttributeSortRulePtr = std::shared_ptr<AttributeSortRule>;

class BinaryOperatorSortRule;
using BinaryOperatorSortRulePtr = std::shared_ptr<BinaryOperatorSortRule>;

class FilterMergeRule;
using FilterMergeRulePtr = std::shared_ptr<FilterMergeRule>;

class FilterPushDownRule;
using FilterPushDownRulePtr = std::shared_ptr<FilterPushDownRule>;

class FilterSplitUpRule;
using FilterSplitUpRulePtr = std::shared_ptr<FilterSplitUpRule>;

class MapUDFsToOpenCLOperatorsRule;
using MapUDFsToOpenCLOperatorsRulePtr = std::shared_ptr<MapUDFsToOpenCLOperatorsRule>;

class PredicateReorderingRule;
using PredicateReorderingRulePtr = std::shared_ptr<PredicateReorderingRule>;

class ProjectBeforeUnionOperatorRule;
using ProjectBeforeUnionOperatorRulePtr = std::shared_ptr<ProjectBeforeUnionOperatorRule>;

class RenameSourceToProjectOperatorRule;
using RenameSourceToProjectOperatorRulePtr = std::shared_ptr<RenameSourceToProjectOperatorRule>;

class RedundancyEliminationRule;
using RedundancyEliminationRulePtr = std::shared_ptr<RedundancyEliminationRule>;

/**
 * @brief This phase is responsible for re-writing the query plan
 */
class QueryRewritePhase {
  public:
    static QueryRewritePhasePtr create(const Configurations::CoordinatorConfigurationPtr& coordinatorConfiguration);

    /**
     * @brief Perform query plan re-write for the input query plan
     * @param queryPlan : the input query plan
     * @return updated query plan
     */
    QueryPlanPtr execute(const QueryPlanPtr& queryPlan);

  private:
    explicit QueryRewritePhase(bool elegantAccelerationEnabled, bool applyRulesImprovingSharingIdentification);

    bool isElegantAccelerationEnabled;
    bool applyRulesImprovingSharingIdentification;
    AttributeSortRulePtr attributeSortRule;
    BinaryOperatorSortRulePtr binaryOperatorSortRule;
    FilterMergeRulePtr filterMergeRule;
    FilterPushDownRulePtr filterPushDownRule;
    FilterSplitUpRulePtr filterSplitUpRule;
    RedundancyEliminationRulePtr redundancyEliminationRule;
    MapUDFsToOpenCLOperatorsRulePtr mapUDFsToOpenCLOperatorsRule;
    PredicateReorderingRulePtr predicateReorderingRule;
    ProjectBeforeUnionOperatorRulePtr projectBeforeUnionOperatorRule;
    RenameSourceToProjectOperatorRulePtr renameSourceToProjectOperatorRule;
};
}// namespace NES::Optimizer
#endif  // NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_QUERYREWRITEPHASE_HPP_

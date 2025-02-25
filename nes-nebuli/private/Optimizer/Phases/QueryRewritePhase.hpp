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

#pragma once

#include <memory>

namespace NES
{

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

namespace Configurations
{

class CoordinatorConfiguration;
using CoordinatorConfigurationPtr = std::shared_ptr<CoordinatorConfiguration>;

}

}

namespace NES::Optimizer
{

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

class PredicateReorderingRule;
using PredicateReorderingRulePtr = std::shared_ptr<PredicateReorderingRule>;

class ProjectBeforeUnionOperatorRule;
using ProjectBeforeUnionOperatorRulePtr = std::shared_ptr<ProjectBeforeUnionOperatorRule>;

class RenameSourceToProjectOperatorRule;
using RenameSourceToProjectOperatorRulePtr = std::shared_ptr<RenameSourceToProjectOperatorRule>;


/**
 * @brief This phase is responsible for re-writing the query plan
 */
class QueryRewritePhase
{
public:
    static QueryRewritePhasePtr create();

    void execute(QueryPlanPtr& queryPlan) const;

private:
    explicit QueryRewritePhase();

    AttributeSortRulePtr attributeSortRule;
    BinaryOperatorSortRulePtr binaryOperatorSortRule;
    FilterMergeRulePtr filterMergeRule;
    FilterPushDownRulePtr filterPushDownRule;
    FilterSplitUpRulePtr filterSplitUpRule;
    PredicateReorderingRulePtr predicateReorderingRule;
    ProjectBeforeUnionOperatorRulePtr projectBeforeUnionOperatorRule;
    RenameSourceToProjectOperatorRulePtr renameSourceToProjectOperatorRule;
};
}

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
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/QueryRewrite/FilterMergeRule.hpp>
// #include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Optimizer/QueryRewrite/FilterSplitUpRule.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Optimizer/QueryRewrite/PredicateReorderingRule.hpp>
#include <Optimizer/QueryRewrite/RenameSourceToProjectOperatorRule.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES::Optimizer
{

std::shared_ptr<QueryRewritePhase> QueryRewritePhase::create()
{
    return std::make_shared<QueryRewritePhase>(QueryRewritePhase());
}

QueryRewritePhase::QueryRewritePhase()
{
    filterMergeRule = FilterMergeRule::create();
    // filterPushDownRule = FilterPushDownRule::create();
    filterSplitUpRule = FilterSplitUpRule::create();
    predicateReorderingRule = PredicateReorderingRule::create();
    renameSourceToProjectOperatorRule = RenameSourceToProjectOperatorRule::create();
}

void QueryRewritePhase::execute(std::shared_ptr<QueryPlan>& queryPlan) const
{
    /// Apply rules necessary for enabling query execution when stream alias or union operators are involved
    queryPlan = renameSourceToProjectOperatorRule->apply(queryPlan);

    /// TODO #105 Once we have added And, Or, and other logical functions, we can call filterMergeRule. Otherwise, it might happen that we have a filter operator with an And function as a child.
    /// Apply rule for filter split up
    queryPlan = filterSplitUpRule->apply(queryPlan);
    /// Apply rule for filter push down optimization
    // queryPlan = filterPushDownRule->apply(queryPlan);
    /// Apply rule for filter merge
    /// queryPlan = filterMergeRule->apply(queryPlan);
    /// Apply rule for filter reordering optimization
    queryPlan = predicateReorderingRule->apply(queryPlan);
}

}

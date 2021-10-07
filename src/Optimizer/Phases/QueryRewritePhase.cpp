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

#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/QueryRewrite/AttributeSortRule.hpp>
#include <Optimizer/QueryRewrite/BinaryOperatorSortRule.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Optimizer/QueryRewrite/ProjectBeforeUnionOperatorRule.hpp>
#include <Optimizer/QueryRewrite/RenameStreamToProjectOperatorRule.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES::Optimizer {

QueryRewritePhasePtr QueryRewritePhase::create(bool applyRulesImprovingSharingIdentification) {
    return std::make_shared<QueryRewritePhase>(QueryRewritePhase(applyRulesImprovingSharingIdentification));
}

QueryRewritePhase::QueryRewritePhase(bool applyRulesImprovingSharingIdentification)
    : applyRulesImprovingSharingIdentification(applyRulesImprovingSharingIdentification) {
    filterPushDownRule = FilterPushDownRule::create();
    renameStreamToProjectOperatorRule = RenameStreamToProjectOperatorRule::create();
    projectBeforeUnionOperatorRule = ProjectBeforeUnionOperatorRule::create();
    attributeSortRule = AttributeSortRule::create();
    binaryOperatorSortRule = BinaryOperatorSortRule::create();
}

QueryPlanPtr QueryRewritePhase::execute(const QueryPlanPtr& queryPlan) {
    auto duplicateQueryPlan = queryPlan->copy();
    if (applyRulesImprovingSharingIdentification) {
        duplicateQueryPlan = attributeSortRule->apply(duplicateQueryPlan);
        duplicateQueryPlan = binaryOperatorSortRule->apply(duplicateQueryPlan);
    }
    duplicateQueryPlan = renameStreamToProjectOperatorRule->apply(duplicateQueryPlan);
    duplicateQueryPlan = projectBeforeUnionOperatorRule->apply(duplicateQueryPlan);
    return filterPushDownRule->apply(duplicateQueryPlan);
}

}// namespace NES::Optimizer
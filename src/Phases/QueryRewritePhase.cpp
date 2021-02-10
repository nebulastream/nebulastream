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

#include <Optimizer/QueryRewrite/DistributeJoinRule.hpp>
#include <Optimizer/QueryRewrite/DistributeWindowRule.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Optimizer/QueryRewrite/ProjectBeforeUnionOperatorRule.hpp>
#include <Optimizer/QueryRewrite/RenameStreamToProjectOperatorRule.hpp>
#include <Phases/QueryRewritePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES {

QueryRewritePhasePtr QueryRewritePhase::create(StreamCatalogPtr streamCatalog) {
    return std::make_shared<QueryRewritePhase>(QueryRewritePhase(streamCatalog));
}

QueryRewritePhase::QueryRewritePhase(StreamCatalogPtr streamCatalog) {
    filterPushDownRule = FilterPushDownRule::create();
    logicalSourceExpansionRule = LogicalSourceExpansionRule::create(streamCatalog);
    distributeWindowRule = DistributeWindowRule::create();
    distributeJoinRule = DistributeJoinRule::create();
    renameStreamToProjectOperatorRule = RenameStreamToProjectOperatorRule::create();
    projectBeforeUnionOperatorRule = ProjectBeforeUnionOperatorRule::create();
}

QueryRewritePhase::~QueryRewritePhase() { NES_DEBUG("~QueryRewritePhase()"); }

QueryPlanPtr QueryRewritePhase::execute(QueryPlanPtr queryPlan) {
    auto duplicateQueryPlan = queryPlan->copy();
    duplicateQueryPlan = renameStreamToProjectOperatorRule->apply(duplicateQueryPlan);
    duplicateQueryPlan = filterPushDownRule->apply(duplicateQueryPlan);
    duplicateQueryPlan = logicalSourceExpansionRule->apply(duplicateQueryPlan);
    duplicateQueryPlan = distributeJoinRule->apply(duplicateQueryPlan);
    duplicateQueryPlan = projectBeforeUnionOperatorRule->apply(duplicateQueryPlan);
    return distributeWindowRule->apply(duplicateQueryPlan);
}

}// namespace NES
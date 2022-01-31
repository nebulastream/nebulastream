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

#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/QueryRewrite/DistributeJoinRule.hpp>
#include <Optimizer/QueryRewrite/DistributeWindowRule.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <utility>

namespace NES::Optimizer {

TopologySpecificQueryRewritePhasePtr TopologySpecificQueryRewritePhase::create(SourceCatalogPtr streamCatalog,
                                                                               bool performOnlySourceOperatorExpansion,
                                                                               uint64_t windowDistributionChildrenThreshold,
                                                                               uint64_t windowDistributionCombinerThreshold) {
    return std::make_shared<TopologySpecificQueryRewritePhase>(
        TopologySpecificQueryRewritePhase(std::move(streamCatalog),
                                          performOnlySourceOperatorExpansion,
                                          windowDistributionChildrenThreshold,
                                          windowDistributionCombinerThreshold));
}

TopologySpecificQueryRewritePhase::TopologySpecificQueryRewritePhase(SourceCatalogPtr streamCatalog,
                                                                     bool performOnlySourceOperatorExpansion,
                                                                     uint64_t windowDistributionChildrenThreshold,
                                                                     uint64_t windowDistributionCombinerThreshold) {
    logicalSourceExpansionRule = LogicalSourceExpansionRule::create(std::move(streamCatalog), performOnlySourceOperatorExpansion);
    distributeWindowRule = DistributeWindowRule::create(windowDistributionChildrenThreshold, windowDistributionCombinerThreshold);
    distributeJoinRule = DistributeJoinRule::create();
}

QueryPlanPtr TopologySpecificQueryRewritePhase::execute(QueryPlanPtr queryPlan) {
    queryPlan = logicalSourceExpansionRule->apply(queryPlan);
    queryPlan = distributeJoinRule->apply(queryPlan);
    return distributeWindowRule->apply(queryPlan);
}

}// namespace NES::Optimizer
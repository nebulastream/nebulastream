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
#include <Optimizer/QueryRewrite/DistributedWindowRule.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Optimizer/QueryRewrite/NemoWindowPinningRule.hpp>
#include <Topology/Topology.hpp>
#include <utility>

namespace NES::Optimizer {

TopologySpecificQueryRewritePhasePtr
TopologySpecificQueryRewritePhase::create(NES::TopologyPtr topology,
                                          Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                          Configurations::OptimizerConfiguration configuration) {
    return std::make_shared<TopologySpecificQueryRewritePhase>(
        TopologySpecificQueryRewritePhase(topology, std::move(sourceCatalog), configuration));
}

TopologySpecificQueryRewritePhase::TopologySpecificQueryRewritePhase(TopologyPtr topology,
                                                                     Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                                                     Configurations::OptimizerConfiguration configuration)
    : topology(topology) {
    logicalSourceExpansionRule =
        LogicalSourceExpansionRule::create(std::move(sourceCatalog), configuration.performOnlySourceOperatorExpansion);
    if (configuration.enableNemoPlacement) {
        distributedWindowRule = NemoWindowPinningRule::create(configuration, topology);
    } else {
        distributedWindowRule = DistributedWindowRule::create(configuration);
    }

    distributeJoinRule = DistributeJoinRule::create();
}

QueryPlanPtr TopologySpecificQueryRewritePhase::execute(QueryPlanPtr queryPlan) {
    queryPlan = logicalSourceExpansionRule->apply(queryPlan);
    queryPlan = distributeJoinRule->apply(queryPlan);
    return distributedWindowRule->apply(queryPlan);
}

}// namespace NES::Optimizer
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

#include <Catalogs/Topology/Topology.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/QueryRewrite/DistributedWindowRule.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Optimizer/QueryRewrite/NemoJoinRule.hpp>
#include <utility>

namespace NES::Optimizer {

TopologySpecificQueryRewritePhasePtr
TopologySpecificQueryRewritePhase::create(NES::TopologyPtr topology,
                                          Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                          Configurations::OptimizerConfiguration optimizerConfiguration) {
    return std::make_shared<TopologySpecificQueryRewritePhase>(
        TopologySpecificQueryRewritePhase(topology, sourceCatalog, std::move(optimizerConfiguration)));
}

TopologySpecificQueryRewritePhase::TopologySpecificQueryRewritePhase(
    TopologyPtr topology,
    Catalogs::Source::SourceCatalogPtr sourceCatalog,
    Configurations::OptimizerConfiguration optimizerConfiguration)
    : topology(topology), optimizerConfiguration(optimizerConfiguration) {
    logicalSourceExpansionRule =
        LogicalSourceExpansionRule::create(sourceCatalog, optimizerConfiguration.performOnlySourceOperatorExpansion);
}

QueryPlanPtr TopologySpecificQueryRewritePhase::execute(QueryPlanPtr queryPlan) {
    queryPlan = logicalSourceExpansionRule->apply(queryPlan);
    if (optimizerConfiguration.enableNemoPlacement) {
        auto nemoJoinRule = NemoJoinRule::create(optimizerConfiguration, topology);
        queryPlan = nemoJoinRule->apply(queryPlan);
    }
    return queryPlan;
}

}// namespace NES::Optimizer
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
#include <Optimizer/QueryRewrite/DistributedMatrixJoinRule.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <StatisticCollection/StatisticProbeHandling/StatisticProbeInterface.hpp>
#include <utility>

namespace NES::Optimizer {

TopologySpecificQueryRewritePhasePtr
TopologySpecificQueryRewritePhase::create(NES::TopologyPtr topology,
                                          Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                          Configurations::OptimizerConfiguration optimizerConfiguration,
                                          Statistic::StatisticProbeHandlerPtr statisticProbeHandler) {
    return std::make_shared<TopologySpecificQueryRewritePhase>(
        TopologySpecificQueryRewritePhase(topology, sourceCatalog, optimizerConfiguration, statisticProbeHandler));
}

TopologySpecificQueryRewritePhase::TopologySpecificQueryRewritePhase(
    TopologyPtr topology,
    const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
    Configurations::OptimizerConfiguration optimizerConfiguration,
    Statistic::StatisticProbeHandlerPtr statisticProbeHandler)
    : topology(topology), optimizerConfiguration(optimizerConfiguration), statisticProbeHandler(statisticProbeHandler) {
    logicalSourceExpansionRule =
        LogicalSourceExpansionRule::create(sourceCatalog, optimizerConfiguration.performOnlySourceOperatorExpansion);
}

QueryPlanPtr TopologySpecificQueryRewritePhase::execute(QueryPlanPtr queryPlan) {
    // We simply write this line here to make sure that the statisticProbeHandler can be used in the optimizer.
    ((void) statisticProbeHandler);

    queryPlan = logicalSourceExpansionRule->apply(queryPlan);
    if (optimizerConfiguration.joinOptimizationMode == DistributedJoinOptimizationMode::MATRIX) {
        auto matrixJoinRule = DistributedMatrixJoinRule::create(optimizerConfiguration, topology);
        queryPlan = matrixJoinRule->apply(queryPlan);
    }
    return queryPlan;
}

}// namespace NES::Optimizer
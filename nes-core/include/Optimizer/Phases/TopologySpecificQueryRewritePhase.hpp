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

#ifndef NES_CORE_INCLUDE_OPTIMIZER_PHASES_TOPOLOGYSPECIFICQUERYREWRITEPHASE_HPP_
#define NES_CORE_INCLUDE_OPTIMIZER_PHASES_TOPOLOGYSPECIFICQUERYREWRITEPHASE_HPP_

#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <memory>

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

namespace Catalogs::Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Catalogs::Source

}// namespace NES

namespace NES::Optimizer {

class TopologySpecificQueryRewritePhase;
using TopologySpecificQueryRewritePhasePtr = std::shared_ptr<TopologySpecificQueryRewritePhase>;

class LogicalSourceExpansionRule;
using LogicalSourceExpansionRulePtr = std::shared_ptr<LogicalSourceExpansionRule>;

class DistributedWindowRule;
using DistributeWindowRulePtr = std::shared_ptr<DistributedWindowRule>;

class DistributeJoinRule;
using DistributeJoinRulePtr = std::shared_ptr<DistributeJoinRule>;

/**
 * @brief This phase is responsible for re-writing the query plan based on the topology information.
 */
class TopologySpecificQueryRewritePhase {
  public:
    /**
     * @brief Create the TopologySpecificQueryRewritePhase with a specific optimizer configuration
     * @param sourceCatalog the catalog of all registered sources
     * @param optimizerConfiguration configuration of the optimizer
     * @return TopologySpecificQueryRewritePhasePtr
     */
    static TopologySpecificQueryRewritePhasePtr create(const TopologyPtr& topology,
                                                       const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                                       Configurations::OptimizerConfiguration optimizerConfiguration);

    /**
     * @brief Perform query plan re-write for the input query plan
     * @param queryPlan : the input query plan
     * @return updated query plan
     */
    QueryPlanPtr execute(QueryPlanPtr queryPlan);

  private:
    explicit TopologySpecificQueryRewritePhase(const TopologyPtr& topology,
                                               const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                               Configurations::OptimizerConfiguration optimizerConfiguration);
    TopologyPtr topology;
    LogicalSourceExpansionRulePtr logicalSourceExpansionRule;
    DistributeWindowRulePtr distributedWindowRule;
    DistributeJoinRulePtr distributeJoinRule;
};
}// namespace NES::Optimizer
#endif// NES_CORE_INCLUDE_OPTIMIZER_PHASES_TOPOLOGYSPECIFICQUERYREWRITEPHASE_HPP_

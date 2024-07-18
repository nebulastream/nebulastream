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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_DISTRIBUTEDNEMOJOINRULE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_DISTRIBUTEDNEMOJOINRULE_HPP_

#include <map>
#include <set>
#include <vector>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES
{
class Topology;
using TopologyPtr = std::shared_ptr<Topology>;
class SourceLogicalOperator;
using SourceLogicalOperatorPtr = std::shared_ptr<SourceLogicalOperator>;
} // namespace NES

namespace NES::Catalogs::Source
{
class SourceCatalogEntry;
using SourceCatalogEntryPtr = std::shared_ptr<SourceCatalogEntry>;
} // namespace NES::Catalogs::Source

namespace NES::Optimizer
{
class DistributedNemoJoinRule;
using DistributedNemoJoinRulePtr = std::shared_ptr<DistributedNemoJoinRule>;

/**
 * @brief DistributedNemoJoinRule which is rewriting a central to a distributed grid-partitioned join based on the physical streams.
 */
class DistributedNemoJoinRule : public BaseRewriteRule
{
public:
    static DistributedNemoJoinRulePtr create(
        Configurations::OptimizerConfiguration configuration,
        TopologyPtr topology,
        const std::map<Catalogs::Source::SourceCatalogEntryPtr, std::set<uint64_t>>& distMap);
    virtual ~DistributedNemoJoinRule() = default;

    /**
     * @brief Searches query plan for a join operator and identifes partial joins. Then it replicates the join operators and
     * pushes them closer to the sources
     * @param queryPlan: the original non-expanded query plan
     * @return expanded logical query plan
     */
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

private:
    explicit DistributedNemoJoinRule(
        Configurations::OptimizerConfiguration configuration,
        TopologyPtr topology,
        const std::map<Catalogs::Source::SourceCatalogEntryPtr, std::set<uint64_t>>& distMap);

    /**
     * Helper function to identify in the distribution map nodes with common keys
     * @return A map that points from key -> a set of workers that share the same key
     */
    std::map<uint64_t, std::set<WorkerId>> getNodesWithCommonKeys();

    /**
     * A helper function to find operators with common keys
     * @param queryPlan
     * @param commonKeys
     * @return A map that points from a key -> set of operators
     */
    static std::map<uint64_t, std::set<SourceLogicalOperatorPtr>>
    getOperatorsWithCommonKeys(const QueryPlanPtr& queryPlan, const std::map<uint64_t, std::set<WorkerId>>& commonKeys);

private:
    TopologyPtr topology;
    Configurations::OptimizerConfiguration configuration;
    std::map<Catalogs::Source::SourceCatalogEntryPtr, std::set<uint64_t>> keyDistributionMap;
};
} // namespace NES::Optimizer

#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_DISTRIBUTEDNEMOJOINRULE_HPP_

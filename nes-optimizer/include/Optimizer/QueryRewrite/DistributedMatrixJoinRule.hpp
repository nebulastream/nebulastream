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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_DISTRIBUTEDMATRIXJOINRULE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_DISTRIBUTEDMATRIXJOINRULE_HPP_

#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>
#include <vector>

namespace NES {
class Topology;
using TopologyPtr = std::shared_ptr<Topology>;
class JoinLogicalOperatorNode;
using JoinLogicalOperatorNodePtr = std::shared_ptr<JoinLogicalOperatorNode>;
class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;
class Node;
using NodePtr = std::shared_ptr<Node>;
}// namespace NES

namespace NES::Optimizer {
class DistributedMatrixJoinRule;
using DistributedMatrixJoinRulePtr = std::shared_ptr<DistributedMatrixJoinRule>;

/**
 * @brief DistributedMatrixJoinRule which is rewriting a central to a distributed grid-partitioned join based on the physical streams.
 */
class DistributedMatrixJoinRule : public BaseRewriteRule {
  public:
    static DistributedMatrixJoinRulePtr create(Configurations::OptimizerConfiguration configuration, TopologyPtr topology);
    virtual ~DistributedMatrixJoinRule() = default;

    /**
     * @brief Searches query plan for a join operator and replicates the join operators across physical sources equal
     * to the distributed grid-based partitioning scheme
     * @param queryPlan: the original non-expanded query plan
     * @return expanded logical query plan
     */
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

  private:
    explicit DistributedMatrixJoinRule(Configurations::OptimizerConfiguration configuration, TopologyPtr topology);

  private:
    TopologyPtr topology;
    Configurations::OptimizerConfiguration configuration;
};
}// namespace NES::Optimizer
#endif // NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_DISTRIBUTEDMATRIXJOINRULE_HPP_

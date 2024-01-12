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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_NEMOJOINRULE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_NEMOJOINRULE_HPP_

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
class NemoJoinRule;
using NemoJoinRulePtr = std::shared_ptr<NemoJoinRule>;

/**
 * @brief This rule currently only set the right number of join input edges
 */
class NemoJoinRule : public BaseRewriteRule {
  public:
    static NemoJoinRulePtr create(Configurations::OptimizerConfiguration configuration, TopologyPtr topology);
    virtual ~NemoJoinRule() = default;

    /**
     * @brief Apply Logical source expansion rule on input query plan
     * @param queryPlan: the original non-expanded query plan
     * @return expanded logical query plan
     */
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

  private:
    explicit NemoJoinRule(Configurations::OptimizerConfiguration configuration, TopologyPtr topology);
    static JoinLogicalOperatorNodePtr createJoinReplica(JoinLogicalOperatorNodePtr joinOperator,
                                                 std::vector<OperatorNodePtr> leftOperators,
                                                 std::vector<OperatorNodePtr> rightOperators,
                                                 std::vector<NodePtr> parents);

  private:
    TopologyPtr topology;
    Configurations::OptimizerConfiguration configuration;
};
}// namespace NES::Optimizer
#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_NEMOJOINRULE_HPP_

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


#ifndef NES_JOINORDEROPTIMIZATIONRULE_HPP_
#define NES_JOINORDEROPTIMIZATIONRULE_HPP_
#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>
#include <Windowing/JoinEdge.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Util/OptimizerPlanOperator.hpp>
#include <Util/AbstractJoinPlanOperator.hpp>


namespace NES {

    class Node;
    using NodePtr = std::shared_ptr<Node>;

}// namespace NES

namespace NES::Optimizer {

class JoinOrderOptimizationRule;
using JoinOrderOptimizationRulePtr = std::shared_ptr<JoinOrderOptimizationRule>;

/**
 * @brief This class is responsible for altering the query plan to optimize the join order based on a cost-model.
 */

class JoinOrderOptimizationRule : public BaseRewriteRule {

  public:
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

    static JoinOrderOptimizationRulePtr create();
    virtual ~JoinOrderOptimizationRule() = default;

  private:
    explicit JoinOrderOptimizationRule();


    /**
     * @brief Checks which join connections between sources are possible in a query plan and returns a list of all possible connections (edges)
     * e.g. if there are three streams: Cars, Trucks, Bikes all have common attributes which allow arbitrary join orders.
     * This function would in this case return a list with Cars |><| Trucks |><| Bikes, Cars |><| Bikes |><| Trucks and Trucks |><| Bikes |><| Cars...
     * @param queryPlan: pointer to the query plan build for the query, joinOperators of the query
     * @return list of source connections
     */
    std::vector<Join::JoinEdge> retrieveJoinEdges(QueryPlanPtr queryPlan, std::vector<JoinLogicalOperatorNodePtr> joinOperators);

    /**
     * @brief Optimize order of join operators according to cost-model
     * @param Sources logical streams to optimize on, joins - edges between streams
     * @return OptimizerPlanOperator that is the optimal join order according to cost-model
     */

    OptimizerPlanOperator optimizeJoinOrder(std::vector<OptimizerPlanOperator> sources, std::vector<Join::JoinEdge> joins);

    /**
     * @brief Filters out the substring that determines the logical source of a join partner.
     * e.g. FieldAccessNode(REGION$id) becomes REGION
     * @param joinKey
     * @return Name of the logical source
     */
    std::string deriveSourceName(FieldAccessExpressionNodePtr joinKey);

    /**
     * @brief provides a string that lists all join edges to ease readability
     * @param joinEdgegs
     */
    std::string listJoinEdges(std::vector<Join::JoinEdge> joinEdges);

    /**
     * @brief Creates for each level (number of involved logical streams) based on the joinEdges possible join candidates
     * @param subs hashmap containing all levels and entries
     * @param level number of involved logical streams
     * @param joins joinEdges giving information about the possible joins.
     * @return updated join table
     */
    std::map<int, std::map<std::set<OptimizerPlanOperator>, OptimizerPlanOperator>> createJoinCandidates(std::map<int, std::map<std::set<OptimizerPlanOperator>, OptimizerPlanOperator>> subs, int level, std::vector<Join::JoinEdge> joins);

    /**
     * @brief sets operator and cumulative costs for a joinStep on level 2 or above.
     */
    void setCosts(AbstractJoinPlanOperator plan);

    // TODO write description once you have better understanding
    /**
     * @brief for a specific level of joins (number of involved logical streams) count the number of possible combinations
     * @param level
     * @return
     */
    std::vector<std::vector<int>> getCountCombs(int level);

    /**
     * @brief Checks if two OptimizerPlanOperators can be joined and if so, returns the joined AbstractJoinPlanOperator
     * @param left OptimizerPlanOperator - left side of the join
     * @param right OptimizerPlanOperator - right side of the join
     * @param joins - all "allowed" join edges, that won't result in a cartesian product
     * @return joined AbstractJoinPlanOperator resulting from joining left and right.
     */
    std::optional<AbstractJoinPlanOperator> join(OptimizerPlanOperator left, OptimizerPlanOperator right, std::vector<Join::JoinEdge> joins);

    /**
     * @brief checks whether the involved logical streams of left plan and right plan have a potential join candidate.
     * @param leftInvolved
     * @param rightInvolved
     * @param edge
     * @return true if there is an edge indicating a possible join and that join did not already happen
     */
    bool isIn(std::set<OptimizerPlanOperator> leftInvolved, std::set<OptimizerPlanOperator> rightInvolved, Join::JoinEdge edge);
};
} // namespace NES::Optimizer
#endif NES_JOINORDEROPTIMIZATIONRULE_HPP_

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
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Util/OptimizerPlanOperator.hpp>
#include <Util/AbstractJoinPlanOperator.hpp>


namespace NES {

    class Node;
    using NodePtr = std::shared_ptr<Node>;

}// namespace NES

namespace NES::Optimizer {

class TimeSequenceList;
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

    /**
     * @brief sets hardcoded Cardinalities for all base sources of the test  classes.
     * @param source
     */
    static int getHardCodedCardinalitiesForSource(OptimizerPlanOperatorPtr source);

  private:
    explicit JoinOrderOptimizationRule();


    /**
     * @brief Checks which join connections between sources are possible in a query plan and returns a list of all possible connections (edges)
     * e.g. if there are three streams: Cars, Trucks, Bikes all have common attributes which allow arbitrary join orders.
     * This function would in this case return a list with Cars |><| Trucks |><| Bikes, Cars |><| Bikes |><| Trucks and Trucks |><| Bikes |><| Cars...
     * @param abstractJoinOperators a vector of AbstractJoinPlanOperatorPtr - which are logical sources with some join optimization relevant information, joinOperators of the query
     * @return list of source connections
     */
    std::vector<Join::JoinEdgePtr> retrieveJoinEdges(std::vector<JoinLogicalOperatorNodePtr> joinOperators, std::vector<AbstractJoinPlanOperatorPtr> abstractJoinOperators);

    /**
     * @brief Retrieves possible (minding time-constraints) join edges between event streams. As they are cartesian products it is not necessary to check the join key,
     * but it is important to check if this generally is in line with the event order given by a sequence operator.
     * @param abstractJoinOperators a vector of AbstractJoinPlanOperatorPtr - which are logical sources with some join optimization relevant information, joinOperators of the query, filter operators attached to joins.
     * @return list of source connections
     */
    std::vector<Join::JoinEdgePtr> retrieveJoinEdges(std::vector<JoinLogicalOperatorNodePtr> joinOperators,
                                                     std::vector<AbstractJoinPlanOperatorPtr> abstractJoinOperators,
                                                     std::vector<FilterLogicalOperatorNodePtr> filterOperators);


    /**
     * @brief Optimize order of join operators according to cost-model
     * @param Sources logical streams to optimize on, joins - edges between streams
     * @return AbstractJoinPlanOperatorPtr that is the optimal join order according to cost-model
     */

    AbstractJoinPlanOperatorPtr optimizeJoinOrder(std::vector<OptimizerPlanOperatorPtr> sources, std::vector<Join::JoinEdgePtr> joins);

    /**
     * @brief Filters out the substring that determines the logical source of a join partner.
     * e.g. FieldAccessNode(REGION$id) becomes REGION
     * @param joinKey
     * @return Name of the logical source
     */
    std::string deriveSourceName(FieldAccessExpressionNodePtr joinKey);

    /**
     * @brief provides a string that lists all join edges to ease readability
     * @param joinEdges
     */
    std::string listJoinEdges(std::vector<Join::JoinEdgePtr> joinEdges);

    /**
     * @brief Creates for each level (number of involved logical streams) based on the joinEdges possible join candidates
     * @param subs hashmap containing all levels and entries
     * @param level number of involved logical streams
     * @param joins joinEdges giving information about the possible joins.
     * @return updated join table
     */
    std::map<int, std::map<std::set<OptimizerPlanOperatorPtr>, AbstractJoinPlanOperatorPtr>> createJoinCandidates(std::map<int, std::map<std::set<OptimizerPlanOperatorPtr>, AbstractJoinPlanOperatorPtr>> subs, int level, std::vector<Join::JoinEdgePtr> joins);

    std::map<int, std::map<std::set<OptimizerPlanOperatorPtr>, AbstractJoinPlanOperatorPtr>>
    createSequenceCandidates(std::map<int, std::map<std::set<OptimizerPlanOperatorPtr>, AbstractJoinPlanOperatorPtr>> subs,
                             int level,
                             std::set<OptimizerPlanOperatorPtr> set,
                             TimeSequenceList* pList,
                             std::vector<AbstractJoinPlanOperatorPtr> vector,
                             std::vector<JoinLogicalOperatorNodePtr> vector1);



    /**
     * @brief sets operator and cumulative costs for a joinStep on level 2 or above.
     */
    void setCosts(AbstractJoinPlanOperatorPtr plan);

    /**
     * @brief for a specific level of joins (number of involved logical streams) add all possible combinations of levels
     * e.g. imagine we have consider all combination where we have 2 involved joins. We can only join them by joining sources of level 1 (base sources) with level 2 sources (already joined once)
     * e.g. for 3 involved joins we could either join  a base source (level 1) with a source that is joined twice already (level 2) or join two sources of level 2 -- meaning two join products.
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
    std::optional<AbstractJoinPlanOperatorPtr> join(AbstractJoinPlanOperatorPtr left, AbstractJoinPlanOperatorPtr right, std::vector<Join::JoinEdgePtr> joins);

    /**
     * @brief checks whether the involved logical streams of left plan and right plan have a potential join candidate.
     * @param leftInvolved
     * @param rightInvolved
     * @param edge
     * @return true if there is an edge indicating a possible join and that join did not already happen
     */
    bool isIn(std::set<OptimizerPlanOperatorPtr> leftInvolved, std::set<OptimizerPlanOperatorPtr> rightInvolved, Join::JoinEdgePtr edge);

    /**
     * @brief adds hardcoded join selectivities to joinEdges
     * @param joinEdges
     */
    void getHardCodedJoinSelectivities(std::vector<Join::JoinEdgePtr> joinEdges);
    /**
     * @brief extracts a list of join steps of the root plan (AbstractJoinPlanOperatorPtr) in a recursive manner
     * Produces a vector of dynamic depth. each level resembles a join.
     * @param root - AbstractJoinPlanOperatorPtr containing potentially a series of joins.
     * @return
     */
    std::any extractJoinOrder(AbstractJoinPlanOperatorPtr root);

    /**
     * @brief function that alters an existing query plan to become a new, updated version with the optimal join ordering according to our cost model.
     @param oldPlan - the pre-existing query plan that is copied and altered
     @param finalPlan - the altered query plan
     @param sourceOperators - sources of the original plan which have been derived already in an earlier step (reused here to save time)
     */
    QueryPlanPtr updateJoinOrder(QueryPlanPtr oldPlan,
                                 AbstractJoinPlanOperatorPtr finalPlan,
                                 const std::vector<SourceLogicalOperatorNodePtr> sourceOperators);
    /**
     * provides the joinOrder in a readable string format. -- is invoked recursively
     * @param joinOrder
     */
    std::string printJoinOrder(std::any joinOrder);

    /**
     * @brief recursively constructs JoinLogicalOperaterNodes and adding leftChild and rightChild respectively as Children.
     * Children can be either another JoinLogicalOperatorNode or the base-case a SourceLogicalOperatorNode
     * In case of a JoinLogicalOperatorNode we again recurse to the next level of the optimal plan and add the children.
     * In case of a SourceLogicalOperatorNode, first, it is checked, whether in the original plan there are parent operators
     * between the join and the source. these are then added calling getPotentialParentNodes.
     * This method also covers cases where one child is a join and the other one a mere source.
     * @param leftChild - leftChild of an AbstractJoinPlanOperator (join or source)
     * @param rightChild - rightChild of an AbstractJoinPlanOperator (join or source)
     * @param joinDefinition - Defines which attributes are to join
     * @param sources - List of source nodes in the original queryplan which is to be exchanged be the altered one.
     * @return
     */
    JoinLogicalOperatorNodePtr constructJoin(const AbstractJoinPlanOperatorPtr& leftChild,
                                             const AbstractJoinPlanOperatorPtr& rightChild,
                                             Join::LogicalJoinDefinitionPtr joinDefinition,
                                             const std::vector<SourceLogicalOperatorNodePtr> sources);


    /**
     * @brief Adds the parent nodes to a NodePtr until a JoinLogicalOperatorNode is found
     * @param sharedPtr - usually a SourceLogicalOperatorNode
     * @return  Topmost node in the hierarchy before a JoinLogicalOperatorNode
     */
    NodePtr getPotentialParentNodes(NodePtr sharedPtr);

    TimeSequenceList* getTimeSequenceList(std::vector<FilterLogicalOperatorNodePtr> filterOperators);
    AbstractJoinPlanOperatorPtr optimizeSequenceOrder(std::vector<OptimizerPlanOperatorPtr> sources,
                                                      TimeSequenceList* globalSequenceOrder,
                                                      std::vector<AbstractJoinPlanOperatorPtr> vector,
                                                      std::vector<JoinLogicalOperatorNodePtr> vector1);
    AbstractJoinPlanOperatorPtr getAbstractJoinPlanOperatorPtr(std::vector<AbstractJoinPlanOperatorPtr> abstractJoinPlanOperators,
                                                               std::shared_ptr<OptimizerPlanOperator> source);
    Join::LogicalJoinDefinitionPtr
    constructSequenceJoinDefinition(std::vector<JoinLogicalOperatorNodePtr> joinLogicalOperatorNodes,
                                    AbstractJoinPlanOperatorPtr leftChild,
                                    AbstractJoinPlanOperatorPtr rightChild);
};
} // namespace NES::Optimizer
#endif NES_JOINORDEROPTIMIZATIONRULE_HPP_

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

#ifndef NES_OPERATORS_INCLUDE_PLANS_QUERY_QUERYPLAN_HPP_
#define NES_OPERATORS_INCLUDE_PLANS_QUERY_QUERYPLAN_HPP_

#include <Identifiers.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Operators/OperatorNode.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <memory>
#include <set>
#include <vector>

namespace NES {

class Source;
using SourcePtr = std::shared_ptr<Source>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class SourceLogicalOperatorNode;
using SourceLogicalOperatorNodePtr = std::shared_ptr<SourceLogicalOperatorNode>;

class SinkLogicalOperatorNode;
using SinkLogicalOperatorNodePtr = std::shared_ptr<SinkLogicalOperatorNode>;

/**
 * @brief The query plan encapsulates a set of operators and provides a set of utility functions.
 */
class QueryPlan {
  public:
    /**
     * @brief Creates a new query plan with a query id, a query sub plan id and a vector of root operators.
     * @param queryId :  the query id
     * @param querySubPlanId : the query sub-plan id
     * @param rootOperators : vector of root Operators
     * @return a pointer to the query plan.
     */
    static QueryPlanPtr create(QueryId queryId, QuerySubPlanId querySubPlanId, std::vector<OperatorNodePtr> rootOperators);

    /**
     * @brief Creates a new query plan with a query id and a query sub plan id.
     * @param queryId :  the query id
     * @param querySubPlanId : the query sub-plan id
     * @return a pointer to the query plan.
     */
    static QueryPlanPtr create(QueryId queryId, QuerySubPlanId querySubPlanId);

    /**
     * @brief Creates a new query plan with a root operator.
     * @param rootOperator The root operator usually a source operator.
     * @return a pointer to the query plan.
     */
    static QueryPlanPtr create(OperatorNodePtr rootOperator);

    /**
     * @brief Creates a new query plan without and operator.
     * @return a pointer to the query plan
     */
    static QueryPlanPtr create();

    /**
     * @brief Get all operators in the query plan
     * @return vector of operators
     */
    std::set<OperatorNodePtr> getAllOperators();


    /**
     * @brief Get all source operators
     * @return vector of logical source operators
     */
    std::vector<SourceLogicalOperatorNodePtr> getSourceOperators();

    /**
     * @brief Get all sink operators
     * @return vector of logical sink operators
     */
    std::vector<SinkLogicalOperatorNodePtr> getSinkOperators();

    /**
     * @brief Appends an operator to the query plan and make the new operator as root.
     * @param operatorNode : new operator
     */
    void appendOperatorAsNewRoot(const OperatorNodePtr& operatorNode);

    /**
     * @brief Returns string representation of the query.
     */
    std::string toString();

    /**
     * @brief Get the list of root operators for the query graph.
     * NOTE: in certain stages the sink operators might not be the root operators
     * @return
     */
    std::vector<OperatorNodePtr> getRootOperators();

    /**
     * add subQuery's rootnode into the current node for merging purpose.
     * Note: improves this when we have to due with multi-root use case.
     * @param newRootOperator
     */
    void addRootOperator(const OperatorNodePtr& newRootOperator);

    /**
     * remove the an operator from the root operator list.
     * @param root
     */
    void removeAsRootOperator(OperatorNodePtr root);

    /**
     * replaces a particular root operator with a new one.
     * @param root
     * @return true if operator was replaced.
     */
    bool replaceRootOperator(const OperatorNodePtr& oldRoot, const OperatorNodePtr& newRoot);

    /**
     * @brief Get all the operators of a specific type
     * @return returns a vector of operators
     */
    template<class T>
    std::vector<std::shared_ptr<T>> getOperatorByType() {
        // Find all the nodes in the query plan
        std::vector<std::shared_ptr<T>> operators;
        // Maintain a list of visited nodes as there are multiple root nodes
        std::set<uint64_t> visitedOpIds;
        for (const auto& rootOperator : rootOperators) {
            auto bfsIterator = BreadthFirstNodeIterator(rootOperator);
            for (auto itr = bfsIterator.begin(); itr != NES::BreadthFirstNodeIterator::end(); ++itr) {
                auto visitingOp = (*itr)->as<OperatorNode>();
                if (visitedOpIds.find(visitingOp->getId()) != visitedOpIds.end()) {
                    // skip rest of the steps as the node found in already visited node list
                    continue;
                }
                visitedOpIds.insert(visitingOp->getId());
                if (visitingOp->instanceOf<T>()) {
                    operators.push_back(visitingOp->as<T>());
                }
            }
        }
        return operators;
    }

    /**
    * @brief Get all the leaf operators in the query plan (leaf operator is the one without any child)
     * Note: in certain stages the source operators might not be Leaf operators
    * @return returns a vector of leaf operators
    */
    std::vector<OperatorNodePtr> getLeafOperators();

    /**
     * Find if the operator with the input Id exists in the plan.
     * Note: This method only check if there exists another operator with same Id or not.
     * Note: The system generated operators are ignored from this check.
     * @param operatorId: Id of the operator
     * @return true if the operator exists else false
     */
    bool hasOperatorWithId(uint64_t operatorId);

    /**
     * @brief Get operator node with input id if present
     * @param operatorId : the input operator id
     * @return operator with the input id
     */
    OperatorNodePtr getOperatorWithId(uint64_t operatorId);

    /**
     * Set the query Id for the plan
     * @param queryId
     */
    void setQueryId(QueryId queryId);

    /**
     * Get the queryId for the plan
     * @return query Id of the plan
     */
    [[nodiscard]] QueryId getQueryId() const;

    /**
     * @brief get query sub plan id
     * @return return query sub plan id
     */
    QuerySubPlanId getQuerySubPlanId() const;

    /**
     * @brief Set query sub plan id
     * @param querySubPlanId : input query sub plan id
     */
    void setQuerySubPlanId(uint64_t querySubPlanId);

    /**
     * @brief Create copy of the query plan
     * @return copy of the query plan
     */
    QueryPlanPtr copy();

    [[nodiscard]] std::string getSourceConsumed() const;

    /**
     * @brief Set the logical sources used in the query
     * @param sourceName: the name of the logical source
     */
    void setSourceConsumed(const std::string& sourceName);

    /**
     * @brief Set query placement strategy
     * @param PlacementStrategy: query placement strategy
     */
    void setPlacementStrategy(Optimizer::PlacementStrategy placementStrategy);

    /**
     * @brief Get the placement strategy for the shared query plan
     * @return placement strategy
     */
    Optimizer::PlacementStrategy getPlacementStrategy() const;

    /**
     * @brief Find all operators between given set of downstream and upstream operators
     * @param downstreamOperators : the downstream operators
     * @param upstreamOperators : the upstream operators
     * @return all operators between (excluding) downstream and upstream operators
     */
    std::set<OperatorNodePtr> findAllOperatorsBetween(const std::set<OperatorNodePtr>& downstreamOperators,
                                                      const std::set<OperatorNodePtr>& upstreamOperators);

  private:
    /**
     * @brief Creates a new query plan with a query id, a query sub plan id and a vector of root operators.
     * @param queryId :  the query id
     * @param querySubPlanId : the query sub-plan id
     * @param rootOperators : vector of root Operators
     */
    QueryPlan(QueryId queryId, QuerySubPlanId querySubPlanId, std::vector<OperatorNodePtr> rootOperators);

    /**
     * @brief Creates a new query plan with a query id and a query sub plan id.
     * @param queryId :  the query id
     * @param querySubPlanId : the query sub-plan id
     */
    QueryPlan(QueryId queryId, QuerySubPlanId querySubPlanId);

    /**
     * @brief initialize query plan with a root operator
     * @param rootOperator
     */
    explicit QueryPlan(OperatorNodePtr rootOperator);

    /**
     * @brief initialize an empty query plan
     */
    QueryPlan();

    /**
     * @brief Find operators between source and target operators
     * @param sourceOperator: the source operator
     * @param targetOperators: the target operator
     * @return empty or operators between source and target operators
     */
    std::set<OperatorNodePtr> findOperatorsBetweenSourceAndTargetOperators(const OperatorNodePtr& sourceOperator,
                                                                           const std::set<OperatorNodePtr>& targetOperators);

    std::vector<OperatorNodePtr> rootOperators{};
    QueryId queryId;
    QuerySubPlanId querySubPlanId;
    std::string sourceConsumed;
    // Default placement strategy is top-down; we set the correct placement strategy in the Experimental Add Request
    Optimizer::PlacementStrategy placementStrategy = Optimizer::PlacementStrategy::TopDown;
};
}// namespace NES
#endif // NES_OPERATORS_INCLUDE_PLANS_QUERY_QUERYPLAN_HPP_

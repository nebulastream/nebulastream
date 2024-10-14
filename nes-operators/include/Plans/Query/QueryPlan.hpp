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

#pragma once

#include <memory>
#include <unordered_set>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/Operator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <Util/QueryState.hpp>


namespace NES
{

class Operator;
using OperatorPtr = std::shared_ptr<Operator>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class SourceNameLogicalOperator;
using SourceNameLogicalOperatorPtr = std::shared_ptr<SourceNameLogicalOperator>;

class SinkLogicalOperator;
using SinkLogicalOperatorPtr = std::shared_ptr<SinkLogicalOperator>;

/**
 * @brief The query plan encapsulates a set of operators and provides a set of utility functions.
 */
class QueryPlan
{
public:
    /**
     * @brief Creates a new query plan with a query id, a query sub plan id and a vector of root operators.
     * @param queryId :  the query id
     * @param rootOperators : vector of root Operators
     * @return a pointer to the query plan.
     */
    static QueryPlanPtr create(QueryId queryId, std::vector<OperatorPtr> rootOperators);

    /**
     * @brief Creates a new query plan with a query id and a query sub plan id.
     * @param queryId :  the query id
     * @return a pointer to the query plan.
     */
    static QueryPlanPtr create(QueryId queryId);

    /**
     * @brief Creates a new query plan with a root operator.
     * @param rootOperator The root operator usually a source operator.
     * @return a pointer to the query plan.
     */
    static QueryPlanPtr create(OperatorPtr rootOperator);

    /**
     * @brief Creates a new query plan without and operator.
     * @return a pointer to the query plan
     */
    static QueryPlanPtr create();

    template <typename LogicalSourceType>
    std::vector<std::shared_ptr<LogicalSourceType>> getSourceOperators() const
    {
        NES_DEBUG("Get all source operators by traversing all the root nodes.");
        std::unordered_set<std::shared_ptr<LogicalSourceType>> sourceOperatorsSet;
        for (const auto& rootOperator : rootOperators)
        {
            auto sourceOptrs = rootOperator->getNodesByType<LogicalSourceType>();
            NES_DEBUG("insert all source operators to the collection");
            sourceOperatorsSet.insert(sourceOptrs.begin(), sourceOptrs.end());
        }
        NES_DEBUG("Found {} source operators.", sourceOperatorsSet.size());
        std::vector<std::shared_ptr<LogicalSourceType>> sourceOperators{sourceOperatorsSet.begin(), sourceOperatorsSet.end()};
        return sourceOperators;
    }

    /**
     * @brief Get all sink operators
     * @return vector of logical sink operators
     */
    std::vector<SinkLogicalOperatorPtr> getSinkOperators() const;

    /**
     * @brief Appends an operator to the query plan and make the new operator as root.
     * @param operatorNode : new operator
     */
    void appendOperatorAsNewRoot(const OperatorPtr& operatorNode);

    /**
     * @brief Returns string representation of the query.
     */
    std::string toString() const;

    /**
     * @brief Get the list of root operators for the query graph.
     * NOTE: in certain stages the sink operators might not be the root operators
     * @return
     */
    std::vector<OperatorPtr> getRootOperators() const;

    /**
     * add subQuery's rootnode into the current node for merging purpose.
     * Note: improves this when we have to due with multi-root use case.
     * @param newRootOperator
     */
    void addRootOperator(const OperatorPtr& newRootOperator);

    /**
     * remove the an operator from the root operator list.
     * @param root
     */
    void removeAsRootOperator(OperatorPtr root);

    /**
     * @brief Get all the operators of a specific type
     * @return returns a vector of operators
     */
    template <class T>
    std::vector<std::shared_ptr<T>> getOperatorByType() const
    {
        /// Find all the nodes in the query plan
        std::vector<std::shared_ptr<T>> operators;
        /// Maintain a list of visited nodes as there are multiple root nodes
        std::set<OperatorId> visitedOpIds;
        for (const auto& rootOperator : rootOperators)
        {
            auto bfsIterator = BreadthFirstNodeIterator(rootOperator);
            for (auto itr = bfsIterator.begin(); itr != NES::BreadthFirstNodeIterator::end(); ++itr)
            {
                auto visitingOp = NES::Util::as<Operator>(*itr);
                if (visitedOpIds.contains(visitingOp->getId()))
                {
                    /// skip rest of the steps as the node found in already visited node list
                    continue;
                }
                visitedOpIds.insert(visitingOp->getId());
                if (NES::Util::instanceOf<T>(visitingOp))
                {
                    operators.push_back(NES::Util::as<T>(visitingOp));
                }
            }
        }
        return operators;
    }

    /**
    * @brief Get all the leaf operators in the query plan (leaf operator is the one without any child)
    * @note: in certain stages the source operators might not be Leaf operators
    * @return returns a vector of leaf operators
    */
    std::vector<OperatorPtr> getLeafOperators() const;

    /**
     * @brief Get all operators in the query plan
     * @return unordered_set of operators
     */
    std::unordered_set<OperatorPtr> getAllOperators() const;

    /**
     * Find if the operator with the input Id exists in the plan.
     * Note: This method only check if there exists another operator with same Id or not.
     * Note: The system generated operators are ignored from this check.
     * @param operatorId: Id of the operator
     * @return true if the operator exists else false
     */
    bool hasOperatorWithId(OperatorId operatorId);

    /**
     * @brief Get operator node with input id if present
     * @param operatorId : the input operator id
     * @return operator with the input id
     */
    OperatorPtr getOperatorWithOperatorId(OperatorId operatorId) const;

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
     * @brief Create copy of the query plan
     * @return copy of the query plan
     */
    QueryPlanPtr copy();

    [[nodiscard]] std::string getSourceConsumed() const;

    /**
     * @brief Set the logical sources used in the query
     * @param sourceName: the name of the logical source
     */
    void setSourceConsumed(std::string_view sourceName);

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
    std::set<OperatorPtr>
    findAllOperatorsBetween(const std::set<OperatorPtr>& downstreamOperators, const std::set<OperatorPtr>& upstreamOperators);

    /**
     * @brief Get state of the query plan
     * @return query state
     */
    QueryState getQueryState() const;

    /**
     * @brief Set state of the query plan
     * @param newState : new query plan state
     */
    void setQueryState(QueryState newState);

    /**
     * @brief Clears all root operators
     */
    void clearRootOperators();

    /**
     * @brief Comparison to another plan and its children nodes by tree traversal.
     * @param otherPlan: the other plan to be compared to this plan
     * @return true, if this and other plan are equal in their structure and operators, false else
     */
    bool compare(QueryPlanPtr& otherPlan);

private:
    /**
     * @brief Creates a new query plan with a query id, a query sub plan id and a vector of root operators.
     * @param queryId :  the query id
     * @param rootOperators : vector of root Operators
     */
    QueryPlan(QueryId queryId, std::vector<OperatorPtr> rootOperators);

    /**
     * @brief Creates a new query plan with a query id and a query sub plan id.
     * @param queryId :  the query id
     */
    explicit QueryPlan(QueryId queryId);

    /**
     * @brief initialize query plan with a root operator
     * @param rootOperator
     */
    explicit QueryPlan(OperatorPtr rootOperator);

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
    std::set<OperatorPtr>
    findOperatorsBetweenSourceAndTargetOperators(const OperatorPtr& sourceOperator, const std::set<OperatorPtr>& targetOperators);

    std::vector<OperatorPtr> rootOperators{};
    QueryId queryId = INVALID_QUERY_ID;
    std::string sourceConsumed;
    QueryState currentState;
    /// Default placement strategy is top-down; we set the correct placement strategy in the Experimental Add Request
    Optimizer::PlacementStrategy placementStrategy = Optimizer::PlacementStrategy::TopDown;
};
} /// namespace NES

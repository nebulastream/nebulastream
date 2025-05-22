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
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Operator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <Util/QueryState.hpp>

namespace NES
{


/// The query plan encapsulates a set of operators and provides a set of utility functions.
class QueryPlan
{
public:
    QueryPlan() = default;
    explicit QueryPlan(std::shared_ptr<Operator> rootOperator);
    explicit QueryPlan(QueryId queryId, std::vector<std::shared_ptr<Operator>> rootOperators);
    static std::shared_ptr<QueryPlan> create(std::shared_ptr<Operator> rootOperator);
    static std::shared_ptr<QueryPlan> create(QueryId queryId, std::vector<std::shared_ptr<Operator>> rootOperators);

    template <typename LogicalSourceType>
    std::vector<std::shared_ptr<LogicalSourceType>> getSourceOperators() const
    {
        NES_DEBUG("Get all source operators by traversing all the root nodes.");
        std::unordered_set<std::shared_ptr<LogicalSourceType>> sourceOperatorsSet;
        for (const auto& rootOperator : rootOperators)
        {
            auto sourceOperators = rootOperator->getNodesByType<LogicalSourceType>();
            NES_DEBUG("insert all source operators to the collection");
            sourceOperatorsSet.insert(sourceOperators.begin(), sourceOperators.end());
        }
        NES_DEBUG("Found {} source operators.", sourceOperatorsSet.size());
        std::vector<std::shared_ptr<LogicalSourceType>> sourceOperators{sourceOperatorsSet.begin(), sourceOperatorsSet.end()};
        return sourceOperators;
    }

    std::vector<std::shared_ptr<SinkLogicalOperator>> getSinkOperators() const;

    void appendOperatorAsNewRoot(const std::shared_ptr<Operator>& operatorNode);

    std::string toString() const;

    /// in certain stages the sink operators might not be the root operators
    std::vector<std::shared_ptr<Operator>> getRootOperators() const;

    /// add subQuery's rootnode into the current node for merging purpose.
    void addRootOperator(const std::shared_ptr<Operator>& newRootOperator);

    void removeAsRootOperator(std::shared_ptr<Operator> root);

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

    /// Get all the leaf operators in the query plan (leaf operator is the one without any child)
    /// @note: in certain stages the source operators might not be Leaf operators
    std::vector<std::shared_ptr<Operator>> getLeafOperators() const;

    std::unordered_set<std::shared_ptr<Operator>> getAllOperators() const;

    /// @note: This method only check if there exists another operator with same Id or not.
    /// @note: The system generated operators are ignored from this check.
    [[nodiscard]] bool hasOperatorWithId(OperatorId operatorId) const;

    std::shared_ptr<Operator> getOperatorWithOperatorId(OperatorId operatorId) const;

    void setQueryId(QueryId queryId);

    [[nodiscard]] QueryId getQueryId() const;

    std::shared_ptr<QueryPlan> copy();

    [[nodiscard]] std::string getSourceConsumed() const;


    /// Set the logical sources used in the query
    void setSourceConsumed(std::string_view sourceName);

    void setPlacementStrategy(Optimizer::PlacementStrategy placementStrategy);

    Optimizer::PlacementStrategy getPlacementStrategy() const;

    /// Find all operators between given set of downstream and upstream operators
    /// @return all operators between (excluding) downstream and upstream operators
    std::set<std::shared_ptr<Operator>> findAllOperatorsBetween(
        const std::set<std::shared_ptr<Operator>>& downstreamOperators, const std::set<std::shared_ptr<Operator>>& upstreamOperators);

    QueryState getQueryState() const;

    void setQueryState(QueryState newState);

    void clearRootOperators();

    /// Comparison to another plan and its children nodes by tree traversal.
    /// @return true, if this and other plan are equal in their structure and operators, false else
    [[nodiscard]] bool compare(const std::shared_ptr<QueryPlan>& otherPlan) const;

private:
    /// Find operators between source and target operators
    /// @return empty or operators between source and target operators
    static std::set<std::shared_ptr<Operator>> findOperatorsBetweenSourceAndTargetOperators(
        const std::shared_ptr<Operator>& sourceOperator, const std::set<std::shared_ptr<Operator>>& targetOperators);

    std::vector<std::shared_ptr<Operator>> rootOperators;
    QueryId queryId = INVALID_QUERY_ID;
    std::string sourceConsumed;
    QueryState currentState;
    /// Default placement strategy is top-down; we set the correct placement strategy in the Experimental Add Request
    Optimizer::PlacementStrategy placementStrategy = Optimizer::PlacementStrategy::TopDown;
};
}

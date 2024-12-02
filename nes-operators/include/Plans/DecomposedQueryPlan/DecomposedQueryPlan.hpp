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
#include <Operators/Operator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/QueryState.hpp>


namespace NES
{

class Operator;
using OperatorPtr = std::shared_ptr<Operator>;

class SinkLogicalOperator;
using SinkLogicalOperatorPtr = std::shared_ptr<SinkLogicalOperator>;

class Node;
using NodePtr = std::shared_ptr<Node>;

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

/**
 * @brief This represents the plan that need to be executed on the worker node
 */
class DecomposedQueryPlan
{
public:
    explicit DecomposedQueryPlan(QueryId queryId, WorkerId workerId);
    explicit DecomposedQueryPlan(QueryId queryId, WorkerId workerId, std::vector<OperatorPtr> rootOperators);

    /// Remove the operator with given id as the root
    bool removeAsRootOperator(OperatorId rootOperatorId);
    void addRootOperator(OperatorPtr newRootOperator);

    /// Replaces a particular root operator with a new one
    bool replaceRootOperator(const OperatorPtr& oldRoot, const OperatorPtr& newRoot);

    /// Appends an operator to the query plan and make the new operator as root
    void appendOperatorAsNewRoot(const OperatorPtr& operatorNode);

    [[nodiscard]] bool hasOperatorWithId(OperatorId operatorId) const;
    [[nodiscard]] OperatorPtr getOperatorWithOperatorId(OperatorId operatorId) const;

    [[nodiscard]] std::vector<OperatorPtr> getRootOperators() const;
    [[nodiscard]] std::vector<OperatorPtr> getLeafOperators() const;

    template <typename LogicalSourceType>
    [[nodiscard]] std::vector<std::shared_ptr<LogicalSourceType>> getSourceOperators() const
    {
        NES_DEBUG("Get all source operators by traversing all the root nodes.");
        std::unordered_set<std::shared_ptr<LogicalSourceType>> sourceOperatorsSet;
        for (const auto& rootOperator : rootOperators)
        {
            auto sourceOperators = rootOperator->getNodesByType<LogicalSourceType>();
            NES_DEBUG("Insert all source operators to the collection");
            sourceOperatorsSet.insert(sourceOperators.begin(), sourceOperators.end());
        }
        NES_DEBUG("Found {} source operators.", sourceOperatorsSet.size());
        std::vector<std::shared_ptr<LogicalSourceType>> sourceOperators{sourceOperatorsSet.begin(), sourceOperatorsSet.end()};
        return sourceOperators;
    }
    [[nodiscard]] std::vector<SinkLogicalOperatorPtr> getSinkOperators() const;

    [[nodiscard]] std::unordered_set<OperatorPtr> getAllOperators() const;

    void setQueryId(QueryId queryId);
    [[nodiscard]] QueryId getQueryId() const;

    void setState(QueryState state);
    [[nodiscard]] QueryState getState() const;

    [[nodiscard]] WorkerId getWorkerId() const;

    [[nodiscard]] DecomposedQueryPlanPtr copy() const;
    [[nodiscard]] std::string toString() const;

    template <class T>
    [[nodiscard]] std::vector<std::shared_ptr<T>> getOperatorByType()
    {
        /// Find all the nodes in the query plan
        std::vector<std::shared_ptr<T>> operators;
        /// Maintain a list of visited nodes as there are multiple root nodes
        std::unordered_set<OperatorId> visitedOpIds;
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

private:
    QueryId queryId;
    WorkerId workerId;
    QueryState currentState = QueryState::MARKED_FOR_DEPLOYMENT;
    std::vector<OperatorPtr> rootOperators; /// Using a shared_ptr, because there are back-references from child to parent(root) operators.
};
}

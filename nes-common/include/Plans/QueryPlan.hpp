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
#include <set>
#include <unordered_set>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Plans/Operator.hpp>


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

    std::vector<std::shared_ptr<SinkLogicalOperator>> getSinkOperators() const;

    void appendOperatorAsNewRoot(const std::shared_ptr<Operator>& operatorNode);

    std::string toString() const;

    /// in certain stages the sink operators might not be the root operators
    std::vector<std::shared_ptr<Operator>> getRootOperators() const;

    /// add subQuery's rootnode into the current node for merging purpose.
    void addRootOperator(const std::shared_ptr<Operator>& newRootOperator)
    {
        ///Check if a root with the id already present
        auto found = std::find_if(
            rootOperators.begin(),
            rootOperators.end(),
            [&](const std::shared_ptr<Operator>& root) { return newRootOperator->id == root->id; });

        /// If not present then add it
        if (found == rootOperators.end())
        {
            rootOperators.push_back(newRootOperator);
        }
        else
        {
            NES_WARNING("Root operator with id {} already present int he plan", newRootOperator->id);
        }
    }

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
            for (auto itr : BFSRange<Operator>(rootOperator))
            {
                if (visitedOpIds.contains(itr->id))
                {
                    /// skip rest of the steps as the node found in already visited node list
                    continue;
                }
                visitedOpIds.insert(itr->id);
                if (NES::Util::instanceOf<T>(itr))
                {
                    operators.push_back(NES::Util::as<T>(itr));
                }
            }
        }
        return operators;
    }

    /// Get all the leaf operators in the query plan (leaf operator is the one without any child)
    /// @note: in certain stages the source operators might not be Leaf operators
    std::vector<std::shared_ptr<Operator>> getLeafOperators() const;

    std::unordered_set<std::shared_ptr<Operator>> getAllOperators() const;

    void setQueryId(QueryId queryId);
    [[nodiscard]] QueryId getQueryId() const;

    std::shared_ptr<QueryPlan> clone() const;

    /// Find all operators between given set of downstream and upstream operators
    /// @return all operators between (excluding) downstream and upstream operators
    std::set<std::shared_ptr<Operator>> findAllOperatorsBetween(
        const std::set<std::shared_ptr<Operator>>& downstreamOperators, const std::set<std::shared_ptr<Operator>>& upstreamOperators);

    [[nodiscard]] bool operator==(const std::shared_ptr<QueryPlan>& otherPlan) const;

private:
    /// Find operators between source and target operators
    /// @return empty or operators between source and target operators
    static std::set<std::shared_ptr<Operator>> findOperatorsBetweenSourceAndTargetOperators(
        const std::shared_ptr<Operator>& sourceOperator, const std::set<std::shared_ptr<Operator>>& targetOperators);

    std::vector<std::shared_ptr<Operator>> rootOperators;
    QueryId queryId = INVALID_QUERY_ID;
};
}

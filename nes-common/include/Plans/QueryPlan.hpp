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
    explicit QueryPlan(std::unique_ptr<Operator> rootOperator);
    explicit QueryPlan(QueryId queryId, std::vector<std::unique_ptr<Operator>> rootOperators);
    static std::unique_ptr<QueryPlan> create(std::unique_ptr<Operator> rootOperator);
    static std::unique_ptr<QueryPlan> create(QueryId queryId, std::vector<std::unique_ptr<Operator>> rootOperators);

    QueryPlan(const QueryPlan& other);
    QueryPlan(QueryPlan&& other);
    QueryPlan& operator=(QueryPlan&& other);

    std::vector<SinkLogicalOperator*> getSinkOperators() const;

    /// Operator is being promoted as the new root by reparenting existing root operators and replacing the current roots
    void promoteOperatorToRoot(std::unique_ptr<Operator> newRoot);
    /// Adds operator to roots vector
    void addRootOperator(std::unique_ptr<Operator> newRootOperator);

    [[nodiscard]] std::string toString() const;

    const std::vector<Operator*> getRootOperators() const;
    std::vector<std::unique_ptr<Operator>> releaseRootOperators();

    template <class T>
    std::vector<T*> getOperatorByType() const
    {
        std::vector<T*> operators;
        std::set<OperatorId> visitedOpIds;
        for (const auto& rootOperator : rootOperators)
        {
            for (Operator* op : BFSRange<Operator>(rootOperator.get())) {
                if (visitedOpIds.contains(op->id)) {
                    continue;
                }
                visitedOpIds.insert(op->id);
                if (T* casted = dynamic_cast<T*>(op)) {
                    operators.push_back(casted);
                }
            }
        }
        return operators;
    }

    /// Get all the leaf operators in the query plan (leaf operator is the one without any child)
    /// @note: in certain stages the source operators might not be Leaf operators
    std::vector<Operator*> getLeafOperators() const;

    std::unordered_set<Operator*> getAllOperators() const;

    void setQueryId(QueryId queryId);
    [[nodiscard]] QueryId getQueryId() const;

    std::unique_ptr<QueryPlan> clone() const;

    /// Find all operators between given set of downstream and upstream operators
    /// @return all operators between (excluding) downstream and upstream operators
    std::set<Operator*> findAllOperatorsBetween(
        const std::set<Operator*>& downstreamOperators,
        const std::set<Operator*>& upstreamOperators) const;

    [[nodiscard]] bool operator==(const std::shared_ptr<QueryPlan>& otherPlan) const;
    [[nodiscard]] bool operator==(const QueryPlan& otherPlan) const;

private:
    /// Owning pointers to the operators
    std::vector<std::unique_ptr<Operator>> rootOperators{};
    QueryId queryId = INVALID_QUERY_ID;
};
}

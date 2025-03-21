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
    explicit QueryPlan(Operator rootOperator);
    explicit QueryPlan(QueryId queryId, std::vector<Operator> rootOperators);
    static std::unique_ptr<QueryPlan> create(Operator rootOperator);
    static std::unique_ptr<QueryPlan> create(QueryId queryId, std::vector<Operator> rootOperators);

    QueryPlan(const QueryPlan& other);
    QueryPlan(QueryPlan&& other);
    QueryPlan& operator=(QueryPlan&& other);

    /// Operator is being promoted as the new root by reparenting existing root operators and replacing the current roots
    void promoteOperatorToRoot(Operator newRoot);
    /// Adds operator to roots vector
    void addRootOperator(Operator newRootOperator);

    [[nodiscard]] std::string toString() const;

    std::vector<Operator> getRootOperators() const;

    bool replaceOperatorRecursively(Operator current, Operator target, Operator replacement)
    {
        auto children = current.getChildren();
        for (auto& child : children)
        {
            if (child == target)
            {
                child = std::move(replacement);
                return true;
            }
            else if (replaceOperatorRecursively(child, target, replacement))
            {
                return true;
            }
        }
        return false;
    }

    /*template <typename... TraitTypes>
    std::vector<Operator> getOperatorsWithTraits()
    {
        std::vector<Operator> operators;
        std::set<OperatorId> visitedOpIds;
        for (const auto& rootOperator : rootOperators)
        {
            for (Operator op : BFSRange<Operator>(rootOperator))
            {
                if (!visitedOpIds.contains(op.getId()))
                {
                    visitedOpIds.insert(op.getId());
                    if (hasTraits<TraitTypes...>(op.getTraitSet()))
                    {
                        operators.emplace_back(op);
                    }
            }
            }
        }
        return operators;
    }*/

    bool replaceOperator(Operator target, Operator replacement)
    {
        bool replaced = false;
        for (auto& root : rootOperators)
        {
            ///replacement.parents = std::move(root.parents);
            replacement.getChildren() = root.getChildren();
            root = std::move(replacement);
            replaced = true;
            break;
        }
        if (!replaced)
        {
            for (auto& root : rootOperators)
            {
                if (replaceOperatorRecursively(root, target, replacement))
                {
                    replaced = true;
                    break;
                }
            }
        }
        return replaced;
    }

    template <class T>
    std::vector<T> getOperatorByType() const
    {
        std::vector<T> operators;
        std::set<OperatorId> visitedOpIds;
        for (const auto& rootOperator : rootOperators)
        {
            for (Operator op : BFSRange<Operator>(rootOperator))
            {
                if (visitedOpIds.contains(op.getId()))
                {
                    continue;
                }
                visitedOpIds.insert(op.getId());
                if (T* casted = dynamic_cast<T*>(op))
                {
                    operators.push_back(*casted);
                }
            }
        }
        return operators;
    }

    /*
    QueryPlan flip() {
        std::unordered_map<Operator, Operator> parentMap;
        std::unordered_map<Operator, Operator> opPtrMap;

        std::function<void(Operator, Operator, Operator )> buildMaps =
            [&](Operator op, Operator parent, Operator opOwner) {
            parentMap[op] = parent;
            opPtrMap[op] = opOwner;
            for (auto& child : op.getChildren()) {
                buildMaps(child, op, child);
            }
        };

        for (auto& root : rootOperators) {
            buildMaps(root, root , root);
        }

        std::vector<Operator> leaves;
        std::function<void(Operator)> findLeaves =
            [&](Operator op) {
            if (op.getChildren().empty()) {
                leaves.push_back(op);
            } else {
                for (auto& child : op.getChildren()) {
                    findLeaves(child);
                }
            }
        };
        for (auto& root : rootOperators) {
            findLeaves(root);
        }

        std::function<Operator(Operator)> buildReversedChain =
            [&](Operator op) -> Operator {
            Operator current = opPtrMap[op];
            current.getChildren().clear();
            if (parentMap.find(op) != parentMap.end()) {
                Operator parent = parentMap[op];
                current.getChildren().push_back(buildReversedChain(parent));
            }
            return current;
        };

        std::vector<Operator> flippedRoots;
        for (Operator leaf : leaves) {
            flippedRoots.push_back(buildReversedChain(leaf));
        }

        return QueryPlan(this->getQueryId(), flippedRoots);
    }*/

    /// Get all the leaf operators in the query plan (leaf operator is the one without any child)
    /// @note: in certain stages the source operators might not be Leaf operators
    std::vector<Operator> getLeafOperators() const;

    std::unordered_set<Operator> getAllOperators() const;

    void setQueryId(QueryId queryId);
    [[nodiscard]] QueryId getQueryId() const;

    [[nodiscard]] bool operator==(const QueryPlan& otherPlan) const;

private:
    /// Owning pointers to the operators
    std::vector<Operator> rootOperators{};
    QueryId queryId = INVALID_QUERY_ID;
};
}

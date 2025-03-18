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

    /// Operator is being promoted as the new root by reparenting existing root operators and replacing the current roots
    void promoteOperatorToRoot(std::unique_ptr<Operator> newRoot);
    /// Adds operator to roots vector
    void addRootOperator(std::unique_ptr<Operator> newRootOperator);

    [[nodiscard]] std::string toString() const;

    std::vector<Operator *> getRootOperators() const;

    std::vector<std::unique_ptr<Operator>> releaseRootOperators();

    bool replaceOperatorRecursively(Operator* current, Operator* target, std::unique_ptr<Operator>& replacement)
    {
        auto& children = current->children;
        for (auto& child : children)
        {
            if (child.get() == target)
            {
                child = std::move(replacement);
                return true;
            }
            else if (replaceOperatorRecursively(child.get(), target, replacement))
            {
                return true;
            }
        }
        return false;
    }

    bool replaceOperator(Operator* target, std::unique_ptr<Operator> replacement)
    {
        bool replaced = false;
        for (auto& root : rootOperators)
        {
            if (root.get() == target)
            {
                replacement->parents = std::move(root->parents);
                replacement->children = std::move(root->children);
                root = std::move(replacement);
                replaced = true;
                break;
            }
        }
        if (!replaced)
        {
            for (auto& root : rootOperators)
            {
                if (replaceOperatorRecursively(root.get(), target, replacement))
                {
                    replaced = true;
                    break;
                }
            }
        }
        return replaced;
    }

    template <class T>
    std::vector<T*> getOperatorByType() const
    {
        std::vector<T*> operators;
        std::set<OperatorId> visitedOpIds;
        for (const auto& rootOperator : rootOperators)
        {
            for (Operator* op : BFSRange<Operator>(rootOperator.get()))
            {
                if (visitedOpIds.contains(op->id))
                {
                    continue;
                }
                visitedOpIds.insert(op->id);
                if (T* casted = dynamic_cast<T*>(op))
                {
                    operators.push_back(casted);
                }
            }
        }
        return operators;
    }

    std::unique_ptr<QueryPlan> flip() {
        std::unordered_map<Operator*, Operator*> parentMap;
        std::unordered_map<Operator*, std::unique_ptr<Operator>*> opPtrMap;

        std::function<void(Operator*, Operator*, std::unique_ptr<Operator>* )> buildMaps =
            [&](Operator* op, Operator* parent, std::unique_ptr<Operator>* opOwner) {
            if (parent != nullptr) {
                parentMap[op] = parent;
            }
            opPtrMap[op] = opOwner;
            for (auto& child : op->children) {
                buildMaps(child.get(), op, &child);
            }
        };

        for (auto& root : rootOperators) {
            buildMaps(root.get(), nullptr, &root);
        }

        std::vector<Operator*> leaves;
        std::function<void(Operator*)> findLeaves =
            [&](Operator* op) {
            if (op->children.empty()) {
                leaves.push_back(op);
            } else {
                for (auto& child : op->children) {
                    findLeaves(child.get());
                }
            }
        };
        for (auto& root : rootOperators) {
            findLeaves(root.get());
        }

        std::function<std::unique_ptr<Operator>(Operator*)> buildReversedChain =
            [&](Operator* op) -> std::unique_ptr<Operator> {
            std::unique_ptr<Operator> current = std::move(*opPtrMap[op]);
            current->children.clear();
            if (parentMap.find(op) != parentMap.end()) {
                Operator* parent = parentMap[op];
                current->children.push_back(buildReversedChain(parent));
            }
            return current;
        };

        std::vector<std::unique_ptr<Operator>> flippedRoots;
        for (Operator* leaf : leaves) {
            flippedRoots.push_back(buildReversedChain(leaf));
        }

        return std::make_unique<QueryPlan>(this->getQueryId(), std::move(flippedRoots));
    }

    /// Get all the leaf operators in the query plan (leaf operator is the one without any child)
    /// @note: in certain stages the source operators might not be Leaf operators
    std::vector<Operator*> getLeafOperators() const;

    std::unordered_set<Operator*> getAllOperators() const;

    void setQueryId(QueryId queryId);
    [[nodiscard]] QueryId getQueryId() const;

    std::unique_ptr<QueryPlan> clone() const;

    [[nodiscard]] bool operator==(const std::shared_ptr<QueryPlan>& otherPlan) const;
    [[nodiscard]] bool operator==(const QueryPlan& otherPlan) const;

private:
    /// Owning pointers to the operators
    std::vector<std::unique_ptr<Operator>> rootOperators{};
    QueryId queryId = INVALID_QUERY_ID;
};
}

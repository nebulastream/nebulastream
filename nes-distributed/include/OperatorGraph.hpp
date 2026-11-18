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

#include <set>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>

namespace NES
{

/// Navigable view on the operators of a logical plan, addressed by their operator id.
/// The original ISQP implementation works on mutable operator nodes that know their parents. The current logical operators are immutable
/// values that only know their children, so ISQP navigates the global plan of a query through this graph.
/// Upstream operators are the children (towards the sources), downstream operators are the parents (towards the sink).
class OperatorGraph
{
public:
    explicit OperatorGraph(const LogicalPlan& plan);

    [[nodiscard]] bool contains(OperatorId id) const;
    [[nodiscard]] const LogicalOperator& getOperator(OperatorId id) const;
    [[nodiscard]] const std::vector<OperatorId>& getUpstreamOperators(OperatorId id) const;
    [[nodiscard]] const std::vector<OperatorId>& getDownstreamOperators(OperatorId id) const;
    [[nodiscard]] std::vector<OperatorId> getRootOperators() const;
    [[nodiscard]] std::vector<OperatorId> getLeafOperators() const;
    [[nodiscard]] std::vector<OperatorId> getAllOperators() const;

    /// Returns true if `candidate` is a transitive downstream operator of `id` (the original's `containAsGrandParent`).
    [[nodiscard]] bool isDownstreamOf(OperatorId candidate, OperatorId id) const;

    /// Returns all operators on the paths from the upstream operators to the downstream operators. The upstream and downstream operators
    /// themselves are not part of the result (the original's `QueryPlan::findAllOperatorsBetween`).
    [[nodiscard]] std::set<OperatorId>
    findAllOperatorsBetween(const std::set<OperatorId>& downstreamOperators, const std::set<OperatorId>& upstreamOperators) const;

private:
    struct Node
    {
        LogicalOperator op;
        std::vector<OperatorId> upstream;
        std::vector<OperatorId> downstream;
    };

    const Node& getNode(OperatorId id) const;

    std::unordered_map<OperatorId, Node> nodes;
};

}

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
#include <vector>
#include <Operators/LogicalOperator.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{


/// Represents the result of a rewrite rule that matched one operator to a subgraph.
/// @Note subgraph direction: root (sink) to leaf (source)
struct RewriteRuleResultSubgraph
{
    using SubGraphRoot = std::shared_ptr<PhysicalOperatorWrapper>;
    using SubGraphLeafs = std::vector<std::shared_ptr<PhysicalOperatorWrapper>>;

    /// Top-level physical operator of subgraph
    SubGraphRoot root;
    /// Bottom-level physical operators of subgraph
    SubGraphLeafs leafs;
};

/// Interface for rewrite rules.
/// For now, the interface only considers lowering rules (logical operator to physical subgraph)
struct AbstractRewriteRule
{
    virtual RewriteRuleResultSubgraph apply(LogicalOperator logicalOperator, QueryId queryId) = 0;
    virtual ~AbstractRewriteRule() = default;
};

}

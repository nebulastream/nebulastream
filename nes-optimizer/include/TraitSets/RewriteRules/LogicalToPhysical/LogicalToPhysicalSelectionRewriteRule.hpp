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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSelectionOperator.hpp>
#include <TraitSets/TraitSet.hpp>
#include <TraitSets/Traits/ChildNodes.hpp>
#include <TraitSets/Traits/PhysicalOperatorTrait.hpp>
#include <TraitSets/Traits/QueryForSubtree.hpp>
#include <TraitSets/RewriteRule.hpp>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Operators/Operator.hpp>

namespace NES
{

struct LogicalToPhysicalSelectionRewriteRule
{
    // Define the input and output TraitSet types
    using InputTraitSet = TraitSet<ChildNodes, QueryForSubtree, LogicalSelectionOperator>;
    using OutputTraitSet = TraitSet<ChildNodes, QueryForSubtree, PhysicalOperatorTrait>;

    // Static apply method as required by the RewriteRuleConcept
    static OutputTraitSet apply(const InputTraitSet& in)
    {
        // Extract the predicate from the LogicalSelectionOperator
        const auto& logicalOp = in.get<LogicalSelectionOperator>();
        auto predicate = logicalOp.getPredicate();

        /// Build the PhysicalSelectionOperator
        auto phyOp = std::make_shared<PhysicalSelectionOperator>();
        phyOp->predicate = predicate;

        /// Construct the output TraitSet
        return OutputTraitSet{
            in.get<ChildNodes>(),
            in.get<QueryForSubtree>(),
            PhysicalOperatorTrait{phyOp}
        };
    }
};
/*
struct LogicalToPhysicalSelectionRewriteRule : RewriteRule
{
    /// Define the input and output trait sets
    using InputTrait = TraitSet<ChildNodes, QueryForSubtree, LogicalSelectionOperator>;
    using OutputTrait = TraitSet<ChildNodes, QueryForSubtree, PhysicalOperatorTrait>;

    /// Static apply method as required by the RewriteRuleConcept
    static OutputTrait apply(const InputTrait& in)
    {
        /// Extract the predicate from the logical selection operator
        auto predicate = in.get<LogicalSelectionOperator>().getPredicate();

        /// Build the PhysicalSelectionOperator
        auto phyOp = std::make_shared<PhysicalSelectionOperator>();
        phyOp->predicate = predicate;

        /// Construct the output trait set
        return {
            in.get<ChildNodes>(),
            in.get<QueryForSubtree>(),
            PhysicalOperatorTrait{phyOp}
        };
    }
};

static_assert(
    RewriteRuleConcept<
        LogicalToPhysicalSelectionRewriteRule,
        TraitSet<ChildNodes, QueryForSubtree, LogicalSelectionOperator>,
        TraitSet<ChildNodes, QueryForSubtree, PhysicalOperatorTrait>
        >,
        "Rule does not satisfy RewriteRuleConcept"
);
*/
}

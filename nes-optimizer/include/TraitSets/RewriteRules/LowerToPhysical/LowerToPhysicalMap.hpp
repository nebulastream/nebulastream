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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <TypeTraits/TraitSet.hpp>
#include <TypeTraits/Traits/Children.hpp>
#include <TypeTraits/Traits/PhysicalOperatorTrait.hpp>
#include <TypeTraits/Traits/QueryForSubtree.hpp>
#include <TypeTraits/RewriteRule/AbstractRewriteRule.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/Operator.hpp>

namespace NES
{

class LowerToPhysicalSelection : TypedAbstractRewriteRule<QueryForSubtree, Operator>
{
    DynamicTraitSet<QueryForSubtree, Operator>* applyTyped(DynamicTraitSet<QueryForSubtree, Operator>*) override
    {
        /// Extract the predicate from the logical selection operator
        auto predicate = in.get<LogicalSelectionOperator>().getPredicate();

        /// Build the PhysicalSelectionOperator
        auto phyOp = std::make_shared<PhysicalSelectionOperator>();
        phyOp->predicate = predicate;

        /// Construct the output trait set
        return {in.get<Children>(), in.get<QueryForSubtree>(), PhysicalOperatorTrait{phyOp}};
    }
};


}

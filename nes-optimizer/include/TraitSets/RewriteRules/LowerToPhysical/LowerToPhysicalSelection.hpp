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
#include <TraitSets/TraitSet.hpp>
#include <TraitSets/Traits/Children.hpp>
#include <TraitSets/Traits/QueryForSubtree.hpp>
#include <TraitSets/RewriteRules/AbstractRewriteRule.hpp>
#include <Operators/LogicalOperators/SelectionLogicalOperator.hpp>
#include <Functions/ConstantValuePhysicalFunction.hpp>
#include <Operators/Operator.hpp>
#include <SelectionPhysicalOperator.hpp>
#include <Functions/FunctionProvider.hpp>

namespace NES::Optimizer
{

struct LowerToPhysicalSelection : TypedAbstractRewriteRule<QueryForSubtree, Operator>
{
    DynamicTraitSet<QueryForSubtree, Operator>* applyTyped(DynamicTraitSet<QueryForSubtree, Operator>* traitSet) override
    {
        auto op = traitSet->get<Operator>();
        auto function = dynamic_cast<SelectionLogicalOperator*>(op)->getPredicate();
        auto func = QueryCompilation::FunctionProvider::lowerFunction(function);
        auto phyOp = SelectionPhysicalOperator(std::move(func));
        return traitSet;
    };
};

}

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

#include <memory>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalSelection.hpp>
#include <RewriteRuleRegistry.hpp>
#include <memory>
#include <utility>
#include <Traits/QueryForSubtree.hpp>
#include <Traits/TraitSet.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Plans/Operator.hpp>
#include <SelectionPhysicalOperator.hpp>

namespace NES::Optimizer
{

std::vector<std::unique_ptr<PhysicalOperator>> LowerToPhysicalSelection::applyToPhysical(DynamicTraitSet<QueryForSubtree, Operator>* traitSet)
{
    auto op = traitSet->get<Operator>();
    const auto ops = dynamic_cast<SelectionLogicalOperator*>(op);
    auto& function = ops->getPredicate();
    auto func = QueryCompilation::FunctionProvider::lowerFunction(function.clone());
    auto layout = std::make_unique<Memory::MemoryLayouts::RowLayout>(ops->getInputSchema(), conf.bufferSize.getValue());
    auto memoryProvider = std::make_unique<RowTupleBufferMemoryProvider>(std::move(layout));

    std::vector<std::unique_ptr<TupleBufferMemoryProvider>> memoryPrividerVec;
    memoryPrividerVec.emplace_back(std::move(memoryProvider));
    auto phyOp = std::make_unique<SelectionPhysicalOperator>(std::move(memoryPrividerVec), std::move(func));

    std::vector<std::unique_ptr<PhysicalOperator>> physicalOperatorVec;
    physicalOperatorVec.emplace_back(std::move(phyOp));
    return physicalOperatorVec;
};

std::unique_ptr<AbstractRewriteRule> RewriteRuleGeneratedRegistrar::RegisterSelectionRewriteRule(RewriteRuleRegistryArguments argument)
{
    return std::make_unique<LowerToPhysicalSelection>(std::move(argument.conf));
}
}
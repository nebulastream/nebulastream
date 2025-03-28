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
#include <utility>
#include <Functions/FunctionProvider.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <SelectionPhysicalOperator.hpp>
#include <Abstract/PhysicalOperator.hpp>

namespace NES::Optimizer
{

std::vector<PhysicalOperatorWrapper> LowerToPhysicalSelection::apply(LogicalOperator logicalOperator)
{
    auto selection = *logicalOperator.get<SelectionLogicalOperator>();
    auto function = selection.getPredicate();
    auto func = QueryCompilation::FunctionProvider::lowerFunction(function);
    auto physicalOperator = SelectionPhysicalOperator(func);
    auto wrapper = PhysicalOperatorWrapper(physicalOperator, logicalOperator.getInputSchemas()[0], logicalOperator.getOutputSchema());
    return {wrapper};
};

std::unique_ptr<AbstractRewriteRule> RewriteRuleGeneratedRegistrar::RegisterSelectionRewriteRule(RewriteRuleRegistryArguments argument)
{
    return std::make_unique<LowerToPhysicalSelection>(argument.conf);
}
}
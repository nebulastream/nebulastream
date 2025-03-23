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
#include <Functions/FunctionProvider.hpp>
#include <MapPhysicalOperator.hpp>
#include <Operators/MapLogicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalMap.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>

namespace NES::Optimizer
{

std::vector<PhysicalOperatorWrapper> LowerToPhysicalMap::applyToPhysical(DynamicTraitSet<QueryForSubtree, Operator>* traitSet)
{
    const auto op = traitSet->get<Operator>();
    const auto ops = dynamic_cast<MapLogicalOperator*>(op);
    auto& function = ops->getMapFunction();
    auto fieldName = function.getField().getFieldName();
    auto func = QueryCompilation::FunctionProvider::lowerFunction(function.clone());
    auto phyOp = std::make_unique<MapPhysicalOperator>(fieldName, std::move(func));
    std::vector<PhysicalOperatorWrapper> resultVec;
    resultVec.emplace_back(std::move(phyOp));
    return resultVec;
}

std::unique_ptr<Optimizer::AbstractRewriteRule> RewriteRuleGeneratedRegistrar::RegisterMapRewriteRule(RewriteRuleRegistryArguments argument)
{
    return std::make_unique<NES::Optimizer::LowerToPhysicalMap>(argument.conf);
}
}

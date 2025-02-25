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
#include <ranges>
#include <utility>

#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Phases/Translations/LowerLogicalToPhysicalOperators.hpp>
#include <QueryCompiler/Phases/Translations/PhysicalOperatorProvider.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::QueryCompilation
{

LowerLogicalToPhysicalOperatorsPtr
LowerLogicalToPhysicalOperators::LowerLogicalToPhysicalOperators::create(const PhysicalOperatorProviderPtr& provider)
{
    return std::make_shared<LowerLogicalToPhysicalOperators>(provider);
}

LowerLogicalToPhysicalOperators::LowerLogicalToPhysicalOperators(PhysicalOperatorProviderPtr provider) : provider(std::move(provider))
{
}

DecomposedQueryPlanPtr LowerLogicalToPhysicalOperators::apply(DecomposedQueryPlanPtr decomposedQueryPlan)
{
    auto isAlreadyLowered = [](const auto& node)
    {
        return not(
            Util::instanceOf<PhysicalOperators::PhysicalOperator>(node) or Util::instanceOf<SourceDescriptorLogicalOperator>(node)
            or Util::instanceOf<SinkLogicalOperator>(node));
    };
    const std::vector<NodePtr> nodes = PlanIterator(*decomposedQueryPlan).snapshot();
    for (const auto& node : nodes | std::views::filter(isAlreadyLowered))
    {
        provider->lower(decomposedQueryPlan, NES::Util::as<LogicalOperator>(node));
    }
    return decomposedQueryPlan;
}

}

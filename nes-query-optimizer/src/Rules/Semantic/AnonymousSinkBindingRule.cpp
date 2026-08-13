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

#include <Rules/Semantic/AnonymousSinkBindingRule.hpp>

#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <vector>

#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/AnonymousSinkLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

LogicalPlan AnonymousSinkBindingRule::apply(const LogicalPlan& queryPlan) const
{
    std::vector<LogicalOperator> newRootOperators;
    for (const auto& rootOperator : queryPlan.getRootOperators())
    {
        if (auto sink = rootOperator.tryGetAs<AnonymousSinkLogicalOperator>(); sink.has_value())
        {
            auto sinkDescriptor = sinkCatalog->createAnonymousSinkDescriptor(
                sink.value()->getSinkSchema(),
                sink.value()->getGeneralSinkConfig(),
                sink.value()->getPluginSinkConfiguration(),
                sink.value()->getOutputFormatterDescriptor());

            TypedLogicalOperator<SinkLogicalOperator> sinkOperator = SinkLogicalOperator::create(sinkDescriptor);
            sinkOperator = sinkOperator->withChildrenUnsafe(sink.value().getChildren());
            newRootOperators.emplace_back(sinkOperator);
        }
        else
        {
            newRootOperators.emplace_back(rootOperator);
        }
    }

    return queryPlan.withRootOperators(newRootOperators);
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> AnonymousSinkBindingRule::neededBy() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType AnonymousSinkBindingRule::create(PlanRuleRegistryArguments arguments)
{
    return AnonymousSinkBindingRule{arguments.sinkCatalog};
}
}

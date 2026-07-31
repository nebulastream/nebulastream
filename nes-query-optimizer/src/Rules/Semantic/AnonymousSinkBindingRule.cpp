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
#include <utility>
#include <vector>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/AnonymousSinkLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

const std::type_info& AnonymousSinkBindingRule::getType()
{
    return typeid(AnonymousSinkBindingRule);
}

std::string_view AnonymousSinkBindingRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> AnonymousSinkBindingRule::dependsOn() const
{
    return {};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> AnonymousSinkBindingRule::requiredBy() const
{
    return {};
}

bool AnonymousSinkBindingRule::operator==(const AnonymousSinkBindingRule& other) const
{
    return sinkCatalog == other.sinkCatalog;
}

LogicalPlan AnonymousSinkBindingRule::apply(const LogicalPlan& queryPlan) const
{
    std::vector<LogicalOperator> newRootOperators;
    for (const auto& rootOperator : queryPlan.getRootOperators())
    {
        if (auto sink = rootOperator.tryGetAs<AnonymousSinkLogicalOperator>(); sink.has_value())
        {
            const auto type = sink.value()->getSinkType();
            const auto config = sink.value()->getSinkConfig();

            auto resolved = SinkCatalog::resolveAnonymousSinkConfig(type, config);
            if (not resolved.has_value())
            {
                throw std::move(resolved).error();
            }
            auto [generalConfig, sinkSchema, pluginSinkConfig, outputFormatterDescriptor] = std::move(resolved).value();

            /// SINK.HOST determines placement, not sink behavior; anonymous sinks must state it explicitly.
            if (generalConfig.host == Host{Host::INVALID})
            {
                throw InvalidConfigParameter("'host'");
            }

            const auto sinkDescriptor = sinkCatalog->createAnonymousSinkDescriptor(
                std::move(sinkSchema), std::move(generalConfig), std::move(pluginSinkConfig), std::move(outputFormatterDescriptor));

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


}

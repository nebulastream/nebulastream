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

namespace NES
{

LogicalPlan AnonymousSinkBindingRule::apply(const LogicalPlan& queryPlan) const
{
    std::vector<LogicalOperator> newRootOperators;
    for (const auto& rootOperator : queryPlan.getRootOperators())
    {
        if (auto sink = rootOperator.tryGetAs<AnonymousSinkLogicalOperator>(); sink.has_value())
        {
            const auto schema = sink.value()->getTargetSchema();
            const auto type = sink.value()->getSinkType();
            auto config = sink.value()->getSinkConfig();
            const auto formatConfig = sink.value()->getFormatConfig();

            /// "host" is not part of the sink config — it determines placement, not sink behavior.
            /// It is stored in the config map only because AnonymousSinkLogicalOperator lacks a dedicated host field.
            auto hostIt = config.find(Identifier::parse("host"));
            if (hostIt == config.end())
            {
                throw InvalidConfigParameter("'host'");
            }
            auto host = Host(hostIt->second);
            config.erase(hostIt);

            const auto sinkDescriptor = sinkCatalog->getAnonymousSink(schema, type, host, config, formatConfig);

            if (!sinkDescriptor.has_value())
            {
                throw InvalidConfigParameter("Failed to create anonymous sink descriptor");
            }

            TypedLogicalOperator<SinkLogicalOperator> sinkOperator = SinkLogicalOperator::create(sinkDescriptor.value());
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
}

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

#include <Rules/Semantic/InlineSinkBindingRule.hpp>

#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/InlineSinkLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES
{


void InlineSinkBindingRule::apply(LogicalPlan& queryPlan) const
{
    std::vector<LogicalOperator> newRootOperators;
    for (const auto& rootOperator : queryPlan.getRootOperators())
    {
        if (auto sink = rootOperator.tryGetAs<InlineSinkLogicalOperator>(); sink.has_value())
        {
            const auto schema = sink.value()->getSchema();
            const auto type = sink.value()->getSinkType();
            auto config = sink.value()->getSinkConfig();
            const auto formatConfig = sink.value()->getFormatConfig();

            /// "host" is not part of the sink config — it determines placement, not sink behavior.
            /// It is stored in the config map only because InlineSinkLogicalOperator lacks a dedicated host field.
            auto hostIt = config.find("host");
            if (hostIt == config.end())
            {
                throw InvalidConfigParameter("'host'");
            }
            auto host = Host(hostIt->second);
            config.erase(hostIt);

            const auto sinkDescriptor = sinkCatalog->getInlineSink(schema, type, host, config, formatConfig);

            if (!sinkDescriptor.has_value())
            {
                throw InvalidConfigParameter("Failed to create inline sink descriptor");
            }

            SinkLogicalOperator sinkOperator{sinkDescriptor.value()};
            sinkOperator = sinkOperator.withChildren(sink.value().getChildren());
            newRootOperators.emplace_back(sinkOperator);
        }
        else
        {
            newRootOperators.emplace_back(rootOperator);
        }
    }

    queryPlan = queryPlan.withRootOperators(newRootOperators);
}


}

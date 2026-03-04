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

#include <LegacyOptimizer/InlineSinkBindingPhase.hpp>

#include <vector>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/InlineSinkLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES
{


void InlineSinkBindingPhase::apply(LogicalPlan& queryPlan) const
{
    std::vector<LogicalOperator> newRootOperators;
    for (const auto& rootOperator : queryPlan.getRootOperators())
    {
        if (auto sink = rootOperator.tryGetAs<InlineSinkLogicalOperator>(); sink.has_value())
        {
            const auto schema = sink.value()->getSchema();
            const auto type = sink.value()->getSinkType();
            const auto config = sink.value()->getSinkConfig();
            const auto formatConfig = sink.value()->getFormatConfig();

            const auto sinkDescriptor = sinkCatalog->getInlineSink(schema, type, config, formatConfig);

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

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

#include <utility>
#include <Operators/Sinks/InlineSinkLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

void InlineSinkBindingPhase::apply(LogicalPlan& queryPlan) const
{
    for (auto rootOperators = queryPlan.getRootOperators(); const auto& rootOperator : rootOperators)
    {
        if (auto sink = rootOperator.tryGetAs<InlineSinkLogicalOperator>(); sink.has_value())
        {
            const auto schema = sink.value()->getSchema();
            const auto type = sink.value()->getSinkType();
            const auto config = sink.value()->getSinkConfig();

            const auto sinkDescriptor = SinkCatalog::getInlineSink(schema, type, config);

            if (!sinkDescriptor.has_value())
            {
                throw InvalidConfigParameter("Failed to create inline sink descriptor");
            }

            const SinkLogicalOperator sinkOperator{sinkDescriptor.value()};

            auto result = replaceOperator(queryPlan, sink->getId(), sinkOperator);

            if (!result.has_value())
            {
                throw InvalidConfigParameter("Failed to create inline sink operator");
            }
            queryPlan = std::move(result.value());
        }
    }
}


}

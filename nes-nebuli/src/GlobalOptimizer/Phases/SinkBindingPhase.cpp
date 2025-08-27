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

#include <GlobalOptimizer/Phases/SinkBindingPhase.hpp>

#include <memory>
#include <ranges>
#include <vector>

#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
LogicalPlan SinkBindingPhase::apply(const LogicalPlan& inputPlan, SharedPtr<const SinkCatalog> sinkCatalog)
{
    return inputPlan.withRootOperators(
        inputPlan.getRootOperators()
        | std::ranges::views::transform(
            [&sinkCatalog](const LogicalOperator& rootOperator) -> LogicalOperator
            {
                const auto sinkOperator = rootOperator.tryGet<SinkLogicalOperator>();
                PRECONDITION(sinkOperator.has_value(), "Root operator must be sink");
                /// Check to centralize the sink binding logic
                /// TODO #897 move sink binding to new query binder
                PRECONDITION(
                    not sinkOperator->getSinkDescriptor().has_value(), "Sink Descriptors should only be set during the sink binding phase");
                const auto sinkDescriptor = sinkCatalog->getSinkDescriptor(sinkOperator->getSinkName());
                if (not sinkDescriptor.has_value())
                {
                    throw UnknownSinkName("{}", sinkOperator->getSinkName());
                }
                return sinkOperator->withSinkDescriptor(sinkDescriptor.value());
            })
        | std::ranges::to<std::vector<LogicalOperator>>());
}
}

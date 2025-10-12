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

#include <LegacyOptimizer/SinkBindingRule.hpp>

#include <ranges>
#include <vector>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>

void NES::SinkBindingRule::apply(LogicalPlan& queryPlan) const
{
    queryPlan = queryPlan.withRootOperators(
        queryPlan.getRootOperators()
        | std::ranges::views::transform(
            [this](const LogicalOperator& rootOperator) -> LogicalOperator
            {
                auto sinkOperator = rootOperator.tryGetAs<SinkLogicalOperator>();
                PRECONDITION(sinkOperator.has_value(), "Root operator must be sink");
                /// Check to centralize the sink binding logic
                /// TODO #897 move sink binding to new query binder


                if (sinkOperator.value()->getSinkDescriptor().has_value())
                {
                    /// SinkOperator already described inline.
                    return sinkOperator.value();
                }

                const auto sinkDescriptor = sinkCatalog->getSinkDescriptor(sinkOperator->get().getSinkName());
                if (not sinkDescriptor.has_value())
                {
                    throw UnknownSinkName("{}", sinkOperator->get().getSinkName());
                }
                return sinkOperator.value()->withSinkDescriptor(sinkDescriptor.value());
            })
        | std::ranges::to<std::vector<LogicalOperator>>());
}

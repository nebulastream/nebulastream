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

#pragma once

#include <memory>
#include <string_view>
#include <utility>
#include <vector>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>
#include <QueryOptimizerAsyncConfiguration.hpp>

namespace NES
{
class SourceCatalog;
class SinkCatalog;

/// Lowers each placed async occurrence to a local handoff sink/source pair.
/// QueryOptimizer runs this rule after placement and before network decomposition, not in
/// the pre-placement PlanRuleRegistry. Final field order/layout and worker placement must
/// already be available; no semantic optimization is performed during this lowering.
class AsyncOperatorDecompositionRule
{
public:
    static constexpr std::string_view NAME = "AsyncOperatorDecompositionRule";

    AsyncOperatorDecompositionRule(
        QueryOptimizerAsyncConfiguration configuration,
        std::shared_ptr<const SourceCatalog> sourceCatalog,
        std::shared_ptr<const SinkCatalog> sinkCatalog)
        : configuration(std::move(configuration)), sourceCatalog(std::move(sourceCatalog)), sinkCatalog(std::move(sinkCatalog))
    {
    }

    /// Reject unsupported fan-out before placement can change or clone the topology.
    static void validateSharedSubplans(const LogicalPlan& plan);
    [[nodiscard]] LogicalPlan apply(LogicalPlan plan) const;

private:
    [[nodiscard]] LogicalOperator splitInvocation(const LogicalOperator& invocation, std::vector<LogicalOperator>& roots) const;

    QueryOptimizerAsyncConfiguration configuration;
    std::shared_ptr<const SourceCatalog> sourceCatalog;
    std::shared_ptr<const SinkCatalog> sinkCatalog;
};

static_assert(RuleConcept<AsyncOperatorDecompositionRule, LogicalPlan>);
}

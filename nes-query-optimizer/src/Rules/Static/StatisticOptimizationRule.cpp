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

#include <Rules/Static/StatisticOptimizationRule.hpp>

#include <iostream>
#include <memory>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>

#include <Operators/LogicalOperator.hpp>
#include <Operators/Statistic/StatisticStoreReaderLogicalOperator.hpp>
#include <Operators/Statistic/StatisticStoreWriterLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <RequestStatisticStatement.hpp>
#include <StatisticRetrievalService.hpp>

namespace NES
{

namespace
{
/// Recursion guard: the statistic build/probe queries the retrieval service submits contain the scalar
/// StatisticStore operators. If this rule ever sees such a plan we must NOT fetch another statistic (that would
/// recurse), so we detect those operators and leave the plan alone. In the daemon wiring the statistic queries
/// already run through a separate plain optimizer, so this is defense-in-depth.
bool containsStatisticStoreOperator(const LogicalOperator& op)
{
    if (op.tryGetAs<StatisticStoreWriterLogicalOperator>().has_value() || op.tryGetAs<StatisticStoreReaderLogicalOperator>().has_value())
    {
        return true;
    }
    for (const auto& child : op.getChildren())
    {
        if (containsStatisticStoreOperator(child))
        {
            return true;
        }
    }
    return false;
}

bool isStatisticQuery(const LogicalPlan& plan)
{
    for (const auto& root : plan.getRootOperators())
    {
        if (containsStatisticStoreOperator(root))
        {
            return true;
        }
    }
    return false;
}
}

StatisticOptimizationRule::StatisticOptimizationRule(std::shared_ptr<const StatisticRetrievalService> retrievalService)
    : retrievalService(std::move(retrievalService))
{
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan StatisticOptimizationRule::apply(LogicalPlan queryPlan) const
{
    /// Never fetch a statistic while optimizing a statistic query itself — that would recurse.
    if (isStatisticQuery(queryPlan))
    {
        return queryPlan;
    }

    /// This is a RequestStatisticBuildStatement (not a probe-only request) on purpose: retrieveStatistic() drives an
    /// ad-hoc build-then-probe round trip — it starts a build query described by this request, probes the result, and
    /// then tears the build down — so the request describes WHAT statistic to build, even though we only read the
    /// value back out.
    /// TODO: instead of building (and tearing down) a statistic ad-hoc on every optimization, start the relevant
    /// build queries up front (continuously) and have this rule only probe/retrieve from those already-running queries.
    const RequestStatisticBuildStatement adHocBuildRequest{
        .domain = DataDomain{.logicalSourceName = "optimizerSource", .fieldName = "value"},
        .metric = Metric::Average,
        .windowSizeMs = 1000,
        .windowAdvanceMs = std::nullopt,
        .eventTimeFieldName = std::nullopt,
        .conditionTrigger = std::nullopt,
        .options = {}};

    /// Fetch the (mock) statistic over the existing build/probe machinery and surface it. In a real adaptive rule
    /// this value would drive a rewrite (e.g. filter reordering by selectivity); for the PoC we only prove the value
    /// reaches the optimizer and then hand the plan back untouched.
    if (const auto value = retrievalService->retrieveStatistic(adHocBuildRequest); value.has_value())
    {
        NES_INFO("StatisticOptimizationRule: retrieved statistic value {}; returning plan unmodified.", value.value());
        std::cout << "StatisticOptimizationRule: retrieved statistic value " << value.value() << "; returning plan unmodified.\n";
    }
    else
    {
        NES_WARNING("StatisticOptimizationRule: no statistic value available; returning plan unmodified.");
        std::cout << "StatisticOptimizationRule: no statistic value available; returning plan unmodified.\n";
    }
    return queryPlan;
}

const std::type_info& StatisticOptimizationRule::getType()
{
    return typeid(StatisticOptimizationRule);
}

std::string_view StatisticOptimizationRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> StatisticOptimizationRule::dependsOn() const
{
    return {};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> StatisticOptimizationRule::requiredBy() const
{
    return {};
}

bool StatisticOptimizationRule::operator==(const StatisticOptimizationRule& other) const
{
    return retrievalService == other.retrievalService;
}
}

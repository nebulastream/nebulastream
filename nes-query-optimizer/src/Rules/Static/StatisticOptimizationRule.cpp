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
#include <string>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>

#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <StatisticRetrievalService.hpp>

namespace NES
{

StatisticOptimizationRule::StatisticOptimizationRule(
    std::shared_ptr<const StatisticRetrievalService> retrievalService, std::string statisticSource)
    : retrievalService(std::move(retrievalService)), statisticSource(std::move(statisticSource))
{
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan StatisticOptimizationRule::apply(LogicalPlan queryPlan) const
{
    /// Probe the build collecting `statisticSource` (deployed earlier by a GET_STATISTICS query) and surface its
    /// value. In a real adaptive rule the value would drive a rewrite (e.g. filter reordering by selectivity); for
    /// the PoC we only prove the requested statistic reaches the optimizer and then hand the plan back untouched.
    if (const auto value = retrievalService->retrieveStatistic(statisticSource); value.has_value())
    {
        NES_INFO(
            "StatisticOptimizationRule: retrieved statistic value {} for source {}; returning plan unmodified.",
            value.value(),
            statisticSource);
        std::cout << "StatisticOptimizationRule: retrieved statistic value " << value.value() << " for source " << statisticSource
                  << "; returning plan unmodified.\n";
    }
    else
    {
        NES_WARNING("StatisticOptimizationRule: no statistic available for source {}; returning plan unmodified.", statisticSource);
        std::cout << "StatisticOptimizationRule: no statistic available for source " << statisticSource
                  << "; returning plan unmodified.\n";
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
    return retrievalService == other.retrievalService && statisticSource == other.statisticSource;
}
}

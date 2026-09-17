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

#include <concepts>
#include <functional>
#include <string>
#include <string_view>
#include <vector>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

using AggregationLogicalFunctionRegistryReturnType = WindowAggregationLogicalFunction;

/// The SQL call's arguments in call order. The parser has already desugared expressions into pre-aggregation
/// projections, so each is either an UnboundFieldAccessLogicalFunction or a ConstantValueLogicalFunction.
struct AggregationLogicalFunctionRegistryArguments
{
    std::vector<LogicalFunction> parameters;
};

using AggregationLogicalFunctionFn
    = std::function<AggregationLogicalFunctionRegistryReturnType(AggregationLogicalFunctionRegistryArguments)>;

struct AggregationLogicalFunctionRegistryEntry
{
    AggregationLogicalFunctionFn create;
    std::string_view name;
    bool isStatistic{false};
};

/// Contract for an aggregation class registered via add_registry_entry(AggregationLogicalFunction <Name>).
///
/// Required:
/// - `static constexpr std::string_view NAME`: the registry key (looked up case-insensitively) and the blob type.
/// - `static AggregationLogicalFunctionRegistryReturnType create(AggregationLogicalFunctionRegistryArguments)`:
///   validates the arguments a SQL call supplied and constructs the function. Throw InvalidQuerySyntax on a bad
///   call so the parser can report it as such.
///
/// Optional:
/// - `static constexpr bool IS_STATISTIC = true`: marks a synopsis (reservoir sample, histogram, sketch, ...).
///   The parser then expects its SQL call to start with the statistic id, `NAME(statisticId, parameters...)`,
///   and always routes the result into a statistic store writer. Unmarked aggregations are ordinary window
///   aggregations that become statistics only when wrapped, `STATISTIC_BUILD(NAME(...), statisticId)`.
template <typename T>
concept MarkedStatisticAggregation = requires {
    { T::IS_STATISTIC } -> std::convertible_to<bool>;
};

/// Builds the registry entry for an aggregation class; referenced by the ENTRY_TEMPLATE in the component's CMake.
template <typename T>
AggregationLogicalFunctionRegistryEntry makeAggregationLogicalFunctionEntry()
{
    bool isStatistic = false;
    if constexpr (MarkedStatisticAggregation<T>)
    {
        isStatistic = T::IS_STATISTIC;
    }
    return {.create = &T::create, .name = T::NAME, .isStatistic = isStatistic};
}

/// Filled by loadBuiltinPlugins() / plugin registration (see cmake/RuntimeRegistrationUtil.cmake).
/// Case-insensitive to mirror the retired BaseRegistry.
class AggregationLogicalFunctionRegistry : public RuntimeRegistry<
                                               AggregationLogicalFunctionRegistry,
                                               std::string,
                                               AggregationLogicalFunctionRegistryEntry,
                                               /*CaseSensitive*/ false>
{
public:
    /// Defined out-of-line (AggregationLogicalFunctionRegistry.cpp) so exactly one instance
    /// exists process-wide even with plugins loaded as shared objects.
    static AggregationLogicalFunctionRegistry& instance();
};

}

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

#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Types/NESType.hpp>
#include <Util/Reflection.hpp>
#include <Util/RuntimeRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

using AggregationLogicalFunctionRegistryReturnType = WindowAggregationLogicalFunction;

struct AggregationLogicalFunctionRegistryArguments
{
    std::vector<AggregationFieldAccess> on;
    bool includeNullValues;
};

using AggregationLogicalFunctionFn
    = std::function<AggregationLogicalFunctionRegistryReturnType(AggregationLogicalFunctionRegistryArguments)>;

using AggregationTypeInferenceFn = std::expected<NESType, Exception> (*)(const NESType&);

/// Marker value representing `(*)` in SQL function calls such as `COUNT(*)`.
struct AsteriskTag
{
    bool operator==(const AsteriskTag&) const = default;
};

[[nodiscard]] NESValue makeAsteriskValue();

struct AggregationLogicalFunctionTypeTrait
{
    Identifier aggregationName;
    std::optional<NESType> resultScalarType;
};

/// Value representation produced when an aggregation functor (`SUM`, `COUNT`, `AVG`, etc.) is applied
/// to an argument in SQL. Preserves compound child expressions so the query binder can desugar them
/// into pre-aggregation projections uniformly for both built-in and plugin aggregations.
struct BoundAggregationCall
{
    Identifier aggregationName;
    AggregationLogicalFunctionFn factory;
    std::variant<LogicalFunction, AsteriskTag> argument;
    bool includeNullValues = false;

    [[nodiscard]] std::expected<WindowAggregationLogicalFunction, Exception>
    instantiateOnField(const AggregationFieldAccess& fieldAccess) const;

    bool operator==(const BoundAggregationCall& other) const
    {
        return aggregationName == other.aggregationName && argument == other.argument && includeNullValues == other.includeNullValues;
    }
};

class AggregationLogicalFunctionRegistry : public RuntimeRegistry<
                                               AggregationLogicalFunctionRegistry,
                                               std::string,
                                               AggregationLogicalFunctionFn,
                                               /*CaseSensitive*/ false>
{
public:
    static constexpr bool EXPOSE_TO_SQL_SCOPE = true;

    static AggregationLogicalFunctionRegistry& instance();
    static NESTypeClass aggregationLogicalFunctionTypeClass();
    static std::vector<NamedNESParametrizedTypeClass> getParametrizedTypeClasses();
};

namespace detail
{
[[nodiscard]] NESFunctor makeAggregationLogicalFunctionFunctorImpl(
    std::string_view key,
    AggregationLogicalFunctionFn factory,
    AggregationTypeInferenceFn inferAggregateType);
}

template <auto CreateFn, auto InferAggregateFn>
[[nodiscard]] NESFunctor aggregationLogicalFunctionFunctor(std::string_view name)
{
    static const NESFunctor instance = detail::makeAggregationLogicalFunctionFunctorImpl(
        name, AggregationLogicalFunctionFn{CreateFn}, AggregationTypeInferenceFn{InferAggregateFn});
    return instance;
}

}

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

#include <AggregationLogicalFunctionRegistry.hpp>

#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypeRegistry.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Types/NESType.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
struct AggregationNESType
{
    Identifier aggregationName;
    std::optional<NESType> instantiatedInputType;
    std::optional<NESType> inferredResultType;
    AggregationLogicalFunctionFn factory;

    bool operator==(const AggregationNESType& other) const
    {
        return aggregationName == other.aggregationName && instantiatedInputType == other.instantiatedInputType
            && inferredResultType == other.inferredResultType;
    }

    [[nodiscard]] std::expected<BoundAggregationCall, Exception> construct(const NESValueParameters& params) const
    {
        if (params.isNamed())
        {
            return std::unexpected{
                InvalidQuerySyntax("Aggregation function '{}' does not accept named arguments", aggregationName)};
        }
        if (params.size() != 1)
        {
            return std::unexpected{InvalidQuerySyntax(
                "Aggregation function '{}' expects exactly 1 argument, got {}", aggregationName, params.size())};
        }
        const auto argVal = params.at(0).value();
        if (argVal.tryGetAs<AsteriskTag>() != nullptr)
        {
            if (aggregationName != Identifier::parse("COUNT"))
            {
                return std::unexpected{InvalidQuerySyntax(
                    "Star argument (*) is only supported for COUNT(*), not {}()", aggregationName)};
            }
            return BoundAggregationCall{
                .aggregationName = aggregationName,
                .factory = factory,
                .argument = AsteriskTag{},
                .includeNullValues = true};
        }
        auto exprExp = nesValueToLogicalFunction(argVal);
        if (!exprExp.has_value())
        {
            return std::unexpected{std::move(exprExp).error()};
        }
        return BoundAggregationCall{
            .aggregationName = aggregationName,
            .factory = factory,
            .argument = std::move(*exprExp),
            .includeNullValues = false};
    }
};

NESType makeAggregationNESType(
    Identifier aggName,
    std::optional<NESType> instantiatedInput,
    std::optional<NESType> inferredResult,
    AggregationLogicalFunctionFn factory)
{
    return NESType{AggregationNESType{
                       .aggregationName = aggName,
                       .instantiatedInputType = std::move(instantiatedInput),
                       .inferredResultType = inferredResult,
                       .factory = std::move(factory)}}
        .withTypeTrait(AggregationLogicalFunctionTypeTrait{
            .aggregationName = aggName,
            .resultScalarType = std::move(inferredResult)});
}

/// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
const NESOptionalTypeParameter INPUT_TYPE{
    Identifier::parse("INPUT_TYPE"),
    "Input scalar type aggregated over the window",
    &DataTypeRegistry::scalarDataTypeClass};

struct AggregationLogicalFunctionFunctor
{
    Identifier aggregationName;
    AggregationTypeInferenceFn inferAggregateType;
    AggregationLogicalFunctionFn factory;

    [[nodiscard]] const AggregationLogicalFunctionFn& getRegistryEntry() const noexcept { return factory; }

    bool operator==(const AggregationLogicalFunctionFunctor& other) const
    {
        return aggregationName == other.aggregationName;
    }

    [[nodiscard]] std::expected<NESType, Exception> instantiate(const NESTypeParameters& typeParams) const
    {
        const auto resolvedExp = createExpectedNESTypeArguments(INPUT_TYPE).resolve(typeParams);
        if (!resolvedExp.has_value())
        {
            return std::unexpected{resolvedExp.error()};
        }
        const auto argTypeOpt = resolvedExp->get(INPUT_TYPE);
        if (!argTypeOpt.has_value())
        {
            return makeAggregationNESType(aggregationName, std::nullopt, std::nullopt, factory);
        }
        auto inferredExp = inferAggregateType(*argTypeOpt);
        if (!inferredExp.has_value())
        {
            return std::unexpected{std::move(inferredExp).error()};
        }
        return makeAggregationNESType(aggregationName, *argTypeOpt, std::move(*inferredExp), factory);
    }
};
}

NESValue makeAsteriskValue()
{
    static const NESType asteriskType{AsteriskTag{}};
    return NESValue::create(asteriskType, AsteriskTag{});
}

std::expected<WindowAggregationLogicalFunction, Exception>
BoundAggregationCall::instantiateOnField(const AggregationFieldAccess& fieldAccess) const
{
    try
    {
        return factory(AggregationLogicalFunctionRegistryArguments{.on = {fieldAccess}, .includeNullValues = includeNullValues});
    }
    catch (const Exception& ex)
    {
        return std::unexpected{ex};
    }
}

AggregationLogicalFunctionRegistry& AggregationLogicalFunctionRegistry::instance()
{
    static AggregationLogicalFunctionRegistry inst;
    return inst;
}

NESTypeClass AggregationLogicalFunctionRegistry::aggregationLogicalFunctionTypeClass()
{
    return NESTypeClass::forTypeTrait<AggregationLogicalFunctionTypeTrait>();
}

std::vector<NamedNESParametrizedTypeClass> AggregationLogicalFunctionRegistry::getParametrizedTypeClasses()
{
    return {NamedNESParametrizedTypeClass::constant(
        QualifiedIdentifier::parse("AggregationLogicalFunction"), aggregationLogicalFunctionTypeClass())};
}

namespace detail
{
NESFunctor makeAggregationLogicalFunctionFunctorImpl(
    const std::string_view key,
    AggregationLogicalFunctionFn factory,
    AggregationTypeInferenceFn inferAggregateType)
{
    return NESFunctor{AggregationLogicalFunctionFunctor{
        .aggregationName = Identifier::parse(std::string{key}),
        .inferAggregateType = inferAggregateType,
        .factory = std::move(factory)}};
}
}

}

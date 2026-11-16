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

#include <DataTypes/DataType.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Types/NESType.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
struct AggregationTypeDescriptor
{
    Identifier aggregationName;
    std::optional<NESType> inferredResultType;
    AggregationLogicalFunctionFn factory;

    bool operator==(const AggregationTypeDescriptor& other) const
    {
        return aggregationName == other.aggregationName && inferredResultType == other.inferredResultType;
    }
};

NESType makeAggregationNESType(Identifier aggName, std::optional<NESType> inferredResult, AggregationLogicalFunctionFn factory)
{
    return NESType::create<AggregationTypeDescriptor, BoundAggregationCall>(
               AggregationTypeDescriptor{
                   .aggregationName = aggName,
                   .inferredResultType = inferredResult,
                   .factory = std::move(factory)},
               [](const AggregationTypeDescriptor& desc, const NESValueParameters& params)
                   -> std::expected<BoundAggregationCall, Exception>
               {
                   if (params.isNamed())
                   {
                       return std::unexpected{
                           InvalidQuerySyntax("Aggregation function '{}' does not accept named arguments", desc.aggregationName)};
                   }
                   if (params.size() != 1)
                   {
                       return std::unexpected{InvalidQuerySyntax(
                           "Aggregation function '{}' expects exactly 1 argument, got {}", desc.aggregationName, params.size())};
                   }
                   const auto argVal = params.at(0).value();
                   if (argVal.tryGetAs<AsteriskTag>() != nullptr)
                   {
                       if (desc.aggregationName != Identifier::parse("COUNT"))
                       {
                           return std::unexpected{InvalidQuerySyntax(
                               "Star argument (*) is only supported for COUNT(*), not {}()", desc.aggregationName)};
                       }
                       return BoundAggregationCall{
                           .aggregationName = desc.aggregationName,
                           .factory = desc.factory,
                           .argument = AsteriskTag{},
                           .includeNullValues = true};
                   }
                   auto exprExp = nesValueToLogicalFunction(argVal);
                   if (!exprExp.has_value())
                   {
                       return std::unexpected{std::move(exprExp).error()};
                   }
                   return BoundAggregationCall{
                       .aggregationName = desc.aggregationName,
                       .factory = desc.factory,
                       .argument = std::move(*exprExp),
                       .includeNullValues = false};
               })
        .withTrait(AggregationLogicalFunctionTypeTrait{
            .aggregationName = aggName,
            .resultScalarType = std::move(inferredResult)});
}
}

NESValue makeAsteriskValue()
{
    static const NESType asteriskType = NESType::createUnconstructible<AsteriskTag, AsteriskTag>(AsteriskTag{});
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
    return NESTypeClass::forTrait<AggregationLogicalFunctionTypeTrait>();
}

std::vector<NamedNESParametrizedTypeClass> AggregationLogicalFunctionRegistry::getParametrizedTypeClasses()
{
    return {NamedNESParametrizedTypeClass::constant(
        QualifiedIdentifier::parse("AggregationLogicalFunction"), aggregationLogicalFunctionTypeClass())};
}

NESFunctor makeAggregationLogicalFunctionFunctor(const std::string_view key, AggregationLogicalFunctionFn factory)
{
    const auto aggId = Identifier::parse(std::string{key});
    return NESFunctor{
        std::move(factory),
        [](const AggregationLogicalFunctionFn& lhs, const AggregationLogicalFunctionFn& rhs) { return &lhs == &rhs; },
        [aggId](const AggregationLogicalFunctionFn& fn, const NESTypeParameters& typeParams) -> std::expected<NESType, Exception>
        {
            if (typeParams.empty())
            {
                return makeAggregationNESType(aggId, std::nullopt, fn);
            }
            if (typeParams.size() != 1)
            {
                return std::unexpected{TypeInferenceException(
                    "Aggregation '{}' expects at most 1 input scalar type parameter, got {}", aggId, typeParams.size())};
            }
            const auto argTypeOpt = typeParams.isNamed()
                ? typeParams.getTypeByName(QualifiedIdentifier::parse("INPUT_TYPE"))
                : typeParams.getTypeAt(0);
            if (!argTypeOpt.has_value())
            {
                return std::unexpected{
                    TypeInferenceException("Aggregation '{}' expects a ScalarDatatype type parameter", aggId)};
            }
            const auto* scalarTrait = argTypeOpt->tryGetTrait<ScalarTypeTrait>();
            if (scalarTrait == nullptr)
            {
                return std::unexpected{TypeInferenceException(
                    "Aggregation '{}' parameter does not satisfy ScalarDatatype", aggId)};
            }
            const auto dtOpt = scalarTrait->tryGetDataType(argTypeOpt->getDescriptor());
            if (!dtOpt.has_value())
            {
                return std::unexpected{TypeInferenceException(
                    "Built-in aggregation '{}' does not support custom non-DataType scalar parameter", aggId)};
            }

            try
            {
                const LogicalOperator dummyProducer{SourceNameLogicalOperator::create(Identifier::parse("_agg_probe"))};
                Field dummyField{dummyProducer, Identifier::parse("_agg_field"), *dtOpt};
                const Schema<Field, Unordered> dummySchema{dummyField};
                auto agg = fn(AggregationLogicalFunctionRegistryArguments{
                    .on = {TypedLogicalFunction<FieldAccessLogicalFunction>{FieldAccessLogicalFunction{dummyField}}},
                    .includeNullValues = false});
                auto inferred = agg.withInferredType(dummySchema);
                return makeAggregationNESType(aggId, inferred.getAggregateType().toNESType(), fn);
            }
            catch (const Exception& ex)
            {
                return std::unexpected{ex};
            }
        }};
}

}

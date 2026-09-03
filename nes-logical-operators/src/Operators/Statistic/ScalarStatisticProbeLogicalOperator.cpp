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

#include <Operators/Statistic/ScalarStatisticProbeLogicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Statistic/StatisticTypes.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

ScalarStatisticProbeLogicalOperator::ScalarStatisticProbeLogicalOperator(
    WeakLogicalOperator self,
    const StatisticId statisticId,
    const StatisticType op,
    DataType valueType,
    Identifier startTsFieldName,
    Identifier endTsFieldName)
    : ManagedByOperator(std::move(self))
    , statisticId(statisticId)
    , op(op)
    , valueType(std::move(valueType))
    , startTsFieldName(std::move(startTsFieldName))
    , endTsFieldName(std::move(endTsFieldName))
{
}

ScalarStatisticProbeLogicalOperator::ScalarStatisticProbeLogicalOperator(
    WeakLogicalOperator self,
    LogicalOperator child,
    const StatisticId statisticId,
    const StatisticType op,
    DataType valueType,
    Identifier startTsFieldName,
    Identifier endTsFieldName)
    : ManagedByOperator(std::move(self))
    , child(std::move(child))
    , statisticId(statisticId)
    , op(op)
    , valueType(std::move(valueType))
    , startTsFieldName(std::move(startTsFieldName))
    , endTsFieldName(std::move(endTsFieldName))
{
    inferLocalSchema();
}

TypedLogicalOperator<ScalarStatisticProbeLogicalOperator> ScalarStatisticProbeLogicalOperator::create(
    const StatisticId statisticId, const StatisticType op, DataType valueType, Identifier startTsFieldName, Identifier endTsFieldName)
{
    return TypedLogicalOperator<ScalarStatisticProbeLogicalOperator>{
        statisticId, op, std::move(valueType), std::move(startTsFieldName), std::move(endTsFieldName)};
}

TypedLogicalOperator<ScalarStatisticProbeLogicalOperator> ScalarStatisticProbeLogicalOperator::create(
    LogicalOperator child,
    const StatisticId statisticId,
    const StatisticType op,
    DataType valueType,
    Identifier startTsFieldName,
    Identifier endTsFieldName)
{
    return TypedLogicalOperator<ScalarStatisticProbeLogicalOperator>{
        std::move(child), statisticId, op, std::move(valueType), std::move(startTsFieldName), std::move(endTsFieldName)};
}

std::string_view ScalarStatisticProbeLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool ScalarStatisticProbeLogicalOperator::operator==(const ScalarStatisticProbeLogicalOperator& rhs) const
{
    return statisticId == rhs.statisticId and op == rhs.op and valueType == rhs.valueType and startTsFieldName == rhs.startTsFieldName
        and endTsFieldName == rhs.endTsFieldName and outputSchema == rhs.outputSchema and traitSet == rhs.traitSet;
}

std::string ScalarStatisticProbeLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "SCALARSTATISTICPROBE(opId: {}, statisticId: {}, op: {}, valueType: {}, traitSet: {})",
            opId,
            statisticId.getRawValue(),
            magic_enum::enum_name(op),
            valueType,
            traitSet.explain(verbosity));
    }
    return fmt::format("SCALARSTATISTICPROBE({}, {})", magic_enum::enum_name(op), statisticId.getRawValue());
}

void ScalarStatisticProbeLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    const auto inputSchema = child->getOutputSchema();

    /// The window bounds are the only thing taken from the incoming record, so they have to be there.
    for (const auto& required : {startTsFieldName, endTsFieldName})
    {
        if (not inputSchema.contains(required))
        {
            throw CannotInferSchema(
                "ScalarStatisticProbe expects the field {} in its input, which provides: {}",
                required,
                fmt::join(inputSchema.getUniqueFieldNames(), ", "));
        }
    }

    /// A fresh stream rather than a pass-through: one row per stored statistic in the probed range, carrying the
    /// lookup key back alongside the reconstructed value.
    const auto uint64Type = DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE);
    const std::vector<UnqualifiedUnboundField> outputFields{
        UnqualifiedUnboundField{Identifier::parse(std::string{StatisticFieldNames::STATISTIC_ID}), uint64Type},
        UnqualifiedUnboundField{Identifier::parse(std::string{StatisticFieldNames::START_TS}), uint64Type},
        UnqualifiedUnboundField{Identifier::parse(std::string{StatisticFieldNames::END_TS}), uint64Type},
        UnqualifiedUnboundField{Identifier::parse(std::string{StatisticFieldNames::NUMBER_OF_SEEN_TUPLES}), uint64Type},
        UnqualifiedUnboundField{Identifier::parse(std::string{StatisticFieldNames::VALUE}), valueType}};

    auto schemaOrCollisions = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
    if (not schemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "Found collisions in the ScalarStatisticProbe output schema: {}",
            Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(schemaOrCollisions.error()));
    }
    outputSchema = std::move(schemaOrCollisions.value());
}

ScalarStatisticProbeLogicalOperator ScalarStatisticProbeLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

TraitSet ScalarStatisticProbeLogicalOperator::getTraitSet() const
{
    return traitSet;
}

ScalarStatisticProbeLogicalOperator ScalarStatisticProbeLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

ScalarStatisticProbeLogicalOperator ScalarStatisticProbeLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for a scalar statistic probe, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

ScalarStatisticProbeLogicalOperator ScalarStatisticProbeLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for a scalar statistic probe, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> ScalarStatisticProbeLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> ScalarStatisticProbeLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator ScalarStatisticProbeLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

Reflected Reflector<TypedLogicalOperator<ScalarStatisticProbeLogicalOperator>>::operator()(
    const TypedLogicalOperator<ScalarStatisticProbeLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedScalarStatisticProbeLogicalOperator{
        .operatorId = op.getId(),
        .statisticId = op->getStatisticId().getRawValue(),
        .op = static_cast<uint64_t>(op->getOp()),
        .valueType = op->getValueType(),
        .startTsFieldName = op->getStartTsFieldName(),
        .endTsFieldName = op->getEndTsFieldName()});
}

Unreflector<TypedLogicalOperator<ScalarStatisticProbeLogicalOperator>>::Unreflector(ContextType operatorMapping)
    : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<ScalarStatisticProbeLogicalOperator>
Unreflector<TypedLogicalOperator<ScalarStatisticProbeLogicalOperator>>::operator()(
    const Reflected& rfl, const ReflectionContext& context) const
{
    auto [id, statisticId, op, valueType, startTsFieldName, endTsFieldName]
        = context.unreflect<detail::ReflectedScalarStatisticProbeLogicalOperator>(rfl);
    const auto decodedOp = magic_enum::enum_cast<StatisticType>(static_cast<uint8_t>(op));
    if (not decodedOp.has_value())
    {
        throw CannotDeserialize("Unknown scalar statistic op: {}", op);
    }
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("ScalarStatisticProbeLogicalOperator requires exactly one child, but got {}", children.size());
    }
    return ScalarStatisticProbeLogicalOperator::create(
        children.at(0),
        StatisticId{statisticId},
        decodedOp.value(),
        std::move(valueType),
        std::move(startTsFieldName),
        std::move(endTsFieldName));
}

}

size_t std::hash<NES::ScalarStatisticProbeLogicalOperator>::operator()(const NES::ScalarStatisticProbeLogicalOperator& op) const noexcept
{
    return folly::hash::hash_combine(
        op.statisticId.getRawValue(), static_cast<uint64_t>(op.op), op.startTsFieldName, op.endTsFieldName);
}

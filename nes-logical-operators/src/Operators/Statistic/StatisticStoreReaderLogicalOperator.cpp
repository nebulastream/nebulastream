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

#include <Operators/Statistic/StatisticStoreReaderLogicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/StatisticIdentifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Statistic/StatisticBlobType.hpp>
#include <Operators/Statistic/StatisticFieldNames.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>

namespace NES
{

StatisticStoreReaderLogicalOperator::StatisticStoreReaderLogicalOperator(
    WeakLogicalOperator self,
    const StatisticId statisticId,
    StatisticBlobType typeName,
    std::vector<StatisticStoreReaderLogicalOperator::PayloadField> payloadFields,
    const StatisticWindowMatch windowMatch)
    : ManagedByOperator(std::move(self))
    , statisticId(statisticId)
    , typeName(std::move(typeName))
    , payloadFields(std::move(payloadFields))
    , windowMatch(windowMatch)
{
}

StatisticStoreReaderLogicalOperator::StatisticStoreReaderLogicalOperator(
    WeakLogicalOperator self,
    LogicalOperator child,
    const StatisticId statisticId,
    StatisticBlobType typeName,
    std::vector<StatisticStoreReaderLogicalOperator::PayloadField> payloadFields,
    const StatisticWindowMatch windowMatch)
    : ManagedByOperator(std::move(self))
    , child(std::move(child))
    , statisticId(statisticId)
    , typeName(std::move(typeName))
    , payloadFields(std::move(payloadFields))
    , windowMatch(windowMatch)
{
    inferLocalSchema();
}

TypedLogicalOperator<StatisticStoreReaderLogicalOperator> StatisticStoreReaderLogicalOperator::create(
    const StatisticId statisticId,
    StatisticBlobType typeName,
    std::vector<StatisticStoreReaderLogicalOperator::PayloadField> payloadFields,
    const StatisticWindowMatch windowMatch)
{
    return TypedLogicalOperator<StatisticStoreReaderLogicalOperator>{
        statisticId, std::move(typeName), std::move(payloadFields), windowMatch};
}

TypedLogicalOperator<StatisticStoreReaderLogicalOperator> StatisticStoreReaderLogicalOperator::create(
    LogicalOperator child,
    const StatisticId statisticId,
    StatisticBlobType typeName,
    std::vector<StatisticStoreReaderLogicalOperator::PayloadField> payloadFields,
    const StatisticWindowMatch windowMatch)
{
    return TypedLogicalOperator<StatisticStoreReaderLogicalOperator>{
        std::move(child), statisticId, std::move(typeName), std::move(payloadFields), windowMatch};
}

std::string_view StatisticStoreReaderLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool StatisticStoreReaderLogicalOperator::operator==(const StatisticStoreReaderLogicalOperator& rhs) const
{
    return statisticId == rhs.statisticId and typeName == rhs.typeName and payloadFields == rhs.payloadFields
        and windowMatch == rhs.windowMatch and outputSchema == rhs.outputSchema and traitSet == rhs.traitSet;
}

std::string StatisticStoreReaderLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "STATISTICSTOREREADER(opId: {}, statisticId: {}, type: {}, payloadFields: {}, traitSet: {})",
            opId,
            statisticId.getRawValue(),
            typeName.getRawValue(),
            fmt::join(std::views::keys(payloadFields), ", "),
            traitSet.explain(verbosity));
    }
    return fmt::format("STATISTICSTOREREADER({}, {})", typeName.getRawValue(), statisticId.getRawValue());
}

void StatisticStoreReaderLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    const auto inputSchema = child->getOutputSchema();

    /// The window bounds are the only thing taken from the incoming record, so they have to be there.
    for (const auto& required :
         {Identifier::parse(std::string{StatisticFieldNames::START_TS}), Identifier::parse(std::string{StatisticFieldNames::END_TS})})
    {
        if (not inputSchema.contains(required))
        {
            throw CannotInferSchema(
                "StatisticStoreReader expects the field {} in its input, which provides: {}",
                required,
                fmt::join(inputSchema.getUniqueFieldNames(), ", "));
        }
    }

    /// A fresh stream rather than a pass-through: one row per stored statistic in the probed range, carrying the
    /// lookup key back alongside the reconstructed value.
    const auto uint64Type = DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE);
    std::vector<UnqualifiedUnboundField> outputFields{
        UnqualifiedUnboundField{Identifier::parse(std::string{StatisticFieldNames::STATISTIC_ID}), uint64Type},
        UnqualifiedUnboundField{Identifier::parse(std::string{StatisticFieldNames::START_TS}), uint64Type},
        UnqualifiedUnboundField{Identifier::parse(std::string{StatisticFieldNames::END_TS}), uint64Type},
        UnqualifiedUnboundField{Identifier::parse(std::string{StatisticFieldNames::NUMBER_OF_SEEN_TUPLES}), uint64Type}};
    for (const auto& [name, dataType] : payloadFields)
    {
        outputFields.emplace_back(name, dataType);
    }

    auto schemaOrCollisions = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
    if (not schemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "Found collisions in the StatisticStoreReader output schema: {}",
            Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(schemaOrCollisions.error()));
    }
    outputSchema = std::move(schemaOrCollisions.value());
}

StatisticStoreReaderLogicalOperator StatisticStoreReaderLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

TraitSet StatisticStoreReaderLogicalOperator::getTraitSet() const
{
    return traitSet;
}

StatisticStoreReaderLogicalOperator StatisticStoreReaderLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

StatisticStoreReaderLogicalOperator StatisticStoreReaderLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for a scalar statistic probe, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

StatisticStoreReaderLogicalOperator StatisticStoreReaderLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for a scalar statistic probe, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> StatisticStoreReaderLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> StatisticStoreReaderLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator StatisticStoreReaderLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

Reflected Reflector<TypedLogicalOperator<StatisticStoreReaderLogicalOperator>>::operator()(
    const TypedLogicalOperator<StatisticStoreReaderLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedStatisticStoreReaderLogicalOperator{
        .operatorId = op.getId(),
        .statisticId = op->getStatisticId().getRawValue(),
        .typeName = op->getTypeName().getRawValue(),
        .payloadFields = op->getPayloadFields(),
        .windowMatch = static_cast<uint8_t>(op->getWindowMatch())});
}

Unreflector<TypedLogicalOperator<StatisticStoreReaderLogicalOperator>>::Unreflector(ContextType operatorMapping)
    : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<StatisticStoreReaderLogicalOperator>
Unreflector<TypedLogicalOperator<StatisticStoreReaderLogicalOperator>>::operator()(
    const Reflected& rfl, const ReflectionContext& context) const
{
    auto [id, statisticId, typeName, payloadFields, windowMatch]
        = context.unreflect<detail::ReflectedStatisticStoreReaderLogicalOperator>(rfl);
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("StatisticStoreReaderLogicalOperator requires exactly one child, but got {}", children.size());
    }
    return StatisticStoreReaderLogicalOperator::create(
        children.at(0),
        StatisticId{statisticId},
        StatisticBlobType{typeName},
        std::move(payloadFields),
        static_cast<StatisticWindowMatch>(windowMatch));
}

}

size_t std::hash<NES::StatisticStoreReaderLogicalOperator>::operator()(const NES::StatisticStoreReaderLogicalOperator& op) const noexcept
{
    return folly::hash::hash_combine(op.statisticId.getRawValue(), op.typeName.getRawValue());
}

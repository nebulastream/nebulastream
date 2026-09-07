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

#include <Operators/AlignLogicalOperator.hpp>

#include <array>
#include <cstddef>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>

#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Hash.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>

namespace NES
{

AlignLogicalOperator::AlignLogicalOperator(WeakLogicalOperator self, AlignStrategy strategy, AlignTimeCharacteristic timestampFields)
    : ManagedByOperator(std::move(self)), strategy(strategy), timestampFields(std::move(timestampFields))
{
}

AlignLogicalOperator::AlignLogicalOperator(
    WeakLogicalOperator self, std::array<LogicalOperator, 2> children, AlignStrategy strategy, AlignTimeCharacteristic timestampFields)
    : ManagedByOperator(std::move(self)), strategy(strategy), children(std::move(children)), timestampFields(std::move(timestampFields))
{
    inferLocalSchema();
}

TypedLogicalOperator<AlignLogicalOperator> AlignLogicalOperator::create(AlignStrategy strategy, AlignTimeCharacteristic timestampFields)
{
    return TypedLogicalOperator<AlignLogicalOperator>{strategy, std::move(timestampFields)};
}

TypedLogicalOperator<AlignLogicalOperator>
AlignLogicalOperator::create(std::array<LogicalOperator, 2> children, AlignStrategy strategy, AlignTimeCharacteristic timestampFields)
{
    return TypedLogicalOperator<AlignLogicalOperator>{std::move(children), strategy, std::move(timestampFields)};
}

std::string_view AlignLogicalOperator::getName() const noexcept
{
    return NAME;
}

AlignLogicalOperator::AlignStrategy AlignLogicalOperator::getAlignStrategy() const
{
    return strategy;
}

AlignTimeCharacteristic AlignLogicalOperator::getTimestampFields() const
{
    return timestampFields;
}

bool AlignLogicalOperator::operator==(const AlignLogicalOperator& rhs) const
{
    return strategy == rhs.strategy and outputSchema == rhs.outputSchema and getTraitSet() == rhs.getTraitSet()
        and timestampFields == rhs.timestampFields;
}

std::string AlignLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    const auto strategyName = std::string(magic_enum::enum_name(strategy));
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("Align(opId: {}, strategy: {}, traitSet: {})", id, strategyName, traitSet.explain(verbosity));
    }
    return fmt::format("Align({})", strategyName);
}

void AlignLogicalOperator::inferLocalSchema()
{
    PRECONDITION(children.has_value(), "Child not set when calling schema inference");

    const std::vector<Field> inputFields = *children | std::views::transform([](const auto& child) { return child.getOutputSchema(); })
        | std::views::join | std::ranges::to<std::vector>();

    auto inputSchemaOrCollisions = Schema<Field, Unordered>::tryCreateCollisionFree(inputFields);
    if (!inputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "Found collisions in input schemas: " + Schema<Field, Unordered>::createCollisionString(inputSchemaOrCollisions.error()));
    }
    const auto& inputSchema = inputSchemaOrCollisions.value();

    this->timestampFields = std::visit(
        [&](const auto& tsFields)
        {
            return AlignTimeCharacteristic{std::array{
                Windowing::TimeCharacteristicWrapper{tsFields[0]}.withInferredSchema(inputSchema),
                Windowing::TimeCharacteristicWrapper{tsFields[1]}.withInferredSchema(inputSchema)}};
        },
        this->timestampFields);

    const auto boundFields = std::get<std::array<Windowing::BoundTimeCharacteristic, 2>>(this->timestampFields);

    if (Windowing::TimeCharacteristicWrapper{boundFields[0]}.getType() != Windowing::TimeCharacteristicWrapper{boundFields[1]}.getType())
    {
        throw UnsupportedQuery("ALIGN requires both sides to use the same kind of time characteristic (event time or ingestion time)");
    }

    const auto tsField = [](const Windowing::BoundTimeCharacteristic& characteristic) -> std::optional<Field>
    {
        if (const auto* eventTime = std::get_if<Windowing::BoundEventTimeCharacteristic>(&characteristic))
        {
            return eventTime->field->getField();
        }
        return std::nullopt;
    };
    const auto anchorTsField = tsField(boundFields[0]);
    const auto otherTsField = tsField(boundFields[1]);

    std::vector<UnqualifiedUnboundField> outputFields;
    for (const auto& field : (*children)[0].getOutputSchema())
    {
        outputFields.emplace_back(field.unbound());
    }
    for (const auto& field : (*children)[1].getOutputSchema())
    {
        if (otherTsField.has_value() && field == otherTsField.value())
        {
            continue;
        }
        outputFields.emplace_back(field.unbound());
    }
    if (!anchorTsField.has_value())
    {
        outputFields.emplace_back(Identifier::parse("timestamp"), DataType::Type::UINT64);
    }

    auto outputSchemaOrCollisions = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
    if (!outputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "Found collisions in input schemas with added fields from align: "
            + Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(outputSchemaOrCollisions.error()));
    }

    this->outputSchema = outputSchemaOrCollisions.value();
}

AlignLogicalOperator AlignLogicalOperator::withInferredSchema() const
{
    PRECONDITION(children.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.children = {(*copy.children)[0].withInferredSchema(), (*copy.children)[1].withInferredSchema()};
    copy.inferLocalSchema();
    return copy;
}

AlignLogicalOperator AlignLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet AlignLogicalOperator::getTraitSet() const
{
    return traitSet;
}

AlignLogicalOperator AlignLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 2, "Can only set exactly two children for align, got {}", children.size());
    auto copy = *this;
    copy.children = std::array{std::move(children.at(0)), std::move(children.at(1))};
    return copy;
}

AlignLogicalOperator AlignLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 2, "Can only set exactly two children for align, got {}", children.size());
    auto copy = *this;
    copy.children = std::array{std::move(children.at(0)), std::move(children.at(1))};
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> AlignLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> AlignLogicalOperator::getChildren() const
{
    if (children.has_value())
    {
        return *children | std::ranges::to<std::vector>();
    }
    return {};
}

std::array<LogicalOperator, 2> AlignLogicalOperator::getBothChildren() const
{
    PRECONDITION(children.has_value(), "Children not set when trying to retrieve align children");
    return *children;
}

Reflected Reflector<TypedLogicalOperator<AlignLogicalOperator>>::operator()(
    const TypedLogicalOperator<AlignLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedAlignLogicalOperator{
        .operatorId = op.getId(), .strategy = op->getAlignStrategy(), .timestampFields = op->getTimestampFields()});
}

Unreflector<TypedLogicalOperator<AlignLogicalOperator>>::Unreflector(ContextType operatorMapping) : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<AlignLogicalOperator>
Unreflector<TypedLogicalOperator<AlignLogicalOperator>>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [operatorId, strategy, timestampFields] = context.unreflect<detail::ReflectedAlignLogicalOperator>(reflected);
    auto foundChildren = plan->getChildrenFor(operatorId, context);
    return AlignLogicalOperator::create(std::array{foundChildren.at(0), foundChildren.at(1)}, strategy, std::move(timestampFields));
}

}

std::size_t std::hash<NES::AlignLogicalOperator>::operator()(const NES::AlignLogicalOperator& alignLogicalOperator) const noexcept
{
    return folly::hash::hash_combine_generic(NES::Hash{}, alignLogicalOperator.strategy, alignLogicalOperator.timestampFields);
}

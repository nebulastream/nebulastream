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

#include <Operators/Windows/StatisticBuildLogicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Reprojecter.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Statistics/Statistic.hpp>
#include <Traits/Trait.hpp>
#include <Util/DynamicBase.hpp>
#include <Util/Hash.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>

namespace NES
{

StatisticBuildLogicalOperator::StatisticBuildLogicalOperator(
    WeakLogicalOperator self,
    const StatisticId statisticId,
    const uint64_t sampleSize,
    const uint64_t seed,
    Windowing::TimeBasedWindowType windowType,
    Windowing::TimeCharacteristic timeCharacteristic)
    : ManagedByOperator(std::move(self))
    , windowType(std::move(windowType))
    , statisticId(statisticId)
    , sampleSize(sampleSize)
    , seed(seed)
    , timestampField(std::move(timeCharacteristic))
{
}

StatisticBuildLogicalOperator::StatisticBuildLogicalOperator(
    WeakLogicalOperator self,
    LogicalOperator child,
    const StatisticId statisticId,
    const uint64_t sampleSize,
    const uint64_t seed,
    Windowing::TimeBasedWindowType windowType,
    Windowing::TimeCharacteristic timeCharacteristic)
    : ManagedByOperator(std::move(self))
    , child(std::move(child))
    , windowType(std::move(windowType))
    , statisticId(statisticId)
    , sampleSize(sampleSize)
    , seed(seed)
    , timestampField(std::move(timeCharacteristic))
{
    inferLocalSchema();
}

TypedLogicalOperator<StatisticBuildLogicalOperator> StatisticBuildLogicalOperator::create(
    const StatisticId statisticId,
    const uint64_t sampleSize,
    const uint64_t seed,
    Windowing::TimeBasedWindowType windowType,
    Windowing::TimeCharacteristic timeCharacteristic)
{
    return TypedLogicalOperator<StatisticBuildLogicalOperator>{
        statisticId, sampleSize, seed, std::move(windowType), std::move(timeCharacteristic)};
}

TypedLogicalOperator<StatisticBuildLogicalOperator> StatisticBuildLogicalOperator::create(
    LogicalOperator child,
    const StatisticId statisticId,
    const uint64_t sampleSize,
    const uint64_t seed,
    Windowing::TimeBasedWindowType windowType,
    Windowing::TimeCharacteristic timeCharacteristic)
{
    return TypedLogicalOperator<StatisticBuildLogicalOperator>{
        std::move(child), statisticId, sampleSize, seed, std::move(windowType), std::move(timeCharacteristic)};
}

void StatisticBuildLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    const Schema<Field, Unordered>& inputSchema = child->getOutputSchema();

    timestampField = Windowing::TimeCharacteristicWrapper{std::move(timestampField)}.withInferredSchema(inputSchema);

    /// The output schema is fixed; tryCreateCollisionFree only guards against the (impossible) case of the
    /// statistic fields colliding with each other.
    const auto outputSchemaOrCollisions
        = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(std::vector(statisticFields.begin(), statisticFields.end()));
    INVARIANT(outputSchemaOrCollisions.has_value(), "The fixed statistic build output fields must not collide");
    outputSchema = outputSchemaOrCollisions.value();
}

std::string_view StatisticBuildLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string StatisticBuildLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "STATISTIC BUILD(opId: {}, statisticId: {}, sampleSize: {}, window type: {})", id, statisticId, sampleSize, windowType);
    }
    return fmt::format("STATISTIC BUILD(statisticId: {}, sampleSize: {})", statisticId, sampleSize);
}

bool StatisticBuildLogicalOperator::operator==(const StatisticBuildLogicalOperator& rhs) const
{
    return statisticId == rhs.statisticId && sampleSize == rhs.sampleSize && seed == rhs.seed && windowType == rhs.windowType
        && timestampField == rhs.timestampField && outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
}

StatisticBuildLogicalOperator StatisticBuildLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Ordered> StatisticBuildLogicalOperator::getOrderedOutputSchema(ChildOutputOrderProvider) const
{
    return statisticFields | RangeBinder{self.lock()} | std::ranges::to<Schema<Field, Ordered>>();
}

const detail::DynamicBase* StatisticBuildLogicalOperator::getDynamicBase() const
{
    return static_cast<const Reprojecter*>(this);
}

TraitSet StatisticBuildLogicalOperator::getTraitSet() const
{
    return traitSet;
}

StatisticBuildLogicalOperator StatisticBuildLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

StatisticBuildLogicalOperator StatisticBuildLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for statistic build, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

StatisticBuildLogicalOperator StatisticBuildLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for statistic build, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> StatisticBuildLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Retrieving output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> StatisticBuildLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator StatisticBuildLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

StatisticId StatisticBuildLogicalOperator::getStatisticId() const
{
    return statisticId;
}

uint64_t StatisticBuildLogicalOperator::getSampleSize() const
{
    return sampleSize;
}

uint64_t StatisticBuildLogicalOperator::getSeed() const
{
    return seed;
}

Windowing::TimeBasedWindowType StatisticBuildLogicalOperator::getWindowType() const
{
    return windowType;
}

std::variant<Windowing::UnboundTimeCharacteristic, Windowing::BoundTimeCharacteristic>
StatisticBuildLogicalOperator::getCharacteristic() const
{
    return timestampField;
}

const UnqualifiedUnboundField& StatisticBuildLogicalOperator::getStatisticStartField() const
{
    return statisticFields[0];
}

const UnqualifiedUnboundField& StatisticBuildLogicalOperator::getStatisticEndField() const
{
    return statisticFields[1];
}

const UnqualifiedUnboundField& StatisticBuildLogicalOperator::getStatisticDataField() const
{
    return statisticFields[2];
}

const UnqualifiedUnboundField& StatisticBuildLogicalOperator::getNumberOfSeenTuplesField() const
{
    return statisticFields[3];
}

std::unordered_map<Field, std::unordered_set<Field>> StatisticBuildLogicalOperator::getAccessedFieldsForOutput() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve accessed fields");
    const auto boundOutputSchema = getOutputSchema();
    const auto inputSchema = child->getOutputSchema();

    /// The statistic samples whole input records, so the blob and the seen-tuples counter depend on every input field.
    const auto allInputFields = inputSchema | std::ranges::to<std::unordered_set<Field>>();

    std::unordered_map<Field, std::unordered_set<Field>> accessedFields;
    for (const auto& outputField : std::array{getStatisticDataField(), getNumberOfSeenTuplesField()})
    {
        const auto boundFieldOpt = boundOutputSchema.getFieldByName(outputField.getFullyQualifiedName());
        INVARIANT(boundFieldOpt.has_value(), "Field {} not found in output schema", outputField);
        accessedFields[boundFieldOpt.value()] = allInputFields;
    }
    return accessedFields;
}

Reflected Reflector<TypedLogicalOperator<StatisticBuildLogicalOperator>>::operator()(
    const TypedLogicalOperator<StatisticBuildLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedStatisticBuildLogicalOperator{
        .operatorId = op.getId(),
        .windowType = op->getWindowType(),
        .statisticId = op->getStatisticId(),
        .sampleSize = op->getSampleSize(),
        .seed = op->getSeed(),
        .timestampField = op->getCharacteristic()});
}

Unreflector<TypedLogicalOperator<StatisticBuildLogicalOperator>>::Unreflector(ContextType plan) : plan(std::move(plan))
{
}

TypedLogicalOperator<StatisticBuildLogicalOperator> Unreflector<TypedLogicalOperator<StatisticBuildLogicalOperator>>::operator()(
    const Reflected& reflected, const ReflectionContext& context) const
{
    auto [id, windowType, statisticId, sampleSize, seed, timestampField]
        = context.unreflect<detail::ReflectedStatisticBuildLogicalOperator>(reflected);
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("StatisticBuildLogicalOperator must have exactly one child, but got {}", children.size());
    }
    return StatisticBuildLogicalOperator::create(children.at(0), statisticId, sampleSize, seed, windowType, timestampField);
}

}

std::size_t std::hash<NES::StatisticBuildLogicalOperator>::operator()(
    const NES::StatisticBuildLogicalOperator& statisticBuildLogicalOperator) const noexcept
{
    return folly::hash::hash_combine_generic(
        NES::Hash{},
        statisticBuildLogicalOperator.windowType,
        statisticBuildLogicalOperator.timestampField,
        statisticBuildLogicalOperator.statisticId.getRawValue(),
        statisticBuildLogicalOperator.sampleSize,
        statisticBuildLogicalOperator.seed);
}

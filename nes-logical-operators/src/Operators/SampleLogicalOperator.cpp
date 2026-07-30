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

#include <Operators/SampleLogicalOperator.hpp>

#include <array>
#include <cstdint>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Schema/Binder.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Traits/Trait.hpp>
#include <Util/Hash.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

SampleLogicalOperator::SampleLogicalOperator(
    WeakLogicalOperator self,
    Windowing::TimeCharacteristic timestampField,
    const uint64_t slotDurationMs,
    std::optional<std::string> strategy,
    std::optional<int32_t> maxRuntimeMs,
    std::optional<int64_t> maxTicks)
    : ManagedByOperator(std::move(self))
    , timestampField(std::move(timestampField))
    , slotDurationMs(slotDurationMs)
    , strategy(strategy.value_or(std::string(DEFAULT_STRATEGY)))
    , maxRuntimeMs(maxRuntimeMs)
    , maxTicks(maxTicks)
{
}

SampleLogicalOperator::SampleLogicalOperator(
    WeakLogicalOperator self,
    std::array<LogicalOperator, 2> children,
    Windowing::TimeCharacteristic timestampField,
    const uint64_t slotDurationMs,
    std::optional<std::string> strategy,
    std::optional<int32_t> maxRuntimeMs,
    std::optional<int64_t> maxTicks)
    : ManagedByOperator(std::move(self))
    , timestampField(std::move(timestampField))
    , slotDurationMs(slotDurationMs)
    , strategy(strategy.value_or(std::string(DEFAULT_STRATEGY)))
    , maxRuntimeMs(maxRuntimeMs)
    , maxTicks(maxTicks)
    , children{std::move(children[0]), std::move(children[1])}
{
    inferLocalSchema();
}

TypedLogicalOperator<SampleLogicalOperator> SampleLogicalOperator::create(
    Windowing::TimeCharacteristic timestampField,
    uint64_t slotDurationMs,
    std::optional<std::string> strategy,
    std::optional<int32_t> maxRuntimeMs,
    std::optional<int64_t> maxTicks)
{
    return TypedLogicalOperator<SampleLogicalOperator>{
        std::move(timestampField), slotDurationMs, std::move(strategy), maxRuntimeMs, maxTicks};
}

TypedLogicalOperator<SampleLogicalOperator> SampleLogicalOperator::create(
    std::array<LogicalOperator, 2> children,
    Windowing::TimeCharacteristic timestampField,
    uint64_t slotDurationMs,
    std::optional<std::string> strategy,
    std::optional<int32_t> maxRuntimeMs,
    std::optional<int64_t> maxTicks)
{
    return TypedLogicalOperator<SampleLogicalOperator>{
        std::move(children), std::move(timestampField), slotDurationMs, std::move(strategy), maxRuntimeMs, maxTicks};
}

std::string_view SampleLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string SampleLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    std::ostringstream timestampFieldStream;
    timestampFieldStream << Windowing::TimeCharacteristicWrapper{timestampField};
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "SAMPLE(opId: {}, timestampField: {}, slotDurationMs: {}, strategy: {}, maxRuntimeMs: {}, maxTicks: {}, traitSet: {})",
            id,
            timestampFieldStream.str(),
            slotDurationMs,
            strategy,
            maxRuntimeMs.value_or(-1),
            maxTicks.value_or(-1),
            traitSet.explain(verbosity));
    }
    return fmt::format("SAMPLE({}ms, {})", slotDurationMs, strategy);
}

bool SampleLogicalOperator::operator==(const SampleLogicalOperator& rhs) const
{
    return timestampField == rhs.timestampField && slotDurationMs == rhs.slotDurationMs && strategy == rhs.strategy
        && maxRuntimeMs == rhs.maxRuntimeMs && maxTicks == rhs.maxTicks && outputSchema == rhs.outputSchema
        && getTraitSet() == rhs.getTraitSet();
}

void SampleLogicalOperator::inferLocalSchema()
{
    PRECONDITION(children.size() == 2, "Sample operator requires exactly two children for schema inference, got {}", children.size());
    const auto inputSchema = children[0].getOutputSchema();
    timestampField = Windowing::TimeCharacteristicWrapper{std::move(timestampField)}.withInferredSchema(inputSchema);

    /// Explicitly forces the output schema to be nullable, regardless of the input schema.
    outputSchema = Schema<UnqualifiedUnboundField, Unordered>{
        unbind(inputSchema)
        | std::views::transform(
            [](const UnqualifiedUnboundField& field)
            {
                return UnqualifiedUnboundField{
                    field.getFullyQualifiedName(), DataType(field.getDataType().type, DataType::NULLABLE::IS_NULLABLE)};
            })};
}

SampleLogicalOperator SampleLogicalOperator::withInferredSchema() const
{
    PRECONDITION(children.size() == 2, "Sample operator requires exactly two children for schema inference, got {}", children.size());
    auto copy = *this;
    copy.children = {children[0].withInferredSchema(), children[1].withInferredSchema()};
    copy.inferLocalSchema();
    return copy;
}

TraitSet SampleLogicalOperator::getTraitSet() const
{
    return traitSet;
}

SampleLogicalOperator SampleLogicalOperator::withTraitSet(TraitSet newTraitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(newTraitSet);
    return copy;
}

SampleLogicalOperator SampleLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> newChildren) const
{
    PRECONDITION(
        newChildren.size() <= 2, "Sample operator accepts at most two children (data source, tick source), got {}", newChildren.size());
    auto copy = *this;
    copy.children = std::move(newChildren);
    return copy;
}

SampleLogicalOperator SampleLogicalOperator::withChildren(std::vector<LogicalOperator> newChildren) const
{
    PRECONDITION(newChildren.size() == 2, "Sample operator requires exactly two children, got {}", newChildren.size());
    auto copy = *this;
    copy.children = std::move(newChildren);
    copy.inferLocalSchema();
    return copy;
}

std::vector<LogicalOperator> SampleLogicalOperator::getChildren() const
{
    return children;
}

Schema<Field, Unordered> SampleLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

Windowing::TimeCharacteristic SampleLogicalOperator::getCharacteristic() const
{
    return timestampField;
}

uint64_t SampleLogicalOperator::getSlotDurationMs() const
{
    return slotDurationMs;
}

std::string SampleLogicalOperator::getStrategy() const
{
    return strategy;
}

std::optional<int32_t> SampleLogicalOperator::getMaxRuntimeMs() const
{
    return maxRuntimeMs;
}

std::optional<int64_t> SampleLogicalOperator::getMaxTicks() const
{
    return maxTicks;
}

Reflected Reflector<TypedLogicalOperator<SampleLogicalOperator>>::operator()(
    const TypedLogicalOperator<SampleLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedSampleLogicalOperator{
        .operatorId = op.getId(),
        .timestampField = op->timestampField,
        .slotDurationMs = op->slotDurationMs,
        .strategy = op->strategy,
        .maxRuntimeMs = op->maxRuntimeMs,
        .maxTicks = op->maxTicks});
}

Unreflector<TypedLogicalOperator<SampleLogicalOperator>>::Unreflector(ContextType operatorMapping) : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<SampleLogicalOperator>
Unreflector<TypedLogicalOperator<SampleLogicalOperator>>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [operatorId, timestampField, slotDurationMs, strategy, maxRuntimeMs, maxTicks]
        = context.unreflect<detail::ReflectedSampleLogicalOperator>(reflected);
    auto foundChildren = plan->getChildrenFor(operatorId, context);
    return SampleLogicalOperator::create(
        std::array{foundChildren.at(0), foundChildren.at(1)},
        std::move(timestampField),
        slotDurationMs,
        std::move(strategy),
        maxRuntimeMs,
        maxTicks);
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterSampleLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return ReflectionContext{}.unreflect<TypedLogicalOperator<SampleLogicalOperator>>(arguments.reflected);
    }

    PRECONDITION(false, "Operator is only built directly via parser or via reflection, not using the registry");
    std::unreachable();
}

}

std::size_t std::hash<NES::SampleLogicalOperator>::operator()(const NES::SampleLogicalOperator& sampleLogicalOperator) const noexcept
{
    return folly::hash::hash_combine_generic(
        NES::Hash{},
        sampleLogicalOperator.timestampField,
        sampleLogicalOperator.slotDurationMs,
        sampleLogicalOperator.strategy,
        sampleLogicalOperator.maxRuntimeMs,
        sampleLogicalOperator.maxTicks);
}

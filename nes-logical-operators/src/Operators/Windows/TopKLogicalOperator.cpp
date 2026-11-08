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

#include <Operators/Windows/TopKLogicalOperator.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Hash.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <Util/RowKind.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>

namespace NES
{

TopKLogicalOperator::TopKLogicalOperator(
    WeakLogicalOperator self,
    std::vector<LogicalFunction> partitionKeys,
    std::vector<SortKey> sortKeys,
    const uint64_t limit,
    Windowing::TimeBasedWindowType windowType,
    Windowing::TimeCharacteristic timeCharacteristic)
    : ManagedByOperator(std::move(self))
    , partitionKeys(std::move(partitionKeys))
    , sortKeys(std::move(sortKeys))
    , limit(limit)
    , windowType(std::move(windowType))
    , timestampField(std::move(timeCharacteristic))
{
}

TopKLogicalOperator::TopKLogicalOperator(
    WeakLogicalOperator self,
    LogicalOperator child,
    std::vector<LogicalFunction> partitionKeys,
    std::vector<SortKey> sortKeys,
    const uint64_t limit,
    Windowing::TimeBasedWindowType windowType,
    Windowing::TimeCharacteristic timeCharacteristic)
    : ManagedByOperator(std::move(self))
    , child(std::move(child))
    , partitionKeys(std::move(partitionKeys))
    , sortKeys(std::move(sortKeys))
    , limit(limit)
    , windowType(std::move(windowType))
    , timestampField(std::move(timeCharacteristic))
{
    inferLocalSchema();
}

TypedLogicalOperator<TopKLogicalOperator> TopKLogicalOperator::create(
    std::vector<LogicalFunction> partitionKeys,
    std::vector<SortKey> sortKeys,
    const uint64_t limit,
    Windowing::TimeBasedWindowType windowType,
    Windowing::TimeCharacteristic timeCharacteristic)
{
    return TypedLogicalOperator<TopKLogicalOperator>{
        std::move(partitionKeys), std::move(sortKeys), limit, std::move(windowType), std::move(timeCharacteristic)};
}

TypedLogicalOperator<TopKLogicalOperator> TopKLogicalOperator::create(
    LogicalOperator child,
    std::vector<LogicalFunction> partitionKeys,
    std::vector<SortKey> sortKeys,
    const uint64_t limit,
    Windowing::TimeBasedWindowType windowType,
    Windowing::TimeCharacteristic timeCharacteristic)
{
    return TypedLogicalOperator<TopKLogicalOperator>{
        std::move(child), std::move(partitionKeys), std::move(sortKeys), limit, std::move(windowType), std::move(timeCharacteristic)};
}

const std::vector<LogicalFunction>& TopKLogicalOperator::getPartitionKeys() const
{
    return partitionKeys;
}

const std::vector<TopKLogicalOperator::SortKey>& TopKLogicalOperator::getSortKeys() const
{
    return sortKeys;
}

uint64_t TopKLogicalOperator::getLimit() const
{
    return limit;
}

Windowing::TimeBasedWindowType TopKLogicalOperator::getWindowType() const
{
    return windowType;
}

Windowing::TimeCharacteristic TopKLogicalOperator::getCharacteristic() const
{
    return timestampField;
}

const UnqualifiedUnboundField& TopKLogicalOperator::getWindowStartField() const
{
    return startField;
}

const UnqualifiedUnboundField& TopKLogicalOperator::getWindowEndField() const
{
    return endField;
}

std::vector<UnqualifiedUnboundField> TopKLogicalOperator::getAddedFields() const
{
    if (not windowType.isUnbounded())
    {
        return {startField, endField};
    }
    return {endField, UnqualifiedUnboundField{rowKindIdentifier(), DataType{DataType::Type::UINT8, DataType::NULLABLE::NOT_NULLABLE}}};
}

void TopKLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    if (limit == 0)
    {
        throw InvalidQuerySyntax("ORDER BY ... LIMIT needs a limit of at least 1");
    }
    if (windowType.isSession())
    {
        throw UnsupportedQuery("ORDER BY ... LIMIT does not support session windows");
    }
    const auto inputSchema = child->getOutputSchema();
    if (isUpdatingStream(inputSchema))
    {
        throw UnsupportedQuery("ORDER BY ... LIMIT cannot consume an updating stream yet");
    }

    const auto inferField = [&inputSchema](const LogicalFunction& function)
    {
        auto inferred = function.withInferredDataType(inputSchema);
        if (not inferred.tryGetAs<FieldAccessLogicalFunction>().has_value())
        {
            throw InvalidQuerySyntax("ORDER BY ... LIMIT ... BY expects plain fields, got {}", inferred.explain(ExplainVerbosity::Short));
        }
        return inferred;
    };
    partitionKeys = partitionKeys | std::views::transform(inferField) | std::ranges::to<std::vector>();
    sortKeys = sortKeys
        | std::views::transform([&inferField](const SortKey& key) { return SortKey{.field = inferField(key.field), .descending = key.descending}; })
        | std::ranges::to<std::vector>();
    timestampField = Windowing::TimeCharacteristicWrapper{std::move(timestampField)}.withInferredSchema(inputSchema);

    std::vector<UnqualifiedUnboundField> outputFields = unbind(inputSchema) | std::ranges::to<std::vector>();
    std::ranges::copy(getAddedFields(), std::back_inserter(outputFields));
    auto outputSchemaOrCollisions = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
    if (not outputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "Found collisions in the output of ORDER BY ... LIMIT: {}",
            Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(outputSchemaOrCollisions.error()));
    }
    outputSchema = std::move(outputSchemaOrCollisions).value();
}

bool TopKLogicalOperator::operator==(const TopKLogicalOperator& rhs) const
{
    return partitionKeys == rhs.partitionKeys and sortKeys == rhs.sortKeys and limit == rhs.limit and windowType == rhs.windowType
        and timestampField == rhs.timestampField and outputSchema == rhs.outputSchema and traitSet == rhs.traitSet;
}

TopKLogicalOperator TopKLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet TopKLogicalOperator::getTraitSet() const
{
    return traitSet;
}

TopKLogicalOperator TopKLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for TopK, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

TopKLogicalOperator TopKLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for TopK, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    copy.inferLocalSchema();
    return copy;
}

std::vector<LogicalOperator> TopKLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator TopKLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

Schema<Field, Unordered> TopKLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

Schema<Field, Ordered> TopKLogicalOperator::getOrderedOutputSchema(const ChildOutputOrderProvider orderProvider) const
{
    INVARIANT(child.has_value(), "Child not set when trying to get ordered output schema");
    std::vector<UnqualifiedUnboundField> fields = orderProvider(child.value()) | RangeUnbinder{} | std::ranges::to<std::vector>();
    std::ranges::copy(getAddedFields(), std::back_inserter(fields));
    return fields | RangeBinder{self.lock()} | std::ranges::to<Schema<Field, Ordered>>();
}

std::string TopKLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    const auto sortKeysExplained = sortKeys
        | std::views::transform([verbosity](const SortKey& key)
                                { return fmt::format("{} {}", key.field.explain(verbosity), key.descending ? "DESC" : "ASC"); });
    const auto partitionKeysExplained
        = partitionKeys | std::views::transform([verbosity](const LogicalFunction& key) { return key.explain(verbosity); });
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "TOPK(opId: {}, order by: [{}], limit: {}, by: [{}], window type: {}, traitSet: {})",
            opId,
            fmt::join(sortKeysExplained, ", "),
            limit,
            fmt::join(partitionKeysExplained, ", "),
            windowType,
            traitSet.explain(verbosity));
    }
    return fmt::format("TOPK({} BY [{}] ORDER BY [{}])", limit, fmt::join(partitionKeysExplained, ", "), fmt::join(sortKeysExplained, ", "));
}

std::string_view TopKLogicalOperator::getName() const noexcept
{
    return NAME;
}

TopKLogicalOperator TopKLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

const detail::DynamicBase* TopKLogicalOperator::getDynamicBase() const
{
    return static_cast<const Reorderer*>(this);
}

Reflected Reflector<TypedLogicalOperator<TopKLogicalOperator>>::operator()(
    const TypedLogicalOperator<TopKLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedTopKLogicalOperator{
            .operatorId = op.getId(),
            .partitionKeys = op->getPartitionKeys(),
            .sortFields = op->getSortKeys() | std::views::transform([](const auto& key) { return key.field; }) | std::ranges::to<std::vector>(),
            .descending
            = op->getSortKeys() | std::views::transform([](const auto& key) { return key.descending; }) | std::ranges::to<std::vector>(),
            .limit = op->getLimit(),
            .windowType = op->getWindowType(),
            .timestampField = op->getCharacteristic()});
}

Unreflector<TypedLogicalOperator<TopKLogicalOperator>>::Unreflector(ContextType plan) : plan(std::move(plan))
{
}

TypedLogicalOperator<TopKLogicalOperator>
Unreflector<TypedLogicalOperator<TopKLogicalOperator>>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [id, partitionKeys, sortFields, descending, limit, windowType, timestampField]
        = context.unreflect<detail::ReflectedTopKLogicalOperator>(reflected);
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1 or sortFields.size() != descending.size())
    {
        throw CannotDeserialize("TopKLogicalOperator needs exactly one child and a direction per sort key");
    }
    std::vector<TopKLogicalOperator::SortKey> sortKeys;
    for (size_t i = 0; i < sortFields.size(); ++i)
    {
        sortKeys.push_back(TopKLogicalOperator::SortKey{.field = sortFields[i], .descending = descending[i]});
    }
    return TopKLogicalOperator::create(children.at(0), std::move(partitionKeys), std::move(sortKeys), limit, windowType, timestampField);
}

}

std::size_t std::hash<NES::TopKLogicalOperator>::operator()(const NES::TopKLogicalOperator& op) const noexcept
{
    auto hash = folly::hash::hash_combine_generic(NES::Hash{}, op.windowType, op.timestampField, op.limit);
    for (const auto& key : op.partitionKeys)
    {
        hash = folly::hash::hash_combine(hash, std::hash<NES::LogicalFunction>{}(key));
    }
    for (const auto& key : op.sortKeys)
    {
        hash = folly::hash::hash_combine(hash, std::hash<NES::LogicalFunction>{}(key.field), key.descending);
    }
    return hash;
}

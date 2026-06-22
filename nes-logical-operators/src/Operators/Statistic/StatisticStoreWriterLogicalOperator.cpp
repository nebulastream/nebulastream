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

#include <Operators/Statistic/StatisticStoreWriterLogicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <fmt/format.h>

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

StatisticStoreWriterLogicalOperator::StatisticStoreWriterLogicalOperator(
    WeakLogicalOperator self, uint64_t statisticId, std::string startFieldName, std::string endFieldName, std::string valueFieldName)
    : ManagedByOperator(std::move(self))
    , statisticId(statisticId)
    , startFieldName(std::move(startFieldName))
    , endFieldName(std::move(endFieldName))
    , valueFieldName(std::move(valueFieldName))
{
}

StatisticStoreWriterLogicalOperator::StatisticStoreWriterLogicalOperator(
    WeakLogicalOperator self,
    LogicalOperator child,
    uint64_t statisticId,
    std::string startFieldName,
    std::string endFieldName,
    std::string valueFieldName)
    : ManagedByOperator(std::move(self))
    , child(std::move(child))
    , statisticId(statisticId)
    , startFieldName(std::move(startFieldName))
    , endFieldName(std::move(endFieldName))
    , valueFieldName(std::move(valueFieldName))
{
    inferLocalSchema();
}

TypedLogicalOperator<StatisticStoreWriterLogicalOperator> StatisticStoreWriterLogicalOperator::create(
    uint64_t statisticId, std::string startFieldName, std::string endFieldName, std::string valueFieldName)
{
    return TypedLogicalOperator<StatisticStoreWriterLogicalOperator>{
        statisticId, std::move(startFieldName), std::move(endFieldName), std::move(valueFieldName)};
}

TypedLogicalOperator<StatisticStoreWriterLogicalOperator> StatisticStoreWriterLogicalOperator::create(
    LogicalOperator child, uint64_t statisticId, std::string startFieldName, std::string endFieldName, std::string valueFieldName)
{
    return TypedLogicalOperator<StatisticStoreWriterLogicalOperator>{
        std::move(child), statisticId, std::move(startFieldName), std::move(endFieldName), std::move(valueFieldName)};
}

std::string_view StatisticStoreWriterLogicalOperator::getName() const noexcept
{
    return NAME;
}

uint64_t StatisticStoreWriterLogicalOperator::getStatisticId() const
{
    return statisticId;
}

const std::string& StatisticStoreWriterLogicalOperator::getStartFieldName() const
{
    return startFieldName;
}

const std::string& StatisticStoreWriterLogicalOperator::getEndFieldName() const
{
    return endFieldName;
}

const std::string& StatisticStoreWriterLogicalOperator::getValueFieldName() const
{
    return valueFieldName;
}

bool StatisticStoreWriterLogicalOperator::operator==(const StatisticStoreWriterLogicalOperator& rhs) const
{
    return statisticId == rhs.statisticId && startFieldName == rhs.startFieldName && endFieldName == rhs.endFieldName
        && valueFieldName == rhs.valueFieldName && outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
}

std::string StatisticStoreWriterLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "STATISTIC_STORE_WRITER(opId: {}, statisticId: {}, start: {}, end: {}, value: {}, traitSet: {})",
            opId,
            statisticId,
            startFieldName,
            endFieldName,
            valueFieldName,
            traitSet.explain(verbosity));
    }
    return fmt::format("STATISTIC_STORE_WRITER(statisticId: {})", statisticId);
}

void StatisticStoreWriterLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    const auto inputSchema = child->getOutputSchema();
    outputSchema = unbind(inputSchema);
}

StatisticStoreWriterLogicalOperator StatisticStoreWriterLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

TraitSet StatisticStoreWriterLogicalOperator::getTraitSet() const
{
    return traitSet;
}

StatisticStoreWriterLogicalOperator StatisticStoreWriterLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

StatisticStoreWriterLogicalOperator StatisticStoreWriterLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for StatisticStoreWriter, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

StatisticStoreWriterLogicalOperator StatisticStoreWriterLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for StatisticStoreWriter, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> StatisticStoreWriterLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> StatisticStoreWriterLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator StatisticStoreWriterLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

Reflected Reflector<TypedLogicalOperator<StatisticStoreWriterLogicalOperator>>::operator()(
    const TypedLogicalOperator<StatisticStoreWriterLogicalOperator>& op) const
{
    return reflect(detail::ReflectedStatisticStoreWriterLogicalOperator{
        .operatorId = op.getId(),
        .statisticId = op->getStatisticId(),
        .startFieldName = op->getStartFieldName(),
        .endFieldName = op->getEndFieldName(),
        .valueFieldName = op->getValueFieldName()});
}

Unreflector<TypedLogicalOperator<StatisticStoreWriterLogicalOperator>>::Unreflector(ContextType operatorMapping)
    : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<StatisticStoreWriterLogicalOperator> Unreflector<TypedLogicalOperator<StatisticStoreWriterLogicalOperator>>::operator()(
    const Reflected& rfl, const ReflectionContext& context) const
{
    auto [id, statisticId, startFieldName, endFieldName, valueFieldName]
        = context.unreflect<detail::ReflectedStatisticStoreWriterLogicalOperator>(rfl);
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("StatisticStoreWriterLogicalOperator requires exactly one child, but got {}", children.size());
    }
    return StatisticStoreWriterLogicalOperator::create(
        children.at(0), statisticId, std::move(startFieldName), std::move(endFieldName), std::move(valueFieldName));
}
}

uint64_t std::hash<NES::StatisticStoreWriterLogicalOperator>::operator()(const NES::StatisticStoreWriterLogicalOperator& op) const noexcept
{
    return op.getStatisticId() ^ (std::hash<std::string>{}(op.getStartFieldName()) << 1) ^ (std::hash<std::string>{}(op.getEndFieldName()) << 2)
        ^ (std::hash<std::string>{}(op.getValueFieldName()) << 3);
}

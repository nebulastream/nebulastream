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

StatisticStoreReaderLogicalOperator::StatisticStoreReaderLogicalOperator(
    WeakLogicalOperator self,
    std::string statisticIdFieldName,
    std::string startFieldName,
    std::string endFieldName,
    std::string valueFieldName)
    : ManagedByOperator(std::move(self))
    , statisticIdFieldName(std::move(statisticIdFieldName))
    , startFieldName(std::move(startFieldName))
    , endFieldName(std::move(endFieldName))
    , valueFieldName(std::move(valueFieldName))
{
}

StatisticStoreReaderLogicalOperator::StatisticStoreReaderLogicalOperator(
    WeakLogicalOperator self,
    LogicalOperator child,
    std::string statisticIdFieldName,
    std::string startFieldName,
    std::string endFieldName,
    std::string valueFieldName)
    : ManagedByOperator(std::move(self))
    , child(std::move(child))
    , statisticIdFieldName(std::move(statisticIdFieldName))
    , startFieldName(std::move(startFieldName))
    , endFieldName(std::move(endFieldName))
    , valueFieldName(std::move(valueFieldName))
{
    inferLocalSchema();
}

TypedLogicalOperator<StatisticStoreReaderLogicalOperator> StatisticStoreReaderLogicalOperator::create(
    std::string statisticIdFieldName, std::string startFieldName, std::string endFieldName, std::string valueFieldName)
{
    return TypedLogicalOperator<StatisticStoreReaderLogicalOperator>{
        std::move(statisticIdFieldName), std::move(startFieldName), std::move(endFieldName), std::move(valueFieldName)};
}

TypedLogicalOperator<StatisticStoreReaderLogicalOperator> StatisticStoreReaderLogicalOperator::create(
    LogicalOperator child,
    std::string statisticIdFieldName,
    std::string startFieldName,
    std::string endFieldName,
    std::string valueFieldName)
{
    return TypedLogicalOperator<StatisticStoreReaderLogicalOperator>{
        std::move(child),
        std::move(statisticIdFieldName),
        std::move(startFieldName),
        std::move(endFieldName),
        std::move(valueFieldName)};
}

std::string_view StatisticStoreReaderLogicalOperator::getName() const noexcept
{
    return NAME;
}

const std::string& StatisticStoreReaderLogicalOperator::getStatisticIdFieldName() const
{
    return statisticIdFieldName;
}

const std::string& StatisticStoreReaderLogicalOperator::getStartFieldName() const
{
    return startFieldName;
}

const std::string& StatisticStoreReaderLogicalOperator::getEndFieldName() const
{
    return endFieldName;
}

const std::string& StatisticStoreReaderLogicalOperator::getValueFieldName() const
{
    return valueFieldName;
}

bool StatisticStoreReaderLogicalOperator::operator==(const StatisticStoreReaderLogicalOperator& rhs) const
{
    return statisticIdFieldName == rhs.statisticIdFieldName && startFieldName == rhs.startFieldName && endFieldName == rhs.endFieldName
        && valueFieldName == rhs.valueFieldName && outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
}

std::string StatisticStoreReaderLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "STATISTIC_STORE_READER(opId: {}, statisticIdField: {}, start: {}, end: {}, value: {}, traitSet: {})",
            opId,
            statisticIdFieldName,
            startFieldName,
            endFieldName,
            valueFieldName,
            traitSet.explain(verbosity));
    }
    return fmt::format("STATISTIC_STORE_READER(value: {})", valueFieldName);
}

void StatisticStoreReaderLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    const auto inputSchema = child->getOutputSchema();
    outputSchema = unbind(inputSchema);
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
    PRECONDITION(children.size() == 1, "Can only set exactly one child for StatisticStoreReader, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

StatisticStoreReaderLogicalOperator StatisticStoreReaderLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for StatisticStoreReader, got {}", children.size());
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
    const TypedLogicalOperator<StatisticStoreReaderLogicalOperator>& op) const
{
    return reflect(detail::ReflectedStatisticStoreReaderLogicalOperator{
        .operatorId = op.getId(),
        .statisticIdFieldName = op->getStatisticIdFieldName(),
        .startFieldName = op->getStartFieldName(),
        .endFieldName = op->getEndFieldName(),
        .valueFieldName = op->getValueFieldName()});
}

Unreflector<TypedLogicalOperator<StatisticStoreReaderLogicalOperator>>::Unreflector(ContextType operatorMapping)
    : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<StatisticStoreReaderLogicalOperator> Unreflector<TypedLogicalOperator<StatisticStoreReaderLogicalOperator>>::operator()(
    const Reflected& rfl, const ReflectionContext& context) const
{
    auto [id, statisticIdFieldName, startFieldName, endFieldName, valueFieldName]
        = context.unreflect<detail::ReflectedStatisticStoreReaderLogicalOperator>(rfl);
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("StatisticStoreReaderLogicalOperator requires exactly one child, but got {}", children.size());
    }
    return StatisticStoreReaderLogicalOperator::create(
        children.at(0),
        std::move(statisticIdFieldName),
        std::move(startFieldName),
        std::move(endFieldName),
        std::move(valueFieldName));
}
}

uint64_t std::hash<NES::StatisticStoreReaderLogicalOperator>::operator()(const NES::StatisticStoreReaderLogicalOperator& op) const noexcept
{
    return std::hash<std::string>{}(op.getStatisticIdFieldName()) ^ (std::hash<std::string>{}(op.getStartFieldName()) << 1)
        ^ (std::hash<std::string>{}(op.getEndFieldName()) << 2) ^ (std::hash<std::string>{}(op.getValueFieldName()) << 3);
}

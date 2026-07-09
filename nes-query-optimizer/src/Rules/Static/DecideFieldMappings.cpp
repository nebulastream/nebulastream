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

#include <Rules/Static/DecideFieldMappings.hpp>

#include <cstdint>
#include <optional>
#include <ranges>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Reprojecter.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Schema/Field.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
std::unordered_map<Field, QualifiedIdentifier> reprojecterMapping(
    const std::unordered_map<Field, std::unordered_set<Field>>& accessedFieldsForOutput, const Schema<Field, Unordered>& outputSchema)
{
    /// We built up a map of the output field names of the child operator, that this projection accesses
    /// And check if there is any non-trivial projection that projects to the same name that could lead to a read-write conflict inside this projection
    /// If so, we redirect the write to a new field name with the field mapping trait WITHOUT changing the operator itself.
    /// By itself, this phase does not change anything in the execution of the plan, the lowering phase has to use this trait and integrate the mapping.
    ///
    /// This phase could be improved by analyzing conflicts and representing the field mappings instead as a DAG of accesses.
    /// Although this might generalize less well for some operators, it could avoid some unnesseccary copies

    /// Map of field names accessed when calculating the output of a field mapped to the fields using them
    /// We use Identifier objects instead of Field since it would be a bit awkward searching for a field of the same name but produced by the child operator
    auto readsToWrites = std::unordered_map<Identifier, std::unordered_set<Field>>{};
    auto nonTrivialReads = std::unordered_map<Identifier, uint64_t>{};
    for (const auto& writeField : outputSchema)
    {
        if (accessedFieldsForOutput.contains(writeField))
        {
            const auto& accessedFieldsIter = accessedFieldsForOutput.find(writeField);
            PRECONDITION(
                accessedFieldsIter != accessedFieldsForOutput.end(),
                "Field mapping trait does not contain mapping for output schema field {}",
                writeField);
            for (const auto& access : accessedFieldsIter->second)
            {
                readsToWrites[access.getLastName()].insert(writeField);
                nonTrivialReads[access.getLastName()]++;
            }
        }
        else
        {
            readsToWrites[writeField.getLastName()].insert(writeField);
        }
    }

    std::unordered_map<Field, QualifiedIdentifier> mapping;
    for (const auto& writeAs : outputSchema)
    {
        auto writeFieldName = writeAs.getLastName();
        const auto readingFields = readsToWrites.find(writeFieldName);
        if (readingFields != readsToWrites.end())
        {
            const auto nonTrivialReadCount = nonTrivialReads[writeFieldName];
            /// A trivial passthrough (e.g. b → b) does not modify the field value, so even if other
            /// projections also read from the same field, there is no write-read conflict.
            const bool writeIsNonTrivial = accessedFieldsForOutput.contains(writeAs);
            /// Find writes to fields that are accessed not just in the projection writing to the field
            if ((writeIsNonTrivial && readingFields->second.contains(writeAs) && nonTrivialReadCount >= 2)
                || (!readingFields->second.contains(writeAs) && nonTrivialReadCount >= 1))
            {
                /// We just append a "new" to the identifier list. Since this is a separate element in the identifier list, we should not
                /// get into troubles with e.g. compound types, as this is just an extra qualifier for the field.
                static const auto NewIdentifier = Identifier::parse("new");
                const auto [_, success] = mapping.try_emplace(writeAs, QualifiedIdentifier::create(writeFieldName, NewIdentifier));
                PRECONDITION(success, "Projection to the same field twice");
                continue;
            }
        }
        const auto [_, success] = mapping.try_emplace(writeAs, QualifiedIdentifier::create(writeFieldName));
        PRECONDITION(success, "Projection to the same field twice");
    }
    return mapping;
}

std::unordered_map<Field, QualifiedIdentifier>
sourceDescriptorMapping(const TypedLogicalOperator<SourceDescriptorLogicalOperator>& logicalOperator)
{
    std::unordered_map<Field, QualifiedIdentifier> mapping;
    for (const auto& field : logicalOperator->getOutputSchema())
    {
        auto [_, success] = mapping.try_emplace(field, field.getLastName());
        PRECONDITION(success, "Duplicate field name in output schema");
    }
    return mapping;
}

std::unordered_map<Field, QualifiedIdentifier> sinkMapping()
{
    return {};
}

std::unordered_map<Field, QualifiedIdentifier>
defaultMapping(const LogicalOperator& logicalOperator, const std::vector<LogicalOperator>& children)
{
    const auto pairs
        = children
        | std::views::transform(
              [](const auto& child)
              /// Unfortunately at the moments the fields might have a pointer to a logical operator without the newly set traitset
              { return child->getOutputSchema() | std::views::transform([&child](const auto& field) { return std::pair{field, child}; }); })
        | std::views::join | std::views::transform([](const auto& pair) { return std::pair{pair.first.getLastName(), pair.second}; })
        | std::ranges::to<std::vector>();
    const auto inputFieldNamesToChildren = std::unordered_map(pairs.begin(), pairs.end());

    std::unordered_map<Field, QualifiedIdentifier> mapping;
    for (const auto& field : logicalOperator->getOutputSchema())
    {
        auto newChildIter = inputFieldNamesToChildren.find(field.getLastName());
        const auto fieldInChild = [&]
        {
            if (newChildIter != inputFieldNamesToChildren.end())
            {
                return newChildIter->second->getOutputSchema().getFieldByName(field.getLastName());
            }
            return std::optional<Field>{};
        }();
        /// If field name exists in child operators, choose the same mapping. If it is a new field name, keep it.
        /// If an operator redefines a field name, then propagating the physical name is not neccessary, but it doesn't break anything and keeps this code simple.
        if (fieldInChild.has_value())
        {
            auto childFieldMapOpt = newChildIter->second->getTraitSet().tryGet<FieldMappingTrait>();
            INVARIANT(childFieldMapOpt.has_value(), "Field mapping trait not set in field mapping recursion");
            /// Look up the child's mapping with the child's own field, not this operator's field. An outer join marks the unmatched
            /// side's output fields nullable, so this operator's field and the child's field share a name but differ in nullability —
            /// and UnqualifiedUnboundField equality includes the datatype, so keying on this operator's field would miss the entry.
            auto mappingInChildOpt = childFieldMapOpt.value()->getMapping(fieldInChild.value().unbound());
            INVARIANT(mappingInChildOpt.has_value(), "Field mapping trait does not contain mapping for field");
            auto [_, success] = mapping.try_emplace(field, mappingInChildOpt.value());
            PRECONDITION(success, "Duplicate field name in output schema");
        }
        else
        {
            auto [_, success] = mapping.try_emplace(field, field.getLastName());
            PRECONDITION(success, "Duplicate field name in output schema");
        }
    }
    return mapping;
}
}

LogicalOperator DecideFieldMappings::apply(const LogicalOperator& logicalOperator) const
{
    const auto children = logicalOperator->getChildren() | std::views::transform([this](const auto& child) { return apply(child); })
        | std::ranges::to<std::vector>();

    const auto mapping = [&]
    {
        if (const auto& reprojecter = logicalOperator.tryGetAs<Reprojecter>())
        {
            return reprojecterMapping((*reprojecter.value())->getAccessedFieldsForOutput(), logicalOperator.getOutputSchema());
        }
        if (const auto& sourceOp = logicalOperator.tryGetAs<SourceDescriptorLogicalOperator>())
        {
            return sourceDescriptorMapping(sourceOp.value());
        }
        if (logicalOperator.tryGetAs<SinkLogicalOperator>())
        {
            return sinkMapping();
        }
        return defaultMapping(logicalOperator, children);
    }();

    FieldMappingTrait fieldMappingTrait{
        mapping | std::views::transform([](const auto& pair) { return std::pair{pair.first.unbound(), pair.second}; })
        | std::ranges::to<std::unordered_map<UnqualifiedUnboundField, QualifiedIdentifier>>()};
    auto traitSet = logicalOperator->getTraitSet();
    const auto success = tryInsert(traitSet, std::move(fieldMappingTrait));
    /// If there is a good reason why we would want to run this multiple times we can also disable this check and replace the trait instance.
    PRECONDITION(success, "Field mapping trait already set");
    return logicalOperator.withTraitSet(std::move(traitSet)).withChildren(children);
}

LogicalPlan DecideFieldMappings::apply(const LogicalPlan& queryPlan) const
{
    PRECONDITION(std::ranges::size(queryPlan.getRootOperators()) == 1, "Currently only one root operator is supported");
    auto newRootOperator = apply(queryPlan.getRootOperators().at(0));
    return queryPlan.withRootOperators({newRootOperator});
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> DecideFieldMappings::needs() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

}

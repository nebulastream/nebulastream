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
#include <unordered_map>
#include <Phases/DecideFieldMappings.hpp>
#include "Functions/FieldAccessLogicalFunction.hpp"
#include "Identifiers/Identifier.hpp"
#include "Operators/ProjectionLogicalOperator.hpp"
#include "Operators/Sinks/SinkLogicalOperator.hpp"
#include "Operators/Sources/SourceDescriptorLogicalOperator.hpp"
#include "Traits/FieldMappingTrait.hpp"

namespace NES
{


LogicalOperator DecideFieldMappings::apply(const LogicalOperator& logicalOperator)
{
    const auto children = logicalOperator->getChildren() | std::views::transform([this](const auto& child) { return apply(child); })
        | std::ranges::to<std::vector>();


    const auto pairs
        = children
        | std::views::transform(
              [](const auto& child)
              /// Unfortunately at the moments the fields might have a pointer to a logical operator without the newly set traitset
              { return child->getOutputSchema() | std::views::transform([&child](const auto& field) { return std::pair{field, child}; }); })
        | std::views::join | std::views::transform([](const auto& pair) { return std::pair{pair.first.getLastName(), pair.second}; })
        | std::ranges::to<std::vector>();
    const auto inputFieldNamesToChildren = std::unordered_map(pairs.begin(), pairs.end());

    auto mapping = std::unordered_map<Field, IdentifierList>{};

    if (const auto reprojecterOpt = logicalOperator.tryGetAs<Reprojecter>())
    {
        /// We built up a map of the output field names of the child operator, that this projection accesses
        /// And check if there is any non-trivial projection that projects to the same name that could lead to a read-write conflict inside this projection
        /// If so, we redirect the write to a new field name with the field mapping trait WITHOUT changing the operator itself.
        /// By itself, this phase does not change anything in the execution of the plan, the lowering phase has to use this trait and integrate the mapping.
        ///
        /// This phase could be improved by analyzing conflicts and representing the field mappings instead as a DAG of accesses.
        /// Although this might generalize less well for some operators, it could avoid some unnesseccary copies
        const auto& reprojecter = reprojecterOpt.value();
        const auto accessedFieldsForOutput = (*reprojecter)->getAccessedFieldsForOutput();

        /// Map of field names accessed when calculating the output of a field mapped to the fields using them
        /// We use Identifier objects instead of Field since it would be a bit awkward searching for a field of the same name but produced by the child operator
        auto readsToWrites = std::unordered_map<Identifier, std::unordered_set<Field>>{};
        auto nonTrivialReads = std::unordered_map<Identifier, uint64_t>{};
        for (const auto& writeField : logicalOperator->getOutputSchema())
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

        for (const auto& writeAs : logicalOperator->getOutputSchema())
        {
            auto writeFieldName = writeAs.getLastName();
            const auto& readingFields = readsToWrites.at(writeFieldName);
            const auto nonTrivialReadCount = nonTrivialReads[writeFieldName];
            /// Find writes to fields that are accessed not just in the projection writing to the field
            if ((readingFields.contains(writeAs) && nonTrivialReadCount >= 2)
                || (!readingFields.contains(writeAs) && nonTrivialReadCount >= 1))
            {
                /// We just append a "new" to the identifier list. Since this is a separate element in the identifier list, we should not
                /// get into troubles with e.g. compound types, as this is just an extra qualifier for the field.
                static constexpr auto newIdentifier = Identifier::parse("new");
                const auto [_, success] = mapping.try_emplace(writeAs, IdentifierList::create(writeFieldName, newIdentifier));
                PRECONDITION(success, "Projection to the same field twice");
            }
            else
            {
                const auto [_, success] = mapping.try_emplace(writeAs, IdentifierList::create(writeFieldName));
                PRECONDITION(success, "Projection to the same field twice");
            }
        }
    }
    else if (const auto sourceOpOpt = logicalOperator.tryGetAs<SourceDescriptorLogicalOperator>())
    {
        for (const auto& field : logicalOperator->getOutputSchema())
        {
            auto [_, success] = mapping.try_emplace(field, field.getLastName());
            PRECONDITION(success, "Duplicate field name in output schema");
        }
    }
    else if (const auto sinkOpOpt = logicalOperator.tryGetAs<SinkLogicalOperator>())
    {
        /// If the child of a sink redefines a field name,
        /// we need to project it back to the original name,
        /// since otherwise the assigned name might get leaked to the outside world via the output formatter of the sink
        // auto childFieldMapOpt = sinkOpOpt.value()->getChild()->getTraitSet().tryGet<FieldMappingTrait>();
        // PRECONDITION(childFieldMapOpt.has_value(), "Field mapping trait not set in field mapping recursion");
        // const auto& chieldFieldMapping = childFieldMapOpt.value();
        //
        // /// We still iterate over the output schema and not the field mapping trait to "self-validate" the behavior of this function
        // /// First we find out whether we need to add an additional projection
        // std::vector<ProjectionLogicalOperator::UnboundProjection> projections{};
        // bool additionalProjection = false;
        // for (const auto& childOutputField : sinkOpOpt.value()->getChild()->getOutputSchema())
        // {
        //     const auto foundMapping = chieldFieldMapping.getMapping(childOutputField.unbound());
        //     INVARIANT(foundMapping.has_value(), "Field mapping trait was not set for child of sink");
        //     if (childOutputField.getFullyQualifiedName() != *foundMapping)
        //     {
        //         additionalProjection = true;
        //         projections.emplace_back(*std::ranges::rbegin(foundMapping.value()), FieldAccessLogicalFunction{childOutputField});
        //     }
        // }
        //
        // if (additionalProjection)
        // {
        //     const auto newProjection
        //         = TypedLogicalOperator<ProjectionLogicalOperator>{projections, ProjectionLogicalOperator::Asterisk{false}}.withChildren(
        //             {sinkOpOpt.value()->getChild()});
        //
        // }
    }
    else
    {
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
                auto mappingInChildOpt = childFieldMapOpt->getMapping(field.unbound());
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
    }

    FieldMappingTrait fieldMappingTrait{
        mapping | std::views::transform([](const auto& pair) { return std::pair{pair.first.unbound(), pair.second}; })
        | std::ranges::to<std::unordered_map<UnboundFieldBase<1>, IdentifierList>>()};
    auto traitSet = logicalOperator->getTraitSet();
    const auto success = tryInsert(traitSet, std::move(fieldMappingTrait));
    /// If there is a good reason why we would want to run this multiple times we can also disable this check and replace the trait instance.
    PRECONDITION(success, "Field mapping trait already set");
    return logicalOperator.withTraitSet(std::move(traitSet)).withChildren(children);
}

LogicalPlan DecideFieldMappings::apply(const LogicalPlan& queryPlan)
{
    PRECONDITION(std::ranges::size(queryPlan.getRootOperators()) == 1, "Currently only one root operator is supported");
    auto newRootOperator = apply(queryPlan.getRootOperators().at(0));
    return queryPlan.withRootOperators({newRootOperator});
}
}

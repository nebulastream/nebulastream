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
#include <../../include/Phases/DecideFieldMappings.hpp>
#include "Functions/FieldAccessLogicalFunction.hpp"
#include "Operators/ProjectionLogicalOperator.hpp"
#include "Operators/Sinks/SinkLogicalOperator.hpp"
#include "Operators/Sources/SourceDescriptorLogicalOperator.hpp"
#include "Traits/FieldMappingTrait.hpp"

namespace NES
{


LogicalOperator DecideFieldMappings::apply(const LogicalOperator& logicalOperator)
{
    const auto childrenMap = logicalOperator->getChildren()
        | std::views::transform([this](const auto& child) { return std::pair{child, apply(child)}; })
        | std::ranges::to<std::unordered_map>();

    auto mapping = std::unordered_map<Field, IdentifierList>{};

    if (const auto projectionOpt = logicalOperator.tryGetAs<ProjectionLogicalOperator>())
    {
        /// We built up a map of the output field names of the child operator, that this projection accesses
        /// And check if there is any non-trivial projection that projects to the same name that could lead to a read-write conflict inside this projection
        /// If so, we redirect the write to a new field name with the field mapping trait WITHOUT changing the operator itself.
        /// By itself, this phase does not change anything in the execution of the plan, the lowering phase has to use this trait and integrate the mapping.
        const auto& projectionOperator = projectionOpt.value();
        auto foundReads = std::unordered_map<Identifier, std::vector<LogicalFunction>>{};
        auto foundWrites = std::unordered_map<Field, LogicalFunction>{};
        for (const auto& [projectAs, projection] : projectionOperator->getProjections())
        {
            auto reads = BFSRange(projection)
                | std::views::filter([](const LogicalFunction& function)
                                     { return function.tryGet<FieldAccessLogicalFunction>().has_value(); })
                | std::views::transform([](const LogicalFunction& function)
                                        { return function.get<FieldAccessLogicalFunction>().getField(); });
            for (const auto& read : reads)
            {
                foundReads[read.getLastName()].push_back(projection);
            }
            const auto [_, success] = foundWrites.try_emplace(projectAs, projection);
            PRECONDITION(success, "Projecting to the same field twice");
        }

        for (const auto& [writeAs, projection] : foundWrites)
        {
            if (!projection.tryGet<FieldAccessLogicalFunction>().has_value() && foundReads.contains(writeAs.getLastName()))
            {
                /// We just append a "new" to the identifier list. Since this is a separate element in the identifier list, we should not
                /// get into troubles with e.g. compound types, as this is just an extra qualifier for the field.
                static constexpr auto newIdentifier = Identifier::parse("new");
                const auto [_, success] = mapping.try_emplace(writeAs, IdentifierList::create(writeAs.getLastName(), newIdentifier));
                PRECONDITION(success, "Projection to the same field twice");
            }
            else
            {
                const auto [_, success] = mapping.try_emplace(writeAs, IdentifierList::create(writeAs.getLastName()));
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
    }
    else
    {
        for (const auto& field : logicalOperator->getOutputSchema())
        {
            auto newChild = childrenMap.at(field.getProducedBy());
            /// If field name exists in child operators, choose the same mapping. If it is a new field name, keep it.
            /// If an operator redefines a field name, then propagating the physical name is not neccessary, but it doesn't break anything and keeps it simple.
            if (const auto fieldAtNewChildOpt = newChild.getOutputSchema().getFieldByName(field.getLastName()))
            {
                auto childFieldMapOpt = newChild->getTraitSet().tryGet<FieldMappingTrait>();
                PRECONDITION(childFieldMapOpt.has_value(), "Field mapping trait not set in field mapping recursion");
                auto mappingInChildOpt = childFieldMapOpt->getMapping(field);
                PRECONDITION(mappingInChildOpt.has_value(), "Field mapping trait does not contain mapping for field");
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

    FieldMappingTrait fieldMappingTrait{std::move(mapping)};
    auto traitSet = logicalOperator->getTraitSet();
    const auto success = tryInsert(traitSet, std::move(fieldMappingTrait));
    /// If there is a good reason why we would want to run this multiple times we can also disable this check and replace the trait instance.
    PRECONDITION(success, "Field mapping trait already set");
    return logicalOperator.withTraitSet(std::move(traitSet))
        .withChildren(childrenMap | std::views::transform([](const auto& pair) { return pair.second; }) | std::ranges::to<std::vector>());
}

LogicalPlan DecideFieldMappings::apply(const LogicalPlan& queryPlan)
{
    PRECONDITION(std::ranges::size(queryPlan.getRootOperators()) == 1, "Currently only one root operator is supported");
    auto newRootOperator = apply(queryPlan.getRootOperators().at(0));
    return queryPlan.withRootOperators({newRootOperator});
}
}

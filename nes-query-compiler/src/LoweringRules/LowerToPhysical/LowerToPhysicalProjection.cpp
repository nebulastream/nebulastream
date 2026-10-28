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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalProjection.hpp>

#include <memory>
#include <optional>
#include <ranges>
#include <utility>
#include <vector>


#include <DataTypes/UnboundField.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/SchemaFactory.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <MapPhysicalOperator.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalProjection::apply(LogicalOperator projectionLogicalOperator)
{
    auto projection = projectionLogicalOperator.getAs<ProjectionLogicalOperator>();
    const auto traitSet = projectionLogicalOperator.getTraitSet();
    const auto childTraitSet = projection->getChild().getTraitSet();

    const auto outputSchema = createPhysicalOutputSchema(traitSet);
    const auto inputSchema = createPhysicalOutputSchema(childTraitSet);

    const auto memoryLayoutTypeTrait = traitSet.get<MemoryLayoutTypeTrait>();
    const auto memoryLayoutType = memoryLayoutTypeTrait->memoryLayout;

    auto accessedFields = projection->getAccessedFields()
        | std::views::transform([](const auto& field) -> QualifiedIdentifier { return field.getFullyQualifiedName(); })
        | std::ranges::to<std::vector>();

    std::shared_ptr<PhysicalOperatorWrapper> child;
    std::shared_ptr<PhysicalOperatorWrapper> leaf;
    auto fieldMappingTrait = traitSet.get<FieldMappingTrait>();
    for (const auto& [fieldName, function] : projection->getProjections())
    {
        auto targetName = fieldMappingTrait->getMapping(unbind(fieldName));
        PRECONDITION(targetName.has_value(), "Projection name was not in field mapping");
        auto physicalFunction
            = QueryCompilation::FunctionProvider::lowerFunction(function, *projection->getChild()->getTraitSet().get<FieldMappingTrait>());
        auto physicalOperator = MapPhysicalOperator(std::move(targetName).value(), physicalFunction);

        child = std::make_shared<PhysicalOperatorWrapper>(
            physicalOperator,
            inputSchema,
            outputSchema,
            memoryLayoutType,
            memoryLayoutType,
            std::nullopt,
            std::nullopt,
            PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE,
            child == nullptr ? std::vector<std::shared_ptr<PhysicalOperatorWrapper>>{} : std::vector{child});

        if (leaf == nullptr)
        {
            leaf = child;
            leaf->setFieldsReadFromInput(std::move(accessedFields));
        }
    }
    INVARIANT(leaf != nullptr, "Projection without any projected fields cannot be lowered");
    return {.root = child, .leaves = {leaf}};
}
}

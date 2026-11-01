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


#include <LoweringRules/LowerToPhysical/LowerToPhysicalUnnest.hpp>

#include <memory>
#include <vector>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/UnnestLogicalOperator.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <UnnestPhysicalOperator.hpp>

namespace NES
{

/// UNNEST is an ordinary intermediate operator of its pipeline: one physical operator for one logical operator.
LoweringRuleResultSubgraph LowerToPhysicalUnnest::apply(LogicalOperator logicalOperator)
{
    const auto unnest = logicalOperator.getAs<UnnestLogicalOperator>();
    const auto traitSet = logicalOperator.getTraitSet();
    const auto childTraitSet = unnest->getChild().getTraitSet();

    /// The physical names of the array (in the child's mapping) and of the element (in this operator's).
    const auto inputField = unnest->getChild().getOutputSchema().getFieldByName(unnest->getInput());
    const auto outputField = unnest->getOutputSchema().getFieldByName(unnest->getOutput());
    PRECONDITION(inputField.has_value() && outputField.has_value(), "UNNEST fields missing from the schemas");
    const auto inputName = childTraitSet.get<FieldMappingTrait>()->getMapping(inputField->unbound());
    const auto outputName = traitSet.get<FieldMappingTrait>()->getMapping(outputField->unbound());
    PRECONDITION(inputName.has_value() && outputName.has_value(), "UNNEST fields missing from the field mappings");

    const auto memoryLayoutType = traitSet.get<MemoryLayoutTypeTrait>()->memoryLayout;
    const auto outputSchema = createPhysicalOutputSchema(traitSet);
    const auto inputSchema = createPhysicalOutputSchema(childTraitSet);

    auto physicalOperator = UnnestPhysicalOperator(inputName.value(), outputName.value());
    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        inputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    std::vector leaves(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leaves = {leaves}};
}

}

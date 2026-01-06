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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalIngestionTimeWatermarkAssigner.hpp>

#include <memory>

#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Watermark/IngestionTimeWatermarkAssignerPhysicalOperator.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include "Traits/FieldMappingTrait.hpp"
#include "Traits/FieldOrderingTrait.hpp"
#include "Util/SchemaFactory.hpp"

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalIngestionTimeWatermarkAssigner::apply(LogicalOperator logicalOperator)
{
    const auto assignOpOpt = logicalOperator.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>();
    PRECONDITION(assignOpOpt.has_value(), "Expected a IngestionTimeWatermarkAssigner");
    PRECONDITION(logicalOperator.getChildren().size() == 1, "Expected exactly one child for IngestionTimeWatermarkAssigner");

    const auto traitSet = logicalOperator.getTraitSet();

    const auto memoryLayoutTypeTrait = traitSet.tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value().memoryLayout;

    const auto outputFieldMappingOpt = traitSet.tryGet<FieldMappingTrait>();
    PRECONDITION(outputFieldMappingOpt.has_value(), "Expected FieldMappingTrait to be set");
    const auto& outputFieldMapping = outputFieldMappingOpt.value();

    const auto fieldOrderingOpt = traitSet.tryGet<FieldOrderingTrait>();
    PRECONDITION(fieldOrderingOpt.has_value(), "Expected a FieldOrderingTrait");
    const UnboundOrderedSchema outputSchema
        = createSchemaFromTraits(outputFieldMapping.getUnderlying(), fieldOrderingOpt->getOrderedFields());

    const auto inputFieldMappingOpt = assignOpOpt.value()->getChild()->getTraitSet().tryGet<FieldMappingTrait>();
    PRECONDITION(inputFieldMappingOpt.has_value(), "Expected FieldMappingTrait of child to be set");
    const auto& inputFieldMapping = inputFieldMappingOpt.value();

    const auto childFieldOrderingOpt = assignOpOpt.value()->getChild().getTraitSet().tryGet<FieldOrderingTrait>();
    PRECONDITION(childFieldOrderingOpt.has_value(), "Expected a FieldOrderingTrait on child");
    const UnboundOrderedSchema inputSchema
        = createSchemaFromTraits(inputFieldMapping.getUnderlying(), childFieldOrderingOpt->getOrderedFields());

    auto physicalOperator = IngestionTimeWatermarkAssignerPhysicalOperator(IngestionTimeFunction());
    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        inputSchema
        outputSchema,
        memoryLayoutType,
        memoryLayoutType);

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leafes(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leafes}};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterIngestionTimeWatermarkAssignerRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalIngestionTimeWatermarkAssigner>(argument.conf);
}
}

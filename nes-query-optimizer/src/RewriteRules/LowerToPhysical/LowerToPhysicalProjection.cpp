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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalProjection.hpp>

#include <cstddef>
#include <memory>
#include <optional>
#include <ranges>
#include <vector>


#include <Functions/FunctionProvider.hpp>
#include <InputFormatters/InputFormatterTupleBufferRefProvider.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <MapPhysicalOperator.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <ScanPhysicalOperator.hpp>
#include "Traits/FieldMappingTrait.hpp"
#include "Traits/FieldOrderingTrait.hpp"
#include "Util/SchemaFactory.hpp"

namespace
{
NES::ScanPhysicalOperator createScanOperator(
    const NES::TypedLogicalOperator<NES::ProjectionLogicalOperator>& projectionOp,
    const size_t bufferSize,
    const NES::UnboundOrderedSchema& inputSchema,
    NES::MemoryLayoutType memoryLayoutType)
{
    const auto sourceOperators
        = projectionOp.getChildren()
        | std::views::filter([](const auto& childOperator)
                             { return childOperator.template tryGetAs<NES::SourceDescriptorLogicalOperator>().has_value(); })
        | std::views::transform(
              [](const auto& sourceChildOperator)
              { return sourceChildOperator.template tryGetAs<NES::SourceDescriptorLogicalOperator>().value()->getSourceDescriptor(); })
        | std::ranges::to<std::vector>();
    PRECONDITION(sourceOperators.size() < 2, "We expect a projection to have at most one source operator as a child.");

    const auto memoryProvider = NES::LowerSchemaProvider::lowerSchema(bufferSize, inputSchema, memoryLayoutType);
    const auto inputFieldNames = inputSchema | std::views::transform([](const auto& field) { return field.getFullyQualifiedName(); })
        | std::ranges::to<std::vector>();
    if (sourceOperators.size() == 1)
    {
        const auto inputFormatterConfig = sourceOperators.front().getParserConfig();
        if (NES::toUpperCase(inputFormatterConfig.parserType) != "NATIVE")
        {
            return NES::ScanPhysicalOperator(
                provideInputFormatterTupleBufferRef(inputFormatterConfig, memoryProvider), std::move(inputFieldNames));
        }
    }
    return NES::ScanPhysicalOperator(memoryProvider, std::move(inputFieldNames));
}

}

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalProjection::apply(LogicalOperator projectionLogicalOperator)
{
    auto projection = projectionLogicalOperator.getAs<ProjectionLogicalOperator>();
    const auto traitSet = projectionLogicalOperator.getTraitSet();

    const auto outputFieldMappingOpt = traitSet.tryGet<FieldMappingTrait>();
    PRECONDITION(outputFieldMappingOpt.has_value(), "Expected FieldMappingTrait to be set");
    const auto& outputFieldMapping = outputFieldMappingOpt.value();

    const auto fieldOrderingOpt = projection->getTraitSet().tryGet<FieldOrderingTrait>();
    PRECONDITION(fieldOrderingOpt.has_value(), "Expected a FieldOrderingTrait");
    const UnboundOrderedSchema outputSchema
        = createSchemaFromTraits(outputFieldMapping.getUnderlying(), fieldOrderingOpt->getOrderedFields());

    const auto inputFieldMappingOpt = projection->getChild()->getTraitSet().tryGet<FieldMappingTrait>();
    PRECONDITION(inputFieldMappingOpt.has_value(), "Expected FieldMappingTrait of child to be set");
    const auto& inputFieldMapping = inputFieldMappingOpt.value();

    const auto childFieldOrderingOpt = projection->getChild().getTraitSet().tryGet<FieldOrderingTrait>();
    PRECONDITION(childFieldOrderingOpt.has_value(), "Expected a FieldOrderingTrait on child");
    const UnboundOrderedSchema inputSchema
        = createSchemaFromTraits(inputFieldMapping.getUnderlying(), childFieldOrderingOpt->getOrderedFields());

    const auto memoryLayoutTypeTrait = projectionLogicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value().memoryLayout;

    auto bufferSize = conf.pageSize.getValue();
    auto scan = createScanOperator(projection, bufferSize, inputSchema, memoryLayoutType);
    auto scanWrapper = std::make_shared<PhysicalOperatorWrapper>(
        scan,
        inputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        std::nullopt,
        std::nullopt,
        PhysicalOperatorWrapper::PipelineLocation::SCAN);

    auto child = scanWrapper;

    for (const auto& [fieldName, function] : projection->getProjections())
    {
        auto physicalFunction = QueryCompilation::FunctionProvider::lowerFunction(function);
        auto physicalOperator = MapPhysicalOperator(fieldName.getLastName(), physicalFunction);
        child = std::make_shared<PhysicalOperatorWrapper>(
            physicalOperator,
            inputSchema,
            outputSchema,
            memoryLayoutType,
            memoryLayoutType,
            std::nullopt,
            std::nullopt,
            PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE,
            std::vector{child});
    }

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    const std::vector leafs(projectionLogicalOperator.getChildren().size(), child);
    return {.root = child, .leafs = {scanWrapper}};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterProjectionRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalProjection>(argument.conf);
}
}

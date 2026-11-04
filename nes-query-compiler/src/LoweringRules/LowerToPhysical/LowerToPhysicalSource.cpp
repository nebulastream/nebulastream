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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalSource.hpp>

#include <memory>
#include <ranges>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/FieldOrderingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/SchemaFactory.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <ScanPhysicalOperator.hpp>
#include <SourceDescriptorPhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalSource::apply(LogicalOperator logicalOperator)
{
    const auto source = logicalOperator.getAs<SourceDescriptorLogicalOperator>();
    const auto traitSet = source->getTraitSet();

    const auto outputOriginIds = traitSet.get<OutputOriginIdsTrait>();

    PRECONDITION(outputOriginIds->size() > 0, "Source must have at least one output origin id");
    auto physicalOperator = SourceDescriptorPhysicalOperator(source->getSourceDescriptor(), (*outputOriginIds)[0]);

    const auto memoryLayoutTypeTrait = traitSet.get<MemoryLayoutTypeTrait>();
    const auto memoryLayoutType = memoryLayoutTypeTrait->memoryLayout;

    const auto outputFieldOrdering = traitSet.get<FieldOrderingTrait>();
    const auto outputFieldMapping = traitSet.get<FieldMappingTrait>();
    static auto networkString = Identifier::parse("network");
    for (const auto& [field, mappedTo] : outputFieldMapping->getUnderlying())
    {
        PRECONDITION(
            field.getFullyQualifiedName() == mappedTo || source->getSourceDescriptor().getSourceType() == networkString,
            "Field mapping is not allowed to rename source attributes");
    }
    const auto outputSchema = createSchemaFromTraits(outputFieldMapping->getUnderlying(), outputFieldOrdering->getOrderedFields());

    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        Schema<QualifiedUnboundField, Ordered>{},
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
    /// The process returns Arrow buffers, which do not leave this pipeline: everything after the scan reads the records.
    static const auto PythonProcessType = Identifier::parse("PythonProcess");
    if (source->getSourceDescriptor().getSourceType() == PythonProcessType)
    {
        auto scan = std::make_shared<PhysicalOperatorWrapper>(
            ScanPhysicalOperator(
                LowerSchemaProvider::lowerSchema(conf.operatorBufferSize, outputSchema, MemoryLayoutType::ARROW_LAYOUT),
                outputSchema | std::views::transform([](const auto& field) { return field.getFullyQualifiedName(); })
                    | std::ranges::to<std::vector>()),
            outputSchema,
            outputSchema,
            MemoryLayoutType::ARROW_LAYOUT,
            memoryLayoutType,
            std::nullopt,
            std::nullopt,
            PhysicalOperatorWrapper::PipelineLocation::SCAN,
            std::vector{wrapper});
        return {.root = scan, .leaves = {}};
    }
    return {.root = wrapper, .leaves = {}};
}
}

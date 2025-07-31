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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalSource.hpp>

#include <memory>
#include <optional>
#include <ranges>

#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <SourcePhysicalOperator.hpp>
#include "DataTypes/UnboundSchema.hpp"
#include "Traits/FieldMappingTrait.hpp"
#include "Traits/FieldOrderingTrait.hpp"
#include "Util/SchemaFactory.hpp"

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalSource::apply(LogicalOperator logicalOperator)
{
    const auto source = logicalOperator.getAs<SourceDescriptorLogicalOperator>();
    const auto traitSet = source->getTraitSet();

    const auto outputOriginIds = traitSet.get<OutputOriginIdsTrait>();
    auto physicalOperator = SourcePhysicalOperator(source->getSourceDescriptor(), outputOriginIds[0]);

    const auto memoryLayoutTypeTrait = traitSet.get<MemoryLayoutTypeTrait>();
    const auto memoryLayoutType = memoryLayoutTypeTrait.memoryLayout;

    const auto outputFieldOrdering = traitSet.get<FieldOrderingTrait>();
    const auto outputFieldMapping = traitSet.get<FieldMappingTrait>();

    for (const auto& [field, mappedTo] : outputFieldMapping.getUnderlying())
    {
        PRECONDITION(field.getFullyQualifiedName() == mappedTo, "Field mapping is not allowed to rename source attributes");
    }
    const UnboundOrderedSchema outputSchema = createSchemaFromTraits(outputFieldMapping.getUnderlying(), outputFieldOrdering.getOrderedFields());

    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator, outputSchema, memoryLayoutType, PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
    return {.root = wrapper, .leafs = {}};
}

RewriteRuleRegistryReturnType RewriteRuleGeneratedRegistrar::RegisterSourceRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalSource>(argument.conf);
}
}

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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalSink.hpp>

#include <memory>

#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <SinkPhysicalOperator.hpp>
#include "DataTypes/UnboundSchema.hpp"
#include "Functions/FieldAccessLogicalFunction.hpp"
#include "Functions/FunctionProvider.hpp"
#include "Functions/LogicalFunction.hpp"
#include "Traits/FieldMappingTrait.hpp"
#include "Traits/FieldOrderingTrait.hpp"
#include "Util/SchemaFactory.hpp"

#include "MapPhysicalOperator.hpp"

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalSink::apply(LogicalOperator logicalOperator)
{
    auto sink = logicalOperator.getAs<SinkLogicalOperator>();
    PRECONDITION(sink->getSinkDescriptor().has_value(), "Expected SinkLogicalOperator to have sink descriptor");

    const auto traitSet = sink->getTraitSet();

    const auto memoryLayoutTypeTrait = traitSet.get<MemoryLayoutTypeTrait>();
    const auto memoryLayoutType = memoryLayoutTypeTrait.memoryLayout;

    const auto inputFieldMapping = sink->getChild()->getTraitSet().get<FieldMappingTrait>();
    const auto childFieldOrdering = sink->getChild().getTraitSet().get<FieldOrderingTrait>();

    const UnboundOrderedSchema inputSchemaWithRedirections
        = createSchemaFromTraits(inputFieldMapping.getUnderlying(), childFieldOrdering.getOrderedFields());
    const auto inputSchema = UnboundOrderedSchema{childFieldOrdering.getOrderedFields()};
    const auto outputSchema = createPhysicalOutputSchema(traitSet);

    /// If in the input to the sink a field has benn renamed by the field mapping,
    /// we need to add a projection that renames it back so that it is aligned with the sinks output format
    /// We might be able to get rid of this if there would be a mapping in the output formatter
    std::vector<std::pair<Identifier, FieldAccessLogicalFunction>> fieldAccesses;
    bool requiresProjection = false;
    for (const auto& childOutputField : sink->getChild()->getOutputSchema())
    {
        const auto foundMapping = inputFieldMapping.getMapping(childOutputField.unbound());
        INVARIANT(foundMapping.has_value(), "Field mapping trait was not set for child of sink");
        if (childOutputField.getFullyQualifiedName() != *foundMapping)
        {
            requiresProjection = true;
            fieldAccesses.emplace_back(*std::ranges::rbegin(foundMapping.value()), FieldAccessLogicalFunction{childOutputField});
        }
    }


    const auto child = [&]
    {
        if (requiresProjection)
        {
            auto currentChild = [&]
            {
                auto physicalFunction = QueryCompilation::FunctionProvider::lowerFunction(std::ranges::begin(fieldAccesses)->second);
                auto physicalOperator = MapPhysicalOperator(std::ranges::begin(fieldAccesses)->first, physicalFunction);
                return std::make_shared<PhysicalOperatorWrapper>(
                    physicalOperator,
                    inputSchemaWithRedirections,
                    inputSchema,
                    memoryLayoutType,
                    memoryLayoutType,
                    PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
            }();

            for (const auto& [fieldName, function] : fieldAccesses | std::views::drop(1))
            {
                auto physicalFunction = QueryCompilation::FunctionProvider::lowerFunction(std::ranges::begin(fieldAccesses)->second);
                auto physicalOperator = MapPhysicalOperator(std::ranges::begin(fieldAccesses)->first, physicalFunction);
                currentChild = std::make_shared<PhysicalOperatorWrapper>(
                    physicalOperator,
                    inputSchemaWithRedirections,
                    inputSchema,
                    memoryLayoutType,
                    memoryLayoutType,
                    std::nullopt,
                    std::nullopt,
                    PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE,
                    std::vector{currentChild});
            }
            return std::optional{currentChild};
        }
        return std::optional<std::shared_ptr<PhysicalOperatorWrapper>>();
    }();


    auto physicalOperator = SinkPhysicalOperator(sink->getSinkDescriptor().value()); /// NOLINT(bugprone-unchecked-optional-access)
    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator, outputSchema, memoryLayoutType, PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    if (child.has_value())
    {
        wrapper->setChildren({child.value()});
    }

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leafes(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leafes}};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterSinkRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalSink>(argument.conf);
}
}

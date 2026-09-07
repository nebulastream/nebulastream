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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalAlignEagerLE.hpp>

#include <memory>
#include <vector>

#include <Align/AlignEagerLEDrivingPhysicalOperator.hpp>
#include <Align/AlignEagerLEOperatorHandler.hpp>
#include <Align/AlignEagerLESearchedPhysicalOperator.hpp>
#include <Align/AlignMergePhysicalOperator.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/AlignLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalAlignEagerLE::apply(LogicalOperator logicalOperator)
{
    auto align = logicalOperator.getAs<AlignLogicalOperator>();
    auto children = align->getBothChildren();
    const auto traitSet = logicalOperator.getTraitSet();
    const auto memoryLayoutType = traitSet.get<MemoryLayoutTypeTrait>()->memoryLayout;

    const auto drivingInputSchema = createPhysicalOutputSchema(children[0].getTraitSet());
    const auto searchedInputSchema = createPhysicalOutputSchema(children[1].getTraitSet());
    const auto outputSchema = createPhysicalOutputSchema(traitSet);

    const auto searchedStateLayout = LowerSchemaProvider::lowerSchema(conf.pageSize.getValue(), searchedInputSchema, memoryLayoutType);
    const auto outputBufferRef = LowerSchemaProvider::lowerSchema(conf.pageSize.getValue(), outputSchema, memoryLayoutType);

    const auto handlerId = getNextOperatorHandlerId();
    const auto handler = std::make_shared<AlignEagerLEOperatorHandler>(searchedStateLayout, searchedStateLayout->getBufferSize());

    auto drivingWrapper = std::make_shared<PhysicalOperatorWrapper>(
        AlignEagerLEDrivingPhysicalOperator(handlerId, searchedStateLayout, outputBufferRef),
        drivingInputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto searchedWrapper = std::make_shared<PhysicalOperatorWrapper>(
        AlignEagerLESearchedPhysicalOperator(handlerId, searchedStateLayout),
        searchedInputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto mergeWrapper = std::make_shared<PhysicalOperatorWrapper>(
        AlignMergePhysicalOperator(outputBufferRef),
        outputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        std::nullopt,
        std::nullopt,
        PhysicalOperatorWrapper::PipelineLocation::SCAN,
        std::vector{drivingWrapper, searchedWrapper});

    return {.root = mergeWrapper, .leaves = {drivingWrapper, searchedWrapper}};
}
}

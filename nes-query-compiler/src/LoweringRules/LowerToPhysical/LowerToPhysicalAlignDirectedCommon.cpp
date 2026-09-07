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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalAlignDirectedCommon.hpp>

#include <memory>
#include <variant>
#include <vector>

#include <Align/AlignDirectedDrivingPhysicalOperator.hpp>
#include <Align/AlignDirectedOperatorHandler.hpp>
#include <Align/AlignDirectedSearchedPhysicalOperator.hpp>
#include <Align/AlignMergePhysicalOperator.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/AlignLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph lowerDirectedAlign(LogicalOperator logicalOperator, const uint64_t pageSize)
{
    auto align = logicalOperator.getAs<AlignLogicalOperator>();
    auto children = align->getBothChildren();
    const auto traitSet = logicalOperator.getTraitSet();
    const auto memoryLayoutType = traitSet.get<MemoryLayoutTypeTrait>()->memoryLayout;

    const auto& drivingChild = children[0];
    const auto& searchedChild = children[1];

    const auto drivingInputSchema = createPhysicalOutputSchema(drivingChild.getTraitSet());
    const auto searchedInputSchema = createPhysicalOutputSchema(searchedChild.getTraitSet());
    const auto outputSchema = createPhysicalOutputSchema(traitSet);

    const auto boundTimestampFields = std::get<std::array<Windowing::BoundTimeCharacteristic, 2>>(align->getTimestampFields());
    auto drivingTimeFunction = TimeFunction::create(boundTimestampFields[0]);
    auto searchedTimeFunction = TimeFunction::create(boundTimestampFields[1]);

    const auto pendingDrivingLayout = std::make_shared<DefaultPagedVectorTupleLayout>(drivingInputSchema);
    const auto searchedLogLayout = std::make_shared<DefaultPagedVectorTupleLayout>(searchedInputSchema);

    const auto handlerId = getNextOperatorHandlerId();
    const auto handler = std::make_shared<AlignDirectedOperatorHandler>(pendingDrivingLayout, searchedLogLayout, pageSize);

    const auto outputBufferRef = LowerSchemaProvider::lowerSchema(pageSize, outputSchema, memoryLayoutType);

    const AlignDirectedDrivingPhysicalOperator drivingOperator{
        handlerId,
        drivingTimeFunction->clone(),
        searchedTimeFunction->clone(),
        pendingDrivingLayout,
        searchedLogLayout,
        outputBufferRef};
    const AlignDirectedSearchedPhysicalOperator searchedOperator{
        handlerId, std::move(drivingTimeFunction), std::move(searchedTimeFunction), pendingDrivingLayout, searchedLogLayout, outputBufferRef};

    auto drivingWrapper = std::make_shared<PhysicalOperatorWrapper>(
        drivingOperator,
        drivingInputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto searchedWrapper = std::make_shared<PhysicalOperatorWrapper>(
        searchedOperator,
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

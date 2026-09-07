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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalAlignNN.hpp>

#include <memory>
#include <utility>
#include <variant>
#include <vector>

#include <Align/AlignMergePhysicalOperator.hpp>
#include <Align/AlignNNAnchorPhysicalOperator.hpp>
#include <Align/AlignNNOperatorHandler.hpp>
#include <Align/AlignNNSearchedPhysicalOperator.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/AlignLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalAlignNN::apply(LogicalOperator logicalOperator)
{
    auto align = logicalOperator.getAs<AlignLogicalOperator>();
    auto children = align->getBothChildren();
    const auto traitSet = logicalOperator.getTraitSet();
    const auto memoryLayoutType = traitSet.get<MemoryLayoutTypeTrait>()->memoryLayout;

    const auto leftInputSchema = createPhysicalOutputSchema(children[0].getTraitSet());
    const auto rightInputSchema = createPhysicalOutputSchema(children[1].getTraitSet());
    const auto outputSchema = createPhysicalOutputSchema(traitSet);

    const auto boundTimestampFields = std::get<std::array<Windowing::BoundTimeCharacteristic, 2>>(align->getTimestampFields());
    auto leftTimeFunction = TimeFunction::create(boundTimestampFields[0]);
    auto rightTimeFunction = TimeFunction::create(boundTimestampFields[1]);

    const auto pendingLeftLayout = std::make_shared<DefaultPagedVectorTupleLayout>(leftInputSchema);
    const auto rightLogLayout = std::make_shared<DefaultPagedVectorTupleLayout>(rightInputSchema);

    const auto handlerId = getNextOperatorHandlerId();
    const auto handler = std::make_shared<AlignNNOperatorHandler>(pendingLeftLayout, rightLogLayout, conf.pageSize.getValue());

    const auto outputBufferRef = LowerSchemaProvider::lowerSchema(conf.pageSize.getValue(), outputSchema, memoryLayoutType);

    auto leftWrapper = std::make_shared<PhysicalOperatorWrapper>(
        AlignNNAnchorPhysicalOperator(
            handlerId, leftTimeFunction->clone(), rightTimeFunction->clone(), pendingLeftLayout, rightLogLayout, outputBufferRef),
        leftInputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto rightWrapper = std::make_shared<PhysicalOperatorWrapper>(
        AlignNNSearchedPhysicalOperator(
            handlerId, std::move(leftTimeFunction), std::move(rightTimeFunction), pendingLeftLayout, rightLogLayout, outputBufferRef),
        rightInputSchema,
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
        std::vector{leftWrapper, rightWrapper});

    return {.root = mergeWrapper, .leaves = {leftWrapper, rightWrapper}};
}
}

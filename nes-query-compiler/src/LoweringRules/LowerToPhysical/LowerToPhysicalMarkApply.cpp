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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalMarkApply.hpp>

#include <memory>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Functions/FunctionProvider.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/MarkApplyLogicalOperator.hpp>
#include <StreamTableJoin/StreamTableJoinOperatorHandler.hpp>
#include <StreamTableJoin/StreamTableJoinPhysicalOperator.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/SchemaFactory.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalMarkApply::apply(LogicalOperator logicalOperator)
{
    const auto apply = logicalOperator.getAs<MarkApplyLogicalOperator>();
    PRECONDITION(apply->getCorrelationFields().empty(), "Physical correlated mark apply is not implemented");
    const auto children = apply->getBothChildren();
    const auto memoryLayout = apply->getTraitSet().get<MemoryLayoutTypeTrait>()->memoryLayout;
    const auto outputSchema = createPhysicalOutputSchema(apply->getTraitSet());
    const auto inputSchema = createPhysicalOutputSchema(children[0].getTraitSet());
    const auto tableSchema = createPhysicalOutputSchema(children[1].getTraitSet());
    const auto outputOrigins = apply->getTraitSet().get<OutputOriginIdsTrait>();
    PRECONDITION(std::ranges::size(*outputOrigins) == 1, "Mark apply expects one output origin");
    const auto outputOrigin = (*outputOrigins)[0];

    const auto inputOrigins = *children[0].getTraitSet().get<OutputOriginIdsTrait>() | std::ranges::to<std::vector<OriginId>>();
    const auto tableOrigins = *children[1].getTraitSet().get<OutputOriginIdsTrait>() | std::ranges::to<std::vector<OriginId>>();
    PRECONDITION(!inputOrigins.empty() && !tableOrigins.empty(), "Mark apply inputs require origins");

    auto combinedMappings = apply->getChildren()
        | std::views::transform([](const auto& child)
                                { return child.getTraitSet().template get<FieldMappingTrait>()->getUnderlying() | std::views::all; })
        | std::views::join | std::views::common | std::ranges::to<std::unordered_map>();
    auto physicalComparison = QueryCompilation::FunctionProvider::lowerFunction(
        apply->getComparisonFunction(), FieldMappingTrait{std::move(combinedMappings)});

    auto inputTupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(inputSchema);
    auto tableTupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(tableSchema);
    auto inputBufferRef = LowerSchemaProvider::lowerSchema(conf.operatorBufferSize.getValue(), inputSchema, memoryLayout);
    auto tableBufferRef = LowerSchemaProvider::lowerSchema(conf.operatorBufferSize.getValue(), tableSchema, memoryLayout);
    const auto handlerId = getNextOperatorHandlerId();
    auto allOrigins = inputOrigins;
    allOrigins.insert(allOrigins.end(), tableOrigins.begin(), tableOrigins.end());
    auto handler = std::make_shared<StreamTableJoinOperatorHandler>(tableOrigins, std::move(allOrigins));

    auto inputWrapper = std::make_shared<PhysicalOperatorWrapper>(
        StreamTableJoinInputPhysicalOperator{handlerId, inputOrigins},
        inputSchema,
        inputSchema,
        memoryLayout,
        memoryLayout,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
    auto tableWrapper = std::make_shared<PhysicalOperatorWrapper>(
        StreamTableJoinInputPhysicalOperator{handlerId, tableOrigins},
        tableSchema,
        tableSchema,
        memoryLayout,
        memoryLayout,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    auto applyWrapper = std::make_shared<PhysicalOperatorWrapper>(
        StreamTableJoinPhysicalOperator{
            handlerId,
            StreamTableJoinPhysicalOperator::JoinType::MARK_APPLY,
            std::move(physicalComparison),
            std::move(inputBufferRef),
            std::move(tableBufferRef),
            std::move(inputTupleLayout),
            std::move(tableTupleLayout),
            outputOrigin,
            Record::RecordFieldIdentifier{apply->getMarkField()}},
        outputSchema,
        outputSchema,
        memoryLayout,
        memoryLayout,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::SCAN,
        std::vector{inputWrapper, tableWrapper});

    return {.root = applyWrapper, .leaves = {inputWrapper, tableWrapper}};
}

LoweringRuleRegistryReturnType
LoweringRuleGeneratedRegistrar::RegisterMarkApplyLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalMarkApply>(argument.conf);
}

}

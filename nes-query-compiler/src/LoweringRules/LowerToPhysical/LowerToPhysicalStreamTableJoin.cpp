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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalStreamTableJoin.hpp>

#include <array>
#include <memory>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Functions/FunctionProvider.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/StreamTableJoinLogicalOperator.hpp>
#include <StreamTableJoin/StreamTableJoinOperatorHandler.hpp>
#include <StreamTableJoin/StreamTableJoinPhysicalOperator.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/SchemaFactory.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalStreamTableJoin::apply(LogicalOperator logicalOperator)
{
    const auto join = logicalOperator.getAs<StreamTableJoinLogicalOperator>();
    const auto children = join->getBothChildren();
    const auto memoryLayout = join->getTraitSet().get<MemoryLayoutTypeTrait>()->memoryLayout;
    const auto outputSchema = createPhysicalOutputSchema(join->getTraitSet());
    const auto streamSchema = createPhysicalOutputSchema(children[0].getTraitSet());
    const auto tableSchema = createPhysicalOutputSchema(children[1].getTraitSet());
    const auto outputOrigins = join->getTraitSet().get<OutputOriginIdsTrait>();
    PRECONDITION(std::ranges::size(*outputOrigins) == 1, "Stream-table join expects one output origin");
    const auto outputOrigin = (*outputOrigins)[0];

    const auto streamOriginsTrait = children[0].getTraitSet().get<OutputOriginIdsTrait>();
    const auto streamOrigins = *streamOriginsTrait | std::ranges::to<std::vector<OriginId>>();
    const auto tableOriginsTrait = children[1].getTraitSet().get<OutputOriginIdsTrait>();
    const auto tableOrigins = *tableOriginsTrait | std::ranges::to<std::vector<OriginId>>();
    PRECONDITION(!streamOrigins.empty() && !tableOrigins.empty(), "Stream-table join inputs require origins");

    auto combinedMappings = join->getChildren()
        | std::views::transform([](const auto& child)
                                { return child.getTraitSet().template get<FieldMappingTrait>()->getUnderlying() | std::views::all; })
        | std::views::join | std::views::common | std::ranges::to<std::unordered_map>();
    auto physicalJoinFunction
        = QueryCompilation::FunctionProvider::lowerFunction(join->getJoinFunction(), FieldMappingTrait{std::move(combinedMappings)});

    std::unique_ptr<TimeFunction> streamTimeFunction;
    std::unique_ptr<TimeFunction> tableTimeFunction;
    if (const auto characteristics = join->getTimeCharacteristics(); characteristics.has_value())
    {
        using BoundCharacteristics = std::array<Windowing::BoundTimeCharacteristic, 2>;
        PRECONDITION(
            std::holds_alternative<BoundCharacteristics>(characteristics.value()), "Expected bound stream-table join time characteristics");
        const auto& bound = std::get<BoundCharacteristics>(characteristics.value());
        streamTimeFunction = TimeFunction::create(bound[0]);
        tableTimeFunction = TimeFunction::create(bound[1]);
    }

    auto streamTupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(streamSchema);
    auto tableTupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(tableSchema);
    auto streamInputBufferRef = LowerSchemaProvider::lowerSchema(conf.operatorBufferSize.getValue(), streamSchema, memoryLayout);
    auto tableInputBufferRef = LowerSchemaProvider::lowerSchema(conf.operatorBufferSize.getValue(), tableSchema, memoryLayout);
    const auto handlerId = getNextOperatorHandlerId();
    auto inputOrigins = streamOrigins;
    inputOrigins.insert(inputOrigins.end(), tableOrigins.begin(), tableOrigins.end());
    auto handler = std::make_shared<StreamTableJoinOperatorHandler>(tableOrigins, std::move(inputOrigins));

    auto streamWrapper = std::make_shared<PhysicalOperatorWrapper>(
        StreamTableJoinInputPhysicalOperator{handlerId, streamOrigins},
        streamSchema,
        streamSchema,
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

    auto joinWrapper = std::make_shared<PhysicalOperatorWrapper>(
        StreamTableJoinPhysicalOperator{
            handlerId,
            join->getJoinType(),
            std::move(physicalJoinFunction),
            std::move(streamInputBufferRef),
            std::move(tableInputBufferRef),
            std::move(streamTupleLayout),
            std::move(tableTupleLayout),
            outputOrigin,
            std::move(streamTimeFunction),
            std::move(tableTimeFunction)},
        outputSchema,
        outputSchema,
        memoryLayout,
        memoryLayout,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::SCAN,
        std::vector{streamWrapper, tableWrapper});

    return {.root = joinWrapper, .leaves = {streamWrapper, tableWrapper}};
}

LoweringRuleRegistryReturnType
LoweringRuleGeneratedRegistrar::RegisterStreamTableJoinLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalStreamTableJoin>(argument.conf);
}

}

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

#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Functions/CastToTypeLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <LoweringRules/LowerToPhysical/TableIndexLowering.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/StreamTableJoinLogicalOperator.hpp>
#include <Operators/Windows/JoinFunctionSplit.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <StreamTableJoin/StreamTableJoinOperatorHandler.hpp>
#include <StreamTableJoin/StreamTableJoinPhysicalOperator.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Overloaded.hpp>
#include <Util/SchemaFactory.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

namespace
{
constexpr uint64_t MIN_TABLE_INDEX_BUCKETS = uint64_t{1} << 16;
}

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
    const FieldMappingTrait combinedMapping{std::move(combinedMappings)};

    /// Equi-join keys become the key of a hash index on the table; the rest of the join function is checked per matching row.
    /// Without equi-join keys, every stream row is checked against every table row.
    const auto [equiKeys, residual] = splitJoinFunction(join->getJoinFunction(), children);
    std::optional<StreamTableJoinIndex> tableIndex;
    std::optional<PhysicalFunction> physicalJoinFunction;
    if (equiKeys.empty())
    {
        physicalJoinFunction = QueryCompilation::FunctionProvider::lowerFunction(join->getJoinFunction(), combinedMapping);
    }
    else
    {
        tableIndex = createTableIndex(
            lowerEquiKeys(
                equiKeys, *children[0].getTraitSet().get<FieldMappingTrait>(), *children[1].getTraitSet().get<FieldMappingTrait>()),
            tableSchema,
            /// One map per query holds the whole table, rather than one per worker and slice, so it can afford more buckets than the
            /// configured default.
            std::max(conf.numberOfPartitions, MIN_TABLE_INDEX_BUCKETS),
            conf);
        if (residual)
        {
            physicalJoinFunction = QueryCompilation::FunctionProvider::lowerFunction(*residual, combinedMapping);
        }
    }

    std::unique_ptr<TimeFunction> streamTimeFunction;
    std::unique_ptr<TimeFunction> tableTimeFunction;
    if (const auto characteristics = join->getTimeCharacteristics(); characteristics.has_value())
    {
        using BoundCharacteristics = std::array<Windowing::BoundTimeCharacteristic, 2>;
        PRECONDITION(
            std::holds_alternative<BoundCharacteristics>(characteristics.value()), "Expected bound stream-table join time characteristics");
        const auto& bound = std::get<BoundCharacteristics>(characteristics.value());
        const auto lowerTimeFunction = [](const Windowing::BoundTimeCharacteristic& characteristic, const FieldMappingTrait& mapping)
        {
            return std::visit(
                Overloaded{
                    [](const Windowing::IngestionTimeCharacteristic&) -> std::unique_ptr<TimeFunction>
                    { return std::make_unique<IngestionTimeFunction>(); },
                    [&](const Windowing::BoundEventTimeCharacteristic& eventTime) -> std::unique_ptr<TimeFunction>
                    {
                        return std::make_unique<EventTimeFunction>(
                            QueryCompilation::FunctionProvider::lowerFunction(eventTime.field, mapping), eventTime.unit);
                    }},
                characteristic);
        };
        streamTimeFunction = lowerTimeFunction(bound[0], *children[0].getTraitSet().get<FieldMappingTrait>());
        tableTimeFunction = lowerTimeFunction(bound[1], *children[1].getTraitSet().get<FieldMappingTrait>());
    }

    auto streamTupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(streamSchema);
    auto tableTupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(tableSchema);
    auto streamInputBufferRef = LowerSchemaProvider::lowerSchema(conf.operatorBufferSize, streamSchema, memoryLayout);
    auto tableInputBufferRef = LowerSchemaProvider::lowerSchema(conf.operatorBufferSize, tableSchema, memoryLayout);
    const auto handlerId = getNextOperatorHandlerId();
    auto inputOrigins = streamOrigins;
    inputOrigins.insert(inputOrigins.end(), tableOrigins.begin(), tableOrigins.end());
    auto pendingRows = createPendingRowsLayout(streamSchema);
    auto handler = std::make_shared<StreamTableJoinOperatorHandler>(
        tableOrigins,
        std::move(inputOrigins),
        createBTreeIndexFactory(pendingRows.entryLayout->getSizeInBytes()),
        tableIndex ? createHashMapIndexFactory(tableIndex->hashMapConfig) : StreamTableJoinOperatorHandler::TableIndexFactory{});


    auto streamWrapper = std::make_shared<PhysicalOperatorWrapper>(
        StreamTableJoinInputPhysicalOperator{},
        streamSchema,
        streamSchema,
        memoryLayout,
        memoryLayout,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    auto tableWrapper = std::make_shared<PhysicalOperatorWrapper>(
        StreamTableJoinInputPhysicalOperator{},
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
            std::move(physicalJoinFunction),
            std::move(tableIndex),
            std::move(pendingRows),
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

}

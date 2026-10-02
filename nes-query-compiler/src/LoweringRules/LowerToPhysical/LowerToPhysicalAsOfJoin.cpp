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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalAsOfJoin.hpp>

#include <array>
#include <memory>
#include <optional>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <AsOfJoin/AsOfJoinPhysicalOperator.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <LoweringRules/LowerToPhysical/TableIndexLowering.hpp>
#include <Operators/AsOfJoinLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Windows/JoinFunctionSplit.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <StreamTableJoin/StreamTableJoinOperatorHandler.hpp>
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

LoweringRuleResultSubgraph LowerToPhysicalAsOfJoin::apply(LogicalOperator logicalOperator)
{
    const auto join = logicalOperator.getAs<AsOfJoinLogicalOperator>();
    const auto children = join->getBothChildren();
    const auto memoryLayout = join->getTraitSet().get<MemoryLayoutTypeTrait>()->memoryLayout;
    const auto outputSchema = createPhysicalOutputSchema(join->getTraitSet());
    const auto leftSchema = createPhysicalOutputSchema(children[0].getTraitSet());
    const auto rightSchema = createPhysicalOutputSchema(children[1].getTraitSet());
    const auto outputOrigins = join->getTraitSet().get<OutputOriginIdsTrait>();
    PRECONDITION(std::ranges::size(*outputOrigins) == 1, "ASOF join expects one output origin");
    const auto outputOrigin = (*outputOrigins)[0];

    const auto leftOrigins = *children[0].getTraitSet().get<OutputOriginIdsTrait>() | std::ranges::to<std::vector<OriginId>>();
    const auto rightOrigins = *children[1].getTraitSet().get<OutputOriginIdsTrait>() | std::ranges::to<std::vector<OriginId>>();
    PRECONDITION(!leftOrigins.empty() && !rightOrigins.empty(), "ASOF join inputs require origins");

    /// The equality conjuncts become the key of the right index. A constant `true` (no ON clause) has no keys; every other predicate is
    /// rejected, because the predecessor must be the newest row among those with equal keys.
    const auto logicalJoinFunction = join->getJoinFunction();
    const auto constantJoinFunction = logicalJoinFunction.tryGetAs<ConstantValueLogicalFunction>();
    const bool predicateFree = constantJoinFunction && constantJoinFunction.value()->getConstantValue() == "true";
    std::vector<EquiJoinKey> equiKeys;
    if (!predicateFree)
    {
        auto split = splitJoinFunction(logicalJoinFunction, children);
        if (split.equiKeys.empty() || split.residual.has_value())
        {
            throw UnsupportedQuery("ASOF JOIN supports only equalities between a field of each input, or no predicate");
        }
        equiKeys = std::move(split.equiKeys);
    }

    using BoundCharacteristics = std::array<Windowing::BoundTimeCharacteristic, 2>;
    const auto characteristics = join->getTimeCharacteristics();
    PRECONDITION(std::holds_alternative<BoundCharacteristics>(characteristics), "Expected bound ASOF join time characteristics");
    const auto& bound = std::get<BoundCharacteristics>(characteristics);
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
    auto leftTimeFunction = lowerTimeFunction(bound[0], *children[0].getTraitSet().get<FieldMappingTrait>());
    auto rightTimeFunction = lowerTimeFunction(bound[1], *children[1].getTraitSet().get<FieldMappingTrait>());

    auto leftTupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(leftSchema);
    auto rightTupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(rightSchema);
    auto leftInputBufferRef = LowerSchemaProvider::lowerSchema(conf.operatorBufferSize, leftSchema, memoryLayout);
    auto rightInputBufferRef = LowerSchemaProvider::lowerSchema(conf.operatorBufferSize, rightSchema, memoryLayout);
    const auto handlerId = getNextOperatorHandlerId();
    auto inputOrigins = leftOrigins;
    inputOrigins.insert(inputOrigins.end(), rightOrigins.begin(), rightOrigins.end());

    auto keys
        = lowerEquiKeys(equiKeys, *children[0].getTraitSet().get<FieldMappingTrait>(), *children[1].getTraitSet().get<FieldMappingTrait>());
    const auto uint64Type = DataTypeProvider::provideDataType(DataType::Type::UINT64);
    const std::vector<QualifiedUnboundField> sortFields{
        QualifiedUnboundField{AsOfJoinPhysicalOperator::KEY_HASH_FIELD, uint64Type},
        QualifiedUnboundField{AsOfJoinPhysicalOperator::ROW_TIMESTAMP_FIELD, uint64Type},
        QualifiedUnboundField{AsOfJoinPhysicalOperator::ROW_NUMBER_FIELD, uint64Type}};
    auto entryFields = sortFields;
    entryFields.insert(entryFields.end(), keys.keyFields.begin(), keys.keyFields.end());
    for (const auto& field : rightSchema)
    {
        entryFields.push_back(field);
    }
    auto entryLayout = std::make_shared<DefaultBTreeTupleLayout>(Schema<QualifiedUnboundField, Ordered>{entryFields});
    auto pendingRows = createPendingRowsLayout(leftSchema);
    auto handler = std::make_shared<StreamTableJoinOperatorHandler>(
        rightOrigins,
        std::move(inputOrigins),
        createBTreeIndexFactory(pendingRows.entryLayout->getSizeInBytes()),
        createBTreeIndexFactory(entryLayout->getSizeInBytes()));
    AsOfJoinIndex index{
        .keyFields = std::move(keys.keyFields),
        .leftKeyFunctions = std::move(keys.streamKeyFunctions),
        .rightKeyFunctions = std::move(keys.tableKeyFunctions),
        .entryLayout = std::move(entryLayout),
        .sortKeyLayout = std::make_shared<DefaultBTreeTupleLayout>(Schema<QualifiedUnboundField, Ordered>{sortFields})};
    auto leftWrapper = std::make_shared<PhysicalOperatorWrapper>(
        AsOfJoinInputPhysicalOperator{},
        leftSchema,
        leftSchema,
        memoryLayout,
        memoryLayout,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
    auto rightWrapper = std::make_shared<PhysicalOperatorWrapper>(
        AsOfJoinInputPhysicalOperator{},
        rightSchema,
        rightSchema,
        memoryLayout,
        memoryLayout,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    const auto joinWrapper = std::make_shared<PhysicalOperatorWrapper>(
        AsOfJoinPhysicalOperator{
            handlerId,
            std::move(index),
            std::move(pendingRows),
            std::move(leftInputBufferRef),
            std::move(rightInputBufferRef),
            std::move(leftTupleLayout),
            std::move(rightTupleLayout),
            outputOrigin,
            std::move(leftTimeFunction),
            std::move(rightTimeFunction)},
        outputSchema,
        outputSchema,
        memoryLayout,
        memoryLayout,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::SCAN,
        std::vector{leftWrapper, rightWrapper});

    return {.root = joinWrapper, .leaves = {leftWrapper, rightWrapper}};
}

}

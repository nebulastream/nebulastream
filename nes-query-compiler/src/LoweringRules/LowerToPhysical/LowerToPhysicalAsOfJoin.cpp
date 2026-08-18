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
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/AsOfJoinLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <StreamTableJoin/StreamTableJoinOperatorHandler.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Overloaded.hpp>
#include <Util/SchemaFactory.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include "Operators/LogicalOperatorFwd.hpp"
#include "Identifiers/Identifiers.hpp"
#include "Interface/Record.hpp"
#include "WindowTypes/Measures/TimeCharacteristic.hpp"
#include "Runtime/Execution/OperatorHandler.hpp"

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

    auto combinedMappings = join->getChildren()
        | std::views::transform([](const auto& child)
                                { return child.getTraitSet().template get<FieldMappingTrait>()->getUnderlying() | std::views::all; })
        | std::views::join | std::views::common | std::ranges::to<std::unordered_map>();
    const auto logicalJoinFunction = join->getJoinFunction();
    const auto constantJoinFunction = logicalJoinFunction.tryGetAs<ConstantValueLogicalFunction>();
    const bool predicateFree = constantJoinFunction && constantJoinFunction.value()->getConstantValue() == "true";
    std::optional<Record::RecordFieldIdentifier> rightKeyField;
    if (!predicateFree)
    {
        const auto equality = logicalJoinFunction.tryGetAs<EqualsLogicalFunction>();
        if (!equality)
        {
            throw UnsupportedQuery("ASOF JOIN supports only direct field equality predicates or no predicate");
        }
        const auto equalityChildren = equality.value()->getChildren();
        const auto firstField = equalityChildren[0].tryGetAs<FieldAccessLogicalFunction>();
        const auto secondField = equalityChildren[1].tryGetAs<FieldAccessLogicalFunction>();
        if (!firstField || !secondField)
        {
            throw UnsupportedQuery("ASOF JOIN supports only direct field equality predicates or no predicate");
        }
        const auto rightLogicalField = [&]
        {
            if (firstField.value()->getField().getProducedBy() == children[1])
            {
                if (secondField.value()->getField().getProducedBy() != children[0])
                {
                    throw UnsupportedQuery("ASOF equality must compare one field from each input");
                }
                return firstField.value()->getField();
            }
            if (firstField.value()->getField().getProducedBy() != children[0]
                || secondField.value()->getField().getProducedBy() != children[1])
            {
                throw UnsupportedQuery("ASOF equality must compare one field from each input");
            }
            return secondField.value()->getField();
        }();
        rightKeyField = children[1].getTraitSet().get<FieldMappingTrait>()->getMapping(rightLogicalField.unbound());
        INVARIANT(rightKeyField.has_value(), "Missing physical mapping for ASOF right key {}", rightLogicalField);
    }
    auto physicalJoinFunction
        = QueryCompilation::FunctionProvider::lowerFunction(logicalJoinFunction, FieldMappingTrait{std::move(combinedMappings)});

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
    auto leftInputBufferRef = LowerSchemaProvider::lowerSchema(conf.operatorBufferSize.getValue(), leftSchema, memoryLayout);
    auto rightInputBufferRef = LowerSchemaProvider::lowerSchema(conf.operatorBufferSize.getValue(), rightSchema, memoryLayout);
    const auto handlerId = getNextOperatorHandlerId();
    auto inputOrigins = leftOrigins;
    inputOrigins.insert(inputOrigins.end(), rightOrigins.begin(), rightOrigins.end());
    auto handler = std::make_shared<StreamTableJoinOperatorHandler>(rightOrigins, std::move(inputOrigins));

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

    const auto makeJoinWrapper = [&](const auto& physicalJoin)
    {
        return std::make_shared<PhysicalOperatorWrapper>(
            std::move(physicalJoin),
            outputSchema,
            outputSchema,
            memoryLayout,
            memoryLayout,
            handlerId,
            handler,
            PhysicalOperatorWrapper::PipelineLocation::SCAN,
            std::vector{leftWrapper, rightWrapper});
    };
    std::shared_ptr<PhysicalOperatorWrapper> joinWrapper;
    if (predicateFree)
    {
        joinWrapper = makeJoinWrapper(PredicateFreeAsOfJoinPhysicalOperator{
            handlerId,
            std::move(physicalJoinFunction),
            std::move(leftInputBufferRef),
            std::move(rightInputBufferRef),
            std::move(leftTupleLayout),
            std::move(rightTupleLayout),
            outputOrigin,
            std::move(leftTimeFunction),
            std::move(rightTimeFunction),
            std::nullopt});
    }
    else
    {
        joinWrapper = makeJoinWrapper(EqualityAsOfJoinPhysicalOperator{
            handlerId,
            std::move(physicalJoinFunction),
            std::move(leftInputBufferRef),
            std::move(rightInputBufferRef),
            std::move(leftTupleLayout),
            std::move(rightTupleLayout),
            outputOrigin,
            std::move(leftTimeFunction),
            std::move(rightTimeFunction),
            std::move(rightKeyField)});
    }

    return {.root = joinWrapper, .leaves = {leftWrapper, rightWrapper}};
}

LoweringRuleRegistryReturnType
LoweringRuleGeneratedRegistrar::RegisterAsOfJoinLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalAsOfJoin>(argument.conf);
}

}

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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalStatisticStoreWriter.hpp>

#include <memory>
#include <utility>
#include <vector>

#include <Identifiers/StatisticIdentifiers.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <LoweringRules/LowerToPhysical/StatisticFieldResolution.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Statistic/StatisticStoreWriterLogicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <StatisticStore/AbstractStatisticStore.hpp>
#include <Statistics/StatisticStoreOperatorHandler.hpp>
#include <Statistics/StatisticStoreWriterPhysicalOperator.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>

#include <Operators/Statistic/StatisticFieldNames.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalStatisticStoreWriter::apply(LogicalOperator logicalOperator)
{
    const auto statisticStoreWriter = logicalOperator.getAs<StatisticStoreWriterLogicalOperator>();
    const auto traitSet = logicalOperator.getTraitSet();

    const auto memoryLayoutTypeTrait = traitSet.get<MemoryLayoutTypeTrait>();
    const auto memoryLayoutType = memoryLayoutTypeTrait->memoryLayout;

    const auto physicalInputSchema = createPhysicalOutputSchema(statisticStoreWriter->getChild()->getTraitSet());
    const auto physicalOutputSchema = createPhysicalOutputSchema(traitSet);

    const auto statisticId = statisticStoreWriter->getStatisticId();
    const auto fieldNames = statisticStoreWriter->getFieldNames();

    const auto payloadFieldName = resolvePhysicalFieldName(physicalInputSchema, fieldNames.inputStatisticData);
    const auto payloadField = physicalInputSchema[payloadFieldName];
    INVARIANT(payloadField.has_value(), "The statistic payload field must be part of the physical input schema");

    StatisticStoreWriterPhysicalOperator::FieldIdentifiers fieldIdentifiers{
        .inputStatisticStart = resolvePhysicalFieldName(physicalInputSchema, fieldNames.inputStatisticStart),
        .inputStatisticEnd = resolvePhysicalFieldName(physicalInputSchema, fieldNames.inputStatisticEnd),
        .inputStatisticData = payloadFieldName,
        .inputNumberOfSeenMeasurements = resolvePhysicalFieldName(physicalInputSchema, fieldNames.inputNumberOfSeenMeasurements),
        .outputStatisticId = resolvePhysicalFieldName(physicalOutputSchema, StatisticFieldNames::STATISTIC_ID),
        .outputStatisticStart = resolvePhysicalFieldName(physicalOutputSchema, StatisticFieldNames::START_TS),
        .outputStatisticEnd = resolvePhysicalFieldName(physicalOutputSchema, StatisticFieldNames::END_TS),
        .outputNumberOfSeenMeasurements = resolvePhysicalFieldName(physicalOutputSchema, StatisticFieldNames::NUMBER_OF_SEEN_MEASUREMENTS),
    };

    auto handlerId = getNextOperatorHandlerId();
    auto handler = std::make_shared<StatisticStoreOperatorHandler>(globalStatisticStore());
    StatisticStoreWriterPhysicalOperator physicalOperator{
        handlerId, statisticId, statisticStoreWriter->getTypeName(), std::move(fieldIdentifiers), payloadField.value().getDataType()};
    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(physicalOperator),
        physicalInputSchema,
        physicalOutputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    std::vector leaves{logicalOperator.getChildren().size(), wrapper};
    return {.root = wrapper, .leaves = {leaves}};
}

}

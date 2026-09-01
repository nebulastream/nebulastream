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
#include <vector>

#include <LoweringRules/AbstractLoweringRule.hpp>
#include <LoweringRules/LowerToPhysical/StatisticFieldResolution.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Operators/Statistic/StatisticStoreWriterLogicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Statistics/StatisticStoreOperatorHandler.hpp>
#include <Statistics/StatisticStoreWriterPhysicalOperator.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>

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
    const auto dataFieldName = statisticDataFieldName(statisticId);

    StatisticStoreWriterPhysicalOperator::FieldIdentifiers fieldIdentifiers{
        .inputStatisticStart = resolvePhysicalFieldName(physicalInputSchema, "STATISTICSTART"),
        .inputStatisticEnd = resolvePhysicalFieldName(physicalInputSchema, "STATISTICEND"),
        .inputStatisticData = resolvePhysicalFieldName(physicalInputSchema, dataFieldName),
        .inputNumberOfSeenMeasurements = resolvePhysicalFieldName(physicalInputSchema, "STATISTICNUMBEROFSEENMEASUREMENTS"),
        .outputStatisticId = resolvePhysicalFieldName(physicalOutputSchema, "statisticId"),
        .outputStatisticStart = resolvePhysicalFieldName(physicalOutputSchema, "statisticStart"),
        .outputStatisticEnd = resolvePhysicalFieldName(physicalOutputSchema, "statisticEnd"),
        .outputNumberOfSeenMeasurements = resolvePhysicalFieldName(physicalOutputSchema, "statisticNumberOfSeenMeasurements"),
    };

    auto handlerId = getNextOperatorHandlerId();
    auto handler = std::make_shared<StatisticStoreOperatorHandler>(statisticStore);

    auto physicalOperator
        = StatisticStoreWriterPhysicalOperator(handlerId, statisticId, statisticStoreWriter->getTypeName(), std::move(fieldIdentifiers));

    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(physicalOperator),
        physicalInputSchema,
        physicalOutputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    std::vector leaves(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leaves = {leaves}};
}

}

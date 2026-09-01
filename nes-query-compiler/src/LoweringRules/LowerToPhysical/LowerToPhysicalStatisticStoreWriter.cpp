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
#include <Operators/Statistic/StatisticStoreWriterLogicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Statistics/StatisticStoreOperatorHandler.hpp>
#include <Statistics/StatisticStoreWriterPhysicalOperator.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/SchemaFactory.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalStatisticStoreWriter::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(statisticStore != nullptr, "The statistic store must be available to lower a StatisticStoreWriter");
    const auto statisticStoreWriter = logicalOperator.getAs<StatisticStoreWriterLogicalOperator>();
    const auto traitSet = logicalOperator.getTraitSet();

    const auto memoryLayoutTypeTrait = traitSet.get<MemoryLayoutTypeTrait>();
    const auto memoryLayoutType = memoryLayoutTypeTrait->memoryLayout;

    const auto outputSchema = createPhysicalOutputSchema(traitSet);
    const auto inputSchema = createPhysicalOutputSchema(statisticStoreWriter->getChild().getTraitSet());

    StatisticStoreWriterPhysicalOperator::FieldIdentifiers fieldIdentifiers{
        .inputStatisticStart = resolvePhysicalFieldName(inputSchema, "statisticStart"),
        .inputStatisticEnd = resolvePhysicalFieldName(inputSchema, "statisticEnd"),
        .inputStatisticData = resolvePhysicalFieldName(inputSchema, "statisticData"),
        .inputNumberOfSeenTuples = resolvePhysicalFieldName(inputSchema, "statisticNumberOfSeenTuples"),
        .outputStatisticId = resolvePhysicalFieldName(outputSchema, "statisticId"),
        .outputStatisticStart = resolvePhysicalFieldName(outputSchema, "statisticStart"),
        .outputStatisticEnd = resolvePhysicalFieldName(outputSchema, "statisticEnd"),
        .outputNumberOfSeenTuples = resolvePhysicalFieldName(outputSchema, "statisticNumberOfSeenTuples")};

    const auto handlerId = getNextOperatorHandlerId();
    auto handler = std::make_shared<StatisticStoreOperatorHandler>(statisticStore);

    const StatisticStoreWriterPhysicalOperator physicalOperator{
        handlerId, statisticStoreWriter->getStatisticId(), statisticStoreWriter->getStatisticType(), fieldIdentifiers};
    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        inputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    std::vector leaves(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leaves = {leaves}};
}

}

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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalStatisticStoreReader.hpp>

#include <memory>
#include <string>
#include <vector>

#include <utility>
#include <Identifiers/StatisticIdentifiers.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <LoweringRules/LowerToPhysical/StatisticFieldResolution.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Statistic/StatisticStoreReaderLogicalOperator.hpp>
#include <StatisticStore/AbstractStatisticStore.hpp>
#include <Statistics/ScalarStatisticIterator.hpp>
#include <Statistics/StatisticIterator.hpp>
#include <Statistics/StatisticStoreOperatorHandler.hpp>
#include <Statistics/StatisticStoreReaderPhysicalOperator.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>

#include <Operators/Statistic/StatisticFieldNames.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalStatisticStoreReader::apply(LogicalOperator logicalOperator)
{
    const auto probe = logicalOperator.getAs<StatisticStoreReaderLogicalOperator>();
    const auto traitSet = logicalOperator.getTraitSet();

    const auto memoryLayoutTypeTrait = traitSet.get<MemoryLayoutTypeTrait>();
    const auto memoryLayoutType = memoryLayoutTypeTrait->memoryLayout;

    const auto outputSchema = createPhysicalOutputSchema(traitSet);
    const auto inputSchema = createPhysicalOutputSchema(probe->getChild().getTraitSet());

    auto handlerId = getNextOperatorHandlerId();
    auto handler = std::make_shared<StatisticStoreOperatorHandler>(globalStatisticStore());

    const StatisticStoreReaderPhysicalOperator::FieldIdentifiers fieldIdentifiers{
        .inputStatisticStart = resolvePhysicalFieldName(inputSchema, StatisticFieldNames::START_TS),
        .inputStatisticEnd = resolvePhysicalFieldName(inputSchema, StatisticFieldNames::END_TS),
        .outputStatisticId = resolvePhysicalFieldName(outputSchema, StatisticFieldNames::STATISTIC_ID),
        .outputStatisticStart = resolvePhysicalFieldName(outputSchema, StatisticFieldNames::START_TS),
        .outputStatisticEnd = resolvePhysicalFieldName(outputSchema, StatisticFieldNames::END_TS),
        .outputNumberOfSeenMeasurements = resolvePhysicalFieldName(outputSchema, StatisticFieldNames::NUMBER_OF_SEEN_MEASUREMENTS)};

    /// Only scalar statistics have a decoder so far: the payload is the single value the aggregation reduced its
    /// window to.
    const auto payloadFields = probe->getPayloadFields();
    if (payloadFields.size() != 1)
    {
        throw CannotInferSchema("A scalar statistic decodes to exactly one column, but {} were declared", payloadFields.size());
    }
    const auto& [payloadName, payloadType] = payloadFields.front();
    const auto statisticIterator = std::make_shared<ScalarStatisticIterator>(
        probe->getTypeName(), payloadType, resolvePhysicalFieldName(outputSchema, payloadName.getOriginalString()));

    const StatisticStoreReaderPhysicalOperator reader{
        handlerId, probe->getStatisticId(), fieldIdentifiers, statisticIterator, probe->getWindowMatch()};

    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        reader,
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

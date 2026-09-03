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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalScalarStatisticProbe.hpp>

#include <memory>
#include <string>
#include <vector>
#include <Identifiers/Identifier.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Statistic/ScalarStatisticProbeLogicalOperator.hpp>
#include <Statistics/StatisticStoreOperatorHandler.hpp>
#include <Statistics/StatisticStoreReader.hpp>
#include <Statistic/StatisticTypes.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalScalarStatisticProbe::apply(LogicalOperator logicalOperator)
{
    const auto probe = logicalOperator.getAs<ScalarStatisticProbeLogicalOperator>();
    const auto traitSet = logicalOperator.getTraitSet();

    const auto memoryLayoutTypeTrait = traitSet.get<MemoryLayoutTypeTrait>();
    const auto memoryLayoutType = memoryLayoutTypeTrait->memoryLayout;

    const auto outputSchema = createPhysicalOutputSchema(traitSet);
    const auto inputSchema = createPhysicalOutputSchema(probe->getChild().getTraitSet());

    /// The handler resolves the store from the process-global registry, so writer and reader converge on the same
    /// instance without either being threaded through the lowering machinery.
    auto handlerId = getNextOperatorHandlerId();
    auto handler = std::make_shared<StatisticStoreOperatorHandler>(statisticStore);

    const StatisticStoreReader reader{
        handlerId,
        probe->getStatisticId(),
        probe->getOp(),
        probe->getValueType(),
        probe->getStartTsFieldName(),
        probe->getEndTsFieldName(),
        Identifier::parse(std::string{StatisticFieldNames::STATISTIC_ID}),
        Identifier::parse(std::string{StatisticFieldNames::START_TS}),
        Identifier::parse(std::string{StatisticFieldNames::END_TS}),
        Identifier::parse(std::string{StatisticFieldNames::NUMBER_OF_SEEN_TUPLES}),
        Identifier::parse(std::string{StatisticFieldNames::VALUE})};

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

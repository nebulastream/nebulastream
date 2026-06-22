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

#include <Interface/Record.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Statistic/StatisticStoreReaderLogicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <fmt/format.h>

#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <Statistic/StatisticStore/StatisticStoreOperatorHandler.hpp>
#include <Statistic/StatisticStore/StatisticStoreReader.hpp>
#include <StatisticStore/AbstractStatisticStore.hpp>

namespace NES
{

namespace
{
/// Resolves an (unqualified) logical field name to the fully-qualified record field identifier in the schema.
/// Matches either the exact fully-qualified name or its trailing "$<name>" qualified suffix.
Record::RecordFieldIdentifier resolveField(const Schema<QualifiedUnboundField, Ordered>& schema, const std::string& name)
{
    for (const auto& field : schema)
    {
        const auto fullyQualified = field.getFullyQualifiedName();
        if (const auto fqnStr = fmt::format("{}", fullyQualified); fqnStr == name || fqnStr.ends_with("$" + name))
        {
            return fullyQualified;
        }
    }
    throw QueryCompilerError("StatisticStoreReader: field '{}' not found in input schema", name);
}
}

LoweringRuleResultSubgraph LowerToPhysicalStatisticStoreReader::apply(LogicalOperator logicalOperator)
{
    const auto reader = logicalOperator.getAs<StatisticStoreReaderLogicalOperator>();
    const auto traitSet = logicalOperator.getTraitSet();
    const auto memoryLayoutType = traitSet.get<MemoryLayoutTypeTrait>()->memoryLayout;

    const auto outputSchema = createPhysicalOutputSchema(traitSet);
    const auto inputSchema = createPhysicalOutputSchema(reader->getChild().getTraitSet());

    const auto handlerId = getNextOperatorHandlerId();
    auto handler = std::make_shared<StatisticStoreOperatorHandler>(statisticStore);

    auto physicalOperator = StatisticStoreReader(
        handlerId,
        resolveField(inputSchema, reader->getStatisticIdFieldName()),
        resolveField(inputSchema, reader->getStartFieldName()),
        resolveField(inputSchema, reader->getEndFieldName()),
        resolveField(inputSchema, reader->getValueFieldName()));

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
    return {.root = wrapper, .leafs = {leaves}};
}

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterStatisticStoreReaderLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalStatisticStoreReader>(argument.conf, argument.statisticStore);
}

}

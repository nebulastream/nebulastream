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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalDelta.hpp>

#include <cstddef>
#include <memory>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Operators/DeltaLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/PlanRenderer.hpp>
#include <DeltaOperatorHandler.hpp>
#include <DeltaPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalDelta::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<DeltaLogicalOperator>(), "Expected a DeltaLogicalOperator");
    const auto delta = logicalOperator.getAs<DeltaLogicalOperator>();

    /// Build physical delta expressions and the delta field layout for serialization.
    std::vector<PhysicalDeltaExpression> physicalExpressions;
    std::vector<DeltaFieldLayoutEntry> deltaFieldLayout;
    size_t deltaFieldsEntrySize = 0;
    for (const auto& [alias, function] : delta->getDeltaExpressions())
    {
        auto physicalFunction = QueryCompilation::FunctionProvider::lowerFunction(function);
        auto targetField = alias.getFieldName();
        auto dataType = function.getDataType();
        physicalExpressions.push_back({std::move(physicalFunction), targetField, dataType});
        deltaFieldLayout.push_back({targetField, dataType.type});
        deltaFieldsEntrySize += dataType.getSizeInBytes();
    }

    /// Build full record layout from the input schema for serializing pending first records.
    const auto inputSchema = logicalOperator.getInputSchemas()[0];
    std::vector<DeltaFieldLayoutEntry> fullRecordLayout;
    size_t fullRecordEntrySize = 0;
    for (const auto& field : inputSchema.getFields())
    {
        fullRecordLayout.push_back({field.name, field.dataType.type});
        fullRecordEntrySize += field.dataType.getSizeInBytes();
    }

    auto handlerId = getNextOperatorHandlerId();
    auto handler = std::make_shared<DeltaOperatorHandler>(deltaFieldsEntrySize, fullRecordEntrySize);

    auto physicalOperator = DeltaPhysicalOperator(
        std::move(physicalExpressions),
        handlerId,
        std::move(deltaFieldLayout),
        deltaFieldsEntrySize,
        std::move(fullRecordLayout),
        fullRecordEntrySize);

    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;
    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        inputSchema,
        logicalOperator.getOutputSchema(),
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leafes(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leafes}};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterDeltaRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalDelta>(argument.conf);
}
}

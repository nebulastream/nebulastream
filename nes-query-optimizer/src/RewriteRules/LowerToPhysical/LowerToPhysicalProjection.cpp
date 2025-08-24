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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalProjection.hpp>

#include <memory>
#include <optional>
#include <ranges>
#include <vector>
#include <Functions/FunctionProvider.hpp>

#include <MemoryLayout/ColumnLayout.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Util/PlanRenderer.hpp>

#include <EmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <MapPhysicalOperator.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <ScanPhysicalOperator.hpp>

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalProjection::apply(LogicalOperator projectionLogicalOperator)
{
    auto projection = projectionLogicalOperator.get<ProjectionLogicalOperator>();
    auto handlerId = getNextOperatorHandlerId();
    auto inputSchema = projectionLogicalOperator.getInputSchemas()[0];
    auto outputSchema = projectionLogicalOperator.getOutputSchema();
    auto bufferSize = conf.pageSize.getValue();

    if (conf.useSingleMemoryLayout)
    {
        bufferSize = conf.operatorBufferSize.getValue();

    }
    if (inputSchema.memoryLayoutType == Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
    {
        auto scanLayout = std::make_shared<Memory::MemoryLayouts::ColumnLayout>(bufferSize, inputSchema);
        auto scanMemoryProvider = std::make_shared<Interface::MemoryProvider::ColumnTupleBufferMemoryProvider>(scanLayout);
        auto scan = ScanPhysicalOperator(scanMemoryProvider, outputSchema.getFieldNames());
        auto scanWrapper = std::make_shared<PhysicalOperatorWrapper>(
            scan, outputSchema, outputSchema, std::nullopt, std::nullopt, PhysicalOperatorWrapper::PipelineLocation::SCAN);

        auto emitLayout = std::make_shared<Memory::MemoryLayouts::RowLayout>(bufferSize, outputSchema);
        auto emitMemoryProvider = std::make_shared<Interface::MemoryProvider::RowTupleBufferMemoryProvider>(emitLayout);
        auto emit = EmitPhysicalOperator(handlerId, emitMemoryProvider);
        auto emitWrapper = std::make_shared<PhysicalOperatorWrapper>(
            emit,
            outputSchema,
            outputSchema,
            handlerId,
            std::make_shared<EmitOperatorHandler>(),
            PhysicalOperatorWrapper::PipelineLocation::EMIT,
            std::vector{scanWrapper});

        /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
        const std::vector leafs(projectionLogicalOperator.getChildren().size(), scanWrapper);
        return {.root = emitWrapper, .leafs = {scanWrapper}};
    }
    if (inputSchema.memoryLayoutType != Schema::MemoryLayoutType::ROW_LAYOUT)
    {
        NES_ERROR("MemoryLayoutType: {} is not supported for Projection", magic_enum::enum_name(inputSchema.memoryLayoutType));
    }
    auto scanLayout = std::make_shared<Memory::MemoryLayouts::RowLayout>(bufferSize, inputSchema);
    auto scanMemoryProvider = std::make_shared<Interface::MemoryProvider::RowTupleBufferMemoryProvider>(scanLayout);
    auto accessedFields = projection.getAccessedFields();
    auto scan = ScanPhysicalOperator(scanMemoryProvider, accessedFields);
    auto scanWrapper = std::make_shared<PhysicalOperatorWrapper>(
        scan, outputSchema, outputSchema, std::nullopt, std::nullopt, PhysicalOperatorWrapper::PipelineLocation::SCAN);

    auto child = scanWrapper;
    for (const auto& [fieldName, function] : projection.getProjections())
    {
        auto physicalFunction = QueryCompilation::FunctionProvider::lowerFunction(function);
        auto physicalOperator = MapPhysicalOperator(
            fieldName.transform([](const auto& identifier) { return identifier.getFieldName(); })
                .value_or(function.explain(ExplainVerbosity::Short)),
            physicalFunction);
        child = std::make_shared<PhysicalOperatorWrapper>(
            physicalOperator,
            outputSchema,
            outputSchema,
            std::nullopt,
            std::nullopt,
            PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE,
            std::vector{child});
    }

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    const std::vector leafs(projectionLogicalOperator.getChildren().size(), child);
    return {.root = child, .leafs = {scanWrapper}};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterProjectionRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalProjection>(argument.conf);
}
}

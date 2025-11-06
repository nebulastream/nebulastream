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

#include <InputFormatters/FormatScanPhysicalOperator.hpp>
#include <Functions/FunctionProvider.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Util/PlanRenderer.hpp>
#include <MapPhysicalOperator.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include "InputFormatters/InputFormatterProvider.hpp"
#include "Operators/Sources/SourceDescriptorLogicalOperator.hpp"

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalProjection::apply(LogicalOperator projectionLogicalOperator)
{
    auto projection = projectionLogicalOperator.get<ProjectionLogicalOperator>();
    auto inputSchema = projectionLogicalOperator.getInputSchemas()[0];
    auto outputSchema = projectionLogicalOperator.getOutputSchema();
    auto bufferSize = conf.operatorBufferSize.getValue();
    // auto accessedFields = projection.getAccessedFields();
    // auto scan = FormatScanPhysicalOperator(outputSchema.getFieldNames(), std::move(inputFormatterTaskPipeline), bufferSize);
    // LogicalOperator
    const auto sourceOperators
        = projectionLogicalOperator.getChildren()
        | std::views::filter([](const auto& childOperator)
                             { return childOperator.template tryGet<SourceDescriptorLogicalOperator>().has_value(); })
        | std::views::transform(
              [](const auto& sourceChildOperator)
              { return sourceChildOperator.template tryGet<SourceDescriptorLogicalOperator>().value().getSourceDescriptor(); })
        | std::ranges::to<std::vector>();
    const bool isFirstOperatorAfterSource = not(sourceOperators.empty());
    auto inputFormatterTaskPipeline = [isFirstOperatorAfterSource, &inputSchema, &sourceOperators]()
    {
        if (isFirstOperatorAfterSource)
        {
            // Todo: how to handle multiple sources? --> should all have the same format, otherwise they should not have the same projection
            // -> could add invariant
            return InputFormatters::InputFormatterProvider::provideInputFormatterTask(
                OriginId(OriginId::INITIAL), inputSchema, sourceOperators.front().getParserConfig());
        }
        return InputFormatters::InputFormatterProvider::provideInputFormatterTask(
            OriginId(OriginId::INITIAL), inputSchema, ParserConfig{.parserType = "Native", .tupleDelimiter = "", .fieldDelimiter = ""});
    }();
    // Todo: problem:
    // - if we use 'outputSchema.getFieldNames()' we try to select input fields using output names (given renaming)

    auto accessedFields = projection.getAccessedFields();
    std::vector<std::string> requiredFields;
    auto scan = FormatScanPhysicalOperator(
        accessedFields, std::move(inputFormatterTaskPipeline), bufferSize, isFirstOperatorAfterSource, requiredFields);
    // Todo: output -> output was already here
    auto scanWrapper = std::make_shared<PhysicalOperatorWrapper>(
        scan, outputSchema, outputSchema, std::nullopt, std::nullopt, PhysicalOperatorWrapper::PipelineLocation::SCAN);

    auto child = scanWrapper;

    // Todo: use below approach
    // auto scanLayout = std::make_shared<Memory::MemoryLayouts::RowLayout>(bufferSize, inputSchema);
    // auto scanMemoryProvider = std::make_shared<Interface::MemoryProvider::RowTupleBufferMemoryProvider>(scanLayout);
    // auto accessedFields = projection.getAccessedFields();
    // auto scan = ScanPhysicalOperator(scanMemoryProvider, accessedFields);
    // auto scanWrapper = std::make_shared<PhysicalOperatorWrapper>(
    //     scan, outputSchema, outputSchema, std::nullopt, std::nullopt, PhysicalOperatorWrapper::PipelineLocation::SCAN);
    //
    // auto child = scanWrapper;
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

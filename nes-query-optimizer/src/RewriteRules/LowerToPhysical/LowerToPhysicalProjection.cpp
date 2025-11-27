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

#include <cstddef>
#include <memory>
#include <optional>
#include <ranges>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Functions/FunctionProvider.hpp>
#include <InputFormatters/InputFormatterTupleBufferRefProvider.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/BufferRef/RowTupleBufferRef.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <MapPhysicalOperator.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <ScanPhysicalOperator.hpp>

namespace
{
NES::ScanPhysicalOperator
createScanOperator(const NES::LogicalOperator& projectionOp, const size_t bufferSize, const NES::Schema& inputSchema)
{
    const auto sourceOperators
        = projectionOp.getChildren()
        | std::views::filter([](const auto& childOperator)
                             { return childOperator.template tryGetAs<NES::SourceDescriptorLogicalOperator>().has_value(); })
        | std::views::transform(
              [](const auto& sourceChildOperator)
              { return sourceChildOperator.template tryGetAs<NES::SourceDescriptorLogicalOperator>().value()->getSourceDescriptor(); })
        | std::ranges::to<std::vector>();
    PRECONDITION(sourceOperators.size() < 2, "We expect a projection to have at most one source operator as a child.");

    const auto memoryProvider = NES::TupleBufferRef::create(bufferSize, inputSchema);
    if (sourceOperators.size() == 1)
    {
        const auto inputFormatterConfig = sourceOperators.front().getParserConfig();
        if (NES::toUpperCase(inputFormatterConfig.parserType) != "NATIVE")
        {
            return NES::ScanPhysicalOperator(provideInputFormatterTupleBufferRef(inputFormatterConfig, memoryProvider));
        }
    }
    return NES::ScanPhysicalOperator(memoryProvider);
}

}

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalProjection::apply(LogicalOperator projectionLogicalOperator)
{
    auto projection = projectionLogicalOperator.getAs<ProjectionLogicalOperator>();
    auto inputSchema = projectionLogicalOperator.getInputSchemas()[0];
    auto outputSchema = projectionLogicalOperator.getOutputSchema();
    auto bufferSize = conf.pageSize.getValue();

    auto scan = createScanOperator(projectionLogicalOperator, bufferSize, inputSchema);
    auto scanWrapper = std::make_shared<PhysicalOperatorWrapper>(
        scan, outputSchema, outputSchema, std::nullopt, std::nullopt, PhysicalOperatorWrapper::PipelineLocation::SCAN);

    auto child = scanWrapper;

    for (const auto& [fieldName, function] : projection->getProjections())
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

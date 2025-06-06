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

#include <memory>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalSource.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ScanPhysicalOperator.hpp>
#include <SourcePhysicalOperator.hpp>
#include <Nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalSource::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGet<SourceDescriptorLogicalOperator>(), "Expected a SourceDescriptorLogicalOperator");
    const auto source = logicalOperator.get<SourceDescriptorLogicalOperator>();

    const auto outputOriginIds = source.getOutputOriginIds();
    PRECONDITION(
        outputOriginIds.size() == 1,
        "SourceDescriptorLogicalOperator should have exactly one origin id, but has {}",
        outputOriginIds.size());
    auto physicalOperator = SourcePhysicalOperator(source.getSourceDescriptor(), outputOriginIds[0]);

    const auto inputSchemas = logicalOperator.getInputSchemas();
    PRECONDITION(
        inputSchemas.size() == 1, "SourceDescriptorLogicalOperator should have exactly one schema, but has {}", inputSchemas.size());

    if (conf.useSingleMemoryLayout.getValue() && conf.memoryLayout.getValue() != conf.memoryLayout.getDefaultValue())
    {
        //add scan (row) and emit (col) behind source
        //schema = source.outputSchema

        auto schema = logicalOperator.getOutputSchema();
        auto rowSchema = *std::make_shared<Schema>(schema); //hopefully deep copy
        rowSchema.memoryLayoutType = Schema::MemoryLayoutType::ROW_LAYOUT;

        auto rowLayout = std::make_shared<Memory::MemoryLayouts::RowLayout>(
            conf.pageSize.getValue(), rowSchema);
        auto rowMemoryProvider = std::make_shared<Nautilus::Interface::MemoryProvider::RowTupleBufferMemoryProvider>(rowLayout);

        auto scan = ScanPhysicalOperator(rowMemoryProvider, schema.getFieldNames());
        auto scanWrapper = std::make_shared<PhysicalOperatorWrapper>(
            physicalOperator, rowSchema, logicalOperator.getOutputSchema(), PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);


        auto columnLayout = std::make_shared<Memory::MemoryLayouts::ColumnLayout>(
                   conf.pageSize.getValue(), schema);
        auto columnMemoryProvider = std::make_shared<Nautilus::Interface::MemoryProvider::ColumnTupleBufferMemoryProvider>(columnLayout);

        auto handlerId = getNextOperatorHandlerId();
        auto emit = EmitPhysicalOperator(handlerId, columnMemoryProvider);
        auto emitWrapper = std::make_shared<PhysicalOperatorWrapper>(
            emit, schema, schema, PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

        const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator, rowSchema, logicalOperator.getOutputSchema(), PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
        wrapper->addChild(emitWrapper);
        emitWrapper->addChild(scanWrapper);
        return {.root = wrapper, .leafs = {emitWrapper}};
    }

    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator, inputSchemas[0], logicalOperator.getOutputSchema(), PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
    return {.root = wrapper, .leafs = {}};
}

RewriteRuleRegistryReturnType RewriteRuleGeneratedRegistrar::RegisterSourceRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalSource>(argument.conf);
}
}

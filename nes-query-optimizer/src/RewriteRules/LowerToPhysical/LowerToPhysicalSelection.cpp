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
#include <Functions/FunctionProvider.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalSelection.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <SelectionPhysicalOperator.hpp>

#include <EmitPhysicalOperator.hpp>
#include <ScanPhysicalOperator.hpp>
#include <Nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>


namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalSelection::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGet<SelectionLogicalOperator>(), "Expected a SelectionLogicalOperator");
    auto selection = logicalOperator.get<SelectionLogicalOperator>();
    auto function = selection.getPredicate();
    auto func = QueryCompilation::FunctionProvider::lowerFunction(function);
    auto physicalOperator = SelectionPhysicalOperator(func);
    auto inputSchema = logicalOperator.getInputSchemas()[0];
    auto outputSchema = logicalOperator.getOutputSchema();
    if (conf.useSingleMemoryLayout.getValue() &&conf.memoryLayout.getValue() != conf.memoryLayout.getDefaultValue())
    {

        auto schema = *std::make_shared<Schema>(logicalOperator.getInputSchemas()[0]);
        auto colSchema = *std::make_shared<Schema>(schema); //hopefully deep copy
        colSchema.memoryLayoutType = Schema::MemoryLayoutType::COLUMNAR_LAYOUT;

        auto rowLayout = std::make_shared<Memory::MemoryLayouts::RowLayout>(
            conf.pageSize.getValue(), schema);
        auto rowMemoryProvider = std::make_shared<Nautilus::Interface::MemoryProvider::RowTupleBufferMemoryProvider>(rowLayout);
        auto handlerId = getNextOperatorHandlerId();
        auto scanRow = ScanPhysicalOperator(rowMemoryProvider, schema.getFieldNames());
        auto emitRow = EmitPhysicalOperator(handlerId, rowMemoryProvider);

        auto scanWrapperRow = std::make_shared<PhysicalOperatorWrapper>(
            scanRow, schema, schema, PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

        auto emitWrapperRow = std::make_shared<PhysicalOperatorWrapper>(
            emitRow, schema, schema, PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

        auto columnLayout = std::make_shared<Memory::MemoryLayouts::ColumnLayout>(
                   conf.pageSize.getValue(), schema);
        auto columnMemoryProvider = std::make_shared<Nautilus::Interface::MemoryProvider::ColumnTupleBufferMemoryProvider>(columnLayout);
        auto newHandlerId = getNextOperatorHandlerId();
        auto scanCol = ScanPhysicalOperator(columnMemoryProvider, colSchema.getFieldNames());
        auto emitCol = EmitPhysicalOperator(newHandlerId, columnMemoryProvider);



        auto scanWrapperCol = std::make_shared<PhysicalOperatorWrapper>(
            scanCol, colSchema, colSchema, PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
        auto emitWrapperCol = std::make_shared<PhysicalOperatorWrapper>(
            emitCol, colSchema, colSchema, PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);


        inputSchema.memoryLayoutType = conf.memoryLayout.getValue();///TODO: do for all operators besides sink and source
        outputSchema.memoryLayoutType = conf.memoryLayout.getValue();

        auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        inputSchema,
        outputSchema,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);


        emitWrapperRow->addChild(scanWrapperCol);
        scanWrapperCol->addChild(wrapper);
        wrapper->addChild(emitWrapperCol);
        emitWrapperCol->addChild(scanWrapperRow);

        // scanWrapperRow->addChild(emitWrapperCol);
        // emitWrapperCol->addChild(wrapper);
        // wrapper->addChild(scanWrapperCol);
        // scanWrapperCol->addChild(emitWrapperRow);
        return {.root = emitWrapperRow, .leafs = {scanWrapperRow}};

    }

    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        logicalOperator.getInputSchemas()[0],
        logicalOperator.getOutputSchema(),
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leafes(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leafes}};
};

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterSelectionRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalSelection>(argument.conf);
}
}

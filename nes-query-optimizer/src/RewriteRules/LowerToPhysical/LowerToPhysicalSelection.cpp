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
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>

#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>

#include <EmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ScanPhysicalOperator.hpp>


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




    /// if function left, right is field access, then use fieldname for outputschemas and swap after selection
    if (conf.useSingleMemoryLayout.getValue())
    {


        auto fieldNames = std::vector<std::string>(); ///get used filedNames in selection

        auto schemaIn = inputSchema;
        schemaIn.memoryLayoutType = conf.memoryLayout.getDefaultValue();
        auto schemaOut = outputSchema;
        schemaOut.memoryLayoutType = conf.memoryLayout.getDefaultValue();

        if (logicalOperator.getChildren()[0].tryGet<SourceDescriptorLogicalOperator>() || logicalOperator.getChildren()[0].tryGet<SourceNameLogicalOperator>())
        {
            /// input schemas until scanSelectionWrapper with full fields
            schemaIn = logicalOperator.getChildren()[0].getOutputSchema(); ///source still has all fields
            schemaIn.memoryLayoutType = conf.memoryLayout.getDefaultValue();
        }
        else
        {
            /// all schemas remain with truncated fields
        }

        auto memoryProviderInRow = std::make_shared<Nautilus::Interface::MemoryProvider::RowTupleBufferMemoryProvider>(
            std::make_shared<Memory::MemoryLayouts::RowLayout>(conf.pageSize.getValue(), schemaIn));

        auto schemaInCol= *std::make_shared<Schema>(schemaIn); //hopefully deep copy
        schemaInCol.memoryLayoutType = conf.memoryLayout.getValue();
        auto memoryProviderInCol = std::make_shared<Nautilus::Interface::MemoryProvider::ColumnTupleBufferMemoryProvider>(
            std::make_shared<Memory::MemoryLayouts::ColumnLayout>(conf.pageSize.getValue(), schemaInCol));


        //auto memoryProviderOutRow = std::make_shared<Nautilus::Interface::MemoryProvider::RowTupleBufferMemoryProvider>(
           // std::make_shared<Memory::MemoryLayouts::RowLayout>(conf.pageSize.getValue(), schemaOut));

        auto schemaOutCol = *std::make_shared<Schema>(schemaOut); //hopefully deep copy
        schemaOutCol.memoryLayoutType = conf.memoryLayout.getValue();
       // auto memoryProviderOutCol = std::make_shared<Nautilus::Interface::MemoryProvider::ColumnTupleBufferMemoryProvider>(
         //   std::make_shared<Memory::MemoryLayouts::ColumnLayout>(conf.pageSize.getValue(), schemaOutCol));


        auto scanSelection = ScanPhysicalOperator(
            memoryProviderInCol,
            schemaOutCol.getFieldNames()); ///have all fields in memProvider but only used ones in projections
        auto handlerId = getNextOperatorHandlerId();
        auto emitSelection = EmitPhysicalOperator(handlerId, memoryProviderInCol);

        auto scanSelectionWrapper = std::make_shared<PhysicalOperatorWrapper>(
            scanSelection, schemaInCol, schemaInCol, PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE); ///TODO: outputSchema schemaOutCol?

        auto emitSelectionWrapper = std::make_shared<PhysicalOperatorWrapper>(
            emitSelection, schemaOutCol, schemaOutCol, handlerId, std::make_shared<EmitOperatorHandler>(),
            PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);


        handlerId = getNextOperatorHandlerId();
        auto scanRow = ScanPhysicalOperator(memoryProviderInRow, schemaIn.getFieldNames());
        auto emitRow = EmitPhysicalOperator(handlerId, memoryProviderInRow);

        auto scanWrapperRow = std::make_shared<PhysicalOperatorWrapper>(
            scanRow, schemaIn, schemaIn, PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

        auto emitWrapperRow = std::make_shared<PhysicalOperatorWrapper>(
            emitRow, schemaOut, schemaOut, handlerId, std::make_shared<EmitOperatorHandler>(), PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);



        handlerId = getNextOperatorHandlerId();
        auto scanCol = ScanPhysicalOperator(memoryProviderInCol, schemaOutCol.getFieldNames());
        auto emitCol = EmitPhysicalOperator(handlerId, memoryProviderInCol);

        auto scanWrapperCol = std::make_shared<PhysicalOperatorWrapper>(
            scanCol, schemaOutCol, schemaOutCol, PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
        auto emitWrapperCol = std::make_shared<PhysicalOperatorWrapper>(
            emitCol, schemaInCol, schemaInCol, handlerId, std::make_shared<EmitOperatorHandler>(),  PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);


        auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        schemaOutCol,
        schemaOutCol,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);


        emitWrapperRow->addChild(scanWrapperCol);
        scanWrapperCol->addChild(emitSelectionWrapper);
        emitSelectionWrapper->addChild(wrapper);
        wrapper->addChild(scanSelectionWrapper);
        scanSelectionWrapper->addChild(emitWrapperCol);
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

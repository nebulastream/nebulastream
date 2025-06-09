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
#include "Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp"

#include <EmitPhysicalOperator.hpp>
#include <EmitOperatorHandler.hpp>
#include <ScanPhysicalOperator.hpp>
#include <Nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <DataTypes/Schema.hpp>


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




    auto fieldNames = std::vector<std::string>(); ///get used filedNames in selection

    auto schemaIn = Schema(conf.memoryLayout.getValue());
    auto schemaOut = Schema(conf.memoryLayout.getValue());

    auto selectionFunc = function.tryGet<NES::GreaterEqualsLogicalFunction>();
    auto res = selectionFunc.value().explain(ExplainVerbosity::Short);
    for (const std::string& fieldName : inputSchema.getFieldNames())
    {
        schemaIn.addField(fieldName, inputSchema.getFieldByName(fieldName)->dataType);

        if (res.find(fieldName) != std::string::npos) { //if the fieldname is part of the selection function
            schemaOut.addField(fieldName, inputSchema.getFieldByName(fieldName)->dataType);
            fieldNames.push_back(fieldName);
        }
    }



    auto scanSelection = ScanPhysicalOperator(
        std::make_shared<Nautilus::Interface::MemoryProvider::RowTupleBufferMemoryProvider>(
            std::make_shared<Memory::MemoryLayouts::RowLayout>(conf.pageSize.getValue(), inputSchema)),
        fieldNames);
    auto emitSelection = EmitPhysicalOperator(getNextOperatorHandlerId(), std::make_shared<Nautilus::Interface::MemoryProvider::RowTupleBufferMemoryProvider>(
       std::make_shared<Memory::MemoryLayouts::RowLayout>(conf.pageSize.getValue(), outputSchema)));

    auto scanSelectionWrapper = std::make_shared<PhysicalOperatorWrapper>(
        scanSelection, schemaIn, schemaIn, PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    auto emitSelectionWrapper = std::make_shared<PhysicalOperatorWrapper>(
        emitSelection, schemaOut, schemaOut, getNextOperatorHandlerId(), std::make_shared<EmitOperatorHandler>(),
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    /// if function left, right is field access, then use fieldname for outputschemas and swap after selection
    if (conf.useSingleMemoryLayout.getValue() &&conf.memoryLayout.getValue() != conf.memoryLayout.getDefaultValue())
    {




        auto colSchemaIn = *std::make_shared<Schema>(schemaIn); //hopefully deep copy
        auto colSchemaOut = *std::make_shared<Schema>(schemaOut);
        colSchemaIn.memoryLayoutType = Schema::MemoryLayoutType::COLUMNAR_LAYOUT;
        colSchemaOut.memoryLayoutType = Schema::MemoryLayoutType::COLUMNAR_LAYOUT;



        auto rowLayout = std::make_shared<Memory::MemoryLayouts::RowLayout>(
            conf.pageSize.getValue(), inputSchema);
        auto rowMemoryProvider = std::make_shared<Nautilus::Interface::MemoryProvider::RowTupleBufferMemoryProvider>(rowLayout);
        auto handlerId = getNextOperatorHandlerId();
        auto scanRow = ScanPhysicalOperator(rowMemoryProvider, inputSchema.getFieldNames());
        auto emitRow = EmitPhysicalOperator(handlerId, rowMemoryProvider);

        auto scanWrapperRow = std::make_shared<PhysicalOperatorWrapper>(
            scanRow, inputSchema, inputSchema, PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

        auto emitWrapperRow = std::make_shared<PhysicalOperatorWrapper>(
            emitRow, outputSchema, outputSchema, handlerId, std::make_shared<EmitOperatorHandler>(), PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

        auto columnLayout = std::make_shared<Memory::MemoryLayouts::ColumnLayout>(
                   conf.pageSize.getValue(), inputSchema);
        auto columnMemoryProvider = std::make_shared<Nautilus::Interface::MemoryProvider::ColumnTupleBufferMemoryProvider>(columnLayout);
        auto newHandlerId = getNextOperatorHandlerId();
        auto scanCol = ScanPhysicalOperator(columnMemoryProvider, colSchemaIn.getFieldNames());
        auto emitCol = EmitPhysicalOperator(newHandlerId, columnMemoryProvider);
        auto newOperatorHandler = std::make_shared<EmitOperatorHandler>();


        auto scanWrapperCol = std::make_shared<PhysicalOperatorWrapper>(
            scanCol, colSchemaOut, colSchemaOut, PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
        auto emitWrapperCol = std::make_shared<PhysicalOperatorWrapper>(
            emitCol, colSchemaIn, colSchemaIn, newHandlerId, newOperatorHandler,  PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);


        inputSchema=scanSelectionWrapper->getOutputSchema().value();//.memoryLayoutType = conf.memoryLayout.getValue();///TODO: do for all operators besides sink and source
        outputSchema=emitSelectionWrapper->getInputSchema().value();//.memoryLayoutType = conf.memoryLayout.getValue();

        auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        inputSchema,
        outputSchema,
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
        scanSelectionWrapper->getOutputSchema().value(),
        emitSelectionWrapper->getInputSchema().value(),
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    scanSelectionWrapper->addChild(wrapper);
    wrapper->addChild(emitSelectionWrapper);

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leafes(logicalOperator.getChildren().size(), scanSelectionWrapper);
    return {.root = emitSelectionWrapper, .leafs = {leafes}};
};

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterSelectionRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalSelection>(argument.conf);
}
}

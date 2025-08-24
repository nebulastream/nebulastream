#include <string>
#include <vector>
#include <Utils.hpp>

#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>

#include <EmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ScanPhysicalOperator.hpp>
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
namespace NES
{
    std::pair<std::shared_ptr<PhysicalOperatorWrapper>, std::shared_ptr<PhysicalOperatorWrapper>> addSwapOperators(LogicalOperator logicalOperator, PhysicalOperator physicalOperator, NES::Configurations::QueryOptimizerConfiguration conf)
    {
        auto inputSchema = logicalOperator.getInputSchemas()[0];
        auto outputSchema = logicalOperator.getOutputSchema();
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
            std::make_shared<Memory::MemoryLayouts::RowLayout>(conf.operatorBufferSize.getValue(), schemaIn));

        auto schemaInCol= *std::make_shared<Schema>(schemaIn); //hopefully deep copy
        schemaInCol.memoryLayoutType = conf.memoryLayout.getValue();
        auto memoryProviderInCol = std::make_shared<Nautilus::Interface::MemoryProvider::ColumnTupleBufferMemoryProvider>(
            std::make_shared<Memory::MemoryLayouts::ColumnLayout>(conf.operatorBufferSize.getValue(), schemaInCol));


        auto memoryProviderOutRow = std::make_shared<Nautilus::Interface::MemoryProvider::RowTupleBufferMemoryProvider>(
            std::make_shared<Memory::MemoryLayouts::RowLayout>(conf.operatorBufferSize.getValue(), schemaOut));

        auto schemaOutCol = *std::make_shared<Schema>(schemaOut); //hopefully deep copy
        schemaOutCol.memoryLayoutType = conf.memoryLayout.getValue();
        auto memoryProviderOutCol = std::make_shared<Nautilus::Interface::MemoryProvider::ColumnTupleBufferMemoryProvider>(
            std::make_shared<Memory::MemoryLayouts::ColumnLayout>(conf.operatorBufferSize.getValue(), schemaOutCol));




        auto scanSelection = ScanPhysicalOperator(
            memoryProviderInCol,
            schemaOutCol.getFieldNames()); ///have all fields in memProvider but only used ones in projections
        auto handlerId = getNextOperatorHandlerId();
        auto emitSelection = EmitPhysicalOperator(handlerId, memoryProviderOutCol);

        /// TODO: only use one MemoryProvider to save memory
        auto memoryProviderInRowNew = std::make_shared<Nautilus::Interface::MemoryProvider::RowTupleBufferMemoryProvider>(
        std::make_shared<Memory::MemoryLayouts::RowLayout>(conf.operatorBufferSize.getValue(), schemaIn));
        auto memoryProviderOutRowNew = std::make_shared<Nautilus::Interface::MemoryProvider::RowTupleBufferMemoryProvider>(
        std::make_shared<Memory::MemoryLayouts::RowLayout>(conf.operatorBufferSize.getValue(), schemaOut));
        auto scanSelectionRow = ScanPhysicalOperator(memoryProviderInRowNew, schemaIn.getFieldNames());
        auto emitSelectionRow = EmitPhysicalOperator(handlerId, memoryProviderInRowNew);

        auto scanSelectionWrapper = std::make_shared<PhysicalOperatorWrapper>(
            scanSelection, schemaInCol, schemaOutCol, PhysicalOperatorWrapper::PipelineLocation::SCAN); ///TODO: outputSchema schemaOutCol?
        auto scanSelectionNew = std::make_shared<PhysicalOperatorWrapper>(
            scanSelectionRow, schemaIn, schemaOut, PhysicalOperatorWrapper::PipelineLocation::SCAN);

        auto emitSelectionWrapper = std::make_shared<PhysicalOperatorWrapper>(
            emitSelection, schemaOutCol, schemaOutCol, handlerId, std::make_shared<EmitOperatorHandler>(),
            PhysicalOperatorWrapper::PipelineLocation::EMIT);
        auto emitSelectionNew = std::make_shared<PhysicalOperatorWrapper>(
            emitSelectionRow, schemaOut, schemaOut, handlerId, std::make_shared<EmitOperatorHandler>(),
            PhysicalOperatorWrapper::PipelineLocation::EMIT);

        handlerId = getNextOperatorHandlerId();
        auto scanRow = ScanPhysicalOperator(memoryProviderInRow, schemaIn.getFieldNames());
        auto emitRow = EmitPhysicalOperator(handlerId, memoryProviderOutRow);

        auto scanWrapperRow = std::make_shared<PhysicalOperatorWrapper>(
            scanRow, schemaIn, schemaIn, PhysicalOperatorWrapper::PipelineLocation::SCAN);

        auto emitWrapperRow = std::make_shared<PhysicalOperatorWrapper>(
            emitRow, schemaOut, schemaOut, handlerId, std::make_shared<EmitOperatorHandler>(), PhysicalOperatorWrapper::PipelineLocation::EMIT);



        handlerId = getNextOperatorHandlerId();
        auto scanCol = ScanPhysicalOperator(memoryProviderOutCol, schemaOutCol.getFieldNames());
        auto emitCol = EmitPhysicalOperator(handlerId, memoryProviderInCol);
        auto scanRowNew = ScanPhysicalOperator(memoryProviderOutRowNew, schemaOut.getFieldNames());
        auto emitRowNew = EmitPhysicalOperator(handlerId, memoryProviderInRowNew);

        auto scanWrapperCol = std::make_shared<PhysicalOperatorWrapper>(
            scanCol, schemaOutCol, schemaOutCol, PhysicalOperatorWrapper::PipelineLocation::SCAN);
        auto scanWrapperRowNew = std::make_shared<PhysicalOperatorWrapper>(
            scanRowNew, schemaOut, schemaOut, PhysicalOperatorWrapper::PipelineLocation::SCAN);

        auto emitWrapperCol = std::make_shared<PhysicalOperatorWrapper>(
            emitCol, schemaInCol, schemaInCol, handlerId, std::make_shared<EmitOperatorHandler>(),  PhysicalOperatorWrapper::PipelineLocation::EMIT);
        auto emitWrapperRowNew = std::make_shared<PhysicalOperatorWrapper>(
            emitRowNew, schemaIn, schemaIn, handlerId, std::make_shared<EmitOperatorHandler>(), PhysicalOperatorWrapper::PipelineLocation::EMIT);

        auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        schemaOutCol,
        schemaOutCol,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

        if (conf.memoryLayout.getValue() == conf.memoryLayout.getDefaultValue())// == Schema::MemoryLayoutType::ROW_LAYOUT)
        {
            if (logicalOperator.tryGet<SelectionLogicalOperator>())
            {
                //swap in col
                emitSelectionNew->addChild(wrapper);
                wrapper->addChild(scanSelectionNew);
                scanSelectionNew->addChild(emitWrapperRowNew);
                emitWrapperRowNew->addChild(scanWrapperRow);
                return std::make_pair(emitSelectionNew, scanWrapperRow);
            }
            if (logicalOperator.tryGet<ProjectionLogicalOperator>())
            {
                //swap back to row
                emitWrapperRow->addChild(scanWrapperRowNew);
                scanWrapperRowNew->addChild(emitSelectionNew);
                emitSelectionNew->addChild(wrapper);
                wrapper->addChild(scanSelectionNew);
                return std::make_pair(emitWrapperRow, scanSelectionNew);
            }
            else
            {
                ///use new row layout memoryProvider
                emitWrapperRow->addChild(scanWrapperRowNew);
                scanWrapperRowNew->addChild(emitSelectionNew);
                emitSelectionNew->addChild(wrapper);
                wrapper->addChild(scanSelectionNew);
                scanSelectionNew->addChild(emitWrapperRowNew);
                emitWrapperRowNew->addChild(scanWrapperRow);
                return std::make_pair(emitWrapperRow, scanWrapperRow);
            }


        }else ///use column layout memoryProvider
        {
            if (logicalOperator.tryGet<SelectionLogicalOperator>())
            {
                emitSelectionWrapper->addChild(wrapper);
                wrapper->addChild(scanSelectionWrapper);
                scanSelectionWrapper->addChild(emitWrapperCol);
                emitWrapperCol->addChild(scanWrapperRow);
                return std::make_pair(emitSelectionWrapper, scanWrapperRow);
            }
            if (logicalOperator.tryGet<ProjectionLogicalOperator>())
            {
                emitWrapperRow->addChild(scanWrapperCol);
                scanWrapperCol->addChild(emitSelectionWrapper);
                emitSelectionWrapper->addChild(wrapper);
                wrapper->addChild(scanSelectionWrapper);
                return std::make_pair(emitWrapperRow, scanSelectionWrapper);
            }

            emitWrapperRow->addChild(scanWrapperCol);
            scanWrapperCol->addChild(emitSelectionWrapper);
            emitSelectionWrapper->addChild(wrapper);
            wrapper->addChild(scanSelectionWrapper);
            scanSelectionWrapper->addChild(emitWrapperCol);
            emitWrapperCol->addChild(scanWrapperRow);
        }


        // scanWrapperRow->addChild(emitWrapperCol);
        // emitWrapperCol->addChild(wrapper);
        // wrapper->addChild(scanWrapperCol);
        // scanWrapperCol->addChild(emitWrapperRow);
        return {emitWrapperRow, scanWrapperRow};
    }
}

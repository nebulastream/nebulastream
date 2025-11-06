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
#include <Util/Common.hpp>

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
    std::pair<std::shared_ptr<PhysicalOperatorWrapper>, std::shared_ptr<PhysicalOperatorWrapper>> addScanAndEmitAroundOperator(const std::shared_ptr<PhysicalOperatorWrapper>& oldOperatorWrapper, MemoryLayoutTypeTrait memoryLayoutTrait, QueryExecutionConfiguration conf, Schema sourceSchema)
    {
        auto res= addSwapBeforeOperator(oldOperatorWrapper, memoryLayoutTrait, conf, sourceSchema);

        auto operatorWrapper= res.first;
        auto swapScanWrapper= res.second;

        auto inputSchema = operatorWrapper->getInputSchema().value();
        auto outputSchema = operatorWrapper->getOutputSchema().value();

        auto provider = Interface::MemoryProvider::TupleBufferMemoryProvider::create(conf.operatorBufferSize, inputSchema);

        auto scan = ScanPhysicalOperator(provider, outputSchema.getFieldNames());
        auto handlerId = getNextOperatorHandlerId();
        auto emit = EmitPhysicalOperator(handlerId, provider);
        auto scanWrapper = std::make_shared<PhysicalOperatorWrapper>(
            scan, inputSchema, outputSchema, PhysicalOperatorWrapper::PipelineLocation::SCAN);

        auto emitWrapper = std::make_shared<PhysicalOperatorWrapper>(
            emit, outputSchema, outputSchema, handlerId, std::make_shared<EmitOperatorHandler>(),
            PhysicalOperatorWrapper::PipelineLocation::EMIT);

        auto children = operatorWrapper->getChildren();
        //filter -> emit -> scan -> else


        scanWrapper->addChild(children[0]);
        operatorWrapper->setChildren({scanWrapper});
        emitWrapper->addChild(operatorWrapper);
        // emit -> filter -> scan -> emit -> scan
        return {emitWrapper, swapScanWrapper};
    }

    std::pair<std::shared_ptr<PhysicalOperatorWrapper>, std::shared_ptr<PhysicalOperatorWrapper>> addSwapBeforeOperator(const std::shared_ptr<PhysicalOperatorWrapper>& operatorWrapper, MemoryLayoutTypeTrait memoryLayoutTrait, QueryExecutionConfiguration conf)
    {
        auto inputSchema = operatorWrapper->getInputSchema().value().withMemoryLayoutType(memoryLayoutTrait.incomingLayoutType);

        return addSwapBeforeOperator(operatorWrapper, memoryLayoutTrait, conf, inputSchema);
    }


    std::pair<std::shared_ptr<PhysicalOperatorWrapper>, std::shared_ptr<PhysicalOperatorWrapper>> addSwapBeforeOperator(const std::shared_ptr<PhysicalOperatorWrapper>& operatorWrapper, MemoryLayoutTypeTrait memoryLayoutTrait, QueryExecutionConfiguration conf, Schema inputSchema)
    {
        inputSchema = inputSchema.withMemoryLayoutType(memoryLayoutTrait.incomingLayoutType);
        auto targetSchema = operatorWrapper->getInputSchema().value().withMemoryLayoutType(memoryLayoutTrait.targetLayoutType);

        auto incomingProvider = Interface::MemoryProvider::TupleBufferMemoryProvider::create(conf.operatorBufferSize, inputSchema); //all fields
        auto targetProvider = Interface::MemoryProvider::TupleBufferMemoryProvider::create(conf.operatorBufferSize, targetSchema); //sink fields

        auto scan = ScanPhysicalOperator(incomingProvider, targetSchema.getFieldNames()); // all fields, but only sink fields projections
        auto handlerId = getNextOperatorHandlerId();
        auto emit = EmitPhysicalOperator(handlerId, targetProvider);
        auto scanWrapper = std::make_shared<PhysicalOperatorWrapper>(
            scan, inputSchema, inputSchema, PhysicalOperatorWrapper::PipelineLocation::SCAN);//TODO: outputSchema to targetSchema? (verm. egal)

        auto emitWrapper = std::make_shared<PhysicalOperatorWrapper>(
            emit, targetSchema, targetSchema, handlerId, std::make_shared<EmitOperatorHandler>(),
            PhysicalOperatorWrapper::PipelineLocation::EMIT);

        operatorWrapper->addChild(emitWrapper);
        emitWrapper->addChild(scanWrapper);

        return {operatorWrapper, scanWrapper};
    }

    MemoryLayoutTypeTrait getMemoryLayoutTypeTrait(const LogicalOperator& logicalOperator)
    {
        auto traitSet = logicalOperator.getTraitSet();

        const auto memoryLayoutIter
            = std::ranges::find_if(traitSet, [](const Trait& trait) { return trait.tryGet<MemoryLayoutTypeTrait>().has_value(); });
        PRECONDITION(memoryLayoutIter != traitSet.end(), "operator must have a memory layout type trait");
        auto memoryLayoutTrait = memoryLayoutIter->get<MemoryLayoutTypeTrait>();
        return memoryLayoutTrait;
    }
}

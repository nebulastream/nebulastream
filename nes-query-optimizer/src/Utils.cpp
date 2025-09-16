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
    std::pair<std::shared_ptr<PhysicalOperatorWrapper>, std::shared_ptr<PhysicalOperatorWrapper>> addSwapBeforeOperator(const std::shared_ptr<PhysicalOperatorWrapper>& operatorWrapper, MemoryLayoutTypeTrait memoryLayoutTrait, QueryExecutionConfiguration conf)
    {
        auto inputSchema = operatorWrapper->getInputSchema().value().withMemoryLayoutType(memoryLayoutTrait.incomingLayoutType);
        auto targetSchema = inputSchema.withMemoryLayoutType(memoryLayoutTrait.targetLayoutType);

        auto incomingProvider = Interface::MemoryProvider::TupleBufferMemoryProvider::create(conf.operatorBufferSize, inputSchema);
        auto targetProvider = Interface::MemoryProvider::TupleBufferMemoryProvider::create(conf.operatorBufferSize, targetSchema);

        auto scan = ScanPhysicalOperator(incomingProvider, inputSchema.getFieldNames());
        auto handlerId = getNextOperatorHandlerId();
        auto emit = EmitPhysicalOperator(handlerId, targetProvider);
        auto scanWrapper = std::make_shared<PhysicalOperatorWrapper>(
            scan, inputSchema, inputSchema, PhysicalOperatorWrapper::PipelineLocation::SCAN);

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

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
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalProjection.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ScanPhysicalOperator.hpp>
#include <EmitOperatorHandler.hpp>
#include <RewriteRuleRegistry.hpp>

namespace NES::Optimizer
{

RewriteRuleResultSubgraph LowerToPhysicalProjection::apply(LogicalOperator projectionLogicalOperator)
{
    auto handlerId = getNextOperatorHandlerId();
    auto inputSchema = projectionLogicalOperator.getInputSchemas()[0];
    auto outputSchema = projectionLogicalOperator.getOutputSchema();
    auto bufferSize = NES::Configurations::DEFAULT_PAGED_VECTOR_SIZE;

    auto scanLayout = std::make_shared<Memory::MemoryLayouts::RowLayout>(inputSchema, bufferSize);
    auto scanMemoryProvider = std::make_shared<RowTupleBufferMemoryProvider>(scanLayout);
    auto scan = ScanPhysicalOperator(scanMemoryProvider, outputSchema.getFieldNames());
    auto scanWrapper = std::make_shared<PhysicalOperatorWrapper>(scan, outputSchema, outputSchema);
    scanWrapper->isScan = true;

    auto emitLayout = std::make_shared<Memory::MemoryLayouts::RowLayout>(outputSchema, bufferSize);
    auto emitMemoryProvider = std::make_shared<RowTupleBufferMemoryProvider>(emitLayout);
    auto emit = EmitPhysicalOperator(handlerId, emitMemoryProvider);
    auto emitWrapper = std::make_shared<PhysicalOperatorWrapper>(emit, outputSchema, outputSchema);
    emitWrapper->isEmit = true;
    emitWrapper->handler = std::make_shared<EmitOperatorHandler>();
    emitWrapper->handlerId = handlerId;

    emitWrapper->children.emplace_back(scanWrapper);

    return {emitWrapper, {scanWrapper}};
}

std::unique_ptr<AbstractRewriteRule> RewriteRuleGeneratedRegistrar::RegisterProjectionRewriteRule(RewriteRuleRegistryArguments argument)
{
    return std::make_unique<LowerToPhysicalProjection>(argument.conf);
}
}

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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalOriginSplit.hpp>

#include <memory>
#include <ranges>
#include <utility>
#include <vector>

#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/OriginSplitLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OriginMappingTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/SchemaFactory.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterProvider.hpp>
#include <LoweringRuleRegistry.hpp>
#include <OriginMappingOperatorHandler.hpp>
#include <PhysicalOperator.hpp>
#include <ScanPhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalOriginSplit::apply(LogicalOperator logicalOperator)
{
    const auto split = logicalOperator.getAs<OriginSplitLogicalOperator>();
    const auto traitSet = logicalOperator.getTraitSet();
    const auto childTraitSet = split->getChild().getTraitSet();

    const auto originMappingTrait = traitSet.get<OriginMappingTrait>();
    PRECONDITION(not originMappingTrait->originMapping.empty(), "An origin split must know the ids its branch stamps");

    const auto memoryLayoutType = traitSet.get<MemoryLayoutTypeTrait>()->memoryLayout;
    /// The branch forwards the records of its child unchanged, so it reads and writes the same schema.
    const auto inputSchema = createPhysicalOutputSchema(childTraitSet);
    const auto outputSchema = createPhysicalOutputSchema(traitSet);

    /// The buffers this branch reads carry the layout of its child, so the scan reads them that way; the emit that closes the
    /// branch writes them out again in the order the branch itself carries.
    const auto memoryProvider = LowerSchemaProvider::lowerSchema(conf.operatorBufferSize.getValue(), inputSchema, memoryLayoutType);
    auto fieldNames = inputSchema | std::views::transform([](const auto& field) { return field.getFullyQualifiedName(); })
        | std::ranges::to<std::vector>();

    /// A branch reading straight from a source gets that source's buffers in the format it ingests, which only an input
    /// formatter turns into records. The pipelining phase does the same when it starts a pipeline on a source, and reads
    /// through the source's own field order there, because a formatter maps columns by that order and not by the branch's.
    const auto bufferRef = [&]() -> std::shared_ptr<TupleBufferRef>
    {
        if (const auto source = split->getChild().tryGetAs<SourceDescriptorLogicalOperator>())
        {
            if (const auto& descriptor = source.value()->getSourceDescriptor(); toUpperCase(descriptor.getInputFormatType()) != "NATIVE")
            {
                const auto sourceMemoryProvider = LowerSchemaProvider::lowerSchema(
                    conf.operatorBufferSize.getValue(), *descriptor.getLogicalSource().getSchema(), memoryLayoutType);
                return provideInputFormatter(descriptor.getInputFormatterDescriptor(), sourceMemoryProvider);
            }
        }
        return memoryProvider;
    }();

    const auto handlerId = getNextOperatorHandlerId();
    auto handler = std::make_shared<OriginMappingOperatorHandler>(originMappingTrait->originMapping);

    /// A scan heads the branch's own pipeline: the pipelining phase closes the pipeline above it and starts a new one
    /// rooted here, so nothing the branch produces carries the id it read.
    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        ScanPhysicalOperator(bufferRef, std::move(fieldNames), handlerId),
        inputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::SCAN);

    return {.root = wrapper, .leaves = {wrapper}};
}

}

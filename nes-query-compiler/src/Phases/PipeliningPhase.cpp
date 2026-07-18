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

#include <Phases/PipeliningPhase.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/BufferRef/RowTupleBufferRef.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <EmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterProvider.hpp>
#include <PhysicalOperator.hpp>
#include <PhysicalPlan.hpp>
#include <Pipeline.hpp>
#include <PipelinedQueryPlan.hpp>
#include <ScanPhysicalOperator.hpp>
#include <SinkPhysicalOperator.hpp>

namespace NES::QueryCompilation::PipeliningPhase
{

namespace
{

using OperatorPipelineMap = std::unordered_map<OperatorId, std::shared_ptr<Pipeline>>;
using MergePointSet = std::unordered_set<OperatorId>;

/// Finds all operators in the DAG that have in-degree > 1 (i.e., are children of multiple parents).
/// These are merge points where a pipeline break is required, because multiple input pipelines
/// converge and the downstream operators must not be duplicated across pipelines.
MergePointSet findMergePoints(const std::vector<std::shared_ptr<PhysicalOperatorWrapper>>& roots)
{
    std::unordered_set<OperatorId> visited;
    MergePointSet mergePoints;
    std::function<void(const std::shared_ptr<PhysicalOperatorWrapper>&)> traverse
        = [&](const std::shared_ptr<PhysicalOperatorWrapper>& wrapper)
    {
        const auto opId = wrapper->getPhysicalOperator().getId();
        if (!visited.insert(opId).second)
        {
            /// Already visited — this operator has in-degree > 1
            mergePoints.insert(opId);
            return;
        }
        for (const auto& child : wrapper->getChildren())
        {
            traverse(child);
        }
    };
    for (const auto& root : roots)
    {
        traverse(root);
    }
    return mergePoints;
}

/// Helper function to add a default scan operator
/// This is used only when the wrapped operator does not already provide a scan
/// @note Once we have refactored the memory layout and schema we can get rid of the configured buffer size.
/// Do not add further parameters here that should be part of the QueryExecutionConfiguration.
PhysicalOperator createScanOperator(
    const Pipeline& prevPipeline,
    const std::optional<Schema<QualifiedUnboundField, Ordered>>& inputSchema,
    const std::optional<MemoryLayoutType>& memoryLayout,
    const uint64_t configuredBufferSize)
{
    INVARIANT(inputSchema.has_value(), "Wrapped operator has no input schema");
    INVARIANT(memoryLayout.has_value(), "Wrapped operator has no input memory layout type");
    if (inputSchema.value().getSizeInBytes() > configuredBufferSize)
    {
        throw TuplesTooLargeForPipelineBufferSize(
            "Got pipeline with an input schema size of {}, which is larger than the configured buffer size of the pipeline, which is {}",
            inputSchema.value().getSizeInBytes(),
            configuredBufferSize);
    }

    const auto memoryProvider = LowerSchemaProvider::lowerSchema(configuredBufferSize, inputSchema.value(), memoryLayout.value());
    /// Instantiate the scan with an InputFormatter, if the prior operator is a source operator that contains a source
    /// descriptor with a parser type other than "NATIVE" (NATIVE data does not require formatting)
    if (prevPipeline.isSourcePipeline())
    {
        const auto inputFormatterConfig
            = prevPipeline.getRootOperator().get<SourceDescriptorPhysicalOperator>().getDescriptor().getInputFormatterDescriptor();
        if (toUpperCase(inputFormatterConfig.getInputFormatterType()) != "NATIVE")
        {
            /// The inputSchema (from the downstream operator) may be reordered (e.g. alphabetically by FieldOrderingTrait).
            /// Using it as the memoryProvider would mis-map CSV column N to the wrong field name.
            /// Always use the source's natural schema order for the InputFormatter's field-to-column mapping.
            const auto& sourceLogicalSchema
                = *prevPipeline.getRootOperator().get<SourceDescriptorPhysicalOperator>().getDescriptor().getLogicalSource().getSchema();
            const auto sourceMemoryProvider
                = LowerSchemaProvider::lowerSchema(configuredBufferSize, sourceLogicalSchema, memoryLayout.value());
            return ScanPhysicalOperator(
                provideInputFormatter(inputFormatterConfig, sourceMemoryProvider),
                inputSchema.value() | std::views::transform([](const auto& field) { return field.getFullyQualifiedName(); })
                    | std::ranges::to<std::vector>());
        }
    }
    return ScanPhysicalOperator(
        memoryProvider,
        *inputSchema | std::views::transform([](const auto& field) { return field.getFullyQualifiedName(); })
            | std::ranges::to<std::vector>());
}

/// Creates a new pipeline that contains a scan followed by the wrappedOpAfterScan. The newly created pipeline is a successor of the prevPipeline
std::shared_ptr<Pipeline> createNewPipelineWithScan(
    const std::shared_ptr<Pipeline>& prevPipeline,
    OperatorPipelineMap& pipelineMap,
    const PhysicalOperatorWrapper& wrappedOpAfterScan,
    const uint64_t configuredBufferSize)
{
    const auto newPipeline = std::make_shared<Pipeline>(createScanOperator(
        *prevPipeline, wrappedOpAfterScan.getInputSchema(), wrappedOpAfterScan.getInputMemoryLayoutType(), configuredBufferSize));
    prevPipeline->addSuccessor(newPipeline, prevPipeline);
    pipelineMap[wrappedOpAfterScan.getPhysicalOperator().getId()] = newPipeline;
    newPipeline->appendOperator(wrappedOpAfterScan.getPhysicalOperator());
    return newPipeline;
}

/// True iff the pipeline's operator chain already ends in an EmitPhysicalOperator.
/// The traversal derives "is the predecessor pipeline already closed?" from the actually built pipeline
/// state instead of carrying it as an explicit policy: the only closure this helper cannot see is a
/// pipeline closed by a CUSTOM emit operator (which is not an EmitPhysicalOperator); that case is covered
/// by the prevOpWrapper location checks it is always combined with.
bool endsInEmit(const Pipeline& pipeline)
{
    std::optional<PhysicalOperator> current = pipeline.getRootOperator();
    while (current->getChild().has_value())
    {
        current = current->getChild();
    }
    return current->tryGet<EmitPhysicalOperator>().has_value();
}

/// Helper function to add a default emit operator
/// This is used only when the wrapped operator does not already provide an emit
/// @note Once we have refactored the memory layout and schema we can get rid of the configured buffer size.
/// Do not add further parameters here that should be part of the QueryExecutionConfiguration.
void addDefaultEmit(
    const std::shared_ptr<Pipeline>& pipeline, const PhysicalOperatorWrapper& wrappedOp, const uint64_t configuredBufferSize)
{
    PRECONDITION(pipeline->isOperatorPipeline(), "Only add emit physical operator to operator pipelines");
    INVARIANT(!endsInEmit(*pipeline), "Pipeline is already closed with an emit; adding a second one would duplicate its output");
    const auto& schema = wrappedOp.getOutputSchema();
    const auto memoryLayoutType = wrappedOp.getOutputMemoryLayoutType();
    INVARIANT(schema.has_value(), "Wrapped operator has no output schema");
    INVARIANT(memoryLayoutType.has_value(), "Wrapped operator has no output memory layout type");

    const auto bufferRef = LowerSchemaProvider::lowerSchema(configuredBufferSize, schema.value(), memoryLayoutType.value());
    /// Create an operator handler for the emit
    const OperatorHandlerId operatorHandlerIndex = getNextOperatorHandlerId();
    pipeline->getOperatorHandlers().emplace(operatorHandlerIndex, std::make_shared<EmitOperatorHandler>());
    pipeline->appendOperator(EmitPhysicalOperator(operatorHandlerIndex, bufferRef));
}

/// Helper functions to add an emit operator that also performs output formatting. A sink should follow such emit operators at all times, since
/// our physical operators cannot handle data that is not column / row formatted.
void addOutputFormattingEmit(
    const std::shared_ptr<Pipeline>& pipeline,
    const PhysicalOperatorWrapper& wrappedOp,
    const uint64_t configuredBufferSize,
    const std::string& outputFormat,
    const std::unordered_map<Identifier, std::string>& config)
{
    PRECONDITION(pipeline->isOperatorPipeline(), "Only add emit physical operator to operator pipelines");
    INVARIANT(!endsInEmit(*pipeline), "Pipeline is already closed with an emit; adding a second one would duplicate its output");
    const auto& schema = wrappedOp.getOutputSchema();
    INVARIANT(schema.has_value(), "Wrapped operator has no output schema");

    const auto bufferRef = LowerSchemaProvider::lowerSchemaWithOutputFormat(configuredBufferSize, schema.value(), outputFormat, config);

    /// Create an operator handler for the emit
    const OperatorHandlerId operatorHandlerIndex = getNextOperatorHandlerId();
    pipeline->getOperatorHandlers().emplace(operatorHandlerIndex, std::make_shared<EmitOperatorHandler>());
    pipeline->appendOperator(EmitPhysicalOperator(operatorHandlerIndex, bufferRef));
}

enum class PipelinePolicy : uint8_t
{
    Continue, /// Uses the current pipeline for the next operator
    ForceNew /// Enforces a new pipeline for the next operator
};

void buildPipelineRecursively(
    const std::shared_ptr<PhysicalOperatorWrapper>& opWrapper,
    const std::shared_ptr<PhysicalOperatorWrapper>& prevOpWrapper,
    const std::shared_ptr<Pipeline>& currentPipeline,
    OperatorPipelineMap& pipelineMap,
    PipelinePolicy policy,
    uint64_t configuredBufferSize,
    const MergePointSet& mergePoints);

/// Recurses into the children of an operator that was just placed into pipeline. A single child continues with
/// childPolicy as before. Multiple children form a fan-out point: every consumer must read the operator's full
/// output, so the pipeline is closed with a single emit and each consumer starts its own successor pipeline.
void recurseIntoChildren(
    const std::shared_ptr<PhysicalOperatorWrapper>& opWrapper,
    const std::shared_ptr<Pipeline>& pipeline,
    OperatorPipelineMap& pipelineMap,
    const PipelinePolicy childPolicy,
    const uint64_t configuredBufferSize,
    const MergePointSet& mergePoints)
{
    const auto children = opWrapper->getChildren();
    if (children.size() <= 1)
    {
        for (const auto& child : children)
        {
            buildPipelineRecursively(child, opWrapper, pipeline, pipelineMap, childPolicy, configuredBufferSize, mergePoints);
        }
        return;
    }

    if (opWrapper->getPipelineLocation() != PhysicalOperatorWrapper::PipelineLocation::EMIT)
    {
        addDefaultEmit(pipeline, *opWrapper, configuredBufferSize);
    }
    /// The consumers observe the closed pipeline themselves (endsInEmit / custom-emit predecessor) and
    /// therefore need no dedicated policy — ForceNew only ensures none of them tries to fuse.
    for (const auto& child : children)
    {
        buildPipelineRecursively(child, opWrapper, pipeline, pipelineMap, PipelinePolicy::ForceNew, configuredBufferSize, mergePoints);
    }
}

void buildPipelineRecursively(
    const std::shared_ptr<PhysicalOperatorWrapper>& opWrapper,
    const std::shared_ptr<PhysicalOperatorWrapper>& prevOpWrapper,
    const std::shared_ptr<Pipeline>& currentPipeline,
    OperatorPipelineMap& pipelineMap,
    PipelinePolicy policy,
    uint64_t configuredBufferSize,
    const MergePointSet& mergePoints)
{
    /// Derived from built state instead of a carried policy: the predecessor pipeline is already closed
    /// iff its chain ends in an emit (the single default emit added at a fan-out point) or the previous
    /// operator is a custom emit (which closes its pipeline by its own nature). A closed pipeline must
    /// never receive further operators or a second emit; consumers of a fan-out point recognize this here
    /// without any dedicated signal.
    const bool predecessorClosed = endsInEmit(*currentPipeline)
        || (prevOpWrapper && prevOpWrapper->getPipelineLocation() == PhysicalOperatorWrapper::PipelineLocation::EMIT);

    /// Check if we've already seen this operator
    const OperatorId opId = opWrapper->getPhysicalOperator().getId();
    if (const auto it = pipelineMap.find(opId); it != pipelineMap.end())
    {
        if (prevOpWrapper and not predecessorClosed)
        {
            /// If the operator is a sink we might have to create an output formatter
            if (auto sink = opWrapper->getPhysicalOperator().tryGet<SinkPhysicalOperator>())
            {
                auto outputFormat = sink->getDescriptor().getFormatType();
                if (toUpperCase(outputFormat) != "NATIVE")
                {
                    addOutputFormattingEmit(
                        currentPipeline,
                        *prevOpWrapper,
                        configuredBufferSize,
                        std::string(outputFormat),
                        sink->getDescriptor().getOutputFormatterConfig());
                }
                else
                {
                    addDefaultEmit(currentPipeline, *prevOpWrapper, configuredBufferSize);
                }
            }
            else
            {
                addDefaultEmit(currentPipeline, *prevOpWrapper, configuredBufferSize);
            }
        }
        currentPipeline->addSuccessor(it->second, currentPipeline);
        return;
    }

    /// Case 1: Custom Scan
    if (opWrapper->getPipelineLocation() == PhysicalOperatorWrapper::PipelineLocation::SCAN)
    {
        if (prevOpWrapper && !predecessorClosed)
        {
            addDefaultEmit(currentPipeline, *prevOpWrapper, configuredBufferSize);
        }
        auto newPipeline = std::make_shared<Pipeline>(opWrapper->getPhysicalOperator());
        if (opWrapper->getHandler() && opWrapper->getHandlerId())
        {
            newPipeline->getOperatorHandlers().emplace(opWrapper->getHandlerId().value(), opWrapper->getHandler().value());
        }
        pipelineMap.emplace(opId, newPipeline);
        currentPipeline->addSuccessor(newPipeline, currentPipeline);
        const auto newPipelinePtr = currentPipeline->getSuccessors().back();
        recurseIntoChildren(opWrapper, newPipelinePtr, pipelineMap, PipelinePolicy::Continue, configuredBufferSize, mergePoints);
        return;
    }

    /// Case 2: Custom Emit – the operator brings its own emit, so the pipeline it ends up in is closed
    /// by it and no default emit is added
    if (opWrapper->getPipelineLocation() == PhysicalOperatorWrapper::PipelineLocation::EMIT)
    {
        if (not prevOpWrapper || predecessorClosed)
        {
            /// The current pipeline is already closed (custom-emit predecessor or the default emit of a
            /// fan-out point) or there is no predecessor to append to, so we need to add a scan before
            /// the current operator to create a new pipeline
            auto newPipeline = createNewPipelineWithScan(currentPipeline, pipelineMap, *opWrapper, configuredBufferSize);
            if (opWrapper->getHandler().has_value())
            {
                /// Create an operator handler for the custom emit operator
                const OperatorHandlerId operatorHandlerIndex = opWrapper->getHandlerId().value();
                newPipeline->getOperatorHandlers().emplace(operatorHandlerIndex, opWrapper->getHandler().value());
            }

            for (auto& child : opWrapper->getChildren())
            {
                buildPipelineRecursively(
                    child, opWrapper, newPipeline, pipelineMap, PipelinePolicy::ForceNew, configuredBufferSize, mergePoints);
            }
        }
        else
        {
            currentPipeline->appendOperator(opWrapper->getPhysicalOperator());
            if (opWrapper->getHandler() && opWrapper->getHandlerId())
            {
                currentPipeline->getOperatorHandlers().emplace(opWrapper->getHandlerId().value(), opWrapper->getHandler().value());
            }
            for (auto& child : opWrapper->getChildren())
            {
                buildPipelineRecursively(
                    child, opWrapper, currentPipeline, pipelineMap, PipelinePolicy::ForceNew, configuredBufferSize, mergePoints);
            }
        }

        return;
    }

    /// Case 3: Sink Operator – treat sinks as pipeline breakers
    if (auto sink = opWrapper->getPhysicalOperator().tryGet<SinkPhysicalOperator>())
    {
        const auto sinkFormat = sink->getDescriptor().getFormatType();
        if (currentPipeline->isSourcePipeline())
        {
            const auto sourceFormat = toUpperCase(currentPipeline->getRootOperator()
                                                      .get<SourceDescriptorPhysicalOperator>()
                                                      .getDescriptor()
                                                      .getInputFormatterDescriptor()
                                                      .getInputFormatterType());

            /// Add a formatting pipeline if the source-sink pipelines do not simply forward natively formatted data
            /// Otherwise, even if both formats are, e.g., 'CSV', the source 'blindly' ingest buffers until they are full, meaning buffers
            /// may start and end with a cut-off tuples (rows in the CSV case)
            /// The sink would output these buffers (out of order if the engine uses multiple threads), producing malformed data
            if (not(sourceFormat == "NATIVE" and toUpperCase(sinkFormat) == "NATIVE"))
            {
                const auto sourcePipeline = std::make_shared<Pipeline>(createScanOperator(
                    *currentPipeline,
                    *currentPipeline->getRootOperator()
                         .get<SourceDescriptorPhysicalOperator>()
                         .getDescriptor()
                         .getLogicalSource()
                         .getSchema(),
                    opWrapper->getInputMemoryLayoutType(),
                    configuredBufferSize));
                currentPipeline->addSuccessor(sourcePipeline, currentPipeline);

                if (toUpperCase(sinkFormat) == "NATIVE")
                {
                    addDefaultEmit(sourcePipeline, *opWrapper, configuredBufferSize);
                }
                else
                {
                    /// The output format is treated in a case sensitive manner, since it serves as a key to the output formatter registry.
                    /// That's why we cannot pass the upper case sinkFormat.
                    addOutputFormattingEmit(
                        sourcePipeline,
                        *opWrapper,
                        configuredBufferSize,
                        std::string(sinkFormat),
                        sink->getDescriptor().getOutputFormatterConfig());
                }

                INVARIANT(sourcePipeline->getRootOperator().getChild().has_value(), "Scan operator requires at least an emit as child.");
                const auto emitOperatorId = sourcePipeline->getRootOperator().getChild().value().getId();
                pipelineMap[emitOperatorId] = sourcePipeline;

                const auto sinkPipeline = std::make_shared<Pipeline>(*sink);
                sourcePipeline->addSuccessor(sinkPipeline, sourcePipeline);
                auto sinkPipelinePtr = sourcePipeline->getSuccessors().back();
                pipelineMap.emplace(opId, sinkPipelinePtr);
                return;
            }
        }
        if (predecessorClosed)
        {
            /// The predecessor pipeline is already closed (native emit at a fan-out point, or a custom
            /// emit). A native sink consumes those buffers directly; any other format gets its own
            /// formatting pipeline, since the existing emit must stay native for potential sibling
            /// consumers.
            auto sinkPredecessor = currentPipeline;
            if (toUpperCase(sinkFormat) != "NATIVE")
            {
                const auto formattingPipeline = std::make_shared<Pipeline>(createScanOperator(
                    *currentPipeline, opWrapper->getInputSchema(), opWrapper->getInputMemoryLayoutType(), configuredBufferSize));
                currentPipeline->addSuccessor(formattingPipeline, currentPipeline);
                addOutputFormattingEmit(
                    formattingPipeline,
                    *opWrapper,
                    configuredBufferSize,
                    std::string(sinkFormat),
                    sink->getDescriptor().getOutputFormatterConfig());
                INVARIANT(
                    formattingPipeline->getRootOperator().getChild().has_value(), "Scan operator requires at least an emit as child.");
                pipelineMap[formattingPipeline->getRootOperator().getChild().value().getId()] = formattingPipeline;
                sinkPredecessor = formattingPipeline;
            }
            const auto sinkPipeline = std::make_shared<Pipeline>(*sink);
            sinkPredecessor->addSuccessor(sinkPipeline, sinkPredecessor);
            pipelineMap.emplace(opId, sinkPredecessor->getSuccessors().back());
            return;
        }
        /// Add emit first if there is one needed (the closed case returned above, so the pipeline is open)
        if (prevOpWrapper)
        {
            if (toUpperCase(sinkFormat) == "NATIVE")
            {
                addDefaultEmit(currentPipeline, *prevOpWrapper, configuredBufferSize);
            }
            else
            {
                addOutputFormattingEmit(
                    currentPipeline,
                    *prevOpWrapper,
                    configuredBufferSize,
                    std::string(sinkFormat),
                    sink->getDescriptor().getOutputFormatterConfig());
            }
        }
        const auto newPipeline = std::make_shared<Pipeline>(*sink);
        currentPipeline->addSuccessor(newPipeline, currentPipeline);
        auto newPipelinePtr = currentPipeline->getSuccessors().back();
        pipelineMap.emplace(opId, newPipelinePtr);
        for (auto& child : opWrapper->getChildren())
        {
            buildPipelineRecursively(
                child, opWrapper, newPipelinePtr, pipelineMap, PipelinePolicy::Continue, configuredBufferSize, mergePoints);
        }
        return;
    }

    /// Force a pipeline break for DAG merge points (operators with multiple parents).
    /// Without this, the operator would be fused into the first parent's pipeline, and the second
    /// parent's data would incorrectly flow through the first parent's operators.
    if (policy == PipelinePolicy::Continue && mergePoints.contains(opId))
    {
        policy = PipelinePolicy::ForceNew;
    }

    /// Case 4: Forced new pipeline (pipeline breaker) for fusible operators
    if (policy == PipelinePolicy::ForceNew)
    {
        if (prevOpWrapper and not predecessorClosed)
        {
            addDefaultEmit(currentPipeline, *opWrapper, configuredBufferSize);
        }
        const auto newPipeline = std::make_shared<Pipeline>(opWrapper->getPhysicalOperator());
        if (auto handlerId = opWrapper->getHandlerId())
        {
            newPipeline->getOperatorHandlers().emplace(*handlerId, opWrapper->getHandler().value());
        }
        currentPipeline->addSuccessor(newPipeline, currentPipeline);
        const auto newPipelinePtr = currentPipeline->getSuccessors().back();
        pipelineMap[opId] = newPipelinePtr;
        PRECONDITION(newPipelinePtr->isOperatorPipeline(), "Only add scan physical operator to operator pipelines");
        newPipelinePtr->prependOperator(
            createScanOperator(*currentPipeline, opWrapper->getInputSchema(), opWrapper->getInputMemoryLayoutType(), configuredBufferSize));
        recurseIntoChildren(opWrapper, newPipelinePtr, pipelineMap, PipelinePolicy::Continue, configuredBufferSize, mergePoints);
        return;
    }

    /// Case 5: Fusible operator – add it to the current pipeline
    if (predecessorClosed)
    {
        /// If the current operator is a fusible operator but the current pipeline is already closed
        /// (custom emit, or the emit of a fan-out point), we need to add a scan before the current
        /// operator to create a new pipeline.
        createNewPipelineWithScan(currentPipeline, pipelineMap, *opWrapper, configuredBufferSize);
    }
    else
    {
        currentPipeline->appendOperator(opWrapper->getPhysicalOperator());
    }

    if (opWrapper->getHandler() && opWrapper->getHandlerId())
    {
        currentPipeline->getOperatorHandlers().emplace(opWrapper->getHandlerId().value(), opWrapper->getHandler().value());
    }
    if (opWrapper->getChildren().empty())
    {
        addDefaultEmit(currentPipeline, *opWrapper, configuredBufferSize);
    }
    else
    {
        recurseIntoChildren(opWrapper, currentPipeline, pipelineMap, PipelinePolicy::Continue, configuredBufferSize, mergePoints);
    }
}

}

std::shared_ptr<PipelinedQueryPlan> apply(const PhysicalPlan& physicalPlan)
{
    const uint64_t configuredBufferSize = physicalPlan.getOperatorBufferSize();
    auto pipelinedPlan = std::make_shared<PipelinedQueryPlan>(physicalPlan.getQueryId(), physicalPlan.getExecutionMode());

    OperatorPipelineMap pipelineMap;
    const auto mergePoints = findMergePoints(physicalPlan.getRootOperators());

    for (const auto& rootWrapper : physicalPlan.getRootOperators())
    {
        auto rootPipeline = std::make_shared<Pipeline>(rootWrapper->getPhysicalOperator());
        const auto opId = rootWrapper->getPhysicalOperator().getId();
        pipelineMap.emplace(opId, rootPipeline);
        pipelinedPlan->addPipeline(rootPipeline);

        for (const auto& child : rootWrapper->getChildren())
        {
            buildPipelineRecursively(
                child, nullptr, rootPipeline, pipelineMap, PipelinePolicy::ForceNew, configuredBufferSize, mergePoints);
        }
    }

    NES_DEBUG("Constructed pipeline plan with {} root pipelines.\n{}", pipelinedPlan->getPipelines().size(), *pipelinedPlan);
    return pipelinedPlan;
}
}

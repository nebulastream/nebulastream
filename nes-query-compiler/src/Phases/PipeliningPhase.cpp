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
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/BufferRef/RowTupleBufferRef.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <EmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterTupleBufferRefProvider.hpp>
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

/// Helper function to add a default scan operator
/// This is used only when the wrapped operator does not already provide a scan
/// @note Once we have refactored the memory layout and schema we can get rid of the configured buffer size.
/// Do not add further parameters here that should be part of the QueryExecutionConfiguration.
PhysicalOperator createScanOperator(
    const Pipeline& prevPipeline,
    const std::optional<Schema>& inputSchema,
    const std::optional<MemoryLayoutType>& memoryLayout,
    const uint64_t configuredBufferSize)
{
    INVARIANT(inputSchema.has_value(), "Wrapped operator has no input schema");
    INVARIANT(memoryLayout.has_value(), "Wrapped operator has no input memory layout type");
    if (inputSchema.value().getSizeOfSchemaInBytes() > configuredBufferSize)
    {
        throw TuplesTooLargeForPipelineBufferSize(
            "Got pipeline with an input schema size of {}, which is larger than the configured buffer size of the pipeline, which is {}",
            inputSchema.value().getSizeOfSchemaInBytes(),
            configuredBufferSize);
    }

    const auto memoryProvider = LowerSchemaProvider::lowerSchema(configuredBufferSize, inputSchema.value(), memoryLayout.value());
    /// Instantiate the scan with an InputFormatterTupleBufferRef, if the prior operatior is a source operator that contains a source descriptor
    /// with a parser type other than "NATIVE" (NATIVE data does not require formatting)
    if (prevPipeline.isSourcePipeline())
    {
        const auto inputFormatterConfig = prevPipeline.getRootOperator().get<SourcePhysicalOperator>().getDescriptor().getParserConfig();
        if (toUpperCase(inputFormatterConfig.parserType) != "NATIVE")
        {
            return ScanPhysicalOperator(
                provideInputFormatterTupleBufferRef(inputFormatterConfig, memoryProvider), inputSchema->getFieldNames());
        }
    }
    return ScanPhysicalOperator(memoryProvider, inputSchema->getFieldNames());
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

/// Helper function to add a default emit operator
/// This is used only when the wrapped operator does not already provide an emit
/// @note Once we have refactored the memory layout and schema we can get rid of the configured buffer size.
/// Do not add further parameters here that should be part of the QueryExecutionConfiguration.
void addDefaultEmit(const std::shared_ptr<Pipeline>& pipeline, const PhysicalOperatorWrapper& wrappedOp, uint64_t configuredBufferSize)
{
    PRECONDITION(pipeline->isOperatorPipeline(), "Only add emit physical operator to operator pipelines");
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
    uint64_t configuredBufferSize,
    const std::string& outputFormat)
{
    PRECONDITION(pipeline->isOperatorPipeline(), "Only add emit physical operator to operator pipelines");
    const auto& schema = wrappedOp.getOutputSchema();
    INVARIANT(schema.has_value(), "Wrapped operator has no output schema");

    const auto bufferRef = LowerSchemaProvider::lowerSchemaWithOutputFormat(configuredBufferSize, schema.value(), outputFormat);

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
    uint64_t configuredBufferSize)
{
    /// Check if we've already seen this operator
    const OperatorId opId = opWrapper->getPhysicalOperator().getId();
    if (const auto it = pipelineMap.find(opId); it != pipelineMap.end())
    {
        if (prevOpWrapper and prevOpWrapper->getPipelineLocation() != PhysicalOperatorWrapper::PipelineLocation::EMIT)
        {
            /// If the operator is a sink we might have to create an output formatter
            if (auto sink = opWrapper->getPhysicalOperator().tryGet<SinkPhysicalOperator>())
            {
                auto outputFormat = toUpperCase(sink->getDescriptor().getFormatType());
                if (outputFormat != "NATIVE")
                {
                    addOutputFormattingEmit(currentPipeline, *prevOpWrapper, configuredBufferSize, std::string(outputFormat));
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
        if (prevOpWrapper && prevOpWrapper->getPipelineLocation() != PhysicalOperatorWrapper::PipelineLocation::EMIT)
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
        for (auto& child : opWrapper->getChildren())
        {
            buildPipelineRecursively(child, opWrapper, newPipelinePtr, pipelineMap, PipelinePolicy::Continue, configuredBufferSize);
        }
        return;
    }

    /// Case 2: Custom Emit – if the operator is explicitly an emit,
    /// it should close the pipeline without adding a default emit
    if (opWrapper->getPipelineLocation() == PhysicalOperatorWrapper::PipelineLocation::EMIT)
    {
        if (not prevOpWrapper || prevOpWrapper->getPipelineLocation() == PhysicalOperatorWrapper::PipelineLocation::EMIT)
        {
            /// If the current operator is an emit operator and the prev operator was also an emit operator, we need to add a scan before the
            /// current operator to create a new pipeline
            auto newPipeline = createNewPipelineWithScan(currentPipeline, pipelineMap, *opWrapper, configuredBufferSize);
            if (opWrapper->getHandler().has_value())
            {
                /// Create an operator handler for the custom emit operator
                const OperatorHandlerId operatorHandlerIndex = opWrapper->getHandlerId().value();
                newPipeline->getOperatorHandlers().emplace(operatorHandlerIndex, opWrapper->getHandler().value());
            }

            for (auto& child : opWrapper->getChildren())
            {
                buildPipelineRecursively(child, opWrapper, newPipeline, pipelineMap, PipelinePolicy::ForceNew, configuredBufferSize);
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
                buildPipelineRecursively(child, opWrapper, currentPipeline, pipelineMap, PipelinePolicy::ForceNew, configuredBufferSize);
            }
        }

        return;
    }

    /// Case 3: Sink Operator – treat sinks as pipeline breakers
    if (auto sink = opWrapper->getPhysicalOperator().tryGet<SinkPhysicalOperator>())
    {
        const auto sinkFormat = toUpperCase(sink->getDescriptor().getFormatType());
        if (currentPipeline->isSourcePipeline())
        {
            const auto sourceFormat = toUpperCase(
                currentPipeline->getRootOperator().get<SourcePhysicalOperator>().getDescriptor().getParserConfig().parserType);

            /// Add a formatting pipeline if the source-sink pipelines do not simply forward natively formatted data
            /// Otherwise, even if both formats are, e.g., 'CSV', the source 'blindly' ingest buffers until they are full, meaning buffers
            /// may start and end with a cut-off tuples (rows in the CSV case)
            /// The sink would output these buffers (out of order if the engine uses multiple threads), producing malformed data
            if (not(sourceFormat == "NATIVE" and sinkFormat == "NATIVE"))
            {
                const auto sourcePipeline = std::make_shared<Pipeline>(createScanOperator(
                    *currentPipeline, opWrapper->getInputSchema(), opWrapper->getInputMemoryLayoutType(), configuredBufferSize));
                currentPipeline->addSuccessor(sourcePipeline, currentPipeline);

                if (sinkFormat == "NATIVE")
                {
                    addDefaultEmit(sourcePipeline, *opWrapper, configuredBufferSize);
                }
                else
                {
                    /// The output format is treated in a case sensitive manner, since it serves as a key to the output formatter registry.
                    /// That's why we cannot pass the upper case sinkFormat.
                    addOutputFormattingEmit(
                        sourcePipeline, *opWrapper, configuredBufferSize, std::string(sink->getDescriptor().getFormatType()));
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
        /// Add emit first if there is one needed
        if (prevOpWrapper and prevOpWrapper->getPipelineLocation() != PhysicalOperatorWrapper::PipelineLocation::EMIT)
        {
            if (sinkFormat == "NATIVE")
            {
                addDefaultEmit(currentPipeline, *prevOpWrapper, configuredBufferSize);
            }
            else
            {
                addOutputFormattingEmit(
                    currentPipeline, *prevOpWrapper, configuredBufferSize, std::string(sink->getDescriptor().getFormatType()));
            }
        }
        const auto newPipeline = std::make_shared<Pipeline>(*sink);
        currentPipeline->addSuccessor(newPipeline, currentPipeline);
        auto newPipelinePtr = currentPipeline->getSuccessors().back();
        pipelineMap.emplace(opId, newPipelinePtr);
        for (auto& child : opWrapper->getChildren())
        {
            buildPipelineRecursively(child, opWrapper, newPipelinePtr, pipelineMap, PipelinePolicy::Continue, configuredBufferSize);
        }
        return;
    }

    /// Case 4: Forced new pipeline (pipeline breaker) for fusible operators
    if (policy == PipelinePolicy::ForceNew)
    {
        if (prevOpWrapper and prevOpWrapper->getPipelineLocation() != PhysicalOperatorWrapper::PipelineLocation::EMIT)
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
        for (auto& child : opWrapper->getChildren())
        {
            buildPipelineRecursively(child, opWrapper, newPipelinePtr, pipelineMap, PipelinePolicy::Continue, configuredBufferSize);
        }
        return;
    }

    /// Case 5: Fusible operator – add it to the current pipeline
    if (prevOpWrapper->getPipelineLocation() == PhysicalOperatorWrapper::PipelineLocation::EMIT)
    {
        /// If the current operator is a fusible operator and the prev operator was an emit operator, we need to add a scan before the
        /// current operator to create a new pipeline.
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
        for (auto& child : opWrapper->getChildren())
        {
            buildPipelineRecursively(child, opWrapper, currentPipeline, pipelineMap, PipelinePolicy::Continue, configuredBufferSize);
        }
    }
}

}

std::shared_ptr<PipelinedQueryPlan> apply(const PhysicalPlan& physicalPlan)
{
    const uint64_t configuredBufferSize = physicalPlan.getOperatorBufferSize();
    auto pipelinedPlan = std::make_shared<PipelinedQueryPlan>(physicalPlan.getQueryId(), physicalPlan.getExecutionMode());

    OperatorPipelineMap pipelineMap;

    for (const auto& rootWrapper : physicalPlan.getRootOperators())
    {
        auto rootPipeline = std::make_shared<Pipeline>(rootWrapper->getPhysicalOperator());
        const auto opId = rootWrapper->getPhysicalOperator().getId();
        pipelineMap.emplace(opId, rootPipeline);
        pipelinedPlan->addPipeline(rootPipeline);

        for (const auto& child : rootWrapper->getChildren())
        {
            buildPipelineRecursively(child, nullptr, rootPipeline, pipelineMap, PipelinePolicy::ForceNew, configuredBufferSize);
        }
    }

    NES_DEBUG("Constructed pipeline plan with {} root pipelines.\n{}", pipelinedPlan->getPipelines().size(), *pipelinedPlan);
    return pipelinedPlan;
}
}

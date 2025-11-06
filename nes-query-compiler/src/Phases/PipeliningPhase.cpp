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
#include <unordered_map>
#include <utility>

#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/FormatScanPhysicalOperator.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <EmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <PhysicalPlan.hpp>
#include <Pipeline.hpp>
#include <PipelinedQueryPlan.hpp>
#include <SinkPhysicalOperator.hpp>
#include "InputFormatters/InputFormatterProvider.hpp"
#include "Operators/Sources/SourceDescriptorLogicalOperator.hpp"
#include "Util/Strings.hpp"
#include "SelectionPhysicalOperator.hpp"

namespace NES::QueryCompilation::PipeliningPhase
{

namespace
{

using OperatorPipelineMap = std::unordered_map<OperatorId, std::shared_ptr<Pipeline>>;

std::vector<Record::RecordFieldIdentifier>
computeRequiredScanProjections(const PhysicalOperatorWrapper& wrappedOp) {
    // Look at the first operator in the upcoming pipeline segment. If it’s a Selection, use its requirements.
    std::set<Record::RecordFieldIdentifier> allRequiredFields;
    if (auto sel = wrappedOp.getPhysicalOperator().tryGet<NES::SelectionPhysicalOperator>()) {
        const auto& v = sel->getRequiredFields();
        if (!v.empty()) { allRequiredFields.insert(v.begin(), v.end()); }
    }
    if (!allRequiredFields.empty())
    {
        return std::vector(allRequiredFields.begin(), allRequiredFields.end());
    }

    // Fallback to all fields
    auto schema = wrappedOp.getInputSchema();
    INVARIANT(schema.has_value(), "Wrapped operator has no input schema");
    return schema->getFieldNames();
}

/// Helper function to add a default scan operator
/// This is used onl y when the wrapped operator does not already provide a scan
/// @note Once we have refactored the memory layout and schema we can get rid of the configured buffer size.
/// Do not add further parameters here that should be part of the QueryExecutionConfiguration.
void addDefaultScan(
    const std::shared_ptr<Pipeline>& prevPipeline,
    const std::shared_ptr<Pipeline>& currentPipeline,
    const PhysicalOperatorWrapper& wrappedOp,
    uint64_t configuredBufferSize)
{
    PRECONDITION(currentPipeline->isOperatorPipeline(), "Only add scan physical operator to operator pipelines");
    auto schema = wrappedOp.getInputSchema();
    INVARIANT(schema.has_value(), "Wrapped operator has no input schema");

    /// Prepend the default scan operator.
    const auto isFirstOperatorAfterSource = prevPipeline->isSourcePipeline();
    auto inputFormatterTaskPipeline = [&prevPipeline, &schema, isFirstOperatorAfterSource]()
    {
        if (isFirstOperatorAfterSource)
        {
            const auto inputFormatterConfig
                = prevPipeline->getRootOperator().get<SourcePhysicalOperator>().getDescriptor().getParserConfig();
            return InputFormatters::InputFormatterProvider::provideInputFormatterTask(
                OriginId(OriginId::INITIAL), schema.value(), inputFormatterConfig);
        }
        return InputFormatters::InputFormatterProvider::provideInputFormatterTask(
            OriginId(OriginId::INITIAL), schema.value(), ParserConfig{.parserType = "Native", .tupleDelimiter = "", .fieldDelimiter = ""});
    }();

    auto requiredProjections = computeRequiredScanProjections(wrappedOp);
    currentPipeline->prependOperator(
        FormatScanPhysicalOperator(schema->getFieldNames(), std::move(inputFormatterTaskPipeline), configuredBufferSize, false, requiredProjections));
}

/// Creates a new pipeline that contains a scan followed by the wrappedOpAfterScan. The newly created pipeline is a successor of the prevPipeline
std::shared_ptr<Pipeline> createNewPiplineWithScan(
    const std::shared_ptr<Pipeline>& prevPipeline,
    OperatorPipelineMap& pipelineMap,
    const PhysicalOperatorWrapper& wrappedOpAfterScan,
    uint64_t configuredBufferSize)
{
    auto schema = wrappedOpAfterScan.getInputSchema();
    INVARIANT(schema.has_value(), "Wrapped operator has no input schema");

    const auto isFirstOperatorAfterSource = prevPipeline->isSourcePipeline();
    auto inputFormatterTaskPipeline = [&prevPipeline, &schema, isFirstOperatorAfterSource]()
    {
        if (isFirstOperatorAfterSource)
        {
            const auto inputFormatterConfig
                = prevPipeline->getRootOperator().get<SourcePhysicalOperator>().getDescriptor().getParserConfig();
            return InputFormatters::InputFormatterProvider::provideInputFormatterTask(
                OriginId(OriginId::INITIAL), schema.value(), inputFormatterConfig);
        }
        return InputFormatters::InputFormatterProvider::provideInputFormatterTask(
            OriginId(OriginId::INITIAL), schema.value(), ParserConfig{.parserType = "Native", .tupleDelimiter = "", .fieldDelimiter = ""});
    }();

    std::vector<Record::RecordFieldIdentifier> requiredFields;
    const auto newPipeline = std::make_shared<Pipeline>(FormatScanPhysicalOperator(
        schema->getFieldNames(), std::move(inputFormatterTaskPipeline), configuredBufferSize, isFirstOperatorAfterSource, requiredFields));
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
    auto schema = wrappedOp.getOutputSchema();
    INVARIANT(schema.has_value(), "Wrapped operator has no output schema");

    auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(configuredBufferSize, schema.value());
    const auto memoryProvider = std::make_shared<Interface::MemoryProvider::RowTupleBufferMemoryProvider>(layout);
    /// Create an operator handler for the emit
    const OperatorHandlerId operatorHandlerIndex = getNextOperatorHandlerId();
    pipeline->getOperatorHandlers().emplace(operatorHandlerIndex, std::make_shared<EmitOperatorHandler>());
    pipeline->appendOperator(EmitPhysicalOperator(operatorHandlerIndex, memoryProvider));
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
    // Todo: given two sources, followed by a union and a sink
    // - Union in front of FormatScan causes below if to trigger and to add the same FormatScan pipeline to the other source
    // - but a FormatScan cannot be shared by sources (raw value parser)
    // - somehow need to create new pipeline for FormatScan, but at the same time make sure the pipelines share the same following pipeline and
    //   that emits are set correctly
    // and not currentPipeline->getRootOperator().tryGet<FormatScanPhysicalOperator>()
    // and not it->second->getRootOperator().tryGet<FormatScanPhysicalOperator>() <-- this fulfills the above, but also reuses the emit, ledaing to an emit without operator handler
    if (const auto it = pipelineMap.find(opId); it != pipelineMap.end())
    {
        if (prevOpWrapper and prevOpWrapper->getPipelineLocation() != PhysicalOperatorWrapper::PipelineLocation::EMIT)
        {
            addDefaultEmit(currentPipeline, *prevOpWrapper, configuredBufferSize);
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
        if (prevOpWrapper->getPipelineLocation() == PhysicalOperatorWrapper::PipelineLocation::EMIT)
        {
            /// If the current operator is an emit operator and the prev operator was also an emit operator, we need to add a scan before the
            /// current operator to create a new pipeline
            auto newPipeline = createNewPiplineWithScan(currentPipeline, pipelineMap, *opWrapper, configuredBufferSize);
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
        if (currentPipeline->isSourcePipeline())
        {
            /// If the immediate successor of a source pipeline is a sink pipeline, inject a scan-emit pipeline.
            /// (We can't genenally skip the scan-emit, even if the formats match, since the sink receives the buffers asynchronously
            ///  and would, without sorting, write out garabage. We could insert a sorting operator, or for sources with a dedicated thread,
            ///  attach the sink directly to the source, to guarantee order (the source should just forward buffers)).
            auto schema = opWrapper->getInputSchema();
            INVARIANT(schema.has_value(), "Wrapped operator has no input schema");

            auto inputFormatterTaskPipeline = [&currentPipeline, &schema]()
            {
                const auto inputFormatterConfig
                    = currentPipeline->getRootOperator().get<SourcePhysicalOperator>().getDescriptor().getParserConfig();
                return InputFormatters::InputFormatterProvider::provideInputFormatterTask(
                    OriginId(OriginId::INITIAL), schema.value(), inputFormatterConfig);
            }();

            std::vector<Record::RecordFieldIdentifier> requiredFields;
            const auto sourcePipeline = std::make_shared<Pipeline>(
                FormatScanPhysicalOperator(schema->getFieldNames(), std::move(inputFormatterTaskPipeline), configuredBufferSize, true, requiredFields));
            currentPipeline->addSuccessor(sourcePipeline, currentPipeline);

            addDefaultEmit(sourcePipeline, *opWrapper, configuredBufferSize);

            INVARIANT(sourcePipeline->getRootOperator().getChild().has_value(), "Scan operator requires at least an emit as child.");
            const auto emitOperatorId = sourcePipeline->getRootOperator().getChild().value().getId();
            pipelineMap[emitOperatorId] = sourcePipeline;

            const auto sinkPipeline = std::make_shared<Pipeline>(*sink);
            sourcePipeline->addSuccessor(sinkPipeline, sourcePipeline);
            auto sinkPipelinePtr = sourcePipeline->getSuccessors().back();
            pipelineMap.emplace(opId, sinkPipelinePtr);
            return;
        }
        /// Add emit first if there is one needed
        if (prevOpWrapper and prevOpWrapper->getPipelineLocation() != PhysicalOperatorWrapper::PipelineLocation::EMIT)
        {
            addDefaultEmit(currentPipeline, *prevOpWrapper, configuredBufferSize);
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
        currentPipeline->addSuccessor(newPipeline, currentPipeline);
        const auto newPipelinePtr = currentPipeline->getSuccessors().back();
        pipelineMap[opId] = newPipelinePtr;
        addDefaultScan(currentPipeline, newPipelinePtr, *opWrapper, configuredBufferSize);
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
        createNewPiplineWithScan(currentPipeline, pipelineMap, *opWrapper, configuredBufferSize);
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

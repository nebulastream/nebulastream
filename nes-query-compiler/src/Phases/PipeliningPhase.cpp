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

#include <EmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <memory>
#include <utility>
#include <variant>
#include <PhysicalOperator.hpp>
#include <Phases/PipeliningPhase.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalPlan.hpp>
#include <Pipeline.hpp>
#include <PipelinedQueryPlan.hpp>
#include <ScanPhysicalOperator.hpp>
#include <SinkPhysicalOperator.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>

namespace NES::QueryCompilation::PipeliningPhase
{

namespace {

using OperatorPipelineMap = std::unordered_map<OperatorId, std::shared_ptr<Pipeline>>;

/// Helper function to add a default scan operator
/// This is used only when the wrapped operator does not already provide a scan
void addDefaultScan(std::shared_ptr<Pipeline> pipeline, const PhysicalOperatorWrapper& wrappedOp)
{
    PRECONDITION(pipeline->isOperatorPipeline(), "Only add scan physical operator to operator pipelines");
    constexpr uint64_t bufferSize = 100; // TODO: adjust buffer size as needed
    auto schema = wrappedOp.inputSchema;
    INVARIANT(schema.has_value(), "Wrapped operator has no input schema");

    auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(schema.value(), bufferSize);
    auto memoryProvider = std::make_shared<RowTupleBufferMemoryProvider>(layout);
    // Prepend the default scan operator.
    pipeline->prependOperator(ScanPhysicalOperator(memoryProvider, schema->getFieldNames()));
}

/// Helper function to add a default emit operator
/// This is used only when the wrapped operator does not already provide an emit
void addDefaultEmit(std::shared_ptr<Pipeline> pipeline, const PhysicalOperatorWrapper& wrappedOp)
{
    PRECONDITION(pipeline->isOperatorPipeline(), "Only add emit physical operator to operator pipelines");
    constexpr uint64_t bufferSize = 100; // TODO: adjust buffer size as needed
    auto schema = wrappedOp.outputSchema;
    INVARIANT(schema.has_value(), "Wrapped operator has no output schema");

    auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(schema.value(), bufferSize);
    auto memoryProvider = std::make_shared<RowTupleBufferMemoryProvider>(layout);
    // Create an operator handler for the emit
    OperatorHandlerId operatorHandlerIndex = getNextOperatorHandlerId();
    pipeline->operatorHandlers.emplace(operatorHandlerIndex, std::make_shared<EmitOperatorHandler>());
    pipeline->appendOperator(EmitPhysicalOperator(operatorHandlerIndex, memoryProvider));
}

void buildPipelineRecursive(
    const std::shared_ptr<PhysicalOperatorWrapper>& opWrapper,
    const std::shared_ptr<PhysicalOperatorWrapper>& prevOpWrapper,
    std::shared_ptr<Pipeline> currentPipeline,
    OperatorPipelineMap& pipelineMap,
    bool forceNewPipeline = false)
{
    OperatorId opId = opWrapper->physicalOperator.getId();

    /// Check if we have already see the operator
    if (auto it = pipelineMap.find(opId); it != pipelineMap.end())
    {
        // Instead of doing nothing, make sure the existing pipeline is linked as a successor.
        // (Optionally, you may wish to check to avoid duplicate insertions.)
        currentPipeline->successorPipelines.push_back(it->second);
        return;
    }

    /// Case 1: Custom Scan – if the wrapped operator explicitly acts as a scan,
    /// then it should open its own pipeline. No default scan is needed
    if (opWrapper->isScan)
    {
        // Add emit first if there is one needed
        if (prevOpWrapper && not prevOpWrapper->isEmit)
        {
            addDefaultEmit(currentPipeline, *prevOpWrapper);
        }
        auto newPipeline = std::make_shared<Pipeline>(opWrapper->physicalOperator);
        if (opWrapper->handler && opWrapper->handlerId)
        {
            newPipeline->operatorHandlers.emplace(opWrapper->handlerId.value(), opWrapper->handler.value());
        }
        pipelineMap[opId] = newPipeline;
        currentPipeline->successorPipelines.push_back(newPipeline);
        auto newPipelinePtr = currentPipeline->successorPipelines.back();
        for (const auto& child : opWrapper->children)
        {
            buildPipelineRecursive(child, opWrapper, newPipelinePtr, pipelineMap);
        }
        return;
    }

    /// Case 2: Custom Emit – if the operator is explicitly an emit,
    /// it should close the pipeline without adding a default emit
    if (opWrapper->isEmit)
    {
        currentPipeline->appendOperator(opWrapper->physicalOperator);
        if (opWrapper->handler && opWrapper->handlerId)
        {
            currentPipeline->operatorHandlers.emplace(opWrapper->handlerId.value(), opWrapper->handler.value());
        }
        pipelineMap[opId] = currentPipeline;
        for (const auto& child : opWrapper->children)
        {
            buildPipelineRecursive(child, opWrapper, currentPipeline, pipelineMap, true);
        }
        return;
    }

    /// Case 3: Sink Operator – treat sinks as pipeline breakers
    if (auto sink = opWrapper->physicalOperator.tryGet<SinkPhysicalOperator>())
    {
        // Add emit first if there is one needed
        if (prevOpWrapper && not prevOpWrapper->isEmit)
        {
            addDefaultEmit(currentPipeline, *prevOpWrapper);
        }

        auto newPipeline = std::make_shared<Pipeline>(*sink);
        currentPipeline->successorPipelines.push_back(newPipeline);
        auto newPipelinePtr = currentPipeline->successorPipelines.back();
        pipelineMap[opId] = newPipelinePtr;
        for (const auto& child : opWrapper->children)
        {
            buildPipelineRecursive(child, opWrapper, newPipelinePtr, pipelineMap);
        }
        return;
    }

    /// Case 4: Forced new pipeline (pipeline breaker) for fusible operators
    if (forceNewPipeline)
    {
        if (prevOpWrapper && not prevOpWrapper->isEmit)
        {
            addDefaultEmit(currentPipeline, *opWrapper);
        }
        auto newPipeline = std::make_shared<Pipeline>(opWrapper->physicalOperator);
        currentPipeline->successorPipelines.push_back(newPipeline);
        auto newPipelinePtr = currentPipeline->successorPipelines.back();
        pipelineMap[opId] = newPipelinePtr;

        addDefaultScan(newPipelinePtr, *opWrapper);

        for (const auto& child : opWrapper->children)
        {
            buildPipelineRecursive(child, opWrapper, newPipelinePtr, pipelineMap);
        }
        return;
    }

    /// Case 5: Fusible operator – add it to the current pipeline
    currentPipeline->appendOperator(opWrapper->physicalOperator);
    if (opWrapper->handler && opWrapper->handlerId)
    {
        currentPipeline->operatorHandlers.emplace(opWrapper->handlerId.value(), opWrapper->handler.value());
    }
    if (opWrapper->children.empty())
    {
        /// End of branch: if the operator doesn't already close with a custom emit,
        /// add a default emit
        addDefaultEmit(currentPipeline, *opWrapper);
    }
    else
    {
        /// Continue processing children within the current pipeline
        for (const auto& child : opWrapper->children)
        {
            buildPipelineRecursive(child, opWrapper, currentPipeline, pipelineMap);
        }
    }
}

}

std::shared_ptr<PipelinedQueryPlan> apply(PhysicalPlan physicalPlan)
{
    auto pipelinedPlan = std::make_shared<PipelinedQueryPlan>(physicalPlan.queryId);

    OperatorPipelineMap pipelineMap;

    for (const auto& rootWrapper : physicalPlan.rootOperators)
    {
        OperatorId opId = rootWrapper->physicalOperator.getId();

        auto rootPipeline = std::make_shared<Pipeline>(rootWrapper->physicalOperator);
        pipelineMap[opId] = rootPipeline;
        pipelinedPlan->pipelines.push_back(rootPipeline);

        for (const auto& child : rootWrapper->children)
        {
            buildPipelineRecursive(child, nullptr, rootPipeline, pipelineMap, true);
        }
    }

    NES_DEBUG("Constructed pipeline plan with {} root pipelines.", pipelinedPlan->pipelines.size());
    std::cout << pipelinedPlan->toString() << "\n";
    return pipelinedPlan;
}
}


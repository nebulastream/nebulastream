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

#include <DefaultEmitPhysicalOperator.hpp>
#include <DefaultScanPhysicalOperator.hpp>
#include <EmitOperatorHandler.hpp>
#include <memory>
#include <utility>
#include <variant>
#include <Abstract/PhysicalOperator.hpp>
#include <Phases/PipeliningPhase.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalPlan.hpp>
#include <Pipeline.hpp>
#include <PipelinedQueryPlan.hpp>
#include <SinkPhysicalOperator.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>

namespace NES::QueryCompilation::PipeliningPhase
{

namespace {

/// Helper function to add a default scan operator.
/// This is used only when the wrapped operator does not already provide a scan.
void addDefaultScan(Pipeline* pipeline, const PhysicalOperatorWrapper& wrappedOp) {
    PRECONDITION(pipeline->isOperatorPipeline(), "Only add scan physical operator to operator pipelines");
    constexpr uint64_t bufferSize = 100; // TODO: adjust buffer size as needed
    auto schema = wrappedOp.inputSchema;
    INVARIANT(schema.has_value(), "Wrapped operator has no input schema");

    auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(schema.value(), bufferSize);
    auto memoryProvider = std::make_shared<RowTupleBufferMemoryProvider>(layout);
    // Prepend the default scan operator.
    pipeline->prependOperator(DefaultScanPhysicalOperator(memoryProvider, schema->getFieldNames()));
}

/// Helper function to add a default emit operator.
/// This is used only when the wrapped operator does not already provide an emit.
void addDefaultEmit(Pipeline* pipeline, const PhysicalOperatorWrapper& wrappedOp) {
    PRECONDITION(pipeline->isOperatorPipeline(), "Only add emit physical operator to operator pipelines");
    constexpr uint64_t bufferSize = 100; // TODO: adjust buffer size as needed
    auto schema = wrappedOp.outputSchema;
    INVARIANT(schema.has_value(), "Wrapped operator has no output schema");

    auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(schema.value(), bufferSize);
    auto memoryProvider = std::make_shared<RowTupleBufferMemoryProvider>(layout);
    // Create an operator handler for the emit.
    OperatorHandlerId operatorHandlerIndex = getNextOperatorHandlerId();
    pipeline->operatorHandlers.emplace(operatorHandlerIndex, std::make_shared<EmitOperatorHandler>());
    pipeline->appendOperator(DefaultEmitPhysicalOperator(operatorHandlerIndex, memoryProvider));
}

void buildPipelineRecursive(
    const std::shared_ptr<PhysicalOperatorWrapper>& opWrapper,
    const std::shared_ptr<PhysicalOperatorWrapper>& prevOpWrapper,
    Pipeline* currentPipeline,
    bool forceNewPipeline = false)
{
    /// Case 1: Custom Scan – if the wrapped operator explicitly acts as a scan,
    /// then it should open its own pipeline. No default scan is needed.
    if (opWrapper->isScan) {
        // Add emit first if there is one needed
        if(not prevOpWrapper->isEmit)
        {
            addDefaultEmit(currentPipeline, *prevOpWrapper);
        }
        auto newPipeline = std::make_unique<Pipeline>(opWrapper->physicalOperator);
        if (opWrapper->handler && opWrapper->handlerId) {
            newPipeline->operatorHandlers.emplace(opWrapper->handlerId.value(), opWrapper->handler.value());
        }
        currentPipeline->successorPipelines.push_back(std::move(newPipeline));
        Pipeline* newPipelinePtr = currentPipeline->successorPipelines.back().get();
        // Process children in a new pipeline (force new pipeline for each branch).
        for (const auto& child : opWrapper->children) {
            buildPipelineRecursive(child, opWrapper, newPipelinePtr, true);
        }
        return;
    }

    /// Case 2: Custom Emit – if the operator is explicitly an emit,
    /// it should close the pipeline without adding a default emit.
    if (opWrapper->isEmit) {
        currentPipeline->appendOperator(opWrapper->physicalOperator);
        if (opWrapper->handler && opWrapper->handlerId) {
            currentPipeline->operatorHandlers.emplace(opWrapper->handlerId.value(), opWrapper->handler.value());
        }
        for (const auto& child : opWrapper->children) {
            buildPipelineRecursive(child, opWrapper, currentPipeline, true);
        }
        return;
    }

    /// Case 3: Sink Operator – treat sinks as pipeline breakers.
    if (auto sink = opWrapper->physicalOperator.tryGet<SinkPhysicalOperator>()) {
        // Add emit first if there is one needed
        if(prevOpWrapper)
        {
            addDefaultEmit(currentPipeline, *prevOpWrapper);
        }

        auto newPipeline = std::make_unique<Pipeline>(*sink);
        currentPipeline->successorPipelines.push_back(std::move(newPipeline));
        Pipeline* newPipelinePtr = currentPipeline->successorPipelines.back().get();
        for (const auto& child : opWrapper->children) {
            buildPipelineRecursive(child, opWrapper, newPipelinePtr);
        }
        return;
    }

    /// Case 4: Forced new pipeline (pipeline breaker) for fusible operators.
    if (forceNewPipeline) {
        auto newPipeline = std::make_unique<Pipeline>(opWrapper->physicalOperator);
        currentPipeline->successorPipelines.push_back(std::move(newPipeline));
        Pipeline* newPipelinePtr = currentPipeline->successorPipelines.back().get();

        addDefaultScan(newPipelinePtr, *opWrapper);

        for (const auto& child : opWrapper->children) {
            buildPipelineRecursive(child, opWrapper, newPipelinePtr);
        }
        return;
    }

    /// Case 5: Fusible operator – add it to the current pipeline.
    currentPipeline->appendOperator(opWrapper->physicalOperator);
    if (opWrapper->handler && opWrapper->handlerId) {
        currentPipeline->operatorHandlers.emplace(opWrapper->handlerId.value(), opWrapper->handler.value());
    }
    if (opWrapper->children.empty()) {
        /// End of branch: if the operator doesn't already close with a custom emit,
        /// add a default emit.
        addDefaultEmit(currentPipeline, *opWrapper);
    } else {
        /// Continue processing children within the current pipeline.
        for (const auto& child : opWrapper->children) {
            buildPipelineRecursive(child, opWrapper, currentPipeline);
        }
    }
}

}

/// The apply function creates a PipelinedQueryPlan from a PhysicalPlan.
std::shared_ptr<PipelinedQueryPlan> apply(std::unique_ptr<PhysicalPlan> physicalPlan) {
    auto pipelinedPlan = std::make_shared<PipelinedQueryPlan>(physicalPlan->queryId);

    /// For each root operator in the physical plan, create a root pipeline.
    for (const auto& rootWrapper : physicalPlan->rootOperators) {
        auto rootPipeline = std::make_unique<Pipeline>(rootWrapper->physicalOperator);
        Pipeline* rootPipelinePtr = rootPipeline.get();
        pipelinedPlan->pipelines.push_back(std::move(rootPipeline));

        /// Process each child of the root operator.
        /// Here we force a new pipeline for each branch to ensure proper boundaries.
        for (const auto& child : rootWrapper->children) {
            buildPipelineRecursive(child, nullptr, rootPipelinePtr,true);
        }
    }

    std::cout << pipelinedPlan->pipelines.size() << "\n";
    NES_DEBUG("Constructed pipeline plan with {} root pipelines.", pipelinedPlan->pipelines.size());
    std::cout << pipelinedPlan->toString() << "\n";
    return pipelinedPlan;
}
}


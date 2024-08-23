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

#pragma once
#include <atomic>
#include <memory>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/ExecutionResult.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/RuntimeEventListener.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES::Runtime::Execution
{

/**
 * @brief An ExecutablePipeline represents a fragment of an overall query.
 * It can contain multiple operators and the implementation of its computation is defined in the ExecutablePipelineStage.
 * Furthermore, it holds the PipelineExecutionContextPtr and a reference to the next pipeline in the query plan.
 */
class ExecutablePipeline : public Reconfigurable, public Runtime::RuntimeEventListener
{
    /// virtual_enable_shared_from_this necessary for double inheritance of enable_shared_from_this
    using inherited0 = Reconfigurable;
    using inherited1 = Runtime::RuntimeEventListener;

    friend class QueryManager;

    enum class PipelineStatus : uint8_t
    {
        PipelineCreated,
        PipelineRunning,
        PipelineStopped,
        PipelineFailed
    };

public:
    explicit ExecutablePipeline(
        PipelineId pipelineId,
        QueryId queryId,
        QueryManagerPtr queryManager,
        PipelineExecutionContextPtr pipelineExecutionContext,
        ExecutablePipelineStagePtr executablePipelineStage,
        uint32_t numOfProducingPipelines,
        std::vector<SuccessorExecutablePipeline> successorPipelines,
        bool reconfiguration = false);

    static ExecutablePipelinePtr create(
        PipelineId pipelineId,
        QueryId queryId,
        const QueryManagerPtr& queryManager,
        const PipelineExecutionContextPtr& pipelineExecutionContext,
        const ExecutablePipelineStagePtr& executablePipelineStage,
        uint32_t numOfProducingPipelines,
        const std::vector<SuccessorExecutablePipeline>& successorPipelines,
        bool reconfiguration = false);

    /// Execute a pipeline stage
    ExecutionResult execute(Memory::TupleBuffer& inputBuffer, WorkerContextRef workerContext);

    [[nodiscard]] bool setup(const QueryManagerPtr& queryManager, const Memory::BufferManagerPtr& bufferManager);
    [[nodiscard]] bool start();
    [[nodiscard]] bool stop(QueryTerminationType terminationType);
    [[nodiscard]] bool fail();

    [[nodiscard]] bool isRunning() const;

    [[nodiscard]] PipelineId getPipelineId() const;
    [[nodiscard]] QueryId getQueryId() const;

    /// returns true if the pipeline contains a function pointer for a reconfiguration task
    [[nodiscard]] bool isReconfiguration() const;

    /// Reconfigure callback called upon a reconfiguration
    void reconfigure(ReconfigurationMessage& task, WorkerContext& context) override;

    /// final reconfigure callback called upon a reconfiguration
    void postReconfigurationCallback(ReconfigurationMessage& task) override;

    const std::vector<SuccessorExecutablePipeline>& getSuccessors() const;

    void onEvent(Runtime::BaseEvent& event) override;
    void onEvent(Runtime::BaseEvent& event, Runtime::WorkerContextRef);

    PipelineExecutionContextPtr getContext() { return pipelineContext; };

private:
    const PipelineId pipelineId;
    const QueryId queryId;
    QueryManagerPtr queryManager;
    ExecutablePipelineStagePtr executablePipelineStage;
    PipelineExecutionContextPtr pipelineContext;
    bool reconfiguration;
    std::atomic<PipelineStatus> pipelineStatus;
    std::atomic<uint32_t> activeProducers = 0;
    std::vector<SuccessorExecutablePipeline> successorPipelines;
};

} /// namespace NES::Runtime::Execution

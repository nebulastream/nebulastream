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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_EXECUTABLEPIPELINE_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_EXECUTABLEPIPELINE_HPP_
#include <atomic>
#include <memory>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/ExecutionResult.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES::Runtime::Execution
{

/// An ExecutablePipeline represents a fragment of an overall query.
/// It can contain multiple operators and the implementation of its computation is defined in the ExecutablePipelineStage.
/// Furthermore, it holds the PipelineExecutionContextPtr and a reference to the next pipeline in the query plan.
class ExecutablePipeline : public Reconfigurable
{
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
        SharedQueryId sharedQueryId,
        DecomposedQueryPlanId decomposedQueryPlanId,
        QueryManagerPtr queryManager,
        PipelineExecutionContextPtr pipelineExecutionContext,
        ExecutablePipelineStagePtr executablePipelineStage,
        uint32_t numOfProducingPipelines,
        std::vector<SuccessorExecutablePipeline> successorPipelines,
        bool reconfiguration);

    static ExecutablePipelinePtr create(
        PipelineId pipelineId,
        SharedQueryId sharedQueryId,
        DecomposedQueryPlanId decomposedQueryPlanId,
        const QueryManagerPtr& queryManager,
        const PipelineExecutionContextPtr& pipelineExecutionContext,
        const ExecutablePipelineStagePtr& executablePipelineStage,
        uint32_t numOfProducingPipelines,
        const std::vector<SuccessorExecutablePipeline>& successorPipelines,
        bool reconfiguration = false);

    /// Execute the pipeline stage, either executing a compiled pipeline stage or interpreting a pipelinestage (typically debugging).
    ExecutionResult execute(TupleBuffer& inputBuffer, WorkerContextRef workerContext);

    /// Lowers a pipelinestage, potentially compiling it (if not interpreted).
    bool setup(const QueryManagerPtr& queryManager, const BufferManagerPtr& bufferManager);

    /// Starts a pipeline stage and passes statemanager and local state counter further to the operator handler
    bool start();

    bool stop(QueryTerminationType terminationType);

    bool fail();

    PipelineId getPipelineId() const;

    DecomposedQueryPlanId getDecomposedQueryPlanId() const;

    bool isRunning() const;

    /// returns true if the pipeline contains a function pointer for a reconfiguration task
    bool isReconfiguration() const;

    void reconfigure(ReconfigurationMessage& task, WorkerContext& context) override;

    void postReconfigurationCallback(ReconfigurationMessage& task) override;

    SharedQueryId getSharedQueryId() const;

    const std::vector<SuccessorExecutablePipeline>& getSuccessors() const;

    PipelineExecutionContextPtr getContext() { return pipelineContext; };

private:
    const PipelineId pipelineId;
    const SharedQueryId sharedQueryId;
    const DecomposedQueryPlanId decomposedQueryPlanId;
    QueryManagerPtr queryManager;
    ExecutablePipelineStagePtr executablePipelineStage;
    PipelineExecutionContextPtr pipelineContext;
    bool reconfiguration;
    std::atomic<PipelineStatus> pipelineStatus;
    std::atomic<uint32_t> activeProducers = 0;
    std::vector<SuccessorExecutablePipeline> successorPipelines;
};

} /// namespace NES::Runtime::Execution

#endif /// NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_EXECUTABLEPIPELINE_HPP_

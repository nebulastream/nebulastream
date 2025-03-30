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
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/OperatorHandlerSlices.hpp>
#include <Runtime/ExecutionResult.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/RuntimeEventListener.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/ThreadBarrier.hpp>
#include <atomic>
#include <memory>
#include <variant>
#include <vector>

namespace NES::Runtime::Execution {

/**
 * @brief An ExecutablePipeline represents a fragment of an overall query.
 * It can contain multiple operators and the implementation of its computation is defined in the ExecutablePipelineStage.
 * Furthermore, it holds the PipelineExecutionContextPtr and a reference to the next pipeline in the query plan.
 */
class ExecutablePipeline : public Reconfigurable, public Runtime::RuntimeEventListener {
    // virtual_enable_shared_from_this necessary for double inheritance of enable_shared_from_this
    using inherited0 = Reconfigurable;
    using inherited1 = Runtime::RuntimeEventListener;

    friend class QueryManager;

    enum class PipelineStatus : uint8_t { PipelineCreated, PipelineRunning, PipelineStopped, PipelineFailed };

  public:
    /**
     * @brief Constructor for an executable pipeline.
     * @param pipelineId The Id of this pipeline
     * @param querySubPlanId the id of the query sub plan
     * @param querySubPlanVersion the version of the query sub plan
     * @param queryManager reference to the queryManager
     * @param pipelineContext the pipeline context
     * @param executablePipelineStage the executable pipeline stage
     * @param numOfProducingPipelines number of producing pipelines
     * @param successorPipelines a vector of successor pipelines
     * @param reconfiguration indicates if this is a reconfiguration task. Default = false.
     * @return ExecutablePipelinePtr
     */
    explicit ExecutablePipeline(PipelineId pipelineId,
                                SharedQueryId sharedQueryId,
                                DecomposedQueryId decomposedQueryId,
                                DecomposedQueryPlanVersion decomposedQueryVersion,
                                QueryManagerPtr queryManager,
                                PipelineExecutionContextPtr pipelineExecutionContext,
                                ExecutablePipelineStagePtr executablePipelineStage,
                                uint32_t numOfProducingPipelines,
                                std::vector<SuccessorExecutablePipeline> successorPipelines,
                                bool reconfiguration,
                                bool isMigrationPipeline);

    /**
     * @brief Factory method to create a new executable pipeline.
     * @param pipelineId The Id of this pipeline
     * @param decomposedQueryId the id of the query sub plan
     * @param querySubPlanVersion the version of the query sub plan
     * @param pipelineContext the pipeline context
     * @param executablePipelineStage the executable pipeline stage
     * @param numOfProducingPipelines number of producing pipelines
     * @param successorPipelines a vector of successor pipelines
     * @param reconfiguration indicates if this is a reconfiguration task. Default = false.
     * @return ExecutablePipelinePtr
     */
    static ExecutablePipelinePtr create(PipelineId pipelineId,
                                        SharedQueryId sharedQueryId,
                                        DecomposedQueryId decomposedQueryId,
                                        DecomposedQueryPlanVersion decomposedQueryVersion,
                                        const QueryManagerPtr& queryManager,
                                        const PipelineExecutionContextPtr& pipelineExecutionContext,
                                        const ExecutablePipelineStagePtr& executablePipelineStage,
                                        uint32_t numOfProducingPipelines,
                                        const std::vector<SuccessorExecutablePipeline>& successorPipelines,
                                        bool reconfiguration = false,
                                        bool isMigrationPipeline = false);

    /**
     * @brief Execute a pipeline stage
     * @param inputBuffer: the input buffer on which to execute the pipeline stage
     * @param workerContext: the worker context
     * @return true if no error occurred
     */
    ExecutionResult execute(TupleBuffer& inputBuffer, WorkerContextRef workerContext);

    /**
   * @brief Initialises a pipeline stage
   * @return boolean if successful
   */
    bool setup(const QueryManagerPtr& queryManager, const BufferManagerPtr& bufferManager);

    /**
     *
     * @brief Starts a pipeline stage and passes statemanager and local state counter further to the operator handler
     * @param stateManager pointer to the current state manager
     * @return Success if pipeline stage started 
     */
    bool start();

    bool shouldRecreate();
    bool recreate();

    /**
     * @brief Stops pipeline stage
     * @param terminationType indicates the termination type see @QueryTerminationType
     * @return  Success if pipeline stage stopped
     */
    bool stop(QueryTerminationType terminationType);

    /**
     * @brief Fails pipeline stage
     * @return true if successful
     */
    bool fail();

    /**
    * @brief Get id of pipeline stage
    * @return pipeline id
    */
    PipelineId getPipelineId() const;

    /**
     * @brief Get query sub plan id.
     * @return QuerySubPlanId.
     */
    DecomposedQueryId getDecomposedQueryId() const;

    /**
     * @brief Get query sub plan version.
     * @return QuerySubPlanVersion.
     */
    DecomposedQueryPlanVersion getDecomposedQueryVersion() const;

    /**
     * @brief Checks if this pipeline is running
     * @return true if pipeline is running.
     */
    bool isRunning() const;

    /**
    * @return returns true if the pipeline contains a function pointer for a reconfiguration task
    */
    bool isReconfiguration() const;

    /**
    * @return returns true if the pipeline is migration pipeline
    */
    bool isMigration() const;

    /**
     * @brief reconfigure callback called upon a reconfiguration
     * @param task the reconfig descriptor
     * @param context the worker context
     */
    void reconfigure(ReconfigurationMessage& task, WorkerContext& context) override;

    /**
     * @brief final reconfigure callback called upon a reconfiguration
     * @param task the reconfig descriptor
     */
    void postReconfigurationCallback(ReconfigurationMessage& task) override;

    /**
     * Stops the specified query from a shared StreamJoinOperatorHandler. The slices that already exist will be used to emit the
     * tuples that were already ingested for this query
     * @note only works if this ExecutablePipeline contains one StreamJoinOperatorHandler
     * @param queryId the id of the query
     */
    void stopQueryFromSharedJoin(QueryId queryId);

    /**
     * Adds a query to a StreamJoinOperatorHandler. This will affect all slices that are currently stored and that will be created
     * in the handler.
     * @note only works if this ExecutablePipeline contains one StreamJoinOperatorHandler
     * @param queryId the identifier to remove this from the then shared StreamJoinOperatorHandler
     * @param deploymentTime the time this query was deployed (should be deduced by the system in a later point)
     * @param sharedJoinApproach will determine which method to call to handle sharing the join
     */
    void addQueryToSharedJoin(QueryId queryId, uint64_t deploymentTime, SharedJoinApproach sharedJoinApproach);

    /**
     * @brief Get query plan id.
     * @return QueryId.
     */
    SharedQueryId getSharedQueryId() const;

    /**
     * @brief Gets the successor pipelines
     * @return SuccessorPipelines
     */
    const std::vector<SuccessorExecutablePipeline>& getSuccessors() const;

    /**
     * @brief API method called upon receiving an event (from downstream)
     * @param event
     */
    void onEvent(Runtime::BaseEvent& event) override;

    /**
     * @brief API method called upon receiving an event (from downstream)
     * @param event
     */
    void onEvent(Runtime::BaseEvent& event, Runtime::WorkerContextRef);

    PipelineExecutionContextPtr getContext() { return pipelineContext; };

  private:
    /**
     * @brief Propgates a reconfiguration downstream
     * @param task: the reconfiguraion message to propagate
     */
    void propagateReconfiguration(ReconfigurationMessage& task);

    /**
     * @brief Propgates an end of stream downstream
     * @param task: the EoS message to propagate
     */
    void propagateEndOfStream(ReconfigurationMessage& task);

    void sendBuffers(OperatorHandlerPtr operatorHandler, WorkerContext& context);

    const PipelineId pipelineId;
    const SharedQueryId sharedQueryId;
    const DecomposedQueryId decomposedQueryId;
    const DecomposedQueryPlanVersion decomposedQueryVersion;
    QueryManagerPtr queryManager;
    ExecutablePipelineStagePtr executablePipelineStage;
    PipelineExecutionContextPtr pipelineContext;
    bool reconfiguration;
    std::atomic<bool> isMigrationPipeline;
    ThreadBarrierPtr preSerBarrier = std::make_shared<ThreadBarrier>(4);
    std::condition_variable conditionSerialized;
    std::once_flag serializeOnceFlag;
    std::mutex serializeMtx;
    std::atomic<bool> serialized = false;
    std::atomic<PipelineStatus> pipelineStatus;
    std::atomic<uint32_t> activeProducers = 0;
    std::atomic<uint64_t> isExecuted = 0;
    std::vector<SuccessorExecutablePipeline> successorPipelines;
};

}// namespace NES::Runtime::Execution

#endif// NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_EXECUTABLEPIPELINE_HPP_

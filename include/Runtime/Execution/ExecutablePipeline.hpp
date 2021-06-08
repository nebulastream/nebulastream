/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_NODEENGINE_EXECUTION_EXECUTABLEPIPELINE_H_
#define NES_INCLUDE_NODEENGINE_EXECUTION_EXECUTABLEPIPELINE_H_
#include <Plans/Query/QuerySubPlanId.hpp>
#include <Runtime/ExecutionResult.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/ReconfigurationMessage.hpp>
#include <functional>
#include <memory>
#include <variant>
#include <vector>

namespace NES::Runtime::Execution {

/**
 * @brief An ExecutablePipeline represents a fragment of an overall query.
 * It can contain multiple operators and the implementation of its computation is defined in the ExecutablePipelineStage.
 * Furthermore, it holds the PipelineExecutionContextPtr and a reference to the next pipeline in the query plan.
 */
class ExecutablePipeline : public Reconfigurable {
    enum class PipelineStatus : uint8_t { PipelineCreated, PipelineRunning, PipelineStopped, PipelineFailed };

  public:
    /**
     * @brief Constructor for an executable pipeline.
     * @param pipelineId The Id of this pipeline
     * @param querySubPlanId the id of the query sub plan
     * @param pipelineContext the pipeline context
     * @param executablePipelineStage the executable pipeline stage
     * @param numOfProducingPipelines number of producing pipelines
     * @param successorPipelines a vector of successor pipelines
     * @param reconfiguration indicates if this is a reconfiguration task. Default = false.
     * @return ExecutablePipelinePtr
     */
    explicit ExecutablePipeline(uint64_t pipelineId,
                                QuerySubPlanId qepId,
                                PipelineExecutionContextPtr pipelineExecutionContext,
                                ExecutablePipelineStagePtr executablePipelineStage,
                                uint32_t numOfProducingPipelines,
                                std::vector<SuccessorExecutablePipeline> successorPipelines,
                                bool reconfiguration);

    ~ExecutablePipeline() override;

    /**
     * @brief Factory method to create a new executable pipeline.
     * @param pipelineId The Id of this pipeline
     * @param querySubPlanId the id of the query sub plan
     * @param pipelineContext the pipeline context
     * @param executablePipelineStage the executable pipeline stage
     * @param numOfProducingPipelines number of producing pipelines
     * @param successorPipelines a vector of successor pipelines
     * @param reconfiguration indicates if this is a reconfiguration task. Default = false.
     * @return ExecutablePipelinePtr
     */
    static ExecutablePipelinePtr create(uint64_t pipelineId,
                                        QuerySubPlanId querySubPlanId,
                                        const PipelineExecutionContextPtr& pipelineExecutionContext,
                                        const ExecutablePipelineStagePtr& executablePipelineStage,
                                        uint32_t numOfProducingPipelines,
                                        const std::vector<SuccessorExecutablePipeline>& successorPipelines,
                                        bool reconfiguration = false);

    /**
     * @brief Execute a pipeline stage
     * @param inputBuffer
     * @param workerContext
     * @return true if no error occurred
     */
    ExecutionResult execute(TupleBuffer& inputBuffer, WorkerContextRef workerContext);

    /**
   * @brief Initialises a pipeline stage
   * @return boolean if successful
   */
    bool setup(const QueryManagerPtr& queryManager, const BufferManagerPtr& bufferManager);

    /**
     * @brief Starts a pipeline stage and passes statemanager and local state counter further to the operator handler
     * @param stateManager pointer to the current state manager
     * @return Success if pipeline stage started 
     */
    bool start(const StateManagerPtr& stateManager);

    /**
     * @brief Stops pipeline stage
     * @return
     */
    bool stop();

    /**
    * @brief Get id of pipeline stage
    * @return
    */
    uint64_t getPipelineId() const;

    /**
     * @brief Get query sub plan id.
     * @return QuerySubPlanId.
     */
    QuerySubPlanId getQuerySubPlanId() const;

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
     * @brief atomically increment number of producers for this pipeline
     */
    void incrementProducerCount();

    /**
     * @brief Gets the successor pipelines
     * @return SuccessorPipelines
     */
    const std::vector<SuccessorExecutablePipeline>& getSuccessors() const;

    /**
     * @brief Adds a new successor pipeline
     * @param predecessorPipeline
     */
    void addSuccessor(const SuccessorExecutablePipeline& successorPipeline);

  private:
    const uint64_t pipelineId;
    const QuerySubPlanId querySubPlanId;
    ExecutablePipelineStagePtr executablePipelineStage;
    PipelineExecutionContextPtr pipelineContext;
    bool reconfiguration;
    std::atomic<PipelineStatus> pipelineStatus;
    std::atomic<uint32_t> activeProducers = 0;
    std::atomic<uint32_t> inProgressTasks = 0;
    std::vector<SuccessorExecutablePipeline> successorPipelines;
    void handleSoftStop(ReconfigurationMessage& task,
                        const std::weak_ptr<ExecutableQueryPlan>& targetQep,
                        const std::function<std::any()>& userdataSupplier);
};

}// namespace NES::Runtime::Execution

#endif /* NES_INCLUDE_NODEENGINE_EXECUTION_EXECUTABLEPIPELINE_H_ */

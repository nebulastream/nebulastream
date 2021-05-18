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

#ifndef INCLUDE_NES_NODEENGINE_EXECUTABLEPIPELINE_H_
#define INCLUDE_NES_NODEENGINE_EXECUTABLEPIPELINE_H_
#include <NodeEngine/Execution/ExecutableQueryPlan.hpp>
#include <NodeEngine/ExecutionResult.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <NodeEngine/Reconfigurable.hpp>
#include <NodeEngine/ReconfigurationMessage.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <memory>
#include <variant>
#include <vector>
namespace NES::NodeEngine::Execution {

/**
 * @brief An ExecutablePipeline represents a fragment of an overall query.
 * It can contain multiple operators and the implementation of its computation is defined in the ExecutablePipelineStage.
 * Furthermore, it holds the PipelineExecutionContextPtr and a reference to the next pipeline in the query plan.
 */
class NewExecutablePipeline : public Reconfigurable {
    enum PipelineStatus : uint8_t { PipelineCreated, PipelineRunning, PipelineStopped, PipelineFailed };

  public:
    explicit NewExecutablePipeline(uint32_t pipelineId,
                                   QuerySubPlanId qepId,
                                   PipelineExecutionContextPtr pipelineExecutionContext,
                                   ExecutablePipelineStagePtr executablePipelineStage,
                                   uint32_t numOfProducingPipelines,
                                   std::vector<SuccessorExecutablePipeline> successorPipelines,
                                   bool reconfiguration);

    /**
     * @brief Destructor of an ExecutablePipeline
     */
    ~NewExecutablePipeline();

    /**
     * @brief Factory method to create a new executable pipeline.
     * @param pipelineId The Id of this pipeline
     * @param querySubPlanId the id of the query sub plan
     * @param executablePipelineStage the executable pipeline stage
     * @param pipelineContext the pipeline context
     * @param nextPipelineStage a pointer to the next pipeline
     * @param reconfiguration indicates if this is a reconfiguration task. Default = false.
     * @return ExecutablePipelinePtr
     */
    static NewExecutablePipelinePtr create(uint32_t pipelineId,
                                           QuerySubPlanId qepId,
                                           PipelineExecutionContextPtr pipelineExecutionContext,
                                           ExecutablePipelineStagePtr executablePipelineStage,
                                           uint32_t numOfProducingPipelines,
                                           std::vector<SuccessorExecutablePipeline> successorPipelines,
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
    bool setup(QueryManagerPtr queryManager, BufferManagerPtr bufferManager);

    /**
     * @brief Starts a pipeline stage and passes statemanager further to the operator handler
     * @param stateManager pointer to the current state manager
     * @return Success if pipeline stage started 
     */
    bool start(StateManagerPtr stateManager);

    /**
     * @brief Stops pipeline stage
     * @return
     */
    bool stop();

    /**
    * @brief Get id of pipeline stage
    * @return
    */
    uint32_t getPipeStageId();

    /**
     * @brief Get the parent query id.
     * @return query id.
     */
    QuerySubPlanId getQepParentId() const;

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
     * @brief Gets the successor pipeline
     * @return SuccessorPipeline
     */
    std::vector<SuccessorExecutablePipeline> getSuccessors();

    /**
     * @brief Adds a new successor pipeline
     * @param predecessorPipeline
     */
    void addSuccessor(SuccessorExecutablePipeline predecessorPipeline);

  private:
    uint32_t pipelineStageId;
    QuerySubPlanId qepId;
    ExecutablePipelineStagePtr executablePipelineStage;
    PipelineExecutionContextPtr pipelineContext;
    bool reconfiguration;
    std::atomic<PipelineStatus> pipelineStatus;
    std::atomic<uint32_t> activeProducers;
    std::vector<SuccessorExecutablePipeline> successorPipelines;
};

}// namespace NES::NodeEngine::Execution

#endif /* INCLUDE_NES_NODEENGINE_EXECUTABLEPIPELINE_H_ */

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

#ifndef INCLUDE_PIPELINESTAGE_H_
#define INCLUDE_PIPELINESTAGE_H_
#include <NodeEngine/Execution/ExecutableQueryPlan.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <memory>
#include <vector>

namespace NES::NodeEngine::Execution {

/**
 * @brief An ExecutablePipeline represents a fragment of an overall query.
 * It can contain multiple operators and the implementation of its computation is defined in the ExecutablePipelineStage.
 * Furthermore, it holds the PipelineExecutionContextPtr and a reference to the next pipeline in the query plan.
 */
class ExecutablePipeline {
  public:
    ExecutablePipeline(uint32_t pipelineId, QuerySubPlanId qepId, ExecutablePipelineStagePtr executablePipelineStage,
                       PipelineExecutionContextPtr pipelineContext, ExecutablePipelinePtr nextPipeline, bool reconfiguration);

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
    static ExecutablePipelinePtr create(uint32_t pipelineId, const QuerySubPlanId querySubPlanId,
                                        ExecutablePipelineStagePtr executablePipelineStage,
                                        PipelineExecutionContextPtr pipelineContext,
                                        const ExecutablePipelinePtr nextPipelineStage, bool reconfiguration = false);

    /**
     * @brief Execute a pipeline stage
     * @param inputBuffer
     * @param workerContext
     * @return true if no error occurred
     */
    bool execute(TupleBuffer& inputBuffer, WorkerContextRef workerContext);

    /**
   * @brief Initialises a pipeline stage
   * @return boolean if successful
   */
    bool setup(QueryManagerPtr queryManager, BufferManagerPtr bufferManager);

    /**
     * @brief Starts a pipeline stage
     * @return boolean if successful
     */
    bool start();

    /**
     * @brief Stops pipeline stage
     * @return
     */
    bool stop();

    /**
     * @brief Get next pipeline stage
     * @return
     */
    ExecutablePipelinePtr getNextPipeline();

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

    ~ExecutablePipeline();

    /**
    * @return returns true if the pipeline contains a function pointer for a reconfiguration task
    */
    bool isReconfiguration() const;

    PipelineStageArity getArity();

  private:
    uint32_t pipelineStageId;
    QuerySubPlanId qepId;
    ExecutablePipelineStagePtr executablePipelineStage;
    ExecutablePipelinePtr nextPipeline;
    PipelineExecutionContextPtr pipelineContext;
    bool reconfiguration;
};

}// namespace NES::NodeEngine::Execution

#endif /* INCLUDE_PIPELINESTAGE_H_ */

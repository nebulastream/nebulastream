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

class ExecutablePipeline {
  public:
    ExecutablePipeline(uint32_t pipelineStageId,
                       QuerySubPlanId qepId,
                       ExecutablePipelineStagePtr  executablePipelineStage,
                       PipelineExecutionContextPtr pipelineContext,
                       ExecutablePipelinePtr nextPipeline);

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

    QuerySubPlanId getQepParentId() const;

    ~ExecutablePipeline() = default;

    /**
    * @return returns true if the pipeline contains a function pointer for a reconfiguration task
    */
    bool isReconfiguration() const;

  public:
    static ExecutablePipelinePtr create(uint32_t pipelineStageId, const QuerySubPlanId querySubPlanId,
                                   ExecutablePipelineStagePtr executablePipelineStage, PipelineExecutionContextPtr pipelineContext,
                                   const ExecutablePipelinePtr nextPipelineStage);

  private:
    uint32_t pipelineStageId;
    QuerySubPlanId qepId;
    ExecutablePipelineStagePtr executablePipelineStage;
    ExecutablePipelinePtr nextPipeline;
    PipelineExecutionContextPtr pipelineContext;
    bool reconfiguration;

  public:
    bool hasWindowHandler();
    bool hasJoinHandler();
};

}// namespace NES

#endif /* INCLUDE_PIPELINESTAGE_H_ */

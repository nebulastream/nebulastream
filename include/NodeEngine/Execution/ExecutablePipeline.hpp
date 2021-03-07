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
#include <NodeEngine/Reconfigurable.hpp>
#include <NodeEngine/ReconfigurationMessage.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <memory>
#include <vector>

namespace NES::NodeEngine::Execution {

/**
 * @brief An ExecutablePipeline represents a fragment of an overall query.
 * It can contain multiple operators and the implementation of its computation is defined in the ExecutablePipelineStage.
 * Furthermore, it holds the PipelineExecutionContextPtr and a reference to the next pipeline in the query plan.
 */
class ExecutablePipeline : public Reconfigurable {
  public:
    explicit ExecutablePipeline(uint32_t pipelineId, QuerySubPlanId qepId, ExecutablePipelineStagePtr executablePipelineStage,
                                PipelineExecutionContextPtr pipelineContext, uint32_t numOfProducingPipelines,
                                ExecutablePipelinePtr nextPipeline, SchemaPtr inputSchema, SchemaPtr outputSchema,
                                bool reconfiguration);

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
                                        PipelineExecutionContextPtr pipelineContext, uint32_t numOfProducingPipelines,
                                        const ExecutablePipelinePtr nextPipelineStage, SchemaPtr inputSchema,
                                        SchemaPtr outputSchema, bool reconfiguration = false);

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

    /**
     * @brief Destructor of an ExecutablePipeline
     */
    ~ExecutablePipeline();

    /**
    * @return returns true if the pipeline contains a function pointer for a reconfiguration task
    */
    bool isReconfiguration() const;

    /**
     * @brief Get the arity (Unary/BinaryLeft/BinaryRight) of this pipeline
     * This is necessary to figure out to which side of a binary operator the pipeline belongs
     * @return the arity of this pipeline
     */
    PipelineStageArity getArity();

    /**
     * @brief Get input schema of the pipeline
     * @return pointer to the input schema
     */
    const SchemaPtr& getInputSchema() const;

    /**
     * @brief Get output schema of the pipeline
     * @return pointer to the output schema
     */
    const SchemaPtr& getOutputSchema() const;

    /**
     * @brief methods to print the content of the pipeline
     * @return string containing the generated code as string
     */
    std::string getCodeAsString();

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
    void pin();

  private:
    uint32_t pipelineStageId;
    QuerySubPlanId qepId;
    ExecutablePipelineStagePtr executablePipelineStage;
    ExecutablePipelinePtr nextPipeline;
    PipelineExecutionContextPtr pipelineContext;
    bool reconfiguration;
    SchemaPtr inputSchema;
    SchemaPtr outputSchema;
    std::atomic<bool> isRunning;
    std::atomic<uint32_t> activeProducers;
};

}// namespace NES::NodeEngine::Execution

#endif /* INCLUDE_PIPELINESTAGE_H_ */

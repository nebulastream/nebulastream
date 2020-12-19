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

#ifndef NES_INCLUDE_QUERYCOMPILER_EXECUTABLEPIPELINESTAGE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_EXECUTABLEPIPELINESTAGE_HPP_
#include <NodeEngine/NodeEngineForwaredRefs.hpp>

namespace NES::NodeEngine::Execution {

/**
 * @brief The executable pipeline stage represents the executable part of a an specific pipeline.
 * For instance, during code generation we generate an implementation of this class, which defines all virtual functions.
 */
class ExecutablePipelineStage {
  public:
    explicit ExecutablePipelineStage() = default;
    /**
    * @brief Must be called only once per executable pipeline and initializes the pipeline execution context.
    * e.g, creates the individual operator states -> window handler
    * @param pipelineExecutionContext
    * @return 0 if no error occurred.
    */
    virtual uint32_t setup(PipelineExecutionContext& pipelineExecutionContext);

    /**
    * @brief Must be called only once per executable pipeline and starts the executable pipeline.
    * e.g. starts the threads for the window handler.
    * @param pipelineExecutionContext
    * @return 0 if no error occurred.
    */
    virtual uint32_t start(PipelineExecutionContext& pipelineExecutionContext);

    /**
    * @brief Must be called exactly once per worker thread and initializes worker local state.
    * For instance a worker local aggregation state.
    * @param pipelineExecutionContext
    * @param workerContext
    * @return 0 if no error occurred.
    */
    virtual uint32_t open(PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext);

    /**
    * @brief Is called once per input buffer and performs the computation of each operator.
    * It can access the state in the PipelineExecutionContext and uns the WorkerContext to
    * identify the current worker thread.
    * @param inputTupleBuffer
    * @param pipelineExecutionContext
    * @param workerContext
    * @return 0 if an error occurred.
    */
    virtual uint32_t execute(TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext,
                             WorkerContext& workerContext) = 0;

    /**
     * @brief Must be called exactly once per worker thread to remove worker local state.
     * @param pipelineExecutionContext
     * @param workerContext
     * @return 0 if no error occurred.
     */
    virtual uint32_t close(PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext);

    /**
     * @brief Must be called exactly once per executable pipeline to remove operator state.
     * @param pipelineExecutionContext
     * @return 0 if no error occurred.
     */
    virtual uint32_t stop(PipelineExecutionContext& pipelineExecutionContext);
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_EXECUTABLEPIPELINESTAGE_HPP_

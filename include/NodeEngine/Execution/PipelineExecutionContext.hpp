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

#ifndef NES_INCLUDE_QUERYCOMPILER_PIPELINEEXECUTIONCONTEXT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PIPELINEEXECUTIONCONTEXT_HPP_
#include <Util/Logger.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <Windowing/JoinForwardRefs.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <functional>
#include <memory>

namespace NES::NodeEngine::Execution {

/**
 * @brief The PipelineExecutionContext is passed to a compiled pipeline and offers basic functionality to interact with the runtime.
 * Via the context, the compile code is able to allocate new TupleBuffers and to emit tuple buffers to the runtime.
 */
class PipelineExecutionContext: public std::enable_shared_from_this<PipelineExecutionContext> {

  public:
    /**
     * @brief The PipelineExecutionContext is passed to the compiled pipeline and enables interaction with the NES runtime.
     * @param bufferManager a reference to the buffer manager to enable allocation from within the pipeline
     * @param emitFunctionHandler an handler to receive the emitted buffers from the pipeline.
     */
    explicit PipelineExecutionContext(QuerySubPlanId queryId, BufferManagerPtr bufferManager,
                                      std::function<void(TupleBuffer&, WorkerContextRef)>&& emitFunctionHandler,
                                      std::function<void(TupleBuffer&)>&& emitToQueryManagerFunctionHandler,
                                      std::vector<OperatorHandlerPtr> operatorHandlers);
    ~PipelineExecutionContext(){
      NES_DEBUG("~PipelineExecutionContext()");
    }
    /**
     * @brief Allocates a new tuple buffer.
     * @return TupleBuffer
     */
    TupleBuffer allocateTupleBuffer();

    /**
     * @brief Emits a output tuple buffer to the runtime. Internally we call the emit function which is a callback to the correct handler.
     * @param outputBuffer the output tuple buffer that is passed to the runtime
     * @param workerContext the worker context
     */
    void emitBuffer(TupleBuffer& outputBuffer, WorkerContext&);

    /**
    * @brief Dispatch a buffer as a new task to the query manager.
    * Consequently, a new task is created and the call returns directly.
    * @param outputBuffer the output tuple buffer that is passed to the runtime
    * @param workerContext the worker context
    */
    void dispatchBuffer(TupleBuffer& outputBuffer);

    /**
     * @brief Retrieve all registered operator handlers.
     * @return  std::vector<OperatorHandlerPtr>
     */
    std::vector<OperatorHandlerPtr> getOperatorHandlers();


    /**
     * @brief Retrieves a Operator Handler at a specific index and cast its to an OperatorHandlerType.
     * @tparam OperatorHandlerType
     * @param index of the operator handler.
     * @return
     */
    template<class OperatorHandlerType>
    auto getOperatorHandler(int index) {
        if(index >= operatorHandlers.size()){
           NES_THROW_RUNTIME_ERROR("PipelineExecutionContext: operator handler at index " + std::to_string(index) + " is not registered");
        }
        return std::static_pointer_cast<OperatorHandlerType>(operatorHandlers[index]);
    }

    std::string toString();


  private:
    /**
     * @brief Id of the local qep that owns the pipeline
     */
    QuerySubPlanId queryId;

    /**
     * @brief Reference to the buffer manager to enable buffer allocation
     */
    BufferManagerPtr bufferManager;
    /**
     * @brief The emit function handler to react on an emitted tuple buffer.
     */
    std::function<void(TupleBuffer&, WorkerContext&)> emitFunctionHandler;

    /**
    * @brief The emit function handler to react on an emitted tuple buffer.
    */
    std::function<void(TupleBuffer&)> emitToQueryManagerFunctionHandler;

    /**
     * @brief List of registered operator handlers.
     */
    const std::vector<std::shared_ptr<OperatorHandler>> operatorHandlers;
};

}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_PIPELINEEXECUTIONCONTEXT_HPP_

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
#include <Plans/Query/QuerySubPlanId.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <functional>
#include <memory>

namespace NES::Join {
class LogicalJoinDefinition;
typedef std::shared_ptr<LogicalJoinDefinition> LogicalJoinDefinitionPtr;
}// namespace NES::Join

namespace NES {

class WorkerContext;
class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

typedef WorkerContext& WorkerContextRef;

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class TupleBuffer;

// TODO Philipp, please clarify if we should introduce WindowManager, StateVars, etc... here
// TODO so that we have one central point that the compiled code uses to access runtime

/**
 * @brief The PipelineExecutionContext is passed to a compiled pipeline and offers basic functionality to interact with the runtime.
 * Via the context, the compile code is able to allocate new TupleBuffers and to emit tuple buffers to the runtime.
 */
class PipelineExecutionContext {

  public:
    /**
     * @brief The PipelineExecutionContext is passed to the compiled pipeline and enables interaction with the NES runtime.
     * @param bufferManager a reference to the buffer manager to enable allocation from within the pipeline
     * @param emitFunctionHandler an handler to receive the emitted buffers from the pipeline.
     */
    explicit PipelineExecutionContext(
        QuerySubPlanId queryId,
        BufferManagerPtr bufferManager,
        std::function<void(TupleBuffer&, WorkerContextRef)>&& emitFunctionHandler,
        Windowing::AbstractWindowHandlerPtr windowHandler,
        Windowing::AbstractWindowHandlerPtr joinHandler,
        Windowing::LogicalWindowDefinitionPtr windowDef,
        NES::Join::LogicalJoinDefinitionPtr joinDef,
        SchemaPtr inputSchema);

    /**
     * @brief Allocates a new tuple buffer.
     * @return TupleBuffer
     */
    TupleBuffer allocateTupleBuffer();

    /**
     * @brief Emits a output tuple buffer to the runtime. Internally we call the emit function which is a callback to the correct handler.
     * @param outputBuffer the output tuple buffer that is passed to the runtime
     */
    void emitBuffer(TupleBuffer& outputBuffer, WorkerContext&);

    /**
     * @brief getter/setter window definition
     * @return
     */
    Windowing::LogicalWindowDefinitionPtr getWindowDef();

    /**
     * @brief
     */
    Windowing::AbstractWindowHandlerPtr getWindowHandler() {
        return windowHandler;
    }

    Windowing::AbstractWindowHandlerPtr getJoinHandler() {
        return joinHandler;
    }

    /**
     * @brief this method is called from the compiled code to get the join handler
     * @tparam KeyTypeLeft
     * @param id
     * @return
     */
    template<template<class> class WindowHandlerType, class KeyType>
    auto getJoinHandler() {
        return std::dynamic_pointer_cast<WindowHandlerType<KeyType>>(joinHandler);
    }

    template<template<class, class, class, class> class WindowHandlerType, class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
    auto getWindowHandler() {
        return std::dynamic_pointer_cast<WindowHandlerType<KeyType, InputType, PartialAggregateType, FinalAggregateType>>(windowHandler);
    }

    /**
     * @brief getter input schema
     * @return
     */
    SchemaPtr getInputSchema();

    /**
     * @brief getter join definition
     */
    Join::LogicalJoinDefinitionPtr getJoinDef();

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

    Windowing::AbstractWindowHandlerPtr windowHandler;
    Windowing::AbstractWindowHandlerPtr joinHandler;

    Windowing::LogicalWindowDefinitionPtr windowDef;
    Join::LogicalJoinDefinitionPtr joinDef;

    SchemaPtr inputSchema;
};

}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_PIPELINEEXECUTIONCONTEXT_HPP_

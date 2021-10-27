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
#ifndef NES_INCLUDE_WINDOWING_WINDOW_HANDLER_WINDOW_OPERATOR_HANDLER_HPP_
#define NES_INCLUDE_WINDOWING_WINDOW_HANDLER_WINDOW_OPERATOR_HANDLER_HPP_

#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <State/StateManager.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
namespace NES::Windowing {

/**
 * @brief Operator handler for window aggregations.
 */
class WindowOperatorHandler : public Runtime::Execution::OperatorHandler {
  public:
    explicit WindowOperatorHandler(LogicalWindowDefinitionPtr windowDefinition, SchemaPtr resultSchema);

    explicit WindowOperatorHandler(LogicalWindowDefinitionPtr windowDefinition,
                                   SchemaPtr resultSchema,
                                   AbstractWindowHandlerPtr windowHandler);

    /**
     * @brief Factory to create new WindowOperatorHandler
     * @param windowDefinition logical window definition
     * @param resultSchema window result schema
     * @return WindowOperatorHandlerPtr
     */
    static WindowOperatorHandlerPtr create(const LogicalWindowDefinitionPtr& windowDefinition, const SchemaPtr& resultSchema);

    /**
    * @brief Factory to create new WindowOperatorHandler
    * @param windowDefinition logical window definition
    * @param resultSchema window result schema
    * @param windowHandler pre initialized window handler
    * @return WindowOperatorHandlerPtr
    */
    static WindowOperatorHandlerPtr create(const LogicalWindowDefinitionPtr& windowDefinition,
                                           const SchemaPtr& resultSchema,
                                           const AbstractWindowHandlerPtr& windowHandler);

    /**
     * @brief Sets the window handler
     * @param windowHandler AbstractWindowHandlerPtr
     */
    void setWindowHandler(AbstractWindowHandlerPtr windowHandler);

    /**
     * @brief Returns a casted window handler
     * @tparam WindowHandlerType
     * @tparam KeyType
     * @tparam InputType
     * @tparam PartialAggregateType
     * @tparam FinalAggregateType
     * @return WindowHandlerType
     */
    template<template<class, class, class, class> class WindowHandlerType,
             class KeyType,
             class InputType,
             class PartialAggregateType,
             class FinalAggregateType>
    auto getWindowHandler() {
        return std::static_pointer_cast<WindowHandlerType<KeyType, InputType, PartialAggregateType, FinalAggregateType>>(
            windowHandler);
    }

    /**
    * @brief Starts window handler
     * @param pipelineExecutionContext pointer to the current pipeline execution context
     * @param localStateVariableId local id of a state on an engine node
     * @param stateManager pointer to the current state manager
    */
    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    /**
    * @brief Stops window handler
     * @param pipelineExecutionContext pointer to the current pipeline execution context
    */
    void stop(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    ~WindowOperatorHandler() override { NES_DEBUG("~WindowOperatorHandler()" + std::to_string(windowHandler.use_count())); }

    LogicalWindowDefinitionPtr getWindowDefinition();

    void reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& context) override;

    void postReconfigurationCallback(Runtime::ReconfigurationMessage& task) override;

    SchemaPtr getResultSchema();

  private:
    LogicalWindowDefinitionPtr windowDefinition;
    AbstractWindowHandlerPtr windowHandler;
    SchemaPtr resultSchema;
};

}// namespace NES::Windowing

#endif  // NES_INCLUDE_WINDOWING_WINDOW_HANDLER_WINDOW_OPERATOR_HANDLER_HPP_

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

#ifndef NES_INCLUDE_WINDOWING_WINDOWHANDLER_JOINOPERATORHANDLER_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWHANDLER_JOINOPERATORHANDLER_HPP_
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Windowing/JoinForwardRefs.hpp>
namespace NES::Join {

/**
 * @brief Operator handler for join.
 */
class JoinOperatorHandler : public Runtime::Execution::OperatorHandler {
  public:
    explicit JoinOperatorHandler(LogicalJoinDefinitionPtr joinDefinition, SchemaPtr resultSchema);
    explicit JoinOperatorHandler(LogicalJoinDefinitionPtr joinDefinition,
                                 SchemaPtr resultSchema,
                                 AbstractJoinHandlerPtr joinHandler);

    /**
    * @brief Factory to create new JoinOperatorHandler
    * @param joinDefinition logical join definition
    * @param resultSchema window result schema
    * @return JoinOperatorHandlerPtr
    */
    static JoinOperatorHandlerPtr create(const LogicalJoinDefinitionPtr& joinDefinition, const SchemaPtr& resultSchema);

    /**
    * @brief Factory to create new JoinOperatorHandler
    * @param joinDefinition logical join definition
    * @param resultSchema window result schema
    * @return JoinOperatorHandlerPtr
    */
    static JoinOperatorHandlerPtr create(const LogicalJoinDefinitionPtr& joinDefinition,
                                         const SchemaPtr& resultSchema,
                                         const AbstractJoinHandlerPtr& joinHandler);
    /**
     * @brief Sets the join handler
     * @param joinHandler AbstractJoinHandlerPtr
     */
    void setJoinHandler(AbstractJoinHandlerPtr joinHandler);

    /**
     * @brief Returns a casted join handler
     * @tparam JoinHandlerType
     * @tparam KeyType
     * @return JoinHandlerType
     */
    template<template<class, class, class> class JoinHandlerType, class KeyType, class LeftStreamType, class RightStreamType>
    auto getJoinHandler() {
        return std::static_pointer_cast<JoinHandlerType<KeyType, LeftStreamType, RightStreamType>>(joinHandler);
    }

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    void stop(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    LogicalJoinDefinitionPtr getJoinDefinition();

    SchemaPtr getResultSchema();

    void reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& context) override;

    void postReconfigurationCallback(Runtime::ReconfigurationMessage& task) override;

  private:
    LogicalJoinDefinitionPtr joinDefinition;
    AbstractJoinHandlerPtr joinHandler;
    SchemaPtr resultSchema;
};
}// namespace NES::Join

#endif  // NES_INCLUDE_WINDOWING_WINDOWHANDLER_JOINOPERATORHANDLER_HPP_

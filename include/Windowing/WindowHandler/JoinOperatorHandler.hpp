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
#include <NodeEngine/Execution/OperatorHandler.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <Windowing/JoinForwardRefs.hpp>
namespace NES::Join{

/**
 * @brief Operator handler for join.
 */
class JoinOperatorHandler : public NodeEngine::Execution::OperatorHandler{
  public:
    JoinOperatorHandler(LogicalJoinDefinitionPtr joinDefinition,
                        SchemaPtr resultSchema);
    JoinOperatorHandler(LogicalJoinDefinitionPtr joinDefinition,
                        SchemaPtr resultSchema,
                        AbstractJoinHandlerPtr joinHandler);

    /**
    * @brief Factory to create new JoinOperatorHandler
    * @param joinDefinition logical join definition
    * @param resultSchema window result schema
    * @return JoinOperatorHandlerPtr
    */
    static JoinOperatorHandlerPtr create(LogicalJoinDefinitionPtr joinDefinition,
                                         SchemaPtr resultSchema);

    /**
    * @brief Factory to create new JoinOperatorHandler
    * @param joinDefinition logical join definition
    * @param resultSchema window result schema
    * @return JoinOperatorHandlerPtr
    */
    static JoinOperatorHandlerPtr create(LogicalJoinDefinitionPtr joinDefinition,
                                         SchemaPtr resultSchema,
                                         AbstractJoinHandlerPtr joinHandler);
    /**
     * @brief Sets the join handler
     * @param joinHandler AbstractJoinHandlerPtr
     */
    void setJoinHandler(AbstractJoinHandlerPtr joinHandler);

    virtual ~JoinOperatorHandler(){
        NES_DEBUG("~JoinOperatorHandler()");
    }

    /**
     * @brief Returns a casted join handler
     * @tparam JoinHandlerType
     * @tparam KeyType
     * @return JoinHandlerType
     */
    template<template<class> class JoinHandlerType, class KeyType>
    auto getJoinHandler() {
        return std::static_pointer_cast<JoinHandlerType<KeyType>>(joinHandler);
    }

    void start(NodeEngine::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;
    void stop(NodeEngine::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    LogicalJoinDefinitionPtr getJoinDefinition();

    SchemaPtr getResultSchema();
  private:
    LogicalJoinDefinitionPtr joinDefinition;
    AbstractJoinHandlerPtr joinHandler;
    SchemaPtr resultSchema;
};


}

#endif//NES_INCLUDE_WINDOWING_WINDOWHANDLER_JOINOPERATORHANDLER_HPP_

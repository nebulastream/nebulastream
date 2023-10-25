/*
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

#ifndef NES_CORE_INCLUDE_WINDOWING_WINDOWHANDLER_BATCHJOINOPERATORHANDLER_HPP_
#define NES_CORE_INCLUDE_WINDOWING_WINDOWHANDLER_BATCHJOINOPERATORHANDLER_HPP_
#include <Operators/LogicalOperators/Windows/Joins/JoinForwardRefs.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/Logger/Logger.hpp>
#include <atomic>

namespace NES::Join::Experimental {
/**
 * @brief Operator handler for batch join.
 */
class BatchJoinOperatorHandler : public Runtime::Execution::OperatorHandler {
  public:
    explicit BatchJoinOperatorHandler(LogicalBatchJoinDefinitionPtr batchJoinDefinition, SchemaPtr resultSchema);
    explicit BatchJoinOperatorHandler(LogicalBatchJoinDefinitionPtr batchJoinDefinition,
                                      SchemaPtr resultSchema,
                                      AbstractBatchJoinHandlerPtr batchJoinHandler);

    /**
    * @brief Factory to create new BatchJoinOperatorHandler
    * @param batchJoinDefinition logical join definition
    * @param resultSchema window result schema
    * @return BatchJoinOperatorHandlerPtr
    */
    static Experimental::BatchJoinOperatorHandlerPtr create(const LogicalBatchJoinDefinitionPtr& batchJoinDefinition,
                                                            const SchemaPtr& resultSchema);

    /**
    * @brief Factory to create new BatchJoinOperatorHandler
    * @param batchJoinDefinition logical join definition
    * @param resultSchema batch join result schema
    * @return BatchJoinOperatorHandlerPtr
    */
    static Experimental::BatchJoinOperatorHandlerPtr create(const LogicalBatchJoinDefinitionPtr& batchJoinDefinition,
                                                            const SchemaPtr& resultSchema,
                                                            const AbstractBatchJoinHandlerPtr& batchJoinHandler);
    /**
     * @brief Sets the join handler
     * @param batchJoinHandler AbstractBatchJoinHandlerPtr - should be of correctly templated class BatchJoinHandler!
     */
    void setBatchJoinHandler(AbstractBatchJoinHandlerPtr batchJoinHandler);

    /**
     * @brief Returns a casted join handler
     * @tparam BatchJoinHandlerType
     * @tparam KeyType
     * @return JoinHandlerType
     */
    template<template<class, class> class BatchJoinHandlerType, class KeyType, class InputTypeBuild>// <-- todo
    auto getBatchJoinHandler() {
        return std::static_pointer_cast<BatchJoinHandlerType<KeyType, InputTypeBuild>>(batchJoinHandler);
    }

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    void stop(Runtime::QueryTerminationType terminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    LogicalBatchJoinDefinitionPtr getBatchJoinDefinition();

    SchemaPtr getResultSchema();

    /**
     * @brief reconfigure callback that will be called per thread. When it is called by every thread on the build side: startProbeSide()
    */
    void reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& context) override;

    void postReconfigurationCallback(Runtime::ReconfigurationMessage& task) override;

    /**
    * @brief Register the ID of the probe pipeline (that holds this operator handler)
    * @param probePipelineID
    */
    void setProbePipelineID(uint64_t probePipelineID) {
        if (this->probePipelineID != 0) {
            NES_WARNING("BatchJoinOperatorHandler::setProbePipelineID called a second time. Previous id: {} New id: {}",
                        this->probePipelineID,
                        probePipelineID);
        }
        this->probePipelineID = probePipelineID;
    }
    /**
    * @brief Register the ID of the build pipeline (that holds this operator handler)
    * @param probePipelineID
    */
    void setBuildPipelineID(uint64_t buildPipelineID) {
        if (this->probePipelineID != 0) {
            NES_WARNING("BatchJoinOperatorHandler::setBuildPipelineID called a second time. Previous id: {} New id: {} ",
                        this->buildPipelineID,
                        buildPipelineID);
        }
        this->buildPipelineID = buildPipelineID;
    }
    /**
    * @brief Get the ID of the probe pipeline (that holds this operator handler)
    * @returns probePipelineID
    */
    uint64_t getProbePipelineID() { return this->probePipelineID; }
    /**
    * @brief Get the ID of the build pipeline (that holds this operator handler)
    * @returns probePipelineID
    */
    uint64_t getBuildPipelineID() { return this->buildPipelineID; }

  private:
    /**
     * @brief Starts all predecessors on the Joins Probe side by calling onEvent(StartSourceEvent)
    */
    void startProbeSide(Runtime::WorkerContext& context);

    LogicalBatchJoinDefinitionPtr batchJoinDefinition;
    AbstractBatchJoinHandlerPtr batchJoinHandler;
    SchemaPtr resultSchema;

    // we save these pointers (DataSources or ExecutablePipelines) to manually start the joins predecessors
    std::vector<Runtime::Execution::PredecessorExecutablePipeline> probePredecessors;
    std::vector<Runtime::Execution::PredecessorExecutablePipeline> buildPredecessors;

    uint64_t probePipelineID;
    uint64_t buildPipelineID;

    uint64_t workerThreadsBuildSideTotal = 0;
    std::atomic<uint64_t> workerThreadsBuildSideFinished = 0;

    bool foundAndStartedBuildSide = false;
    bool foundProbeSide = false;
    bool startedProbeSide = false;
};
}// namespace NES::Join::Experimental

#endif// NES_CORE_INCLUDE_WINDOWING_WINDOWHANDLER_BATCHJOINOPERATORHANDLER_HPP_

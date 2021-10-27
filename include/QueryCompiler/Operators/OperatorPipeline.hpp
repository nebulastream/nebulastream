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
#ifndef NES_INCLUDE_QUERY_COMPILER_OPERATORS_OPERATOR_PIPELINE_HPP_
#define NES_INCLUDE_QUERY_COMPILER_OPERATORS_OPERATOR_PIPELINE_HPP_

#include <Nodes/Node.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <memory>
#include <vector>
namespace NES {
namespace QueryCompilation {

/**
 * @brief Defines a single pipeline, which contains of a query plan of operators.
 * Each pipeline can have N successor and predecessor pipelines.
 */
class OperatorPipeline : public std::enable_shared_from_this<OperatorPipeline> {
  public:
    /**
     * @brief The type of a pipeline.
     * Source/Sink pipelines only have a single source and sink operator.
     * Operator pipelines consist of arbitrary operators, except sources and sinks.
     */
    enum Type { SourcePipelineType, SinkPipelineType, OperatorPipelineType };

    /**
     * @brief Creates a new operator pipeline
     * @return OperatorPipelinePtr
     */
    static OperatorPipelinePtr create();

    /**
     * @brief Creates a new source pipeline.
     * @return OperatorPipelinePtr
     */
    static OperatorPipelinePtr createSourcePipeline();

    /**
     * @brief Creates a new sink pipeline.
     * @return OperatorPipelinePtr
     */
    static OperatorPipelinePtr createSinkPipeline();

    /**
     * @brief Adds a successor pipeline to the current one.
     * @param successor
     */
    void addSuccessor(const OperatorPipelinePtr& pipeline);

    /**
     * @brief Adds a predecessor pipeline to the current one.
     * @param predecessor
     */
    void addPredecessor(const OperatorPipelinePtr& pipeline);

    /**
     * @brief Removes a particular predecessor pipeline.
     * @param predecessor
     */
    void removePredecessor(const OperatorPipelinePtr& pipeline);

    /**
     * @brief Removes a particular successor pipeline.
     * @param successor
     */
    void removeSuccessor(const OperatorPipelinePtr& pipeline);

    /**
     * @brief Gets list of all predecessors
     * @return std::vector<OperatorPipelinePtr>
     */
    std::vector<OperatorPipelinePtr> getPredecessors() const;

    /**
     * @brief Gets list of all sucessors
     * @return std::vector<OperatorPipelinePtr>
     */
    std::vector<OperatorPipelinePtr> const& getSuccessors() const;

    /**
     * @brief Removes all predecessors
     */
    void clearPredecessors();

    /**
     * @brief Removes all successors
     */
    void clearSuccessors();

    /**
     * @brief Returns the query plan
     * @return QueryPlanPtr
     */
    QueryPlanPtr getQueryPlan();

    /**
     * @brief Returns the pipeline id
     * @return pipeline id.
     */
    uint64_t getPipelineId() const;

    /**
     * @brief Sets the type of an pipeline to Source, Sink, or Operator
     * @param pipelineType
     */
    void setType(Type pipelineType);

    /**
     * @brief Prepends a new operator to this pipeline.
     * @param newRootOperator
     */
    void prependOperator(OperatorNodePtr newRootOperator);

    /**
     * @brief Checks if this pipeline has an operator.
     * @return true if pipeline has an operator.
     */
    bool hasOperators() const;

    /**
     * @brief Indicates if this is a source pipeline.
     * @return true if source pipeline
     */
    bool isSourcePipeline() const;

    /**
     * @brief Indicates if this is a sink pipeline.
     * @return true if sink pipeline
     */
    bool isSinkPipeline() const;

    /**
     * @brief Indicates if this is a operator pipeline.
     * @return true if operator pipeline
     */
    bool isOperatorPipeline() const;

  protected:
    OperatorPipeline(uint64_t pipelineId, Type pipelineType);

  private:
    uint64_t id;
    std::vector<std::shared_ptr<OperatorPipeline>> successorPipelines;
    std::vector<std::weak_ptr<OperatorPipeline>> predecessorPipelines;
    QueryPlanPtr queryPlan;
    Type pipelineType;
};
}// namespace QueryCompilation

}// namespace NES

#endif  // NES_INCLUDE_QUERY_COMPILER_OPERATORS_OPERATOR_PIPELINE_HPP_

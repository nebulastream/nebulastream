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
#pragma once

#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>

namespace NES::QueryCompilation
{
/**
* @brief Returns the next free pipeline id
* @return node id
*/
PipelineId getNextPipelineId();

/**
 * @brief Defines a single pipeline, which contains of a query plan of operators.
 * Each pipeline can have N successor and predecessor pipelines.
 */
class OperatorPipeline : public std::enable_shared_from_this<OperatorPipeline>
{
public:
    /**
     * @brief The type of a pipeline.
     * Source/Sink pipelines only have a single source and sink operator.
     * Operator pipelines consist of arbitrary operators, except sources and sinks.
     */
    enum class Type : uint8_t
    {
        SourcePipelineType,
        SinkPipelineType,
        OperatorPipelineType
    };

    /**
     * @brief Creates a new operator pipeline
     * @return std::shared_ptr<OperatorPipeline>
     */
    static std::shared_ptr<OperatorPipeline> create();

    /**
     * @brief Creates a new sink pipeline.
     * @return std::shared_ptr<OperatorPipeline>
     */
    static std::shared_ptr<OperatorPipeline> createSinkPipeline();

    /**
     * @brief Adds a successor pipeline to the current one.
     * @param successor
     */
    void addSuccessor(const std::shared_ptr<OperatorPipeline>& pipeline);

    /**
     * @brief Adds a predecessor pipeline to the current one.
     * @param predecessor
     */
    void addPredecessor(const std::shared_ptr<OperatorPipeline>& pipeline);

    /**
     * @brief Removes a particular predecessor pipeline.
     * @param predecessor
     */
    void removePredecessor(const std::shared_ptr<OperatorPipeline>& pipeline);

    /**
     * @brief Removes a particular successor pipeline.
     * @param successor
     */
    void removeSuccessor(const std::shared_ptr<OperatorPipeline>& pipeline);

    /**
     * @brief Gets list of all predecessors
     * @return std::vector<std::shared_ptr<OperatorPipeline>>
     */
    std::vector<std::shared_ptr<OperatorPipeline>> getPredecessors() const;

    /**
     * @brief Gets list of all sucessors
     * @return std::vector<std::shared_ptr<OperatorPipeline>>
     */
    const std::vector<std::shared_ptr<OperatorPipeline>>& getSuccessors() const;

    /**
     * @brief Removes all predecessors
     */
    void clearPredecessors();

    /**
     * @brief Removes all successors
     */
    void clearSuccessors();

    /**
     * @brief Returns the decomposed query plan
     * @return std::shared_ptr<DecomposedQueryPlan>
     */
    std::shared_ptr<DecomposedQueryPlan> getDecomposedQueryPlan();

    /**
     * @brief Returns the pipeline id
     * @return pipeline id.
     */
    PipelineId getPipelineId() const;

    /**
     * @brief Sets the type of an pipeline to Source, Sink, or Operator
     * @param pipelineType
     */
    void setType(Type pipelineType);

    /**
     * @brief Prepends a new operator to this pipeline.
     * @param newRootOperator
     */
    void prependOperator(std::shared_ptr<Operator> newRootOperator);

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
    const std::vector<OperatorId>& getOperatorIds() const;

    /**
     * @brief Creates a string representation of this OperatorPipeline
     * @return std::string
     */
    std::string toString() const;

protected:
    OperatorPipeline(PipelineId pipelineId, Type pipelineType);

private:
    PipelineId id;
    std::vector<std::shared_ptr<OperatorPipeline>> successorPipelines;
    std::vector<std::weak_ptr<OperatorPipeline>> predecessorPipelines;
    std::shared_ptr<DecomposedQueryPlan> decomposedQueryPlan;
    std::vector<OperatorId> operatorIds;
    Type pipelineType;
};
}

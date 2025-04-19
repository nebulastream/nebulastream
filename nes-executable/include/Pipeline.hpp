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
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp> // TODO lets see if this is the right place here
#include <Plans/QueryPlan.hpp>

namespace NES
{

/// @brief Defines a single pipeline, which contains of a query plan of operators.
/// Each pipeline can have N successor and predecessor pipelines.
class Pipeline : std::enable_shared_from_this<Pipeline>
{
public:
    // TODO we might can get rid of it as we now have a variant
    /// @brief The type of pipeline
    /// Source/Sink pipelines only have a single source and sink operator.
    /// Operator pipelines consist of arbitrary operators, except sources and sinks.
    enum class Type : uint8_t
    {
        SourcePipelineType,
        SinkPipelineType,
        PipelineType
    };

    // TODO this could also be an enum...
    std::string getPipelineProviderType() const { return "Interpreter"; };

    static std::shared_ptr<Pipeline> create();
    static std::shared_ptr<Pipeline> createSourcePipeline();
    static std::shared_ptr<Pipeline> createSinkPipeline();

    // TODO make public vector
    void addSuccessor(const std::shared_ptr<Pipeline>& pipeline);
    void addPredecessor(const std::shared_ptr<Pipeline>& pipeline);

    void removePredecessor(const std::shared_ptr<Pipeline>& pipeline);
    void removeSuccessor(const std::shared_ptr<Pipeline>& pipeline);

    std::vector<std::shared_ptr<Pipeline>> getPredecessors() const;
    std::vector<std::shared_ptr<Pipeline>>& getSuccessors() const;

    void prependOperator(std::shared_ptr<Operator> newRootOperator);

    std::vector<OperatorHandler> getOperatorHandlers() const { return operatorHandlers; }

    void clearPredecessors();
    void clearSuccessors();

    std::shared_ptr<QueryPlan> getQueryPlan();

    PipelineId getPipelineId() const;
    void setType(Type pipelineType);

    //void prependOperator(std::shared_ptr<Operator> newRootOperator);

    bool hasOperators() const;

    bool isSourcePipeline() const;
    bool isSinkPipeline() const;
    bool isPipeline() const;
    const std::vector<OperatorId>& getOperatorIds() const;

    std::string toString() const;

protected:
    Pipeline(PipelineId pipelineId, Type pipelineType);

private:
    PipelineId id;
    std::vector<std::shared_ptr<Pipeline>> successorPipelines;
    std::vector<std::weak_ptr<Pipeline>> predecessorPipelines;
    std::shared_ptr<QueryPlan> physicalQueryPlan;
    std::vector<OperatorHandler> operatorHandlers;
    std::vector<OperatorId> operatorIds;
    Type pipelineType;
};

}
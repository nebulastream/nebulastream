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

// TODO: We should remove this one

#include <Identifiers/Identifiers.hpp>
#include <Plans/PhysicalQueryPlan.hpp>

namespace NES::QueryCompilation
{
/// @brief Returns the next free pipeline id
PipelineId getNextPipelineId();

/// @brief Defines a single pipeline, which contains of a query plan of operators.
/// Each pipeline can have N successor and predecessor pipelines.
class OperatorPipeline : public std::enable_shared_from_this<OperatorPipeline>
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
        OperatorPipelineType
    };

    static std::shared_ptr<OperatorPipeline> create();
    static std::shared_ptr<OperatorPipeline> createSourcePipeline();
    static std::shared_ptr<OperatorPipeline> createSinkPipeline();

    // TODO make public vector
    void addSuccessor(const std::shared_ptr<OperatorPipeline>& pipeline);
    void addPredecessor(const std::shared_ptr<OperatorPipeline>& pipeline);

    void removePredecessor(const std::shared_ptr<OperatorPipeline>& pipeline);
    void removeSuccessor(const std::shared_ptr<OperatorPipeline>& pipeline);

    std::vector<std::shared_ptr<OperatorPipeline>> getPredecessors() const;
    std::vector<std::shared_ptr<OperatorPipeline>> const& getSuccessors() const;

    void prependOperator(std::shared_ptr<Operator> newRootOperator);

    void clearPredecessors();
    void clearSuccessors();

    std::shared_ptr<PhysicalQueryPlan> getPhysicalQueryPlan();

    PipelineId getPipelineId() const;
    void setType(Type pipelineType);

    void prependOperator(std::shared_ptr<PhysicalOperatorNode> newRootOperator);

    bool hasOperators() const;

    bool isSourcePipeline() const;
    bool isSinkPipeline() const;
    bool isOperatorPipeline() const;
    const std::vector<OperatorId>& getOperatorIds() const;

    std::string toString() const;

protected:
    OperatorPipeline(PipelineId pipelineId, Type pipelineType);

private:
    PipelineId id;
    std::vector<std::shared_ptr<OperatorPipeline>> successorPipelines;
    std::vector<std::weak_ptr<OperatorPipeline>> predecessorPipelines;
    std::shared_ptr<PhysicalQueryPlan> physicalQueryPlan;
    std::vector<OperatorId> operatorIds;
    Type pipelineType;
};
}


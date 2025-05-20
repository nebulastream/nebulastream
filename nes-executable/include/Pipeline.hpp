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
#include <optional>
#include <ostream>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/ExecutionMode.hpp>
#include <Util/Logger/Formatter.hpp>
#include <PhysicalOperator.hpp>
#include <SinkPhysicalOperator.hpp>
#include <SourcePhysicalOperator.hpp>

namespace NES
{

PipelineId getNextPipelineId();

/// @brief Defines a single pipeline, which contains of a query plan of operators.
/// Each pipeline can have N successor and predecessor pipelines.
/// We make use of shared_ptr reference counting of Pipelines to track the lifetime const of query plans
struct Pipeline
{
    /// These constructors are used during the @link PipeliningPhase to create initial pipeline segments
    /// from root operators of a @link PhysicalPlan. The actual role of the pipeline (source, sink, or intermediate)
    /// is determined by the dynamic type of the provided 'op'.

    /// Constructs a pipeline with the given PhysicalOperator as its root.
    explicit Pipeline(PhysicalOperator op);

    /// Constructs a pipeline explicitly rooted with a SourcePhysicalOperator.
    explicit Pipeline(const SourcePhysicalOperator& op);

    /// Constructs a pipeline explicitly rooted with a SinkPhysicalOperator.
    explicit Pipeline(const SinkPhysicalOperator& op);
    Pipeline() = delete;

    [[nodiscard]] bool isSourcePipeline() const;
    [[nodiscard]] bool isOperatorPipeline() const;
    [[nodiscard]] bool isSinkPipeline() const;

    void appendOperator(const PhysicalOperator& newOp);
    void prependOperator(PhysicalOperator newOp);

    friend std::ostream& operator<<(std::ostream& os, const Pipeline& p);

    [[nodiscard]] std::optional<Nautilus::Configurations::ExecutionMode> getExecutionMode() const;
    void setExecutionMode(Nautilus::Configurations::ExecutionMode mode);

    [[nodiscard]] const PhysicalOperator& getRootOperator() const;
    void setRootOperator(const PhysicalOperator& op);

    [[nodiscard]] PipelineId getPipelineId() const;

    [[nodiscard]] const std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() const;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers();

    void addSuccessor(const std::shared_ptr<Pipeline>& successor, const std::weak_ptr<Pipeline>& self);

    void removePredecessor(const Pipeline& pipeline);
    [[nodiscard]] const std::vector<std::shared_ptr<Pipeline>>& getSuccessors() const;

    void clearSuccessors();
    void clearPredecessors();
    void removeSuccessor(const Pipeline& pipeline);

private:
    std::optional<Nautilus::Configurations::ExecutionMode> executionMode;
    PhysicalOperator rootOperator;
    const PipelineId pipelineId;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandlers;
    std::vector<std::shared_ptr<Pipeline>> successorPipelines;
    std::vector<std::weak_ptr<Pipeline>> predecessorPipelines;
};
}
FMT_OSTREAM(NES::Pipeline);

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

#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <PhysicalOperator.hpp>
#include <SinkPhysicalOperator.hpp>
#include <SourcePhysicalOperator.hpp>

namespace NES
{

PipelineId getNextPipelineId();

/// @brief Defines a single pipeline, which contains of a query plan of operators.
/// Each pipeline can have N successor and predecessor pipelines.
/// We make use of shared_ptr reference counting of Pipelines to track the lifetime of query plans
struct Pipeline
{
    explicit Pipeline(PhysicalOperator op);
    explicit Pipeline(SourcePhysicalOperator op);
    explicit Pipeline(SinkPhysicalOperator op);
    Pipeline() = delete;

    [[nodiscard]] bool isSourcePipeline() const;
    [[nodiscard]] bool isOperatorPipeline() const;
    [[nodiscard]] bool isSinkPipeline() const;

    void appendOperator(PhysicalOperator newOp);
    void prependOperator(PhysicalOperator newOp);

    friend std::ostream& operator<<(std::ostream& os, const Pipeline& p);

    enum class ProviderType : uint8_t
    {
        Interpreter,
        Compiler
    };

    ProviderType providerType = ProviderType::Interpreter;
    PhysicalOperator rootOperator;
    const PipelineId pipelineId;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandlers;

    void addSuccessor(const std::shared_ptr<Pipeline>& successor, const std::weak_ptr<Pipeline>& self);


    void removePredecessor(const Pipeline& pipeline);

    [[nodiscard]] const std::vector<std::shared_ptr<Pipeline>>& getSuccessors() const;

    void clearSuccessors();

    void clearPredecessors();

    void removeSuccessor(const Pipeline& pipeline);

private:
    std::vector<std::shared_ptr<Pipeline>> successorPipelines;
    std::vector<std::weak_ptr<Pipeline>> predecessorPipelines;
};
}
FMT_OSTREAM(NES::Pipeline);

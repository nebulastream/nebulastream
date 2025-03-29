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

#include <iterator>
#include <vector>
#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Plans/QueryPlan.hpp>
#include <PhysicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>

namespace NES
{

namespace
{
PipelineId getNextPipelineId()
{
    static std::atomic_uint64_t id = INITIAL_PIPELINE_ID.getRawValue();
    return PipelineId(id++);
}
}

/// @brief Defines a single pipeline, which contains of a query plan of operators.
/// Each pipeline can have N successor and predecessor pipelines.
struct Pipeline : std::enable_shared_from_this<Pipeline>
{
    explicit Pipeline() : id(getNextPipelineId()), providerType(ProviderType::Compiler) {};
    /// Virtual destructor to make Pipeline polymorphic
    virtual ~Pipeline() = default;
    [[nodiscard]] std::string toString() const;

    const PipelineId id;

    enum class ProviderType : uint8_t {
        Interpreter,
        Compiler
    };
    const ProviderType providerType;

    std::vector<std::shared_ptr<Pipeline>> successorPipelines;
    std::vector<std::weak_ptr<Pipeline>> predecessorPipelines;
    std::vector<OperatorHandler> operatorHandlers;
    ///std::vector<OperatorId> operatorIds;

    using PipelineOperator = std::variant<std::shared_ptr<PhysicalOperator>,
                                          std::shared_ptr<SourceDescriptorLogicalOperator>,
                                          std::shared_ptr<SinkLogicalOperator>>;

    virtual bool hasOperators() = 0;
    ///virtual void appendOperator(std::shared_ptr<PipelineOperator> op) = 0;
    virtual void prependOperator(std::shared_ptr<PipelineOperator> op) = 0;
};


struct OperatorPipeline : public Pipeline
{
    void prependOperator(std::shared_ptr<PipelineOperator> op)
    {
        PRECONDITION(std::holds_alternative<std::shared_ptr<PhysicalOperator>>(*op), "Should have hold a PhysicalOperator");
        operators.insert(operators.begin(), std::get<std::shared_ptr<PhysicalOperator>>(*op));
        //plan->appendOperatorAsNewRoot(std::get<std::shared_ptr<PhysicalOperator>>(*op)); ///.emplace_back(std::get<std::shared_ptr<PhysicalOperator>>(*op));
    }

    bool hasOperators()
    {
        return operators.size() != 0;
    }

    std::shared_ptr<PhysicalOperator> scanOperator;
    std::vector<std::shared_ptr<PhysicalOperator>> operators;
    std::shared_ptr<PhysicalOperator> emitOperator;
};

struct SourcePipeline : public Pipeline
{
    void prependOperator(std::shared_ptr<PipelineOperator> op)
    {
        PRECONDITION(std::holds_alternative<std::shared_ptr<SourceDescriptorLogicalOperator>>(*op), "Should have hold a SourceDescriptorLogicalOperator");
        sourceOperator = (std::get<std::shared_ptr<SourceDescriptorLogicalOperator>>(*op));
    }

    bool hasOperators()
    {
        return sourceOperator != nullptr;
    }

    std::shared_ptr<SourceDescriptorLogicalOperator> sourceOperator;
};

struct SinkPipeline : public Pipeline
{
    void prependOperator(std::shared_ptr<PipelineOperator> op)
    {
        PRECONDITION(std::holds_alternative<std::shared_ptr<SinkLogicalOperator>>(*op), "Should have hold a SinkLogicalOperator");
        sinkOperator = (std::get<std::shared_ptr<SinkLogicalOperator>>(*op));
    }

    bool hasOperators()
    {
        return sinkOperator != nullptr;
    }

    std::shared_ptr<SinkLogicalOperator> sinkOperator;
};

}
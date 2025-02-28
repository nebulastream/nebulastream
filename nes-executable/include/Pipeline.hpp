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
#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/QueryPlan.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Abstract/PhysicalOperator.hpp>

namespace NES
{
/// @brief Defines a single pipeline, which contains of a query plan of operators.
/// Each pipeline can have N successor and predecessor pipelines.
struct Pipeline
{
    explicit Pipeline();
    /// Virtual destructor to make Pipeline polymorphic
    virtual ~Pipeline() = default;

    virtual std::string toString() const = 0;
    friend std::ostream& operator<<(std::ostream& os, const Pipeline& t);

    const PipelineId id;

    enum class ProviderType : uint8_t {
        Interpreter,
        Compiler
    };
    const ProviderType providerType;

    std::vector<std::unique_ptr<Pipeline>> successorPipelines;
    std::vector<std::weak_ptr<Pipeline>> predecessorPipelines;
    std::vector<OperatorHandler> operatorHandlers;

    using PipelineOperator = std::variant<std::unique_ptr<PhysicalOperator>,
                                          std::unique_ptr<SourceDescriptorLogicalOperator>,
                                          std::unique_ptr<SinkLogicalOperator>>;
    /// non-owning view on pipeline operator
    using PipelineOperatorView = std::variant<
        const PhysicalOperator*,
        const SourceDescriptorLogicalOperator*,
        const SinkLogicalOperator*
        >;
    PipelineOperatorView toOperatorView(const Operator* op) {
        if (auto phys = dynamic_cast<const PhysicalOperator*>(op))
            return phys;
        if (auto src = dynamic_cast<const SourceDescriptorLogicalOperator*>(op))
            return src;
        if (auto sink = dynamic_cast<const SinkLogicalOperator*>(op))
            return sink;
        throw std::runtime_error("Unsupported operator type");
    }

    virtual bool hasOperators() = 0;
    ///virtual void appendOperator(std::shared_ptr<PipelineOperator> op) = 0;
    virtual void prependOperator(PipelineOperator op) = 0;
};


struct OperatorPipeline final : public Pipeline
{
    void prependOperator(PipelineOperator op) override
    {
        PRECONDITION(std::holds_alternative<std::unique_ptr<PhysicalOperator>>(op), "Should have hold a PhysicalOperator");
        operators.insert(operators.begin(), std::move(std::get<std::unique_ptr<PhysicalOperator>>(op)));
        //plan->promoteOperatorToRoot(std::get<std::shared_ptr<PhysicalOperator>>(*op)); ///.emplace_back(std::get<std::shared_ptr<PhysicalOperator>>(*op));
    }


    [[nodiscard]] std::string toString() const override;

    bool hasOperators() override
    {
        return operators.size() != 0;
    }

    std::unique_ptr<PhysicalOperator> scanOperator;
    std::vector<std::unique_ptr<PhysicalOperator>> operators;
    std::unique_ptr<PhysicalOperator> emitOperator;
};

struct SourcePipeline final : public Pipeline
{
    void prependOperator(PipelineOperator op) override
    {
        PRECONDITION(std::holds_alternative<std::unique_ptr<SourceDescriptorLogicalOperator>>(op), "Should have hold a SourceDescriptorLogicalOperator");
        sourceOperator = std::move((std::get<std::unique_ptr<SourceDescriptorLogicalOperator>>(op)));
    }

    [[nodiscard]] std::string toString() const override;

    bool hasOperators() override
    {
        return sourceOperator != nullptr;
    }

    // TODO check if this should be as well use value semantics
    std::unique_ptr<SourceDescriptorLogicalOperator> sourceOperator;
};

struct SinkPipeline final : public Pipeline
{
    void prependOperator(PipelineOperator op) override
    {
        PRECONDITION(std::holds_alternative<std::unique_ptr<SinkLogicalOperator>>(op), "Should have hold a SinkLogicalOperator");
        sinkOperator = std::move((std::get<std::unique_ptr<SinkLogicalOperator>>(op)));
    }

    [[nodiscard]] std::string toString() const override;

    bool hasOperators() override
    {
        return sinkOperator != nullptr;
    }

    // TODO check if this should be as well use value semantics
    std::unique_ptr<SinkLogicalOperator> sinkOperator;
};

}
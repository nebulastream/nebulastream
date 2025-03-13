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
#include <Abstract/PhysicalOperator.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <ErrorHandling.hpp>
#include <SinkPhysicalOperator.hpp>
#include <SourcePhysicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>

namespace
{
inline NES::PipelineId getNextPipelineId() {
    static std::atomic_uint64_t id = NES::INITIAL_PIPELINE_ID.getRawValue();
    return NES::PipelineId(id++);
}
std::string operatorChainToString(const NES::PhysicalOperator& op, int indent = 0) {
    std::ostringstream oss;
    std::string indentation(indent, ' ');
    oss << indentation << op.toString() << "\n";
    if (auto childOpt = op.getChild()) {
        oss << operatorChainToString(childOpt.value(), indent + 2);
    }
    return oss.str();
}
}

namespace NES
{
/// @brief Defines a single pipeline, which contains of a query plan of operators.
/// Each pipeline can have N successor and predecessor pipelines.
struct Pipeline {
    enum class ProviderType : uint8_t {
        Interpreter,
        Compiler
    };

    const PipelineId pipelineId = getNextPipelineId();
    ProviderType providerType;

    std::vector<std::shared_ptr<Pipeline>> successorPipelines;
    std::vector<std::weak_ptr<Pipeline>> predecessorPipelines;
    std::unordered_map<uint64_t, std::shared_ptr<OperatorHandler>> operatorHandlers;

    std::unordered_map<uint64_t, std::shared_ptr<OperatorHandler>> releaseOperatorHandlers()
    {
        return std::move(operatorHandlers);
    }

    PhysicalOperator rootOperator;

    Pipeline(PhysicalOperator op) : rootOperator(std::move(op))
    {
    }

    Pipeline(SourcePhysicalOperator op) : rootOperator(std::move(op))
    {
    }

    Pipeline(SinkPhysicalOperator op) : rootOperator(std::move(op))
    {
    }

    bool isSourcePipeline()
    {
        return rootOperator.tryGet<SourcePhysicalOperator>();
    }

    bool isOperatorPipeline()
    {
        return not (isSinkPipeline() or isSourcePipeline());
    }

    bool isSinkPipeline()
    {
        return rootOperator.tryGet<SinkPhysicalOperator>();
    }

    std::string getProviderType() const {
        switch (providerType) {
            case ProviderType::Interpreter: return "Interpreter";
            case ProviderType::Compiler: return "Compiler";
            default: return "Unknown";
        }
    }

    void prependOperator(PhysicalOperator newOp) {
        PRECONDITION(isSourcePipeline() or isSinkPipeline(), "Cannot add new operator to source or sink pipeline");
        newOp.setChild(rootOperator);
        rootOperator = newOp;
    }

    static PhysicalOperator appendOperatorHelper(PhysicalOperator op, const PhysicalOperator& newOp) {
        if (not op.getChild()) {
            op.setChild(newOp);
            return op;
        } else {
            PhysicalOperator child = op.getChild().value();
            child = appendOperatorHelper(child, newOp);
            op.setChild(child);
            return op;
        }
    }

    void appendOperator(PhysicalOperator newOp) {
        PRECONDITION(not (isSourcePipeline() or isSinkPipeline()), "Cannot add new operator to source or sink pipeline");
        rootOperator = appendOperatorHelper(rootOperator, newOp);
    }

    std::string toString() const {
        std::ostringstream oss;
        oss << "PipelineId: " << pipelineId.getRawValue() << "\n";
        oss << "Provider: " << getProviderType() << "\n";
        oss << "Successors: " << successorPipelines.size() << "\n";
        oss << "Predecessors: " << predecessorPipelines.size() << "\n";
        oss << "Operator Chain:\n";
        oss << operatorChainToString(rootOperator);
        return oss.str();
    }

    friend std::ostream& operator<<(std::ostream& os, const Pipeline& p) {
        os << p.toString();
        return os;
    }
};

}
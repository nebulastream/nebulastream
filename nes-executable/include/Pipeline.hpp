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

namespace NES
{

/// @brief Defines a single pipeline, which contains of a query plan of operators.
/// Each pipeline can have N successor and predecessor pipelines.
struct Pipeline {
    Pipeline(PhysicalOperator op);
    Pipeline(SourcePhysicalOperator op);
    Pipeline(SinkPhysicalOperator op);
    Pipeline() = delete;

    bool isSourcePipeline();
    bool isOperatorPipeline();
    bool isSinkPipeline();

    std::string getProviderType() const;

    void appendOperator(PhysicalOperator newOp);
    void prependOperator(PhysicalOperator newOp);
    std::unordered_map<uint64_t, std::shared_ptr<OperatorHandler>> releaseOperatorHandlers();

    std::string toString() const;
    friend std::ostream& operator<<(std::ostream& os, const Pipeline& p);

    enum class ProviderType : uint8_t {
        Interpreter,
        Compiler
    };

    ProviderType providerType;
    PhysicalOperator rootOperator;
    const PipelineId pipelineId;

    std::vector<std::shared_ptr<Pipeline>> successorPipelines;
    std::vector<std::weak_ptr<Pipeline>> predecessorPipelines;
    std::unordered_map<uint64_t, std::shared_ptr<OperatorHandler>> operatorHandlers;
};

}
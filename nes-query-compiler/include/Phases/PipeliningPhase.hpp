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

#include <cstdint>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <utility>
#include <variant>
#include <API/Schema.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Operator.hpp>
#include <Plans/QueryPlan.hpp>
#include <AbstractPhysicalOperator.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <ScanPhysicalOperator.hpp>
#include <PipelinedQueryPlan.hpp>

namespace NES::QueryCompilation::PipeliningPhase
{

/// List of strikebreakers. For now hardcoded
static const std::tuple<std::string> pipelineBreakers = {"Join"};


void processFusibleOperator(
    const std::shared_ptr<PipelinedQueryPlan>& pipelinePlan,
    std::unordered_map<std::shared_ptr<Operator>, std::shared_ptr<Pipeline>>& pipelineOperatorMap,
    const std::shared_ptr<Pipeline>& currentPipeline,
    const std::shared_ptr<PhysicalOperatorNode>& currentOperator);

void processPipelineBreakerOperator(
    const std::shared_ptr<PipelinedQueryPlan>& pipelinePlan,
    std::unordered_map<std::shared_ptr<Operator>, std::shared_ptr<Pipeline>>& pipelineOperatorMap,
    const std::shared_ptr<Pipeline>& currentPipeline,
    const std::shared_ptr<PhysicalOperatorNode>& currentOperator);

void processSink(
    const std::shared_ptr<PipelinedQueryPlan>& pipelinePlan,
    std::unordered_map<std::shared_ptr<Operator>, std::shared_ptr<Pipeline>>& pipelineOperatorMap,
    const std::shared_ptr<Pipeline>& currentPipeline,
    const std::shared_ptr<PhysicalOperatorNode>& currentOperator);

void processSource(
    const std::shared_ptr<PipelinedQueryPlan>& pipelinePlan,
    std::unordered_map<std::shared_ptr<Operator>, std::shared_ptr<Pipeline>>&,
    std::shared_ptr<Pipeline> currentPipeline,
    const std::shared_ptr<PhysicalOperatorNode>& currentOperator);

void process(
    const std::shared_ptr<PipelinedQueryPlan>& pipelinePlan,
    std::unordered_map<std::shared_ptr<Operator>, std::shared_ptr<Pipeline>>& pipelineOperatorMap,
    const std::shared_ptr<Pipeline>& currentPipeline,
    const std::shared_ptr<PhysicalOperatorNode>& currentOperator);

/// During this step we create a PipelinedQueryPlan out of the QueryPlan obj
std::shared_ptr<PipelinedQueryPlan> apply(const std::unique_ptr<QueryPlan> queryPlan);

}

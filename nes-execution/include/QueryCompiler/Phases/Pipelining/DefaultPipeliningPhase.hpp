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

#include <map>
#include <memory>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Operator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/Pipelining/OperatorFusionPolicy.hpp>
#include <QueryCompiler/Phases/Pipelining/PipeliningPhase.hpp>

namespace NES::QueryCompilation
{

/**
 * @brief The default pipelining phase,
 * which splits query plans in pipelines of operators according to specific operator fusion policy.
 */
class DefaultPipeliningPhase : public PipeliningPhase
{
public:
    ~DefaultPipeliningPhase() override = default;
    /**
     * @brief Creates a new pipelining phase with a operator fusion policy.
     * @param operatorFusionPolicy Policy to determine if an operator can be fused.
     * @return std::shared_ptr<PipeliningPhase>
     */
    static std::shared_ptr<PipeliningPhase> create(const std::shared_ptr<OperatorFusionPolicy>& operatorFusionPolicy);
    explicit DefaultPipeliningPhase(std::shared_ptr<OperatorFusionPolicy> operatorFusionPolicy);
    std::shared_ptr<PipelineQueryPlan> apply(std::shared_ptr<DecomposedQueryPlan> decomposedQueryPlan) override;

protected:
    void process(
        const std::shared_ptr<PipelineQueryPlan>& pipeline,
        std::map<std::shared_ptr<Operator>, std::shared_ptr<OperatorPipeline>>& pipelineOperatorMap,
        const std::shared_ptr<OperatorPipeline>& currentPipeline,
        const std::shared_ptr<Operator>& currentOperator);
    void processSink(
        const std::shared_ptr<PipelineQueryPlan>& pipelinePlan,
        std::map<std::shared_ptr<Operator>, std::shared_ptr<OperatorPipeline>>& pipelineOperatorMap,
        const std::shared_ptr<OperatorPipeline>& currentPipeline,
        const SinkLogicalOperator& currentOperator);
    static void processSource(
        const std::shared_ptr<PipelineQueryPlan>& pipelinePlan,
        std::map<std::shared_ptr<Operator>, std::shared_ptr<OperatorPipeline>>& pipelineOperatorMap,
        std::shared_ptr<OperatorPipeline> currentPipeline,
        const std::shared_ptr<SourceDescriptorLogicalOperator>& sourceOperator);
    void processMultiplex(
        const std::shared_ptr<PipelineQueryPlan>& pipelinePlan,
        std::map<std::shared_ptr<Operator>, std::shared_ptr<OperatorPipeline>>& pipelineOperatorMap,
        std::shared_ptr<OperatorPipeline> currentPipeline,
        const std::shared_ptr<PhysicalOperators::PhysicalOperator>& currentOperator);
    void processDemultiplex(
        const std::shared_ptr<PipelineQueryPlan>& pipelinePlan,
        std::map<std::shared_ptr<Operator>, std::shared_ptr<OperatorPipeline>>& pipelineOperatorMap,
        std::shared_ptr<OperatorPipeline> currentPipeline,
        const std::shared_ptr<PhysicalOperators::PhysicalOperator>& currentOperator);
    void processFusibleOperator(
        const std::shared_ptr<PipelineQueryPlan>& pipelinePlan,
        std::map<std::shared_ptr<Operator>, std::shared_ptr<OperatorPipeline>>& pipelineOperatorMap,
        const std::shared_ptr<OperatorPipeline>& currentPipeline,
        const std::shared_ptr<PhysicalOperators::PhysicalOperator>& currentOperator);
    void processPipelineBreakerOperator(
        const std::shared_ptr<PipelineQueryPlan>& pipelinePlan,
        std::map<std::shared_ptr<Operator>, std::shared_ptr<OperatorPipeline>>& pipelineOperatorMap,
        const std::shared_ptr<OperatorPipeline>& currentPipeline,
        const std::shared_ptr<PhysicalOperators::PhysicalOperator>& currentOperator);

private:
    std::shared_ptr<OperatorFusionPolicy> operatorFusionPolicy;
};
}

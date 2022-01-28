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
#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELINING_DEFAULTPIPELININGPHASE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELINING_DEFAULTPIPELININGPHASE_HPP_
#include <QueryCompiler/Phases/Pipelining/PipeliningPhase.hpp>
#include <map>
namespace NES {
namespace QueryCompilation {

/**
 * @brief The default pipelining phase,
 * which splits query plans in pipelines of operators according to specific operator fusion policy.
 */
class DefaultPipeliningPhase : public PipeliningPhase {
  public:
    ~DefaultPipeliningPhase() override = default;
    /**
     * @brief Creates a new pipelining phase with a operator fusion policy.
     * @param operatorFusionPolicy Policy to determine if an operator can be fused.
     * @return PipeliningPhasePtr
     */
    static PipeliningPhasePtr create(const OperatorFusionPolicyPtr& operatorFusionPolicy);
    explicit DefaultPipeliningPhase(OperatorFusionPolicyPtr operatorFusionPolicy);
    PipelineQueryPlanPtr apply(QueryPlanPtr queryPlan) override;

  protected:
    void process(const PipelineQueryPlanPtr& pipeline,
                 std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                 const OperatorPipelinePtr& currentPipeline,
                 const PhysicalOperators::PhysicalOperatorPtr& currentOperators);
    void processSink(const PipelineQueryPlanPtr& pipelinePlan,
                     std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                     const OperatorPipelinePtr& currentPipeline,
                     const PhysicalOperators::PhysicalOperatorPtr& currentOperator);
    static void processSource(const PipelineQueryPlanPtr& pipelinePlan,
                              std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                              OperatorPipelinePtr currentPipeline,
                              const PhysicalOperators::PhysicalOperatorPtr& sourceOperator);
    void processMultiplex(const PipelineQueryPlanPtr& pipelinePlan,
                          std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                          OperatorPipelinePtr currentPipeline,
                          const PhysicalOperators::PhysicalOperatorPtr& currentOperator);
    void processDemultiplex(const PipelineQueryPlanPtr& pipelinePlan,
                            std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                            OperatorPipelinePtr currentPipeline,
                            const PhysicalOperators::PhysicalOperatorPtr& currentOperator);
    void processFusibleOperator(const PipelineQueryPlanPtr& pipelinePlan,
                                std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                                const OperatorPipelinePtr& currentPipeline,
                                const PhysicalOperators::PhysicalOperatorPtr& currentOperator);
    void processPipelineBreakerOperator(const PipelineQueryPlanPtr& pipelinePlan,
                                        std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                                        const OperatorPipelinePtr& currentPipeline,
                                        const PhysicalOperators::PhysicalOperatorPtr& currentOperator);

  private:
    OperatorFusionPolicyPtr operatorFusionPolicy;
};
}// namespace QueryCompilation
}// namespace NES

#endif  // NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELINING_DEFAULTPIPELININGPHASE_HPP_

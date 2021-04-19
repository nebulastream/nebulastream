#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELINING_DEFAULTPIPELININGPHASE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELINING_DEFAULTPIPELININGPHASE_HPP_
#include <QueryCompiler/Phases/Pipelining/PipeliningPhase.hpp>
#include <map>
namespace NES {
namespace QueryCompilation {
class DefaultPipeliningPhase: public PipeliningPhase {
  public:
    static PipeliningPhasePtr create(PipelineBreakerPolicyPtr pipelineBreakerPolicy);
    DefaultPipeliningPhase(PipelineBreakerPolicyPtr pipelineBreakerPolicy);
    PipelineQueryPlanPtr apply(QueryPlanPtr queryPlan) override;
  protected:
    void process(PipelineQueryPlanPtr pipeline,
                 std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                 OperatorPipelinePtr currentPipeline,
                 PhysicalOperators::PhysicalOperatorPtr currentOperators);
    void processSink(PipelineQueryPlanPtr pipelinePlan, std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                     OperatorPipelinePtr currentPipeline, PhysicalOperators::PhysicalOperatorPtr currentOperator);
    void processSource(PipelineQueryPlanPtr pipelinePlan, std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                       OperatorPipelinePtr currentPipeline, PhysicalOperators::PhysicalOperatorPtr sourceOperator);
    void processMultiplex(PipelineQueryPlanPtr pipelinePlan, std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                          OperatorPipelinePtr currentPipeline, PhysicalOperators::PhysicalOperatorPtr currentOperator);
    void processDemultiplex(PipelineQueryPlanPtr pipelinePlan, std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                            OperatorPipelinePtr currentPipeline, PhysicalOperators::PhysicalOperatorPtr currentOperator);
    void processFusibleOperator(PipelineQueryPlanPtr pipelinePlan, std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                                OperatorPipelinePtr currentPipeline, PhysicalOperators::PhysicalOperatorPtr currentOperator);
    void processPipelineBreakerOperator(PipelineQueryPlanPtr pipelinePlan, std::map<OperatorNodePtr, OperatorPipelinePtr>& pipelineOperatorMap,
                                        OperatorPipelinePtr currentPipeline, PhysicalOperators::PhysicalOperatorPtr currentOperator);

  private:
    PipelineBreakerPolicyPtr pipelineBreakerPolicy;
};
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELINING_DEFAULTPIPELININGPHASE_HPP_

#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_TRANSLATETOEXECUTABLEQUERYPLANPHASE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_TRANSLATETOEXECUTABLEQUERYPLANPHASE_HPP_

#include <NodeEngine/Execution/NewExecutableQueryPlan.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

#include <NodeEngine/Execution/NewExecutablePipeline.hpp>
#include <vector>

namespace NES {
namespace QueryCompilation {

class TranslateToExecutableQueryPlanPhase {
  public:
    static TranslateToExecutableQueryPlanPhasePtr create();
    NodeEngine::Execution::NewExecutableQueryPlanPtr apply(PipelineQueryPlanPtr pipelineQueryPlan,
                                                           NodeEngine::NodeEnginePtr nodeEngine);

  private:

    void processSource(OperatorPipelinePtr pipeline, std::vector<DataSourcePtr>& sources, std::vector<DataSinkPtr>& sinks,
                       std::vector<NodeEngine::Execution::NewExecutablePipelinePtr>& executablePipelines,
                       NodeEngine::NodeEnginePtr nodeEngine);

    NodeEngine::Execution::SuccessorPipeline processSuccessor(OperatorPipelinePtr pipeline, std::vector<DataSourcePtr>& sources, std::vector<DataSinkPtr>& sinks,
                                                              std::vector<NodeEngine::Execution::NewExecutablePipelinePtr>& executablePipelines,
                                                              NodeEngine::NodeEnginePtr nodeEngine);

    NodeEngine::Execution::SuccessorPipeline processSink(OperatorPipelinePtr pipeline, std::vector<DataSourcePtr>& sources, std::vector<DataSinkPtr>& sinks,
                       std::vector<NodeEngine::Execution::NewExecutablePipelinePtr>& executablePipelines,
                       NodeEngine::NodeEnginePtr nodeEngine);

    NodeEngine::Execution::SuccessorPipeline processOperatorPipeline(OperatorPipelinePtr pipeline, std::vector<DataSourcePtr>& sources, std::vector<DataSinkPtr>& sinks,
                                                     std::vector<NodeEngine::Execution::NewExecutablePipelinePtr>& executablePipelines,
                                                     NodeEngine::NodeEnginePtr nodeEngine);


   };
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_TRANSLATETOEXECUTABLEQUERYPLANPHASE_HPP_

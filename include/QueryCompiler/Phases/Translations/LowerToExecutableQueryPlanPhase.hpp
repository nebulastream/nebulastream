/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_LOWERTOEXECUTABLEQUERYPLANPHASE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_LOWERTOEXECUTABLEQUERYPLANPHASE_HPP_

#include <NodeEngine/Execution/ExecutableQueryPlan.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

#include <NodeEngine/Execution/ExecutablePipeline.hpp>
#include <vector>

namespace NES {
namespace QueryCompilation {

class LowerToExecutableQueryPlanPhase {
  public:
    LowerToExecutableQueryPlanPhase(DataSinkProviderPtr sinkProvider, DataSourceProviderPtr sourceProvider);
    static LowerToExecutableQueryPlanPhasePtr create(DataSinkProviderPtr sinkProvider, DataSourceProviderPtr sourceProvider);
    NodeEngine::Execution::ExecutableQueryPlanPtr apply(PipelineQueryPlanPtr pipelineQueryPlan,
                                                           NodeEngine::NodeEnginePtr nodeEngine);

  private:
    DataSinkProviderPtr sinkProvider;
    DataSourceProviderPtr sourceProvider;
    void processSource(OperatorPipelinePtr pipeline,
                       std::vector<DataSourcePtr>& sources,
                       std::vector<DataSinkPtr>& sinks,
                       std::vector<NodeEngine::Execution::ExecutablePipelinePtr>& executablePipelines,
                       NodeEngine::NodeEnginePtr nodeEngine,
                       QueryId queryId,
                       QuerySubPlanId subQueryPlanId,
                       std::map<uint64_t, NodeEngine::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap);

    NodeEngine::Execution::SuccessorExecutablePipeline
    processSuccessor(OperatorPipelinePtr pipeline,
                     std::vector<DataSourcePtr>& sources,
                     std::vector<DataSinkPtr>& sinks,
                     std::vector<NodeEngine::Execution::ExecutablePipelinePtr>& executablePipelines,
                     NodeEngine::NodeEnginePtr nodeEngine,
                     QueryId queryId,
                     QuerySubPlanId subQueryPlanId,
                     std::map<uint64_t, NodeEngine::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap);

    NodeEngine::Execution::SuccessorExecutablePipeline
    processSink(OperatorPipelinePtr pipeline,
                std::vector<DataSourcePtr>& sources,
                std::vector<DataSinkPtr>& sinks,
                std::vector<NodeEngine::Execution::ExecutablePipelinePtr>& executablePipelines,
                NodeEngine::NodeEnginePtr nodeEngine,
                QueryId queryId,
                QuerySubPlanId subQueryPlanId);

    NodeEngine::Execution::SuccessorExecutablePipeline
    processOperatorPipeline(OperatorPipelinePtr pipeline,
                            std::vector<DataSourcePtr>& sources,
                            std::vector<DataSinkPtr>& sinks,
                            std::vector<NodeEngine::Execution::ExecutablePipelinePtr>& executablePipelines,
                            NodeEngine::NodeEnginePtr nodeEngine,
                            QueryId queryId,
                            QuerySubPlanId subQueryPlanId,
                            std::map<uint64_t, NodeEngine::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap);
};
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_LOWERTOEXECUTABLEQUERYPLANPHASE_HPP_

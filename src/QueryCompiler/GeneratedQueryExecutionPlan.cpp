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

#include <NodeEngine/Execution/PipelineExecutionContext.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <Util/Logger.hpp>
#include <utility>
namespace NES {

GeneratedQueryExecutionPlan::GeneratedQueryExecutionPlan(QueryId queryId, QuerySubPlanId querySubPlanId,
                                                         std::vector<DataSourcePtr>&& sources, std::vector<DataSinkPtr>&& sinks,
                                                         std::vector<NodeEngine::Execution::ExecutablePipelinePtr>&& pipelines, NodeEngine::QueryManagerPtr&& queryManager,
                                                         NodeEngine::BufferManagerPtr&& bufferManager)
    : NodeEngine::Execution::ExecutableQueryPlan(queryId, querySubPlanId, std::move(sources), std::move(sinks), std::move(pipelines),
                         std::move(queryManager), std::move(bufferManager)) {
    // sanity checks
    for (auto& stage : this->pipelines) {
        NES_ASSERT(!!stage, "Invalid stage");
    }
}

}// namespace NES

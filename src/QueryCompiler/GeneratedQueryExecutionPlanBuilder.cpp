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

#include <QueryCompiler/GeneratableOperators/GeneratableOperator.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlanBuilder.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <Util/Logger.hpp>
#include <utility>
namespace NES {

GeneratedQueryExecutionPlanBuilder::GeneratedQueryExecutionPlanBuilder() {
    setBufferManager(nullptr);
    setQueryManager(nullptr);
    setCompiler(nullptr);
    setQueryId(0);
}

GeneratedQueryExecutionPlanBuilder GeneratedQueryExecutionPlanBuilder::create() { return GeneratedQueryExecutionPlanBuilder(); }

BufferManagerPtr GeneratedQueryExecutionPlanBuilder::getBufferManager() const { return bufferManager; }

QueryId GeneratedQueryExecutionPlanBuilder::getQueryId() const { return queryId; }

GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::addPipeline(NodeEngine::Execution::ExecutablePipelinePtr pipeline) {
    pipelines.push_back(pipeline);
    return *this;
}

GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::setBufferManager(BufferManagerPtr bufferManager) {
    this->bufferManager = std::move(bufferManager);
    return *this;
}

GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::setQueryManager(NodeEngine::QueryManagerPtr queryManager) {
    this->queryManager = std::move(queryManager);
    return *this;
}

NodeEngine::QueryManagerPtr GeneratedQueryExecutionPlanBuilder::getQueryManager() const { return queryManager; }

NodeEngine::Execution::ExecutableQueryPlanPtr GeneratedQueryExecutionPlanBuilder::build() {
    NES_ASSERT(bufferManager, "GeneratedQueryExecutionPlanBuilder: Invalid bufferManager");
    NES_ASSERT(queryManager, "GeneratedQueryExecutionPlanBuilder: Invalid queryManager");
    NES_ASSERT(!sources.empty(), "GeneratedQueryExecutionPlanBuilder: Invalid number of sources");
    NES_ASSERT(!sinks.empty(), "GeneratedQueryExecutionPlanBuilder: Invalid number of sinks");
    NES_ASSERT(queryId != INVALID_QUERY_ID, "GeneratedQueryExecutionPlanBuilder: Invalid Query Id");
    NES_ASSERT(querySubPlanId != INVALID_QUERY_SUB_PLAN_ID, "GeneratedQueryExecutionPlanBuilder: Invalid Query Subplan Id");
    NES_ASSERT(queryCompiler, "GeneratedQueryExecutionPlanBuilder: Invalid compiler or no stages");

    if (pipelines.empty() && !leaves.empty()) {
        for (auto& operatorPlan : leaves) {
            queryCompiler->compile(*this, operatorPlan);
        }
        NES_ASSERT(!pipelines.empty(), "GeneratedQueryExecutionPlanBuilder: No stages after query compilation");
        std::reverse(pipelines.begin(), pipelines.end());// this is necessary, check plan generator documentation
    }

    return std::make_shared<GeneratedQueryExecutionPlan>(queryId, querySubPlanId, std::move(sources), std::move(sinks),
                                                         std::move(pipelines), std::move(queryManager), std::move(bufferManager));
}

std::vector<DataSinkPtr>& GeneratedQueryExecutionPlanBuilder::getSinks() { return sinks; }

GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::addSink(DataSinkPtr sink) {
    sinks.emplace_back(sink);
    return *this;
}

GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::addSource(DataSourcePtr source) {
    sources.emplace_back(source);
    return *this;
}

GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::setQueryId(QueryId queryId) {
    this->queryId = std::move(queryId);
    return *this;
}

GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::setCompiler(QueryCompilerPtr queryCompiler) {
    this->queryCompiler = std::move(queryCompiler);
    return *this;
}

GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::addOperatorQueryPlan(OperatorNodePtr queryPlan) {
    leaves.emplace_back(queryPlan);
    return *this;
}

DataSourcePtr GeneratedQueryExecutionPlanBuilder::getSource(uint64_t index) { return sources[index]; }

DataSinkPtr GeneratedQueryExecutionPlanBuilder::getSink(uint64_t index) { return sinks[index]; }

uint64_t GeneratedQueryExecutionPlanBuilder::getNumberOfPipelineStages() const { return pipelines.size(); }

GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::setQuerySubPlanId(QuerySubPlanId querySubPlanId) {
    this->querySubPlanId = querySubPlanId;
    return *this;
}

QuerySubPlanId GeneratedQueryExecutionPlanBuilder::getQuerySubPlanId() const { return querySubPlanId; }

}// namespace NES
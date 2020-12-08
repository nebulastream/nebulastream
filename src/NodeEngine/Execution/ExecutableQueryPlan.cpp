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

#include <NodeEngine/Execution/ExecutableQueryPlan.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <utility>

namespace NES {

ExecutableQueryPlan::ExecutableQueryPlan(QueryId queryId, QuerySubPlanId querySubPlanId, std::vector<DataSourcePtr>&& sources,
                                       std::vector<DataSinkPtr>&& sinks, std::vector<PipelineStagePtr>&& stages,
                                       QueryManagerPtr&& queryManager, BufferManagerPtr&& bufferManager)
    : queryId(queryId), querySubPlanId(querySubPlanId), sources(std::move(sources)), sinks(std::move(sinks)),
      stages(std::move(stages)), queryManager(std::move(queryManager)), bufferManager(std::move(bufferManager)),
      qepStatus(Created) {}

QueryId ExecutableQueryPlan::getQueryId() { return queryId; }

std::vector<PipelineStagePtr>& ExecutableQueryPlan::getStages() { return stages; }

QuerySubPlanId ExecutableQueryPlan::getQuerySubPlanId() const { return querySubPlanId; }

ExecutableQueryPlan::~ExecutableQueryPlan() {
    NES_DEBUG("destroy qep " << queryId << " " << querySubPlanId);
    NES_ASSERT(qepStatus.load() == Stopped || qepStatus.load() == ErrorState,
               "QueryPlan is created but not executing " << queryId);
    sources.clear();
    stages.clear();
}

bool ExecutableQueryPlan::stop() {
    bool ret = true;
    //    NES_ASSERT(qepStatus == Running, "QueryPlan is created but not executing " << queryId << " " << querySubPlanId);

    auto expected = Running;
    if (qepStatus.compare_exchange_strong(expected, Stopped)) {
        NES_DEBUG("QueryExecutionPlan: stop " << queryId << " " << querySubPlanId);
        for (auto& stage : stages) {
            if (!stage->stop()) {
                NES_ERROR("QueryExecutionPlan: stop failed for stage " << stage);
                ret = false;
            }
        }
    }

    if (!ret) {
        qepStatus.store(ErrorState);
    }
    return ret;
}

ExecutableQueryPlan::QueryExecutionPlanStatus ExecutableQueryPlan::getStatus() { return qepStatus.load(); }

bool ExecutableQueryPlan::setup() {
    NES_DEBUG("QueryExecutionPlan: setup " << queryId << " " << querySubPlanId);
    auto expected = Created;
    if (qepStatus.compare_exchange_strong(expected, Deployed)) {
        for (auto& stage : stages) {
            if (!stage->setup(queryManager, bufferManager)) {
                NES_ERROR("QueryExecutionPlan: setup failed!" << queryId << " " << querySubPlanId);
                this->stop();
                return false;
            }
        }
    } else {
        NES_THROW_RUNTIME_ERROR("QEP expected to be Created but was not");
    }
    return true;
}

bool ExecutableQueryPlan::start() {
    NES_DEBUG("QueryExecutionPlan: start " << queryId << " " << querySubPlanId);
    auto expected = Deployed;
    if (qepStatus.compare_exchange_strong(expected, Running)) {
        for (auto& stage : stages) {
            if (!stage->start()) {
                NES_ERROR("QueryExecutionPlan: start failed! " << queryId << " " << querySubPlanId);
                this->stop();
                return false;
            }
        }
    } else {
        NES_THROW_RUNTIME_ERROR("QEP expected to be Deployed but was not");
    }
    return true;
}

QueryManagerPtr ExecutableQueryPlan::getQueryManager() { return queryManager; }

BufferManagerPtr ExecutableQueryPlan::getBufferManager() { return bufferManager; }

std::vector<DataSourcePtr> ExecutableQueryPlan::getSources() const { return sources; }

std::vector<DataSinkPtr> ExecutableQueryPlan::getSinks() const { return sinks; }

PipelineStagePtr ExecutableQueryPlan::getStage(uint64_t index) const { return stages[index]; }

uint64_t ExecutableQueryPlan::getStageSize() const { return stages.size(); }

void ExecutableQueryPlan::print() {
    for (auto source : sources) {
        NES_INFO("Source:" << source);
        NES_INFO("\t Generated Buffers=" << source->getNumberOfGeneratedBuffers());
        NES_INFO("\t Generated Tuples=" << source->getNumberOfGeneratedTuples());
        NES_INFO("\t Schema=" << source->getSourceSchemaAsString());
    }
    for (auto sink : sinks) {
        NES_INFO("Sink:" << sink);
        NES_INFO("\t Generated Buffers=" << sink->getNumberOfWrittenOutBuffers());
        NES_INFO("\t Generated Tuples=" << sink->getNumberOfWrittenOutTuples());
    }
}

}// namespace NES

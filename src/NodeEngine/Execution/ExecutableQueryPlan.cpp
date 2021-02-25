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

#include <NodeEngine/Execution/ExecutablePipeline.hpp>
#include <NodeEngine/Execution/ExecutableQueryPlan.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <utility>

namespace NES::NodeEngine::Execution {

ExecutableQueryPlan::ExecutableQueryPlan(QueryId queryId, QuerySubPlanId querySubPlanId, std::vector<DataSourcePtr>&& sources,
                                         std::vector<DataSinkPtr>&& sinks, std::vector<ExecutablePipelinePtr>&& pipelines,
                                         QueryManagerPtr&& queryManager, BufferManagerPtr&& bufferManager)
    : queryId(queryId), querySubPlanId(querySubPlanId), sources(std::move(sources)), sinks(std::move(sinks)),
      pipelines(std::move(pipelines)), queryManager(std::move(queryManager)), bufferManager(std::move(bufferManager)),
      qepStatus(Created), numOfProducers(this->sources.size()) {
    // nop
}

void ExecutableQueryPlan::pin() {
    numOfProducers++;
}

QueryId ExecutableQueryPlan::getQueryId() { return queryId; }

std::vector<ExecutablePipelinePtr> ExecutableQueryPlan::getPipelines() { return pipelines; }

QuerySubPlanId ExecutableQueryPlan::getQuerySubPlanId() const { return querySubPlanId; }

ExecutableQueryPlan::~ExecutableQueryPlan() {
    NES_DEBUG("destroy qep " << queryId << " " << querySubPlanId);
    NES_ASSERT(qepStatus.load() == Stopped || qepStatus.load() == ErrorState,
               "QueryPlan is created but not executing " << queryId);
    sources.clear();
    pipelines.clear();
}

std::future<ExecutableQueryPlanResult> ExecutableQueryPlan::getTerminationFuture() {
    return qepTerminationStatusFuture.get_future();
}

bool ExecutableQueryPlan::fail() {
    bool ret = true;
    auto expected = Running;
    if (qepStatus.compare_exchange_strong(expected, ErrorState)) {
        NES_DEBUG("QueryExecutionPlan: fail " << queryId << " " << querySubPlanId);
        for (auto& stage : pipelines) {
            if (!stage->stop()) {
                NES_ERROR("QueryExecutionPlan: fail failed for stage " << stage);
                ret = false;
            }
        }
    }

    if (!ret) {
        qepStatus.store(ErrorState);
    }
    return ret;
}

ExecutableQueryPlanStatus ExecutableQueryPlan::getStatus() { return qepStatus.load(); }

bool ExecutableQueryPlan::setup() {
    NES_DEBUG("QueryExecutionPlan: setup " << queryId << " " << querySubPlanId);
    auto expected = Created;
    if (qepStatus.compare_exchange_strong(expected, Deployed)) {
        for (auto& stage : pipelines) {
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
        for (auto& stage : pipelines) {
            NES_DEBUG("ExecutableQueryPlan::start qep=" << stage->getQepParentId() << " pipe=" << stage->getPipeStageId()
                                                        << " next=" << stage->getNextPipeline()
                                                        << " arity=" << stage->getArity());
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

uint32_t ExecutableQueryPlan::getNumberOfPipelines() { return pipelines.size(); }

QueryManagerPtr ExecutableQueryPlan::getQueryManager() { return queryManager; }

BufferManagerPtr ExecutableQueryPlan::getBufferManager() { return bufferManager; }

std::vector<DataSourcePtr> ExecutableQueryPlan::getSources() const { return sources; }

std::vector<DataSinkPtr> ExecutableQueryPlan::getSinks() const { return sinks; }

ExecutablePipelinePtr ExecutableQueryPlan::getPipeline(uint64_t index) const { return pipelines[index]; }

void ExecutableQueryPlan::reconfigure(ReconfigurationTask& task, WorkerContext& context) {
    Reconfigurable::reconfigure(task, context);
    for (auto& stage : pipelines) {
        stage->reconfigure(task, context);
    }
    for (auto& sink : sinks) {
        sink->reconfigure(task, context);
    }
}

bool ExecutableQueryPlan::stop() {
    bool ret = true;
    auto expected = Running;
    NES_DEBUG("QueryExecutionPlan: stop " << queryId << " " << querySubPlanId);
    if (qepStatus.compare_exchange_strong(expected, Stopped)) {
        NES_DEBUG("QueryExecutionPlan: stop " << queryId << "-" << querySubPlanId << " is marked as stopped now");
        for (auto& stage : pipelines) {
            if (!stage->stop()) {
                NES_ERROR("QueryExecutionPlan: stop failed for stage " << stage);
                ret = false;
            }
        }
        qepTerminationStatusFuture.set_value(ExecutableQueryPlanResult::Ok);
        return true;
    }
    if (!ret) {
        qepStatus.store(ErrorState);
        qepTerminationStatusFuture.set_value(ExecutableQueryPlanResult::Error);
    }
    return false;
}

void ExecutableQueryPlan::postReconfigurationCallback(ReconfigurationTask& task) {
    Reconfigurable::postReconfigurationCallback(task);
    switch (task.getType()) {
        case EndOfStream: {
            if (numOfProducers.fetch_sub(1) == 1) {
                stop();
            } else {
                return; // we must not invoke end of stream on stages and sinks if the qep was not stopped
            }
            break;
        }
        default: {
            break;
        }
    }
    for (auto& stage : pipelines) {
        stage->postReconfigurationCallback(task);
    }
    for (auto& sink : sinks) {
        sink->postReconfigurationCallback(task);
    }
}

}// namespace NES::NodeEngine::Execution

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
#include <NodeEngine/QueryManager.hpp>
#include <QueryCompiler/SinkReconfiguration.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>

namespace NES::NodeEngine {

SinkReconfiguration::SinkReconfiguration(std::vector<NES::DataSinkPtr> sinks,
                                         NES::NodeEngine::Execution::ExecutableQueryPlanPtr qep)
    : sinks(std::move(sinks)), qep(qep) {
    //nop
}

void SinkReconfiguration::reconfigure(ReconfigurationTask& task, WorkerContext& context) {
    Reconfigurable::reconfigure(task, context);
    switch (task.getType()) {
        case Initialize: {
            break;
        }
        default: {
            NES_THROW_RUNTIME_ERROR("SinkReconfiguration: task type not supported");
        }
    }
}

void SinkReconfiguration::destroyCallback(ReconfigurationTask& task) {
    Reconfigurable::destroyCallback(task);
    switch (task.getType()) {
        case Initialize: {
            auto start = std::chrono::system_clock::now();
            auto end = std::chrono::system_clock::now();
            auto existingSinks = qep->getSinks();
            auto sinkComparator = [](DataSinkPtr aSink, DataSinkPtr bSink) {
                return aSink->getOperatorId() < bSink->getOperatorId();
            };
            std::sort(existingSinks.begin(), existingSinks.begin(), sinkComparator);
            std::sort(sinks.begin(), sinks.end(), sinkComparator);
            std::vector<DataSinkPtr> sinksToBeAdded;
            std::set_difference(sinks.begin(), sinks.end(), existingSinks.begin(), existingSinks.end(),
                                std::back_inserter(sinksToBeAdded), sinkComparator);
            std::vector<DataSinkPtr> sinksToBeDeleted;
            std::set_difference(existingSinks.begin(), existingSinks.end(), sinks.begin(), sinks.end(),
                                std::back_inserter(sinksToBeDeleted), sinkComparator);
            std::vector<DataSinkPtr> finalSinks;
            std::set_difference(existingSinks.begin(), existingSinks.end(), sinksToBeDeleted.begin(), sinksToBeDeleted.end(),
                                std::back_inserter(finalSinks), sinkComparator);
            for (const auto& sinkToAdd : sinksToBeAdded) {
                NES_DEBUG("SinkReconfiguration::destroyCallback: set up sink " << sinkToAdd->toString() << " for QueryId "
                                                                               << qep->getQueryId());
                sinkToAdd->setup();
            }
            finalSinks.insert(finalSinks.end(), sinksToBeAdded.begin(), sinksToBeAdded.end());
            qep->updateSinks(finalSinks);
            for (const auto& sinkToDelete : sinksToBeDeleted) {
                NES_DEBUG("SinkReconfiguration::destroyCallback: shut down sink " << sinkToDelete->toString() << " for QueryId "
                                                                                  << qep->getQueryId());
                sinkToDelete->shutdown();
            }
            end = std::chrono::system_clock::now();
            NES_TIMER("BDAPRO2Tracking: reconfiguration tasks - worker logic - (microseconds) : "
                      << "(" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << ")");
            break;
        }
        default: {
            NES_THROW_RUNTIME_ERROR("SinkReconfiguration: task type not supported");
        }
    }
}
void SinkReconfiguration::setup(QueryManagerPtr queryManager, QuerySubPlanId querySubPlanId) {
    queryManager->addReconfigurationTask(querySubPlanId, ReconfigurationTask(querySubPlanId, NES::NodeEngine::Initialize, this),
                                         true);
}
}// namespace NES::NodeEngine
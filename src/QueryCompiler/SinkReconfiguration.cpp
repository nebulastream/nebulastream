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
            auto existingSinks = qep->getSinks();
            for (auto existingSink : existingSinks) {
                existingSink->shutdown();
            }
            for (auto newSink : sinks) {
                newSink->setup();
            }
            qep->addSinks(sinks);
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
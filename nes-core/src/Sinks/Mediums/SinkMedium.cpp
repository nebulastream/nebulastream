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

#include <API/Schema.hpp>
#include <Components/NesWorker.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <utility>

namespace NES {

SinkMedium::SinkMedium(SinkFormatPtr sinkFormat,
                       Runtime::NodeEnginePtr nodeEngine,
                       QuerySubPlanId querySubPlanId)
    : sinkFormat(std::move(sinkFormat)), nodeEngine(std::move(nodeEngine)), querySubPlanId(querySubPlanId) {
    watermarkProcessor = std::make_unique<Windowing::MultiOriginWatermarkProcessor>(1);
    NES_DEBUG("SinkMedium:Init Data Sink!");
}

uint64_t SinkMedium::getNumberOfWrittenOutBuffers() {
    std::unique_lock lock(writeMutex);
    return sentBuffer;
}
uint64_t SinkMedium::getNumberOfWrittenOutTuples() {
    std::unique_lock lock(writeMutex);
    return sentTuples;
}

SchemaPtr SinkMedium::getSchemaPtr() const { return sinkFormat->getSchemaPtr(); }

std::string SinkMedium::getSinkFormat() { return sinkFormat->toString(); }

bool SinkMedium::getAppendAsBool() const { return append; }

QuerySubPlanId SinkMedium::getParentPlanId() const { return querySubPlanId; }

std::string SinkMedium::getAppendAsString() const {
    if (append) {
        return "APPEND";
    }
    return "OVERWRITE";
}

bool SinkMedium::notifyEpochTermination(uint64_t epochBarrier) const {
    uint64_t queryId = nodeEngine->getQueryManager()->getQueryId(querySubPlanId);
    NES_ASSERT(queryId >= 0, "SinkMedium: no queryId found for querySubPlanId");
    if (std::shared_ptr<NesWorker> nesWorker = this->nodeEngine->getNesWorker().lock()) {
        bool success = nesWorker->notifyEpochTermination(epochBarrier, queryId);
        if (success) {
            return true;
        }
    }
    return false;
}

void SinkMedium::reconfigure(Runtime::ReconfigurationMessage & message, Runtime::WorkerContext & context) {
    Reconfigurable::reconfigure(message, context);
}
void SinkMedium::postReconfigurationCallback(Runtime::ReconfigurationMessage & message) {
    Reconfigurable::postReconfigurationCallback(message);
    switch (message.getType()) {
        case Runtime::SoftEndOfStream:
        case Runtime::HardEndOfStream: {
            shutdown();
            break;
        }
        default: {
            break;
        }
    }
}
}// namespace NES

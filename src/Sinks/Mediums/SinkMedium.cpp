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

#include <API/Schema.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <utility>

namespace NES {

SinkMedium::SinkMedium(OperatorId logicalOperatorId, SinkFormatPtr sinkFormat, QuerySubPlanId parentPlanId)
    : sinkFormat(std::move(sinkFormat)), logicalOperatorId(logicalOperatorId), parentPlanId(parentPlanId) {
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

SinkMedium::~SinkMedium() { NES_DEBUG("Destroy Data Sink  " << this); }

void SinkMedium::addNewProducer() { numOfProducers.fetch_add(1); }

SchemaPtr SinkMedium::getSchemaPtr() const { return sinkFormat->getSchemaPtr(); }

std::string SinkMedium::getSinkFormat() { return sinkFormat->toString(); }

bool SinkMedium::getAppendAsBool() const { return append; }

std::string SinkMedium::getAppendAsString() const {
    if (append) {
        return "APPEND";
    }
    return "OVERWRITE";
}
void SinkMedium::reconfigure(Runtime::ReconfigurationMessage& message, Runtime::WorkerContext& context) {
    Reconfigurable::reconfigure(message, context);
}
void SinkMedium::postReconfigurationCallback(Runtime::ReconfigurationMessage& message) {
    Reconfigurable::postReconfigurationCallback(message);
    switch (message.getType()) {
        case Runtime::SoftEndOfStream:
        case Runtime::HardEndOfStream: {
            if (numOfProducers.fetch_sub(1) == 1) {
                shutdown();
                break;
            }
        }
        default: {
            break;
        }
    }
}
}// namespace NES

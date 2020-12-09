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

#include <NodeEngine/QueryManager.hpp>
#include <Sinks/Mediums/PrintSink.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <sstream>
#include <string>

namespace NES {
PrintSink::PrintSink(SinkFormatPtr format, QuerySubPlanId parentPlanId, std::ostream& pOutputStream)
    : SinkMedium(format, parentPlanId), outputStream(pOutputStream) {}

PrintSink::~PrintSink() {}

std::string PrintSink::toString() { return "PRINT_SINK"; }

SinkMediumTypes PrintSink::getSinkMediumType() { return PRINT_SINK; }

bool PrintSink::writeData(TupleBuffer& inputBuffer, NodeEngine::WorkerContextRef) {
    std::unique_lock lock(writeMutex);
    NES_DEBUG("PrintSink: getSchema medium " << toString() << " format " << sinkFormat->toString());

    if (!inputBuffer.isValid()) {
        NES_ERROR("PrintSink::writeData input buffer invalid");
        return false;
    }
    if (!schemaWritten) {
        NES_DEBUG("PrintSink::getData: write schema");
        auto schemaBuffer = sinkFormat->getSchema();
        if (schemaBuffer) {
            NES_DEBUG("PrintSink::getData: write schema of size " << schemaBuffer->getNumberOfTuples());
            std::string ret = "";
            char* bufferAsChar = schemaBuffer->getBufferAs<char>();
            for (uint64_t i = 0; i < schemaBuffer->getNumberOfTuples(); i++) {
                ret = ret + bufferAsChar[i];
            }
            outputStream << ret << std::endl;
        } else {
            NES_DEBUG("PrintSink::getData: no schema buffer to write");
        }
        NES_DEBUG("PrintSink::writeData: schema is =" << sinkFormat->getSchemaPtr()->toString());
        schemaWritten = true;
    } else {
        NES_DEBUG("PrintSink::getData: schema already written");
    }

    NES_DEBUG("PrintSink::getData: write data");
    auto dataBuffers = sinkFormat->getData(inputBuffer);
    for (auto buffer : dataBuffers) {
        NES_DEBUG("PrintSink::getData: write buffer of size " << buffer.getNumberOfTuples());
        std::string ret = "";
        char* bufferAsChar = buffer.getBufferAs<char>();
        for (uint64_t i = 0; i < buffer.getNumberOfTuples(); i++) {
            ret = ret + bufferAsChar[i];
        }
        NES_DEBUG("PrintSink::getData: write buffer str= " << ret);
        outputStream << ret << std::endl;
    }

    return true;
}

const std::string PrintSink::toString() const {
    std::stringstream ss;
    ss << "PRINT_SINK(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << "), ";
    return ss.str();
}

void PrintSink::setup() {
    // currently not required
}
void PrintSink::shutdown() {
    // currently not required
}

}// namespace NES

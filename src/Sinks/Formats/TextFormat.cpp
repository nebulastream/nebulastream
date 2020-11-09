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
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Formats/TextFormat.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cstring>
#include <iostream>
namespace NES {

TextFormat::TextFormat(SchemaPtr schema, BufferManagerPtr bufferManager) : SinkFormat(schema, bufferManager) {
}

std::optional<TupleBuffer> TextFormat::getSchema() {
    //noting to do as this is part of pretty print
    return std::nullopt;
}

std::vector<TupleBuffer> TextFormat::getData(TupleBuffer& inputBuffer) {
    std::vector<TupleBuffer> buffers;

    if (inputBuffer.getNumberOfTuples() == 0) {
        NES_WARNING("TextFormat::getData: write watermark-only buffer");
        buffers.push_back(inputBuffer);
        return buffers;
    }

    std::string bufferContent = UtilityFunctions::prettyPrintTupleBuffer(inputBuffer, schema);
    size_t contentSize = bufferContent.length();
    NES_DEBUG("TextFormat::getData content size=" << contentSize << " content=" << bufferContent);

    if (inputBuffer.getBufferSize() < contentSize) {
        NES_DEBUG("TextFormat::getData: content is larger than one buffer");
        size_t numberOfBuffers = contentSize / inputBuffer.getBufferSize();
        for (size_t i = 0; i < numberOfBuffers; i++) {
            std::string copyString = bufferContent.substr(0, contentSize);
            bufferContent = bufferContent.substr(contentSize, bufferContent.length() - contentSize);
            NES_DEBUG("TextFormat::getData: copy string=" << copyString << " new content=" << bufferContent);
            auto buf = this->bufferManager->getBufferBlocking();
            std::copy(copyString.begin(), copyString.end(), buf.getBuffer());
            buf.setNumberOfTuples(contentSize);
            buffers.push_back(buf);
        }
        NES_DEBUG("TextFormat::getData: successfully copied buffer=" << numberOfBuffers);
    } else {
        NES_DEBUG("TextFormat::getData: content fits in one buffer");
        auto buf = this->bufferManager->getBufferBlocking();
        std::memcpy(buf.getBuffer(), bufferContent.c_str(), contentSize);
        buf.setNumberOfTuples(contentSize);
        buffers.push_back(buf);
    }
    return buffers;
}

std::string TextFormat::toString() {
    return "TEXT_FORMAT";
}

SinkFormatTypes TextFormat::getSinkFormat() {
    return TEXT_FORMAT;
}

}// namespace NES
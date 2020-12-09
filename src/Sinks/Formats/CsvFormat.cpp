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
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cstring>
#include <iostream>
namespace NES {

CsvFormat::CsvFormat(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager) : SinkFormat(schema, bufferManager) {}

std::optional<TupleBuffer> CsvFormat::getSchema() {
    auto buf = this->bufferManager->getBufferBlocking();
    std::stringstream ss;
    uint64_t numberOfFields = schema->fields.size();
    for (uint64_t i = 0; i < numberOfFields; i++) {
        ss << schema->fields[i]->toString();
        if (i < numberOfFields - 1) {
            ss << ",";
        }
    }
    ss << std::endl;
    ss.seekg(0, std::ios::end);
    if (ss.tellg() > buf.getBufferSize()) {
        NES_THROW_RUNTIME_ERROR("Schema buffer is too large");
    }
    std::string schemaString = ss.str();
    std::memcpy(buf.getBuffer(), schemaString.c_str(), ss.tellg());
    buf.setNumberOfTuples(ss.tellg());
    return buf;
}

std::vector<TupleBuffer> CsvFormat::getData(TupleBuffer& inputBuffer) {
    std::vector<TupleBuffer> buffers;

    if (inputBuffer.getNumberOfTuples() == 0) {
        NES_WARNING("CsvFormat::getData: write watermark-only buffer");
        buffers.push_back(inputBuffer);
        return buffers;
    }
    std::string bufferContent = UtilityFunctions::printTupleBufferAsCSV(inputBuffer, schema);
    uint64_t contentSize = bufferContent.length();
    if (inputBuffer.getBufferSize() < contentSize) {
        NES_DEBUG("CsvFormat::getData: content is larger than one buffer");
        uint64_t numberOfBuffers = std::ceil(double(contentSize) / double(inputBuffer.getBufferSize()));
        for (uint64_t i = 0; i < numberOfBuffers - 1; i++) {
            auto buf = this->bufferManager->getBufferBlocking();
            std::string contentWithSingleBufferSize = bufferContent.substr(0, buf.getBufferSize());
            bufferContent = bufferContent.substr(buf.getBufferSize(), bufferContent.length() - buf.getBufferSize());
            NES_TRACE("CsvFormat::getData: add following content to buffer ="
                      << contentWithSingleBufferSize << "\nRemaining content for next buffer =" << bufferContent);
            NES_ASSERT(contentWithSingleBufferSize.size() == buf.getBufferSize(),
                       "CsvFormat: Content size is not equal to buffer size and will waste space in a buffer.");
            std::copy(contentWithSingleBufferSize.begin(), contentWithSingleBufferSize.end(), buf.getBuffer());
            buf.setNumberOfTuples(contentWithSingleBufferSize.size());
            buffers.emplace_back(buf);
        }
        auto buf = this->bufferManager->getBufferBlocking();
        NES_ASSERT(bufferContent.size() <= buf.getBufferSize(), "CsvFormat: Remaining is too big and wont fit.");
        std::copy(bufferContent.begin(), bufferContent.end(), buf.getBuffer());
        buf.setNumberOfTuples(bufferContent.size());
        buffers.emplace_back(buf);
        NES_DEBUG("CsvFormat::getData: successfully copied buffer=" << numberOfBuffers);
    } else {
        NES_DEBUG("CsvFormat::getData: content fits in one buffer");
        auto buf = this->bufferManager->getBufferBlocking();
        std::memcpy(buf.getBufferAs<char>(), bufferContent.c_str(), contentSize);
        buf.setNumberOfTuples(contentSize);
        buffers.push_back(buf);
    }
    return buffers;
}

std::string CsvFormat::toString() { return "CSV_FORMAT"; }

SinkFormatTypes CsvFormat::getSinkFormat() { return CSV_FORMAT; }

}// namespace NES
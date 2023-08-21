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
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Formats/TextFormat.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstring>
#include <iostream>
#include <utility>
namespace NES {

TextFormat::TextFormat(SchemaPtr schema, Runtime::BufferManagerPtr bufferManager)
    : SinkFormat(std::move(schema), std::move(bufferManager)) {}

std::vector<Runtime::TupleBuffer> TextFormat::getData(Runtime::TupleBuffer& inputBuffer) {
    std::vector<Runtime::TupleBuffer> buffers;

    if (inputBuffer.getNumberOfTuples() == 0) {
        NES_WARNING("TextFormat::getData: write watermark-only buffer");
        return buffers;
    }

    auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, inputBuffer.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, inputBuffer);

    std::string bufferContent = buffer.toString(schema);
    uint64_t contentSize = bufferContent.length();
    NES_DEBUG("TextFormat::getData size={} content:\n{}", contentSize, bufferContent);

    if (inputBuffer.getBufferSize() < contentSize) {
        NES_TRACE("TextFormat::getData: content is larger than one buffer");
        uint64_t numberOfBuffers = contentSize / inputBuffer.getBufferSize();
        for (uint64_t i = 0; i < numberOfBuffers; i++) {
            std::string copyString = bufferContent.substr(0, contentSize);
            bufferContent = bufferContent.substr(contentSize, bufferContent.length() - contentSize);
            NES_TRACE("TextFormat::getData: copy string={} new content={}", copyString, bufferContent);
            auto buf = this->bufferManager->getBufferBlocking();
            std::copy(copyString.begin(), copyString.end(), buf.getBuffer());
            buf.setNumberOfTuples(contentSize);
            buffers.push_back(buf);
        }
        NES_TRACE("TextFormat::getData: successfully copied buffer= {}", numberOfBuffers);
    } else {
        NES_TRACE("TextFormat::getData: content fits in one buffer");
        auto buf = this->bufferManager->getBufferBlocking();
        std::memcpy(buf.getBuffer(), bufferContent.c_str(), contentSize);
        buf.setNumberOfTuples(contentSize);
        buffers.push_back(buf);
    }
    return buffers;
}

std::string TextFormat::toString() { return "TEXT_FORMAT"; }

FormatTypes TextFormat::getSinkFormat() { return FormatTypes::TEXT_FORMAT; }

FormatIterator TextFormat::getTupleIterator(Runtime::TupleBuffer&) { NES_NOT_IMPLEMENTED(); }

}// namespace NES
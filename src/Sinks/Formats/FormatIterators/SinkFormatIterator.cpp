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
#include <Sinks/Formats/FormatIterators/SinkFormatIterator.hpp>

namespace NES {

SinkFormatIterator::SinkFormatIterator(SchemaPtr schema, NodeEngine::TupleBufferPtr buffer) : schema(schema), buffer(buffer){};
SinkFormatIterator::SinkFormatIterator() = default;

SinkFormatIterator::iteratorPtr SinkFormatIterator::begin() { return std::make_shared<SinkFormatIterator::iterator>(iterator(buffer, schema, 0)); }

SinkFormatIterator::iteratorPtr SinkFormatIterator::end() {
    return std::make_shared<SinkFormatIterator::iterator>(iterator(buffer, schema, buffer->getNumberOfTuples() -1)); }

SinkFormatIterator::iterator::iterator(NodeEngine::TupleBufferPtr buffer, SchemaPtr schema, uint64_t index) {
    bufferPtr = buffer,
    schemaPtr = schema,
    bufferIndex = index;
}

SinkFormatIterator::iterator::iterator() = default;



bool SinkFormatIterator::iterator::operator!=(const std::shared_ptr<SinkFormatIterator::iterator> other) const {
    // todo currently we only check if we reached the end of the iterator.
    if (bufferIndex == other->bufferIndex) {
        return false;
    };
    return true;
}

// TODO is this correct?
uint8_t SinkFormatIterator::iterator::operator*() {
    return this->bufferPtr->getBuffer()[bufferIndex * schemaPtr->getSchemaSizeInBytes()];
}

void SinkFormatIterator::iterator::operator++() {
    if(bufferIndex == bufferPtr->getNumberOfTuples() -1) {
        NES_DEBUG("End of iterator reached. Can not proceed");
    } else {
        ++bufferIndex;
    }
}

}// namespace NES

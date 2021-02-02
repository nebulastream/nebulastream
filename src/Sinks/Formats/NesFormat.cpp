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

#include "SerializableOperator.pb.h"
#include <API/Schema.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Util/Logger.hpp>
#include <iostream>
namespace NES {

NesFormat::NesFormat(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager) : SinkFormat(schema, bufferManager) {
    serializedSchema = new SerializableSchema();
}

std::optional<NodeEngine::TupleBuffer> NesFormat::getSchema() {
    auto buf = this->bufferManager->getBufferBlocking();
    SerializableSchema* protoBuff = SchemaSerializationUtil::serializeSchema(schema, serializedSchema);
    bool success = protoBuff->SerializeToArray(buf.getBuffer(), protoBuff->ByteSize());
    NES_DEBUG("NesFormat::getSchema: write schema"
              << " success=" << success);
    buf.setNumberOfTuples(protoBuff->ByteSize());
    return buf;
}

std::vector<NodeEngine::TupleBuffer> NesFormat::getData(NodeEngine::TupleBuffer& inputBuffer) {
    std::vector<NodeEngine::TupleBuffer> buffers;

    if (inputBuffer.getNumberOfTuples() == 0) {
        NES_WARNING("NesFormat::getData: write watermark-only buffer");
        buffers.push_back(inputBuffer);
        return buffers;
    }

    //in this case we don't need to
    buffers.push_back(inputBuffer);
    return buffers;
}

std::string NesFormat::toString() { return "NES_FORMAT"; }

SinkFormatTypes NesFormat::getSinkFormat() { return NES_FORMAT; }
}// namespace NES
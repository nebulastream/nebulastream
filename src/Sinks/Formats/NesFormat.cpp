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
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <utility>
namespace NES {

NesFormat::NesFormat(SchemaPtr schema, Runtime::BufferManagerPtr bufferManager)
    : SinkFormat(std::move(schema), std::move(bufferManager)) {
    serializedSchema = std::make_shared<SerializableSchema>();
}

std::optional<Runtime::TupleBuffer> NesFormat::getSchema() {
    auto buf = this->bufferManager->getBufferBlocking();
    SerializableSchemaPtr protoBuff = SchemaSerializationUtil::serializeSchema(schema, serializedSchema.get());
    bool const success = protoBuff->SerializeToArray(buf.getBuffer(), protoBuff->ByteSizeLong());
    NES_ASSERT2_FMT(success, "cannot serialize");
    NES_DEBUG("NesFormat::getSchema: write schema"
              << " success=" << success);
    buf.setNumberOfTuples(protoBuff->ByteSizeLong());
    return buf;
}

std::vector<Runtime::TupleBuffer> NesFormat::getData(Runtime::TupleBuffer& inputBuffer) {
    std::vector<Runtime::TupleBuffer> buffers;

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

FormatTypes NesFormat::getSinkFormat() { return NES_FORMAT; }

FormatIterator NesFormat::getTupleIterator(Runtime::TupleBuffer&) { NES_NOT_IMPLEMENTED(); }
}// namespace NES
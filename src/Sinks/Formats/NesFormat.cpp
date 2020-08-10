#include "SerializableOperator.pb.h"
#include <API/Schema.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Util/Logger.hpp>
#include <iostream>
namespace NES {

NesFormat::NesFormat(SchemaPtr schema, BufferManagerPtr bufferManager) : SinkFormat(schema, bufferManager) {
    serializedSchema = new SerializableSchema();
}

std::optional<TupleBuffer> NesFormat::getSchema() {
    auto buf = this->bufferManager->getBufferBlocking();
    SerializableSchema* protoBuff = SchemaSerializationUtil::serializeSchema(schema, serializedSchema);
    bool success = protoBuff->SerializeToArray(buf.getBuffer(), protoBuff->ByteSize());
    NES_DEBUG("NesFormat::getSchema: write schema"
              << " success=" << success);
    buf.setNumberOfTuples(protoBuff->ByteSize());
    return buf;
}

std::vector<TupleBuffer> NesFormat::getData(TupleBuffer& inputBuffer) {
    std::vector<TupleBuffer> buffers;

    if (inputBuffer.getNumberOfTuples() == 0) {
        NES_WARNING("NesFormat::getData: write watermark-only buffer");
        buffers.push_back(inputBuffer);
        return buffers;
    }

    //in this case we don't need to
    buffers.push_back(inputBuffer);
    return buffers;
}

std::string NesFormat::toString() {
    return "NES_FORMAT";
}

SinkFormatTypes NesFormat::getSinkFormat() {
    return NES_FORMAT;
}
}// namespace NES
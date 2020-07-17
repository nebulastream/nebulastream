#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include "SerializableOperator.pb.h"
namespace NES {

NesFormat::NesFormat(SchemaPtr schema, std::string filePath, bool append) : SinkFormat(schema, filePath, append) {
}

bool NesFormat::writeSchema() {
    std::ofstream outputFile;
    size_t idx = filePath.rfind(".");
    std::string shrinkedPath = filePath.substr(0, idx + 1);
    std::string schemaFile = shrinkedPath + "schema";
    outputFile.open(schemaFile, std::ofstream::binary | std::ofstream::trunc);
    SerializableSchema* serializedSchema = new SerializableSchema();
    SerializableSchema* serial = SchemaSerializationUtil::serializeSchema(schema, serializedSchema);
    outputFile.write(reinterpret_cast<char*>(serial), schema->getSchemaSizeInBytes());
}

bool NesFormat::writeData(TupleBuffer& inputBuffer) {
    std::ofstream outputFile;
    if(append)
    {
        NES_DEBUG("file binary appending in path=" << filePath);
        outputFile.open(filePath, std::ofstream::binary | std::ios::out | std::ofstream::app);
    }
    else{
        NES_DEBUG("file binary overwriting in path=" << filePath);
        outputFile.open(filePath, std::ofstream::binary | std::ofstream::trunc);
    }

    outputFile.write(reinterpret_cast<char*>(inputBuffer.getBuffer()), inputBuffer.getBufferSize());

    outputFile.close();
}

std::string NesFormat::getFormatAsString() {
    return "NesFormat";
}
}// namespace NES
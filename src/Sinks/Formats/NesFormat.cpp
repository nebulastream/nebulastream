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
    SerializableSchema* protoBuff = SchemaSerializationUtil::serializeSchema(schema, serializedSchema);
    bool success = protoBuff->SerializePartialToOstream(&outputFile);
    NES_DEBUG("NesFormat::writeSchema: write to " << schemaFile << " success=" << success);
    outputFile.close();

    return true;
}

bool NesFormat::writeData(TupleBuffer& inputBuffer) {
    if(inputBuffer.getNumberOfTuples() == 0)
    {
        NES_WARNING("NesFormat::writeData: Try to write empty buffer");
        return false;
    }

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

    size_t posBefore = outputFile.tellp();
    outputFile.write(reinterpret_cast<char*>(inputBuffer.getBuffer()), inputBuffer.getBufferSize());
    size_t posAfter = outputFile.tellp();
    outputFile.close();

    if(posAfter > posBefore)
    {
        NES_DEBUG("NesFormat::writeData: wrote buffer of length=" << posAfter - posBefore<< " successfully");
        return true;
    }
    else{
        NES_ERROR("NesFormat::writeData: write buffer failed posBefore=" << posBefore << " posAfter=" << posAfter);
        return false;
    }
}

std::string NesFormat::toString() {
    return "NES_FORMAT";
}
}// namespace NES
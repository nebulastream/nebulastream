#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Formats/TextFormat.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <Util/UtilityFunctions.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>

namespace NES {

TextFormat::TextFormat(SchemaPtr schema, std::string filePath, bool append) : SinkFormat(schema, filePath, append) {
}

bool TextFormat::writeSchema() {
    //noting to do as this is part of pretty print
}

bool TextFormat::writeData(TupleBuffer& inputBuffer)
{
    std::ofstream outputFile;
    if (append) {
        NES_DEBUG("file appending in path=" << filePath);
        outputFile.open(filePath, std::ofstream::out | std::ofstream::app);
    } else {
        NES_DEBUG("file overwriting in path=" << filePath);
        outputFile.open(filePath, std::ofstream::out | std::ios::out | std::ofstream::trunc);
    }
    std::string bufferContent = UtilityFunctions::prettyPrintTupleBuffer(inputBuffer, schema);
    outputFile << bufferContent;
    outputFile.close();
}

std::string TextFormat::getFormatAsString() {
    return "TextFormat";
}

}// namespace NES
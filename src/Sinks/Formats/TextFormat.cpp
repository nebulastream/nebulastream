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
    return true;
}

bool TextFormat::writeData(TupleBuffer& inputBuffer)
{
    if(inputBuffer.getNumberOfTuples() == 0)
    {
        NES_WARNING("TextFormat::writeData: Try to write empty buffer");
        return false;
    }

    std::ofstream outputFile;
    if (append) {
        NES_DEBUG("file appending in path=" << filePath);
        outputFile.open(filePath, std::ofstream::out | std::ofstream::app);
    } else {
        NES_DEBUG("file overwriting in path=" << filePath);
        outputFile.open(filePath, std::ofstream::out | std::ios::out | std::ofstream::trunc);
    }
    size_t posBefore = outputFile.tellp();

    std::string bufferContent = UtilityFunctions::prettyPrintTupleBuffer(inputBuffer, schema);
    outputFile << bufferContent;

    size_t posAfter = outputFile.tellp();
    outputFile.close();

    if(bufferContent.length() > 0 && posAfter > posBefore)
    {
        NES_DEBUG("TextFormat::writeData: wrote buffer of length=" << bufferContent.length() << " successfully");
        return true;
    }
    else{
        NES_ERROR("TextFormat::writeData: write buffer failed posBefore=" << posBefore << " posAfter=" << posAfter << " bufferContent=" << bufferContent);
        return false;
    }
}

std::string TextFormat::toString() {
    return "TEXT_FORMAT";
}

}// namespace NES
#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Formats/JsonFormat.hpp>
#include <Util/Logger.hpp>
#include <iostream>

namespace NES {

JsonFormat::JsonFormat(SchemaPtr schema, std::string filePath, bool append) : SinkFormat(schema, filePath, append){

}

bool JsonFormat::writeSchema()
{
    NES_NOT_IMPLEMENTED();
}

bool JsonFormat::writeData(TupleBuffer& inputBuffer)
{
    NES_NOT_IMPLEMENTED();
}

std::string JsonFormat::getFormatAsString()
{
    return "JsonFormat";
}
}// namespace NES
#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Formats/SinkFormat.hpp>
#include <Util/Logger.hpp>
#include <iostream>

namespace NES {

SinkFormat::SinkFormat(SchemaPtr schema, std::string filePath, bool append): schema(schema), filePath(filePath), append(append)
{

}

}
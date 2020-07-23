#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Formats/JsonFormat.hpp>
#include <Util/Logger.hpp>
#include <iostream>

namespace NES {

JsonFormat::JsonFormat(SchemaPtr schema, BufferManagerPtr bufferManager) : SinkFormat(schema, bufferManager) {
}

std::vector<TupleBuffer> JsonFormat::getData(TupleBuffer& inputBuffer){
    NES_NOT_IMPLEMENTED();
}

std::optional<TupleBuffer> JsonFormat::getSchema(){
    NES_NOT_IMPLEMENTED();
}

std::string JsonFormat::toString() {
    return "JSON_FORMAT";
}
SinkFormats JsonFormat::getSinkFormat() {
    return JSON_FORMAT;
}

}// namespace NES
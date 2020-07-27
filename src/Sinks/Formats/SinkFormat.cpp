#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Formats/SinkFormat.hpp>
#include <Util/Logger.hpp>
#include <iostream>

namespace NES {

SinkFormat::SinkFormat(SchemaPtr schema, BufferManagerPtr bufferManager) : schema(schema), bufferManager(bufferManager) {
}

SchemaPtr SinkFormat::getSchemaPtr() {
    return schema;
}

void SinkFormat::setSchemaPtr(SchemaPtr schema) {
    this->schema = schema;
}

}// namespace NES
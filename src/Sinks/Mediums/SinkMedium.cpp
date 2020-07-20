#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger.hpp>
#include <iostream>

namespace NES {

SinkMedium::SinkMedium(SchemaPtr _schema)
    : schema(_schema),
      sentBuffer(0),
      sentTuples(0),
      schemaWritten(false){
    NES_DEBUG("SinkMedium:Init Data Sink!");
}

SinkMedium::SinkMedium()
    : schema(Schema::create()),
      sentBuffer(0),
      sentTuples(0),
      schemaWritten(false){
    NES_DEBUG("SinkMedium:Init Default Data Sink!");
}

SchemaPtr SinkMedium::getSchema() const {
    return schema;
}

size_t SinkMedium::getNumberOfWrittenOutBuffers() {
    return sentBuffer;
}
size_t SinkMedium::getNumberOfWrittenOutTuples() {
    return sentTuples;
}

void SinkMedium::setSchema(SchemaPtr pSchema) {
    schema = pSchema;
}

SinkMedium::~SinkMedium() {
    NES_DEBUG("Destroy Data Sink  " << this);
}

std::string SinkMedium::getSinkFormat()
{
    return sinkFormat->getFormatAsString();
}

std::string SinkMedium::getWriteMode()
{
    if(append)
    {
        return "APPEND";
    }
    else{
        return "OVERWRITE";
    }
}

bool SinkMedium::getAppendAsBool()
{
    return append;
}

std::string SinkMedium::getAppendAsString()
{
    if(append)
    {
        return "APPEND";
    }
    else{
        return "OVERWRITE";
    }
}
}// namespace NES

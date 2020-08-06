#include <API/Schema.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <utility>

namespace NES {

SinkMedium::SinkMedium(SinkFormatPtr sinkFormat)
    : sinkFormat(std::move(sinkFormat)),
      sentBuffer(0),
      sentTuples(0),
      schemaWritten(false),
      append(false),
      writeMutex(){
    NES_DEBUG("SinkMedium:Init Data Sink!");
}

size_t SinkMedium::getNumberOfWrittenOutBuffers() {
    return sentBuffer;
}
size_t SinkMedium::getNumberOfWrittenOutTuples() {
    return sentTuples;
}

SinkMedium::~SinkMedium() {
    NES_DEBUG("Destroy Data Sink  " << this);
}

SchemaPtr SinkMedium::getSchemaPtr() const {
    return sinkFormat->getSchemaPtr();
}

std::string SinkMedium::getSinkFormat() {
    return sinkFormat->toString();
}

bool SinkMedium::getAppendAsBool() {
    return append;
}

std::string SinkMedium::getAppendAsString() {
    if (append) {
        return "APPEND";
    } else {
        return "OVERWRITE";
    }
}
}// namespace NES

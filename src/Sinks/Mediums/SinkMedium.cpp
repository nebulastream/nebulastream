#include <API/Schema.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <utility>

namespace NES {

SinkMedium::SinkMedium(SinkFormatPtr sinkFormat, QuerySubPlanId parentPlanId)
    : sinkFormat(std::move(sinkFormat)),
      parentPlanId(parentPlanId),
      sentBuffer(0),
      sentTuples(0),
      schemaWritten(false),
      append(false),
      writeMutex() {
    NES_DEBUG("SinkMedium:Init Data Sink!");
}

size_t SinkMedium::getNumberOfWrittenOutBuffers() {
    std::unique_lock lock(writeMutex);
    return sentBuffer;
}
size_t SinkMedium::getNumberOfWrittenOutTuples() {
    std::unique_lock lock(writeMutex);
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

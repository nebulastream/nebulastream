#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger.hpp>
#include <iostream>

namespace NES {

SinkMedium::SinkMedium(SinkFormatPtr sinkFormat)
    : sinkFormat(sinkFormat),
      sentBuffer(0),
      sentTuples(0),
      schemaWritten(false) {
    NES_DEBUG("SinkMedium:Init Data Sink!");
}

//SinkMedium::SinkMedium()
//    :  sentBuffer(0),
//        sentTuples(0),
//        schemaWritten(false) {
//        NES_DEBUG("SinkMedium:Init Default Data Sink!");
//    }

    size_t SinkMedium::getNumberOfWrittenOutBuffers() {
        return sentBuffer;
    }
    size_t SinkMedium::getNumberOfWrittenOutTuples() {
        return sentTuples;
    }

    SinkMedium::~SinkMedium() {
        NES_DEBUG("Destroy Data Sink  " << this);
    }

//    void SinkMedium::setSchemaPtr(SchemaPtr pSchema) {
//
//        sinkFormat->setSchemaPtr(pSchema);
//    }
//
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

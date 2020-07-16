#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/DataSink.hpp>
#include <Util/Logger.hpp>
#include <iostream>

namespace NES {

std::string DataSink::getFormatAsString()
{
    if(format == JSON_FORMAT)
    {
        return "JSON_FORMAT";
    }
    else if(format == CSV_FORMAT)
    {
        return "CSV_FORMAT";
    }
    else if(format == NES_FORMAT)
    {
        return "NES_FORMAT";
    }
    else if(format == TEXT_FORMAT)
    {
        return "NES_FORMAT";
    }
    else
    {
        return "UNKNOWN";
    }
}

std::string DataSink::getMediumAsString()
{
    if(medium == ZMQ_SINK)
    {
        return "ZMQ_SINK";
    }
    else if(medium == FILE_SINK)
    {
        return "FILE_SINK";
    }
    else if(medium == KAFKA_SINK)
    {
        return "KAFKA_SINK";
    }
    else if(medium == PRINT_SINK)
    {
        return "PRINT_SINK";
    }
    else if(medium == NETWORK_SINK)
    {
        return "NETWORK_SINK";
    }
    else
    {
        return "UNKNOWN";
    }
}



DataSink::DataSink(SchemaPtr _schema)
    : schema(_schema),
      sentBuffer(0),
      sentTuples(0) {
    NES_DEBUG("DataSink:Init Data Sink!");
}

DataSink::DataSink()
    : schema(Schema::create()),
      sentBuffer(0),
      sentTuples(0) {
    NES_DEBUG("DataSink:Init Default Data Sink!");
}

SchemaPtr DataSink::getSchema() const {
    return schema;
}

bool DataSink::writeDataInBatch(
    std::vector<TupleBuffer>& input_buffers) {
    for (auto& buf : input_buffers) {
        if (!writeData(buf)) {
            return false;
        }
    }
    return true;
}

size_t DataSink::getNumberOfSentBuffers() {
    return sentBuffer;
}
size_t DataSink::getNumberOfSentTuples() {
    return sentTuples;
}

void DataSink::setSchema(SchemaPtr pSchema) {
    schema = pSchema;
}

DataSink::~DataSink() {
    NES_DEBUG("Destroy Data Sink  " << this);
}

}// namespace NES

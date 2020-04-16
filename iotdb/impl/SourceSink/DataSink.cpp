#include <cassert>
#include <iostream>
#include <memory>
#include <NodeEngine/TupleBuffer.hpp>
#include <API/Schema.hpp>
#include <boost/serialization/export.hpp>
#include <SourceSink/DataSink.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(NES::DataSink)

#include <Util/Logger.hpp>

namespace NES {

DataSink::DataSink(SchemaPtr _schema)
    : schema(_schema),
      sentBuffer(0),
      sentTuples(0) {
  NES_DEBUG("DataSink:Init Data Sink!")
}

DataSink::DataSink()
    : schema(Schema::create()),
      sentBuffer(0),
      sentTuples(0) {
  NES_DEBUG("DataSink:Init Default Data Sink!")
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
  NES_DEBUG("Destroy Data Sink  " << this)
}

}  // namespace NES

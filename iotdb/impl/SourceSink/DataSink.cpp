#include <SourceSink/DataSink.hpp>

#include <cassert>
#include <iostream>
#include <memory>

#include <API/Schema.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(NES::DataSink)


#include <Util/Logger.hpp>

namespace NES {

DataSink::DataSink(const Schema& _schema)
    : schema(_schema),
      sentBuffer(0),
      sentTuples(0) {
  IOTDB_DEBUG("DataSink:Init Data Sink!")
}
DataSink::DataSink()
    : schema(Schema::create()),
      sentBuffer(0),
      sentTuples(0) {
  IOTDB_DEBUG("DataSink:Init Default Data Sink!")
}

const Schema& DataSink::getSchema() const {
  return schema;
}

bool DataSink::writeDataInBatch(
    const std::vector<TupleBufferPtr>& input_buffers) {
  for (const auto& buf : input_buffers) {
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

void DataSink::setSchema(const Schema& pSchema) {
  schema = pSchema;
}

DataSink::~DataSink() {
  IOTDB_DEBUG("Destroy Data Sink  " << this)
}
}  // namespace NES

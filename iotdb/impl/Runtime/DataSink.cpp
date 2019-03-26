#include <cassert>
#include <iostream>
#include <memory>

#include <API/Schema.hpp>
#include <Runtime/DataSink.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::DataSink)

#include <Runtime/ZmqSink.hpp>
#include <Runtime/PrintSink.hpp>
#include <Runtime/FileOutputSink.hpp>
#include <Runtime/YSBPrintSink.hpp>

#include <Util/Logger.hpp>
#include <Util/ErrorHandling.hpp>

namespace iotdb {

DataSink::DataSink(const Schema &_schema) : schema(_schema), processedBuffer(0), processedTuples(0) { std::cout << "Init Data Sink!" << std::endl; }
DataSink::DataSink() : schema(Schema::create()), processedBuffer(0), processedTuples(0) { std::cout << "Init Data Sink!" << std::endl; }

const Schema &DataSink::getSchema() const { return schema; }

bool DataSink::writeData(const std::vector<TupleBuffer*> &input_buffers){
  for(const auto& buf : input_buffers){
      if(!writeData(buf)){
         return false;
      }
  }
  return true;
}

bool DataSink::writeData(const std::vector<TupleBufferPtr> &input_buffers){
  std::vector<TupleBuffer*> buffs;
  for(const auto& buf : input_buffers){
      buffs.push_back(buf.get());
  }
  return writeData(buffs);
}

bool DataSink::writeData(const TupleBufferPtr input_buffer){
  return writeData(input_buffer.get());
}

DataSink::~DataSink() {
	IOTDB_DEBUG("Destroy Data Sink  " << this) }


const DataSinkPtr createTestSink() {
  // instantiate TestSink
  IOTDB_FATAL_ERROR("Called unimplemented Function");
}

const DataSinkPtr createYSBPrintSink()
{
	return std::make_shared<YSBPrintSink>();
}

const DataSinkPtr createRemoteTCPSink(const Schema &schema, const std::string &server_ip, int port) {
  // instantiate RemoteSocketSink
  IOTDB_FATAL_ERROR("Called unimplemented Function");
}

const DataSinkPtr createBinaryFileSink(const std::string &path_to_file) {
    return std::make_shared<FileOutputSink>(/* path_to_file */);
}

const DataSinkPtr createBinaryFileSink(const Schema &schema, const std::string &path_to_file) {
    return std::make_shared<FileOutputSink>(schema /*, path_to_file */);
}

const DataSinkPtr createPrintSink(std::ostream& out) {
    return std::make_shared<PrintSink>(/* out */);
}

const DataSinkPtr createPrintSink(const Schema &schema, std::ostream& out) {
    return std::make_shared<PrintSink>(schema /*, out */);
}

const DataSinkPtr createZmqSink(const Schema &schema, const std::string &host, const uint16_t port) {
  return std::make_shared<ZmqSink>(schema, host, port);
}

} // namespace iotdb

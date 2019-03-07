#include <cassert>
#include <iostream>
#include <memory>

#include <Runtime/DataSink.hpp>
#include <Runtime/ZmqSink.hpp>
#include <Runtime/PrintSink.hpp>
#include <Util/ErrorHandling.hpp>

namespace iotdb {

DataSink::DataSink(const Schema &_schema) : schema(_schema) { std::cout << "Init Data Sink!" << std::endl; }

const Schema &DataSink::getSchema() const { return schema; }

DataSink::~DataSink() { std::cout << "Destroy Data Sink " << this << std::endl; }

const DataSinkPtr createTestSink() {
  // instantiate TestSink
  IOTDB_FATAL_ERROR("Called unimplemented Function");
}

const DataSinkPtr createYSBPrintSink(const Schema& schema)
{
	return std::make_shared<YSBPrintSink>(schema);
}


const DataSinkPtr createBinaryFileSink(const Schema &schema, const std::string &path_to_file) {
  // instantiate BinaryFileSink
  IOTDB_FATAL_ERROR("Called unimplemented Function");
}

const DataSinkPtr createRemoteTCPSink(const Schema &schema, const std::string &server_ip, int port) {
  // instantiate RemoteSocketSink
  IOTDB_FATAL_ERROR("Called unimplemented Function");
}

const DataSinkPtr createZmqSink(const Schema &schema, const std::string &host, const uint16_t port,
                                const std::string &topic) {
  return std::make_shared<ZmqSink>(schema, host, port, topic);
}

} // namespace iotdb

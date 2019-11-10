#include <SourceSink/FileOutputSink.hpp>
#include <SourceSink/PrintSink.hpp>
#include <SourceSink/ZmqSink.hpp>
#include <SourceSink/DataSink.hpp>
#include "../../include/YSB_legacy/YSBPrintSink.hpp"

namespace iotdb {

const DataSinkPtr createTestSink() {
  IOTDB_FATAL_ERROR("Called unimplemented Function");
  IOTDB_NOT_IMPLEMENTED
}

const DataSinkPtr createYSBPrintSink() {
  return std::make_shared<YSBPrintSink>();
}

const DataSinkPtr createBinaryFileSink(const std::string& path_to_file) {
  return std::make_shared<FileOutputSink>(/* path_to_file */);
}

const DataSinkPtr createBinaryFileSink(const Schema& schema,
                                       const std::string& path_to_file) {
  return std::make_shared<FileOutputSink>(schema /*, path_to_file */);
}

const DataSinkPtr createPrintSinkWithoutSchema(std::ostream& out) {
  return std::make_shared<PrintSink>(out);
}

const DataSinkPtr createPrintSinkWithSink(const Schema& schema, std::ostream& out) {
  return std::make_shared<PrintSink>(schema, out);
}

const DataSinkPtr createZmqSink(const Schema& schema, const std::string& host,
                                const uint16_t port) {
  return std::make_shared<ZmqSink>(schema, host, port);
}

}

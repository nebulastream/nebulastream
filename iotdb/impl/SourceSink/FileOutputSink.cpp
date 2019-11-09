#include <cstring>
#include <memory>
#include <sstream>
#include <string>

#include <Util/Logger.hpp>
#include <SourceSink/FileOutputSink.hpp>
#include <SourceSink/DataSink.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::FileOutputSink);

namespace iotdb {

FileOutputSink::FileOutputSink()
    : DataSink() {
}
FileOutputSink::FileOutputSink(const Schema& schema)
    : DataSink(schema) {
}

bool FileOutputSink::writeData(const TupleBufferPtr input_buffer) {
  IOTDB_FATAL_ERROR("Called Uninplemented Function!");
}

const std::string FileOutputSink::toString() const {
  std::stringstream ss;
  ss << "PRINT_SINK(";
  ss << "SCHEMA(" << schema.toString() << "), ";
  return ss.str();
}

void FileOutputSink::setup()
{

}

void FileOutputSink::shutdown()
{

}

}  // namespace iotdb

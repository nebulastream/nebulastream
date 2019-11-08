#include <cstring>
#include <memory>
#include <sstream>
#include <string>

#include <Runtime/DataSink.hpp>
#include <Runtime/FileOutputSink.hpp>
#include <Util/Logger.hpp>

namespace iotdb {

FileOutputSink::FileOutputSink() : DataSink() {}
FileOutputSink::FileOutputSink(const Schema& schema) : DataSink(schema) {}
FileOutputSink::~FileOutputSink() {}

bool FileOutputSink::writeData(const TupleBufferPtr input_buffer) { IOTDB_FATAL_ERROR("Called Uninplemented Function!"); }

const std::string FileOutputSink::toString() const
{
    std::stringstream ss;
    ss << "PRINT_SINK(";
    ss << "SCHEMA(" << schema.toString() << "), ";
    return ss.str();
}

} // namespace iotdb

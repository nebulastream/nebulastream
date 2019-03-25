#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <zmq.hpp>

#include <Runtime/PrintSink.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::PrintSink)
#include <Util/Logger.hpp>
#include <Runtime/Dispatcher.hpp>
namespace iotdb {

PrintSink::PrintSink(const Schema &schema)
    : DataSink(schema){}

PrintSink::~PrintSink() { }


bool PrintSink::writeData(const TupleBuffer* input_buffer) {

  std::cout << iotdb::toString(input_buffer,this->getSchema()) << std::endl;

  return true;
}


const std::string PrintSink::toString() const {
  std::stringstream ss;
  ss << "PRINT_SINK(";
  ss << "SCHEMA(" << schema.toString() << "), ";
  return ss.str();
}


} // namespace iotdb

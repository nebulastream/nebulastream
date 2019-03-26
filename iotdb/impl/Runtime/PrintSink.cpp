#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <zmq.hpp>

#include <Runtime/Dispatcher.hpp>
#include <Runtime/PrintSink.hpp>
#include <Util/Logger.hpp>

BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::PrintSink)

namespace iotdb {

PrintSink::PrintSink() : DataSink() {}

PrintSink::PrintSink(const Schema& schema) : DataSink(schema) {}

PrintSink::~PrintSink() {}

bool PrintSink::writeData(const TupleBuffer* input_buffer)
{
    std::cout << iotdb::toString(input_buffer, this->getSchema()) << std::endl;
    return true;
}

const std::string PrintSink::toString() const
{
    std::stringstream ss;
    ss << "PRINT_SINK(";
    ss << "SCHEMA(" << schema.toString() << "), ";
    return ss.str();
}

} // namespace iotdb
